"""
veille_prix.py — Veille concurrentielle maplatine.com
Fusion des deux approches :
  - Tavily pour la découverte automatique des URLs (votre approche)
  - Sélecteurs CSS depuis l'onglet Configuration du Sheet (approche collègue)
  - Timeout dur par requête — aucun blocage possible
  - Zéro Claude API
"""

import os, re, json, time, logging, gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup

# ── LOGGING ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────────
TAVILY_API_KEY  = os.environ.get("TAVILY_API_KEY", "")
GOOGLE_JSON     = os.environ.get("GOOGLE_JSON")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")

MAX_URLS        = 6     # résultats Tavily max par produit
REQUEST_TIMEOUT = 8     # timeout dur par requête HTTP (secondes)
DELAY_BETWEEN   = 0.3   # délai entre requêtes (secondes)
WRITE_EVERY     = 20    # écriture dans Sheets tous les N produits

URL_BLACKLIST = [
    r"/marque/", r"/brand/", r"/categorie/", r"/category/", r"/collection/",
    r"/recherche", r"/search", r"srsltid=", r"\?q=", r"\?s=",
    r"/blog/", r"/forum/", r"/avis/", r"/guide/", r"/news/",
]

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept":          "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
}

# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if GOOGLE_JSON:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_JSON), scopes=scopes
        )
    else:
        creds = Credentials.from_service_account_file(
            "google_credentials.json", scopes=scopes
        )
    return gspread.authorize(creds)


def read_catalogue(client) -> list:
    sh   = client.open_by_key(GOOGLE_SHEET_ID)
    ws   = sh.get_worksheet(0)           # onglet 0 = Catalogue
    rows = ws.get_all_values()
    out  = []
    for row in rows[2:]:                 # ligne 0=titre, ligne 1=en-têtes
        ref     = str(row[0]).strip()
        libelle = str(row[1]).strip()
        if not ref or not libelle:
            continue
        prix_mpl = _parse_price(str(row[2]).strip() if len(row) > 2 else "")
        out.append({"ref": ref, "libelle": libelle, "prix_mpl": prix_mpl})
    log.info(f"{len(out)} produits chargés")
    return out


def read_config(client) -> dict:
    """
    Lit l'onglet Configuration (onglet 7, index 7) :
    Col A = domaine, Col B = sélecteur, Col C = type (Meta/CSS/JSON-LD)
    Retourne un dict { "domain.com": {"selector": "...", "type": "Meta"} }
    """
    try:
        sh   = client.open_by_key(GOOGLE_SHEET_ID)
        ws   = sh.get_worksheet(7)       # onglet 7 = Configuration
        rows = ws.get_all_values()
        cfg  = {}
        for row in rows[2:]:            # ligne 0=titre, ligne 1=en-têtes
            if len(row) < 3:
                continue
            domain   = str(row[0]).strip().lower().replace("www.", "")
            selector = str(row[1]).strip()
            typ      = str(row[2]).strip()
            if domain and selector:
                cfg[domain] = {"selector": selector, "type": typ}
        log.info(f"{len(cfg)} règles de configuration chargées")
        return cfg
    except Exception as e:
        log.warning(f"Impossible de lire Configuration : {e}")
        return {}


def write_rows(client, rows: list):
    sh = client.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.get_worksheet(1)             # onglet 1 = Historique Prix
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    log.info(f"  → {len(rows)} ligne(s) écrite(s)")


# ── TAVILY ────────────────────────────────────────────────────
def tavily_search(libelle: str) -> list:
    try:
        resp = requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key":             TAVILY_API_KEY,
                "query":               f"{libelle} prix acheter france",
                "search_depth":        "basic",
                "max_results":         12,
                "include_raw_content": False,
                "include_answer":      False,
                "exclude_domains": [
                    "maplatine.com", "facebook.com", "instagram.com",
                    "youtube.com", "wikipedia.org", "reddit.com",
                    "pinterest.com", "leboncoin.fr", "vinted.fr",
                    "tiktok.com", "twitter.com",
                ],
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        results = resp.json().get("results", [])
        items   = []
        seen    = set()
        for r in results:
            url = r.get("url", "")
            if not url or not _is_product_url(url):
                continue
            domain = _domain(url)
            if domain in seen:
                continue
            seen.add(domain)
            items.append({
                "url":     url,
                "title":   r.get("title", ""),
                "snippet": r.get("content", ""),
            })
            if len(items) >= MAX_URLS:
                break
        return items
    except Exception as e:
        log.warning(f"  Tavily erreur: {e}")
        return []


# ── EXTRACTION PRIX ───────────────────────────────────────────
def extract_price(item: dict, config: dict):
    """
    Cascade d'extraction :
    1. Snippet Tavily (instantané, zéro requête)
    2. Scraping page avec sélecteur depuis Configuration si disponible
    3. Fallback automatique : JSON-LD → Meta → itemprop
    """
    # 1. Snippet / titre
    p = _price_from_text(item["snippet"]) or _price_from_text(item["title"])
    if p:
        return p

    # 2. Scraping avec timeout dur
    return _scrape(item["url"], config)


def _scrape(url: str, config: dict):
    domain = _domain(url)
    rule   = config.get(domain, {})

    try:
        resp = requests.get(
            url, headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        typ  = rule.get("type", "").upper()
        sel  = rule.get("selector", "")

        # ── Utiliser le sélecteur de Configuration en priorité ──
        if sel and typ == "CSS":
            tag = soup.select_one(sel)
            if tag:
                p = _clean(tag.get_text())
                if p:
                    return p

        if sel and typ == "META":
            tag = (soup.find("meta", property=sel)
                   or soup.find("meta", attrs={"name": sel})
                   or soup.find("meta", attrs={"itemprop": sel}))
            if tag and tag.get("content"):
                p = _clean(tag["content"])
                if p:
                    return p

        # ── Fallback automatique ──────────────────────────────

        # JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data  = json.loads(script.string or "")
                nodes = data if isinstance(data, list) else [data]
                for node in nodes:
                    p = _price_from_node(node)
                    if p:
                        return p
                    for sub in node.get("@graph", []):
                        p = _price_from_node(sub)
                        if p:
                            return p
            except Exception:
                continue

        # Meta générique
        for prop in ["product:price:amount", "og:price:amount"]:
            tag = soup.find("meta", property=prop)
            if tag and tag.get("content"):
                p = _clean(tag["content"])
                if p:
                    return p

        # itemprop price
        tag = soup.find(attrs={"itemprop": "price"})
        if tag:
            p = _clean(tag.get("content") or tag.get_text())
            if p:
                return p

        return None

    except requests.exceptions.Timeout:
        log.debug(f"  Timeout {REQUEST_TIMEOUT}s — {url}")
        return None
    except Exception:
        return None


def _price_from_node(node: dict):
    if "offers" in node:
        off = node["offers"]
        if isinstance(off, list):
            off = off[0]
        if isinstance(off, dict) and "price" in off:
            return _clean(str(off["price"]))
    if node.get("@type") == "Offer" and "price" in node:
        return _clean(str(node["price"]))
    return None


def _price_from_text(text: str):
    if not text:
        return None
    for pat in [
        r"(\d{1,5})[,.](\d{2})\s*€",
        r"(\d{1,5})\s*€",
        r"€\s*(\d{1,5}[,.]\d{2})",
        r"(\d{1,5}[,.]\d{2})\s*EUR",
    ]:
        m = re.search(pat, text)
        if m:
            p = _clean(m.group(0).replace("€", "").replace("EUR", "").strip())
            if p:
                return p
    return None


# ── UTILITAIRES ───────────────────────────────────────────────
def _clean(s: str):
    if not s:
        return None
    s = str(s).replace("€", "").replace("EUR", "").replace("\xa0", "").strip()
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        val = float(s)
        return val if 10 < val < 150_000 else None
    except ValueError:
        return None


def _parse_price(s: str):
    return _clean(s.replace(" ", ""))


def _domain(url: str) -> str:
    try:
        return url.split("/")[2].replace("www.", "").lower()
    except Exception:
        return url


def _is_product_url(url: str) -> bool:
    u = url.lower()
    for p in URL_BLACKLIST:
        if re.search(p, u):
            return False
    return True


def _platform(url: str) -> str:
    u = url.lower()
    for key, name in {
        "amazon": "amazon", "fnac": "fnac", "darty": "darty",
        "boulanger": "boulanger", "cdiscount": "cdiscount",
        "son-video": "son-video", "sono-elec": "sono-elec",
        "hifilink": "hifilink", "ebay": "ebay", "ldlc": "ldlc",
        "rakuten": "rakuten", "cultura": "cultura",
        "thomann": "thomann", "sonovente": "sonovente",
    }.items():
        if key in u:
            return name
    return "autre"


# ── MAIN ──────────────────────────────────────────────────────
def main():
    today = datetime.now().strftime("%d/%m/%Y")
    log.info(f"=== Veille prix démarrée — {today} ===")

    client   = get_client()
    products = read_catalogue(client)
    config   = read_config(client)       # sélecteurs CSS depuis le Sheet
    buffer   = []

    for i, p in enumerate(products, 1):
        log.info(f"[{i}/{len(products)}] {p['ref']} — {p['libelle'][:55]}")

        items = tavily_search(p["libelle"])
        log.info(f"  {len(items)} URL(s) trouvée(s)")

        for item in items:
            prix = extract_price(item, config)
            if not prix:
                continue

            ecart_eur = round(prix - p["prix_mpl"], 2) if p["prix_mpl"] else ""
            ecart_pct = round((ecart_eur / p["prix_mpl"]) * 100, 2) if p["prix_mpl"] else ""
            domain    = _domain(item["url"])
            sign      = "+" if isinstance(ecart_pct, float) and ecart_pct > 0 else ""
            log.info(f"  ✓ {domain} — {prix} € ({sign}{ecart_pct if isinstance(ecart_pct, float) else '?'}%)")

            buffer.append([
                today,
                p["ref"],
                p["libelle"],
                p["prix_mpl"] if p["prix_mpl"] else "",
                domain,
                _platform(item["url"]),
                item["title"],
                prix,
                ecart_eur,
                ecart_pct,
                item["url"],
            ])
            time.sleep(DELAY_BETWEEN)

        if len(buffer) >= WRITE_EVERY:
            write_rows(client, buffer)
            buffer = []

    if buffer:
        write_rows(client, buffer)

    log.info("=== Veille terminée ===")


if __name__ == "__main__":
    main()
