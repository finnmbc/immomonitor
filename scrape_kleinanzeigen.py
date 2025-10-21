import requests
from bs4 import BeautifulSoup
import re
import time
import random
import pandas as pd
from urllib.parse import urljoin, urlsplit, urlunsplit
from datetime import datetime, timedelta

# =====================================================
# CONFIG
# =====================================================
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"}

ORTE = {
    #"Bremen": "bremen/c196l1",
    "Ritterhude": "ritterhude/c196l9789"
}

# =====================================================
# HILFSFUNKTIONEN
# =====================================================

def get_html(url):
    """HTML einer Seite abrufen"""
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def parse_relative_date(date_text):
    """Wandelt 'Heute'/'Gestern'/'13.10.2025' in echtes Datum um"""
    date_text = date_text.strip()
    today = datetime.today().date()

    if re.search(r"heute", date_text, re.I):
        return today
    elif re.search(r"gestern", date_text, re.I):
        return today - timedelta(days=1)
    elif (m := re.search(r"(\d{1,2}\.\d{1,2}\.\d{4})", date_text)):
        try:
            return datetime.strptime(m.group(1), "%d.%m.%Y").date()
        except ValueError:
            return None
    else:
        return None


def parse_results_page(html, base):
    """Extrahiert Anzeige-Links UND Datum von einer Ergebnisseite"""
    soup = BeautifulSoup(html, "html.parser")
    ads = []

    for ad in soup.select("article.aditem"):
        # Link
        link_tag = ad.select_one("a[href*='/s-anzeige/']")
        if not link_tag:
            continue
        href = link_tag["href"]
        full_link = urljoin(base, href.split("?")[0])

        # Datum (steht oft in einem <div> oder <span>)
        date_tag = ad.find(string=re.compile(r"(\d{1,2}\.\d{1,2}\.\d{4})|Heute|Gestern"))
        date_posted = parse_relative_date(date_tag) if date_tag else None

        ads.append({"url": full_link, "date_posted": date_posted})

    return ads


def get_total_pages(html):
    """Ermittelt die Gesamtseitenzahl aus der Navigation"""
    soup = BeautifulSoup(html, "html.parser")
    pages = {int(m.group(1)) for a in soup.find_all("a", href=True)
             if (m := re.search(r"/seite:(\d+)", a["href"]))}
    return max(pages) if pages else 1


def build_page_url(base, n):
    """Erzeugt die URL für Seite n"""
    p = urlsplit(base)
    path = p.path
    idx = path.find("/c196l")
    if idx == -1:
        return base
    new = path[:idx] + f"/seite:{n}" + path[idx:]
    return urlunsplit((p.scheme, p.netloc, new, p.query, p.fragment))


def parse_price(val):
    """Wandelt Text in Zahl um"""
    if not val:
        return None
    val = val.replace("€", "").replace("EUR", "").replace(" ", "").replace(".", "").strip()
    try:
        return float(val)
    except ValueError:
        return None


def parse_ad_page(html, url):
    """Extrahiert Details aus einer Anzeige"""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Preis
    price = None
    if (m := re.search(r"([\d\.\s,]+)\s*(€|EUR)", text)):
        price = parse_price(m.group(1))

    # Fläche
    area = None
    if (m := re.search(r"(\d{1,4}[.,]?\d{0,2})\s?m(?:²|2)", text)):
        try:
            area = float(m.group(1).replace(",", "."))
        except ValueError:
            pass

    # PLZ
    plz = None
    if (m := re.search(r"\b(\d{5})\b", text)):
        plz = m.group(1)

    price_per_m2 = None
    if price and area and area > 0:
        price_per_m2 = round(price / area, 2)

    return {"url": url, "price": price, "area": area, "price_per_m2": price_per_m2, "plz": plz}


def scrape_city(name, slug, delay=(0.5, 1.0)):
    """Scraped alle Anzeigen einer Stadt"""
    base_url = f"https://www.kleinanzeigen.de/s-wohnung-kaufen/{slug}"
    base = "https://www.kleinanzeigen.de"

    print(f"\n🔍 {name} wird gescraped …")
    html1 = get_html(base_url)
    pages = get_total_pages(html1)
    print(f"  → {pages} Seiten gefunden.")

    all_ads = []
    for p in range(1, pages + 1):
        html = html1 if p == 1 else get_html(build_page_url(base_url, p))
        all_ads += parse_results_page(html, base)
        time.sleep(random.uniform(*delay))

    print(f"  → {len(all_ads)} Anzeigen gefunden.")

    data = []
    for i, ad in enumerate(all_ads, 1):
        try:
            html = get_html(ad["url"])
            info = parse_ad_page(html, ad["url"])
            info["date_posted"] = ad["date_posted"]
            info["ort"] = name
            data.append(info)
        except Exception as e:
            print(f"  ⚠️ Fehler bei {ad['url']}: {e}")
        time.sleep(random.uniform(*delay))
        print(f"    [{i}/{len(all_ads)}]")

    return pd.DataFrame(data)


# =====================================================
# MAIN
# =====================================================
def main():
    all_dfs = []
    for name, slug in ORTE.items():
        df = scrape_city(name, slug)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)

        # Nur relevante Spalten exportieren
        cols = ["ort", "plz", "price_per_m2", "date_posted"]
        result[cols].to_csv("data/angebote_kleinanzeigen.csv", index=False, encoding="utf-8-sig")

        print("\n💾 CSV-Datei gespeichert: data/angebote_kleinanzeigen.csv")
        print(result[cols].head(10))
    else:
        print("❌ Keine Daten gesammelt.")


if __name__ == "__main__":
    main()
