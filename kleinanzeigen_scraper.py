# kleinanzeigen_scraper.py
import requests
from bs4 import BeautifulSoup
import re
import time
import random
import pandas as pd
from urllib.parse import urljoin, urlparse

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# ---------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------

def get_search_page(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_results_page(html, base_url):
    """
    Extrahiert NUR echte Anzeigen-Links von der Suchseite
    (beginnen mit /s-anzeige/).
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.select("a[href*='/s-anzeige/']"):
        href = a["href"]
        if href.startswith("/s-anzeige/"):
            full = urljoin(base_url, href.split("?")[0])
            if full not in links:
                links.append(full)

    return links


def parse_ad_page(html, url):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Titel
    title = None
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)
    elif soup.title:
        title = soup.title.get_text(strip=True)

    # Preis
    price = None
    sel = soup.select_one(".price-block__price, .aditem-main--middle--price, .is--price, [data-testid='ad-price']")
    if sel:
        price = sel.get_text(" ", strip=True)
    else:
        m = re.search(r"([\d\.\,]+)\s*€", text)
        price = m.group(0) if m else None

    # Fläche (m²)
    area = None
    m = re.search(r"(\d{1,4}[,\.\s]?\d{0,2})\s?m(?:²|2)", text)
    if m:
        area = m.group(1).replace(" ", "").replace(".", "").replace(",", ".")

    # Zimmer
    rooms = None
    m = re.search(r"(\d[\d\,\.]?)\s*(Zi|Zimmer)\b", text, flags=re.IGNORECASE)
    if m:
        rooms = m.group(1).replace(",", ".")

    # PLZ
    zip_code = None
    m = re.search(r"\b(\d{5})\b", text)
    if m:
        zip_code = m.group(1)

    # Preis numerisch
    price_eur = None
    if price:
        m = re.search(r"([\d\.\,]+)", price.replace(".", "").replace("\xa0", ""))
        if m:
            try:
                price_eur = float(m.group(1).replace(".", "").replace(",", "."))
            except ValueError:
                price_eur = None

    # Zahlen konvertieren
    area_val = None
    try:
        if area:
            area_val = float(area)
    except ValueError:
        area_val = None

    rooms_val = None
    try:
        if rooms:
            rooms_val = float(rooms)
    except ValueError:
        rooms_val = None

    return {
        "url": url,
        "title": title,
        "price_raw": price,
        "price_eur": price_eur,
        "area_m2": area_val,
        "rooms": rooms_val,
        "zip": zip_code,
    }


def scrape_kleinanzeigen(search_url, pages=3, delay_min=1.0, delay_max=3.0):
    """
    Scraped mehrere Ergebnisseiten (pages) und ruft alle Anzeigen-Detailseiten ab.
    """
    base = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(search_url))
    results = []
    seen = set()

    for page in range(1, pages + 1):
        # Seitenpaginierung: ?page=2 etc.
        url = search_url if page == 1 else f"{search_url}?page={page}"
        print(f"\n→ Lade Suchseite: {url}")

        html = get_search_page(url)
        links = parse_results_page(html, base)
        print(f"  Gefundene Anzeigen auf Seite {page}: {len(links)}")

        for ad_url in links:
            if ad_url in seen:
                continue
            seen.add(ad_url)
            try:
                print(f"    Lade Inserat: {ad_url}")
                time.sleep(random.uniform(delay_min, delay_max))
                ad_html = get_search_page(ad_url)
                ad = parse_ad_page(ad_html, ad_url)
                results.append(ad)
            except Exception as e:
                print(f"    ⚠️ Fehler beim Laden/Parsen: {e}")

        time.sleep(random.uniform(delay_min, delay_max))

    df = pd.DataFrame(results)
    return df


# ---------------------------------------------------------
# Hauptteil
# ---------------------------------------------------------

if __name__ == "__main__":
    search = "https://www.kleinanzeigen.de/s-wohnung-kaufen/ritterhude/c196l9789"  # deine Region
    df = scrape_kleinanzeigen(search, pages=3, delay_min=1.0, delay_max=2.5)

    # Preis pro m² berechnen, falls möglich
    df["price_per_m2"] = df.apply(
        lambda row: row["price_eur"] / row["area_m2"]
        if row["price_eur"] and row["area_m2"] and row["area_m2"] > 0
        else None,
        axis=1,
    )

    # Ergebnisse speichern
    out_path = "data/kleinanzeigen.csv"
    df.to_csv(out_path, index=False)
    print(f"\n✅ Gespeichert: {out_path}")
    print(df.head())
