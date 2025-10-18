# immoscout_scraper.py (angepasst)
from playwright.sync_api import sync_playwright
import pandas as pd
import re
import time

def scrape_immoscout(region_url, max_pages=2):
    offers = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=200)  # sichtbar zum Testen
        page = browser.new_page()
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })

        for page_num in range(1, max_pages + 1):
            # ✅ Erste Seite ohne Parameter, ab Seite 2 mit pagenumber
            if page_num == 1:
                url = region_url
            else:
                url = f"{region_url}?pagenumber={page_num}"

            print(f"Scraping: {url}")
            page.goto(url, timeout=60000)
            page.wait_for_load_state("networkidle")
            time.sleep(2)

            cards = page.locator('article, [data-testid="result-list-entry"]')
            count = cards.count()
            print(f"→ {count} Einträge gefunden auf Seite {page_num}")

            for i in range(count):
                try:
                    entry = cards.nth(i)
                    html = entry.inner_html()

                    title = entry.locator("h2, h3, h5").first.inner_text()
                    link = entry.locator("a").first.get_attribute("href")

                    # Preis
                    m_price = re.search(r"([\d\.\,]+)\s*€", html)
                    price = m_price.group(1) if m_price else None

                    # Fläche
                    m_area = re.search(r"([\d\.\,]+)\s*m²", html)
                    area = m_area.group(1) if m_area else None

                    # Zimmer
                    m_rooms = re.search(r"([\d\.\,]+)\s*Zi", html)
                    rooms = m_rooms.group(1) if m_rooms else None

                    offers.append({
                        "title": title.strip() if title else None,
                        "price": price.strip() if price else None,
                        "area": area.strip() if area else None,
                        "rooms": rooms.strip() if rooms else None,
                        "link": f"https://www.immobilienscout24.de{link}" if link else None,
                    })
                except Exception as e:
                    print("⚠️ Fehler beim Extrahieren:", e)

            time.sleep(1)

        browser.close()

    df = pd.DataFrame(offers)
    if df.empty:
        raise ValueError("Keine Objekte gefunden – prüfe URL oder Selektoren.")

    # Bereinigung
    for col in ["price", "area", "rooms"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.extract(r"(\d+\.?\d*)")[0]
            .astype(float)
        )

    return df


if __name__ == "__main__":
    url = "https://www.immobilienscout24.de/Suche/de/nordrhein-westfalen/koeln/wohnung-kaufen"
    df = scrape_immoscout(url, max_pages=3)
    print(df.head())
    df.to_csv("data/immoscout_koeln.csv", index=False)
