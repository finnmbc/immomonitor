import streamlit as st
import pandas as pd
import time, random, re, requests, os, hashlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

# =====================================================
# Setup
# =====================================================
st.set_page_config(page_title="🏠 Immobilien-Monitor", layout="wide")

@st.cache_data
def load_locations():
    return pd.read_csv("data/orte.csv")

orte_df = load_locations()
orte_liste = orte_df["name"].tolist()

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"}

# =====================================================
# Cache-Setup (lokal)
# =====================================================
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def cache_filename(ort, radius):
    key = f"{ort}_{radius}"
    key_hash = hashlib.md5(key.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{key_hash}.parquet")

def save_cached_data(df, ort, radius):
    path = cache_filename(ort, radius)
    df.attrs["scraped_at"] = time.strftime("%d.%m.%Y %H:%M:%S")
    df.to_parquet(path)

def load_cached_data(ort, radius, max_age_hours=24):
    path = cache_filename(ort, radius)
    if os.path.exists(path):
        age_hours = (time.time() - os.path.getmtime(path)) / 3600
        if age_hours < max_age_hours:
            df = pd.read_parquet(path)
            df.attrs["scraped_at"] = df.attrs.get("scraped_at", time.strftime("%d.%m.%Y %H:%M:%S"))
            return df
    return None

# =====================================================
# Scraper-Funktionen
# =====================================================
def get_html(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def parse_price(val: str):
    if not val:
        return None
    val = val.replace("€", "").replace("EUR", "").replace(" ", "").strip()
    val = val.replace(".", "").replace(",", ".")
    try:
        return float(val)
    except ValueError:
        return None

def parse_results_page(html, base):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("a[href*='/s-anzeige/']"):
        href = a["href"]
        if href.startswith("/s-anzeige/"):
            links.append(urljoin(base, href.split("?")[0]))
    return list(dict.fromkeys(links))

def get_total_pages(html):
    soup = BeautifulSoup(html, "html.parser")
    pages = {int(m.group(1)) for a in soup.find_all("a", href=True)
             if (m := re.search(r"/seite:(\d+)", a["href"]))}
    return max(pages) if pages else 1

def parse_ad_page(html, url):
    soup = BeautifulSoup(html, "html.parser")

    main_section = (
        soup.select_one("#viewad-main")
        or soup.select_one("main")
        or soup.select_one("article")
        or soup
    )
    text = main_section.get_text(" ", strip=True)
    title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Unbekannt"

    price = None
    vb_flag = False
    m_price = re.search(r"(Kaufpreis|Preis|Gesamtpreis)?\s*([\d\.\s,]+)\s*(€|EUR)", text, re.I)
    if m_price:
        price = parse_price(m_price.group(2))
        if re.search(r"\bVB\b", text[m_price.end():m_price.end() + 20], re.I):
            vb_flag = True
    else:
        m_vb = re.search(r"\bVB\b", text)
        if m_vb:
            part = text[m_vb.end():m_vb.end() + 20]
            if re.search(r"\d{5}", part):
                vb_flag = True
                price = 0.0

    area = None
    if (m := re.search(r"(\d{1,5}[,\.\s]?\d{0,2})\s?m(?:²|2)\b", text)):
        try:
            area = float(m.group(1).replace(",", ".").replace(" ", ""))
        except ValueError:
            area = None

    details = {}
    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            details[dt.get_text(strip=True)] = dd.get_text(strip=True)

    rooms = None
    if "Zimmer" in details:
        val = details["Zimmer"]
        if (m := re.search(r"(\d{1,2}(?:[.,]\d)?)", val)):
            rooms = float(m.group(1).replace(",", "."))
    if rooms is None:
        if (m := re.search(r"\b(\d{1,2}(?:[.,]\d)?)\s*(?:Zimmer|Zi)\b", text, re.I)):
            rooms = float(m.group(1).replace(",", "."))
        elif (m := re.search(r"(?:Zimmer|Zi)\s*[:\-]?\s*(\d{1,2}(?:[.,]\d)?)\b", text, re.I)):
            rooms = float(m.group(1).replace(",", "."))

    year_built = None
    if "Baujahr" in details:
        if (m := re.search(r"(\d{4})", details["Baujahr"])): year_built = int(m.group(1))
    elif (m := re.search(r"[Bb]aujahr[:\s]+(\d{4})", text)): year_built = int(m.group(1))

    ort = None
    if (m := re.search(r"\b(\d{5}\s+[A-ZÄÖÜa-zäöüß\- ]+)\b", text)): ort = m.group(1).strip()

    def extract_val(label):
        val = details.get(label)
        if val and (m := re.search(r"([\d\.\s,]+)", val)):
            return parse_price(m.group(1))
        return None

    hausgeld = extract_val("Hausgeld")
    kaltmiete = extract_val("Kaltmiete")

    if hausgeld is None and (m := re.search(r"Hausgeld[:\s]+([\d\.\s,]+)\s*(€|EUR)", text, re.I)):
        hausgeld = parse_price(m.group(1))
    if kaltmiete is None and (m := re.search(r"Kaltmiete[:\s]+([\d\.\s,]+)\s*(€|EUR)", text, re.I)):
        kaltmiete = parse_price(m.group(1))

    img = soup.select_one("img[src*='https://img.kleinanzeigen.de']")
    img_url = img["src"] if img else None

    return {
        "url": url, "title": title, "price": price, "vb": vb_flag,
        "area": area, "rooms": rooms, "year_built": year_built,
        "ort": ort, "image": img_url, "hausgeld": hausgeld, "kaltmiete": kaltmiete,
    }

def build_page_url(base, n):
    p = urlsplit(base)
    path = p.path
    idx = path.find("/c196l")
    if idx == -1:
        return base
    new = path[:idx] + f"/seite:{n}" + path[idx:]
    return urlunsplit((p.scheme, p.netloc, new, p.query, p.fragment))

def scrape_all(url, pb_placeholder, countdown_placeholder, delay=(0.1, 0.2)):
    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    html1 = get_html(url)
    pages = get_total_pages(html1)

    all_links = []
    for p in range(1, pages + 1):
        html = html1 if p == 1 else get_html(build_page_url(url, p))
        all_links += parse_results_page(html, base)
        time.sleep(random.uniform(*delay))

    all_links = list(dict.fromkeys(all_links))
    total = len(all_links)
    if total == 0:
        return pd.DataFrame()

    total_time = int(total * 1)
    pb = pb_placeholder.progress(0)
    start_time = time.time()
    results = []

    for i, link in enumerate(all_links, 1):
        elapsed = time.time() - start_time
        remaining = max(int(total_time - elapsed), 0)
        mins, secs = divmod(remaining, 60)
        if remaining > 0:
            countdown_placeholder.markdown(
                f"<div style='display:flex;align-items:center;gap:6px;'>"
                f"<div style='border:3px solid #f3f3f3;border-top:3px solid #1E88E5;"
                f"border-radius:50%;width:14px;height:14px;"
                f"animation:spin 1s linear infinite;'></div>"
                f"<span style='font-weight:600;font-size:0.9rem;'>{mins:02d}:{secs:02d}</span>"
                f"</div><style>@keyframes spin{{0%{{transform:rotate(0deg);}}100%{{transform:rotate(360deg);}}}}</style>",
                unsafe_allow_html=True,
            )
        else:
            countdown_placeholder.markdown("✅ Fertig!")
        try:
            html = get_html(link)
            results.append(parse_ad_page(html, link))
        except Exception:
            pass
        pb.progress(i / total)
        time.sleep(random.uniform(*delay))

    pb.empty()
    countdown_placeholder.empty()
    df = pd.DataFrame(results)

    if not df.empty:
        df["price_per_m2"] = df.apply(
            lambda r: round(r["price"] / r["area"], 2)
            if pd.notnull(r["price"]) and pd.notnull(r["area"]) and r["area"] > 0
            else None,
            axis=1,
        )
    return df

# =====================================================
# Sidebar + Filter
# =====================================================
st.sidebar.header("🏠 IMMOnitor")

ort = st.sidebar.selectbox("Ort", orte_liste, index=None, placeholder="Ort wählen…")
radius_opts = {"Ganzer Ort": "", "+5 km": "r5", "+10 km": "r10", "+20 km": "r20", "+30 km": "r30", "+50 km": "r50"}
radius = st.sidebar.selectbox("Radius", list(radius_opts.keys()))
pb_placeholder = st.sidebar.empty()
countdown_placeholder = st.sidebar.empty()

# =====================================================
# Scraping mit Cache
# =====================================================
if ort:
    slug = orte_df.loc[orte_df["name"] == ort, "slug"].values[0]
    base = f"https://www.kleinanzeigen.de/s-wohnung-kaufen/{slug}{radius_opts[radius]}"
    if st.sidebar.button("Anzeigen abrufen"):
        with st.spinner("Scraping läuft …"):
            df = scrape_all(base, pb_placeholder, countdown_placeholder)
        if not df.empty:
            save_cached_data(df, ort, radius)
            st.session_state["scraped_df"] = df
    else:
        cached = load_cached_data(ort, radius)
        if cached is not None:
            st.session_state["scraped_df"] = cached

# =====================================================
# Anzeige
# =====================================================
if "scraped_df" in st.session_state and not st.session_state["scraped_df"].empty:
    df = st.session_state["scraped_df"].copy()

    # Zeitstempel anzeigen
    scraped_time = df.attrs.get("scraped_at", None)
    if scraped_time:
        st.markdown(f"🕒 **Daten zuletzt aktualisiert am:** {scraped_time}")

    @st.cache_data
    def load_reference_prices():
        return pd.read_csv("data/ergebnisse.csv", dtype={"plz": str})

    preise_df = load_reference_prices()
    df["plz"] = df["ort"].str.extract(r"(\d{5})")
    df = df.merge(preise_df, on="plz", how="left")

    df["deal_score"] = df.apply(
        lambda r: round(r["price_per_m2"] / r["avg_offiziell"], 2)
        if pd.notnull(r["price_per_m2"]) and pd.notnull(r["avg_offiziell"]) and r["avg_offiziell"] > 0
        else None,
        axis=1,
    )

    # --- Filter
    min_price = st.sidebar.number_input("Preis ab (€)", 0, 10_000_000, 0, step=10_000)
    max_price = st.sidebar.number_input("Preis bis (€)", 0, 10_000_000, 2_000_000, step=10_000)
    min_qmprice = st.sidebar.number_input("€/m² ab", 0, 50_000, 0, step=50)
    max_qmprice = st.sidebar.number_input("€/m² bis", 0, 50_000, 50_000, step=50)
    min_area = st.sidebar.number_input("Fläche ab (m²)", 0, 1000, 0, step=10)
    max_area = st.sidebar.number_input("Fläche bis (m²)", 0, 2000, 500, step=10)
    min_rooms = st.sidebar.number_input("Zimmer ab", 0.0, 20.0, 0.0, step=0.5)
    max_rooms = st.sidebar.number_input("Zimmer bis", 0.0, 20.0, 20.0, step=0.5)
    min_year = st.sidebar.number_input("Baujahr ab", 1800, 2100, 1900)
    max_year = st.sidebar.number_input("Baujahr bis", 1800, 2100, 2100)

    def safe_between(series, low, high):
        return series.fillna((low + high) / 2).between(low, high)

    df = df[
        safe_between(df["price"], min_price, max_price)
        & safe_between(df["price_per_m2"], min_qmprice, max_qmprice)
        & safe_between(df["area"], min_area, max_area)
        & safe_between(df["rooms"], min_rooms, max_rooms)
        & safe_between(df["year_built"], min_year, max_year)
    ]

    sort_choice = st.selectbox(
        "Sortiere nach",
        [
            "Bester Deal (Verhältnis zum Ø Ortspreis)",
            "Preis aufsteigend",
            "Preis absteigend",
            "Preis pro m² aufsteigend",
            "Preis pro m² absteigend"#,
            #"Günstigste €/m² zuerst"
        ],
        key="sort_choice"
    )

    if "Deal" in sort_choice:
        sort_col = "deal_score"; ascending = True
    elif "Günstigste" in sort_choice:
        sort_col = "price_per_m2"; ascending = True
    else:
        sort_col = "price_per_m2" if "Preis pro m²" in sort_choice else "price"
        ascending = "aufsteigend" in sort_choice

    df[sort_col] = pd.to_numeric(df[sort_col], errors="coerce")
    df = df.sort_values(sort_col, ascending=ascending, na_position="last")

    n_cols = 3
    for i in range(0, len(df), n_cols):
        cols = st.columns(n_cols, gap="large")
        for j, col in enumerate(cols):
            if i + j >= len(df): break
            row = df.iloc[i + j]

            price_display = "VB" if row.get("vb") and row["price"] == 0 else (
                f"{int(row['price']):,}".replace(",", " ") + " €" if pd.notnull(row["price"]) else "-"
            )
            qm_display = f"{row['price_per_m2']:.2f} €/m²" if row.get("price_per_m2") else "-"
            area_display = f"{row['area']:.0f} m²" if row["area"] else "-"
            rooms_display = f"{row['rooms']:.1f}".replace(".", ",") if pd.notnull(row['rooms']) else "-"
            year_display = str(int(row["year_built"])) if pd.notnull(row["year_built"]) else "-"
            ort_display = row["ort"] or "-"
            deal_display = f"{row['deal_score']:.2f}" if pd.notnull(row.get("deal_score")) else "-"

            ort_min = f"{int(row['min_preis']):,}".replace(",", " ") + " €" if pd.notnull(row.get("min_preis")) else "-"
            ort_max = f"{int(row['max_preis']):,}".replace(",", " ") + " €" if pd.notnull(row.get("max_preis")) else "-"
            ort_avg = f"{int(row['avg_offiziell']):,}".replace(",", " ") + " €" if pd.notnull(row.get("avg_offiziell")) else "-"

            hausgeld_display = f"{int(row['hausgeld'])} €" if pd.notnull(row.get('hausgeld')) else "-"
            kaltmiete_display = f"{int(row['kaltmiete'])} €" if pd.notnull(row.get('kaltmiete')) else "-"
            img_url = row["image"] or "https://via.placeholder.com/400x300?text=Kein+Bild"

            with col:
                st.markdown(f"""
                    <div style='background:#fff;border-radius:12px;
                        box-shadow:0 2px 10px rgba(0,0,0,0.15);
                        overflow:hidden;display:flex;flex-direction:column;
                        height:700px;width:100%;margin-bottom:50px;'>
                        <div style='height:300px;display:flex;align-items:center;
                                    justify-content:center;background:#f9f9f9;'>
                            <img src='{img_url}' style='max-height:100%;max-width:100%;object-fit:contain;'>
                        </div>
                        <div style='padding:0.8em 1em;flex-grow:1;'>
                            <h4 style='margin:0 0 0.3em 0;font-size:1.05rem;'>{row["title"][:80]}</h4>
                            <p>💰 {price_display} ({qm_display})</p>
                            <p>📐 {area_display} | {rooms_display} Zi | {year_display}</p>
                            <p>📊 Ortsüblich: {ort_min} - {ort_max} (Ø {ort_avg})</p>
                            <p>🏆 Faktor: {deal_display}</p>
                            <p>💶 Hausgeld: {hausgeld_display} | KM: {kaltmiete_display}</p>
                            <p>📍 {ort_display}</p>
                        </div>
                        <div style='padding:0.8em 1em 1.2em 1em;'>
                            <a href='{row["url"]}' target='_blank'>
                                <button style='width:100%;background:#1E88E5;border:none;color:white;padding:10px 0;
                                    border-radius:8px;font-weight:600;cursor:pointer;'>🔗 Anzeige öffnen</button>
                            </a>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
