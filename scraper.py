"""
scraper.py  – UK Abattoir + Processing Plant Scraper
=====================================================
Establishment types covered:
  SLAUGHTERHOUSE        – Slaughterhouse == Yes
  GAME_HANDLER          – Game_Handling_Establishment == Yes
  CUTTING_PLANT         – Cutting_Plant == Yes
"""

import csv, io, logging, os, re, sqlite3, time
from datetime import datetime
from io import BytesIO

import requests
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text_to_fp
from pdfminer.layout import LAParams

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "slaughterhouses.db")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SlaughterhouseChecker/1.0)"}
RELIGIOUS_KEYWORDS = ["halal","kosher","shechita","religious","non-stun","non stun","watok","dhabiha"]
GB_PAT      = re.compile(r"\bGB\s*(\d{3,5})\b", re.IGNORECASE)
POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b', re.IGNORECASE)
PHONE_RE    = re.compile(r'(?:Tel|Phone|Telephone|T)[:\s]*([\d\s\+\(\)]{7,})', re.IGNORECASE)

FSA_EW_CATALOG = "https://data.food.gov.uk/catalog/datasets/1e61736a-2a1a-4c6a-b8b1-e45912ebc8e3"
FSA_NI_CATALOG = "https://data.food.gov.uk/catalog/datasets/dae35822-ca4e-41a2-b2af-b10b6163085a"
FSS_SCOT_CSV   = "https://www.foodstandards.gov.scot/sites/default/files/2025-12/Approved%20Establishments%20in%20Scotland_0.csv"


# ── Helpers ───────────────────────────────────────────────────────────────────

def pdf_to_text(pdf_bytes: bytes) -> str:
    output = BytesIO()
    extract_text_to_fp(BytesIO(pdf_bytes), output, laparams=LAParams(), output_type="text", codec="utf-8")
    return output.getvalue().decode("utf-8", errors="ignore")


def fetch_gb_numbers(url: str, is_pdf: bool = False) -> set:
    r = requests.get(url, headers=HEADERS, timeout=40)
    r.raise_for_status()
    text = pdf_to_text(r.content) if is_pdf else r.content.decode("utf-8", errors="ignore")
    return set(GB_PAT.findall(text))


def get_latest_csv_from_fsa_catalog(catalog_url: str) -> str:
    r = requests.get(catalog_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "fsaopendata.blob.core.windows.net" in href and href.endswith(".csv"):
            return href
    raise RuntimeError(f"No CSV found on {catalog_url}")


# ── FSA CSV parsers ───────────────────────────────────────────────────────────

def _establishment_type(row: dict) -> str:
    if row.get("slaughterhouse","").lower() == "yes":
        return "SLAUGHTERHOUSE"
    if row.get("game_handling_establishment","").lower() == "yes":
        return "GAME_HANDLER"
    if row.get("cutting_plant","").lower() == "yes":
        return "CUTTING_PLANT"
    return "OTHER"


def parse_fsa_csv(url: str, country_override: str = "") -> list:
    log.info(f"Downloading FSA CSV: {url}")
    r = requests.get(url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    content = r.content.decode("utf-8-sig", errors="replace")
    reader  = csv.DictReader(io.StringIO(content))
    rows = []
    for raw in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in raw.items()}

        is_slaughterhouse = row.get("slaughterhouse","").lower() == "yes"
        is_game           = row.get("game_handling_establishment","").lower() == "yes"
        is_cutting        = row.get("cutting_plant","").lower() == "yes"

        if not (is_slaughterhouse or is_game or is_cutting):
            continue

        approval = row.get("appno","").strip().upper()
        if not approval:
            continue

        all_text = " ".join([
            row.get("all_activities",""),
            row.get("remarks",""),
            row.get("tradingname",""),
        ]).lower()

        rows.append({
            "approval_number":    approval,
            "name":               row.get("tradingname",""),
            "address_line1":      row.get("address1",""),
            "address_line2":      row.get("address2",""),
            "town":               row.get("town",""),
            "county":             row.get("address3",""),
            "postcode":           row.get("postcode",""),
            "country":            country_override or row.get("country",""),
            "activities_raw":     row.get("all_activities",""),
            "establishment_type": _establishment_type(row),
            "fsa_religious_flag": any(kw in all_text for kw in RELIGIOUS_KEYWORDS),
        })

    slaughterhouses = sum(1 for r in rows if r["establishment_type"] in ("SLAUGHTERHOUSE","GAME_HANDLER"))
    cutting_plants  = sum(1 for r in rows if r["establishment_type"] == "CUTTING_PLANT")
    log.info(f"FSA CSV: {slaughterhouses} slaughterhouses/game handlers, {cutting_plants} cutting plants")
    return rows


def parse_scotland_csv(url: str) -> list:
    log.info(f"Downloading Scotland CSV: {url}")
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    content = r.content.decode("utf-8-sig", errors="replace")
    lines   = content.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        lower = line.lower()
        if "approval" in lower and ("number" in lower or "no" in lower):
            header_idx = i
            break
        if "tradingname" in lower or "trading name" in lower:
            header_idx = i
            break
    if header_idx is None:
        header_idx = 0
    log.info(f"Scotland CSV: header at row {header_idx}: {lines[header_idx][:120]}")
    reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
    rows   = []
    for raw in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in raw.items()}
        activities = (row.get("all activities approved","") or row.get("all_activities","")).lower()
        is_slaughterhouse = (
            row.get("slaughterhouse","").lower() == "yes"
            or "slaughter" in activities
        )
        is_game    = row.get("game_handling_establishment","").lower() == "yes" or "game handling" in activities
        is_cutting = row.get("cutting_plant","").lower() == "yes" or "cutting plant" in activities

        if not (is_slaughterhouse or is_game or is_cutting):
            continue

        approval = (
            row.get("approval number","") or row.get("appno","") or row.get("approval no","")
        ).strip().upper()
        if not approval:
            continue

        if is_slaughterhouse:
            est_type = "SLAUGHTERHOUSE"
        elif is_game:
            est_type = "GAME_HANDLER"
        elif is_cutting:
            est_type = "CUTTING_PLANT"
        else:
            est_type = "OTHER"

        rows.append({
            "approval_number":    approval,
            "name":               row.get("trading name","") or row.get("tradingname",""),
            "address_line1":      row.get("address 1","") or row.get("address1",""),
            "address_line2":      row.get("address 2","") or row.get("address2",""),
            "town":               row.get("address 3","") or row.get("town",""),
            "county":             row.get("address 4","") or row.get("county",""),
            "postcode":           row.get("post code","") or row.get("postcode",""),
            "country":            "Scotland",
            "activities_raw":     row.get("all activities approved","") or row.get("all_activities",""),
            "establishment_type": est_type,
            "fsa_religious_flag": False,
        })
    log.info(f"Scotland CSV: {len(rows)} establishments found")
    return rows


# ── Certification body scrapers ───────────────────────────────────────────────

def scrape_hmc() -> set:
    log.info("Fetching HMC certified PDF…")
    numbers = set()
    for url, is_pdf in [
        ("https://halalhmc.org/wp-content/uploads/certified-outlets/meats.pdf", True),
        ("https://halalhmc.org/suppliers/", False),
    ]:
        try:
            found = fetch_gb_numbers(url, is_pdf=is_pdf)
            if found:
                numbers |= found
                log.info(f"HMC: {len(found)} from {url}")
            time.sleep(1)
        except Exception as e:
            log.warning(f"HMC failed {url}: {e}")
    log.info(f"HMC total: {len(numbers)}")
    return numbers


def scrape_hfa() -> set:
    log.info("Fetching HFA certified list…")
    numbers = set()
    for url in [
        "https://halalfoodauthority.com/certified-slaughterhouses/",
        "https://halalfoodauthority.com/certified-companies/",
        "https://www.halalfoodauthority.com/",
    ]:
        try:
            found = fetch_gb_numbers(url)
            if found:
                numbers |= found
                log.info(f"HFA: {len(found)} from {url}")
            time.sleep(1)
        except Exception as e:
            log.warning(f"HFA failed {url}: {e}")
    log.info(f"HFA total: {len(numbers)}")
    return numbers


def scrape_shechita() -> set:
    log.info("Fetching Shechita UK list…")
    numbers = set()
    for url in [
        "https://www.shechitauk.org/",
        "https://www.shechitauk.org/about-shechita/",
        "https://www.shechitauk.org/contact/",
    ]:
        try:
            found = fetch_gb_numbers(url)
            if found:
                numbers |= found
                log.info(f"Shechita: {len(found)} from {url}")
            time.sleep(1)
        except Exception as e:
            log.warning(f"Shechita failed {url}: {e}")
    log.info(f"Shechita total: {len(numbers)}")
    return numbers


# ── HMC Outlets ───────────────────────────────────────────────────────────────

def _normalise_postcode(pc: str) -> str:
    pc = pc.upper().strip()
    if pc and " " not in pc and len(pc) > 3:
        pc = pc[:-3].strip() + " " + pc[-3:]
    return pc


# HMC's own category labels → our internal codes
# Note: HMC uses "Restaurants and Takeaways" as ONE category — no separate takeaway type
HMC_CATEGORY_MAP = {
    "restaurants and takeaways": "RESTAURANT",
    "caterers":                  "RESTAURANT",
    "caterer":                   "RESTAURANT",
    "restaurant":                "RESTAURANT",
    "butchers":                  "BUTCHER_SHOP",
    "butcher":                   "BUTCHER_SHOP",
    "dessert shops":             "DESSERT",
    "dessert":                   "DESSERT",
    "other":                     "OTHER",
}


def get_outlet_urls_from_sitemap() -> list:
    urls = []
    for page_num in range(1, 30):
        sitemap_url = f"https://halalhmc.org/wp-sitemap-posts-outlets-{page_num}.xml"
        try:
            r = requests.get(sitemap_url, headers=HEADERS, timeout=20)
            if r.status_code == 404:
                if page_num == 1:
                    log.warning("No outlets sitemap found")
                break
            r.raise_for_status()
            soup = BeautifulSoup(r.content, "xml")
            page_urls = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
            if not page_urls:
                break
            urls.extend(page_urls)
            log.info(f"Sitemap page {page_num}: {len(page_urls)} URLs (total {len(urls)})")
            if len(page_urls) < 2000:
                break
            time.sleep(0.5)
        except Exception as e:
            log.warning(f"Sitemap page {page_num} error: {e}")
            break
    return urls


def scrape_outlet_page(url: str) -> dict | None:
    """
    Scrape a single HMC outlet page.

    Confirmed HTML structure (from page source):
      Name:        h1.page-title
      Category:    div.category-name > p          e.g. "Caterers"
      Address:     div.outlet-address > p tags
      Phone:       div.outlet-number > a
      Coordinates: div.marker[data-lat][data-lng]  ← already geocoded!
    """
    try:
        r    = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # ── Name ─────────────────────────────────────────────────────────────
        h1   = soup.find("h1", class_="page-title") or soup.find("h1")
        name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        # ── Category (HMC's own label) ────────────────────────────────────
        cat_el      = soup.select_one("div.category-name p")
        hmc_category = cat_el.get_text(strip=True).lower() if cat_el else ""

        if "butcher" in hmc_category:
            outlet_type = "BUTCHER_SHOP"
        elif "restaurant" in hmc_category or "takeaway" in hmc_category or "caterer" in hmc_category:
            outlet_type = "RESTAURANT"
        elif "dessert" in hmc_category:
            outlet_type = "DESSERT"
        else:
            outlet_type = "OTHER"

        # ── Address ───────────────────────────────────────────────────────
        addr_el  = soup.select_one("div.outlet-address")
        address  = ""
        postcode = ""
        town     = ""
        if addr_el:
            parts    = [p.get_text(" ", strip=True) for p in addr_el.find_all("p") if p.get_text(strip=True)]
            address  = " ".join(parts)
            # Postcode is typically the last <p>
            pcm = POSTCODE_RE.search(address)
            if pcm:
                postcode = _normalise_postcode(pcm.group(1))
            # Town: last comma-separated segment before postcode
            if postcode and postcode.replace(" ","") in address.replace(" ",""):
                pre   = address[:address.upper().find(postcode.replace(" ","").upper())].rstrip(", ")
                segs  = [s.strip() for s in re.split(r'[,\n]', pre) if s.strip()]
                town  = segs[-1] if segs else ""

        # ── Phone ─────────────────────────────────────────────────────────
        phone_el = soup.select_one("div.outlet-number a")
        phone    = phone_el.get_text(strip=True) if phone_el else ""

        # ── Coordinates (already in the page — no geocoding needed!) ──────
        marker = soup.select_one("div.marker[data-lat][data-lng]")
        lat    = float(marker["data-lat"]) if marker else None
        lng    = float(marker["data-lng"]) if marker else None

        return {
            "name":        name,
            "address":     address,
            "town":        town,
            "postcode":    postcode,
            "phone":       phone,
            "outlet_type": outlet_type,
            "source_url":  url,
            "latitude":    lat,
            "longitude":   lng,
        }
    except Exception as e:
        log.warning(f"Failed to scrape {url}: {e}")
        return None


def scrape_hmc_outlets() -> list:
    outlet_urls = get_outlet_urls_from_sitemap()
    if not outlet_urls:
        return []
    log.info(f"Scraping {len(outlet_urls)} outlet pages…")
    outlets = []
    for i, url in enumerate(outlet_urls):
        outlet = scrape_outlet_page(url)
        if outlet:
            outlets.append(outlet)
        if (i + 1) % 100 == 0:
            log.info(f"Progress: {i + 1}/{len(outlet_urls)} scraped, {len(outlets)} outlets so far")
        time.sleep(0.4)

    # Log type breakdown
    from collections import Counter
    counts = Counter(o["outlet_type"] for o in outlets)
    log.info(f"Outlet types: {dict(counts)}")
    return outlets


def upsert_outlets(con, outlets):
    now = datetime.utcnow().isoformat()
    con.execute("DELETE FROM hmc_outlets")
    for o in outlets:
        con.execute("""
            INSERT INTO hmc_outlets
              (name, address, town, postcode, phone, outlet_type,
               source_url, latitude, longitude, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (o["name"],o["address"],o["town"],o["postcode"],
              o["phone"],o["outlet_type"],o["source_url"],
              o["latitude"],o["longitude"],now))
    con.commit()
    geocoded = sum(1 for o in outlets if o["latitude"])
    log.info(f"hmc_outlets: {len(outlets)} records, {geocoded} geocoded.")



# ── HMC Schools ───────────────────────────────────────────────────────────────

def scrape_hmc_schools() -> list:
    """
    Scrape HMC certified schools from wp-sitemap-posts-schools-N.xml.
    School pages only contain a name (no address/phone/coordinates).
    """
    log.info("Fetching HMC school URLs from sitemap…")
    school_urls = []
    for page_num in range(1, 10):
        sitemap_url = f"https://halalhmc.org/wp-sitemap-posts-schools-{page_num}.xml"
        try:
            r = requests.get(sitemap_url, headers=HEADERS, timeout=20)
            if r.status_code == 404:
                break
            r.raise_for_status()
            soup = BeautifulSoup(r.content, "xml")
            page_urls = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
            if not page_urls:
                break
            school_urls.extend(page_urls)
            log.info(f"Schools sitemap page {page_num}: {len(page_urls)} URLs (total {len(school_urls)})")
            if len(page_urls) < 2000:
                break
            time.sleep(0.5)
        except Exception as e:
            log.warning(f"Schools sitemap page {page_num} error: {e}")
            break

    if not school_urls:
        log.warning("No school URLs found in sitemap")
        return []

    log.info(f"Scraping {len(school_urls)} school pages…")
    schools = []
    for i, url in enumerate(school_urls):
        try:
            r    = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            h1   = soup.find("h1", class_="page-title") or soup.find("h1")
            name = h1.get_text(strip=True) if h1 else ""
            if not name:
                continue

            # Address — same structure as outlets, may or may not be present
            addr_el  = soup.select_one("div.outlet-address")
            address  = ""
            postcode = ""
            town     = ""
            if addr_el:
                parts   = [p.get_text(" ", strip=True) for p in addr_el.find_all("p") if p.get_text(strip=True)]
                address = " ".join(parts)
                pcm     = POSTCODE_RE.search(address)
                if pcm:
                    postcode = _normalise_postcode(pcm.group(1))
                if postcode and postcode.replace(" ","") in address.replace(" ",""):
                    pre  = address[:address.upper().find(postcode.replace(" ","").upper())].rstrip(", ")
                    segs = [s.strip() for s in re.split(r"[,\n]", pre) if s.strip()]
                    town = segs[-1] if segs else ""

            # Phone
            phone_el = soup.select_one("div.outlet-number a")
            phone    = phone_el.get_text(strip=True) if phone_el else ""

            # Coordinates
            marker = soup.select_one("div.marker[data-lat][data-lng]")
            lat    = float(marker["data-lat"]) if marker else None
            lng    = float(marker["data-lng"]) if marker else None

            schools.append({
                "name":       name,
                "address":    address,
                "town":       town,
                "postcode":   postcode,
                "phone":      phone,
                "source_url": url,
                "latitude":   lat,
                "longitude":  lng,
            })

            if (i + 1) % 100 == 0:
                log.info(f"Schools progress: {i + 1}/{len(school_urls)} scraped")
            time.sleep(0.4)
        except Exception as e:
            log.warning(f"Failed to scrape school {url}: {e}")

    log.info(f"Schools scrape complete: {len(schools)} schools")
    return schools


def upsert_schools(con, schools: list):
    now = datetime.utcnow().isoformat()
    con.execute("DELETE FROM hmc_schools")
    for s in schools:
        con.execute("""
            INSERT INTO hmc_schools
              (name, address, town, postcode, phone, source_url, latitude, longitude, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (s["name"], s["address"], s["town"], s["postcode"],
              s["phone"], s["source_url"], s["latitude"], s["longitude"], now))
    con.commit()
    geocoded = sum(1 for s in schools if s["latitude"])
    log.info(f"hmc_schools: {len(schools)} records, {geocoded} with coordinates.")


# ── Database ──────────────────────────────────────────────────────────────────

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS slaughterhouses (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            approval_number   TEXT UNIQUE NOT NULL,
            name              TEXT,
            address_line1     TEXT,
            address_line2     TEXT,
            town              TEXT,
            county            TEXT,
            postcode          TEXT,
            country           TEXT,
            activities_raw    TEXT,
            establishment_type TEXT NOT NULL DEFAULT 'SLAUGHTERHOUSE',
            slaughter_status  TEXT NOT NULL DEFAULT 'STANDARD',
            certified_by      TEXT,
            last_updated      TEXT
        );
        CREATE TABLE IF NOT EXISTS scrape_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at         TEXT,
            fsa_total      INTEGER,
            non_stun       INTEGER,
            stun_religious INTEGER,
            mixed          INTEGER,
            standard       INTEGER
        );
        CREATE TABLE IF NOT EXISTS hmc_schools (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            address      TEXT,
            town         TEXT,
            postcode     TEXT,
            phone        TEXT,
            source_url   TEXT,
            latitude     REAL,
            longitude    REAL,
            last_updated TEXT
        );
        CREATE TABLE IF NOT EXISTS hmc_outlets (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            address      TEXT,
            town         TEXT,
            postcode     TEXT,
            phone        TEXT,
            outlet_type  TEXT,
            source_url   TEXT,
            latitude     REAL,
            longitude    REAL,
            last_updated TEXT
        );
    """)
    # Add establishment_type column if upgrading from old schema
    try:
        con.execute("ALTER TABLE slaughterhouses ADD COLUMN establishment_type TEXT NOT NULL DEFAULT 'SLAUGHTERHOUSE'")
        con.commit()
    except Exception:
        pass  # Column already exists
    return con


def classify(row, hmc, hfa, shechita):
    n           = row["approval_number"]
    in_hmc      = n in hmc
    in_hfa      = n in hfa
    in_shechita = n in shechita
    # Cutting plants: if HMC-certified they're confirmed non-stun supply chain
    # If not cert body matched but FSA-flagged, mark MIXED
    if in_hmc or in_shechita:
        bodies = ",".join(filter(None, ["HMC" if in_hmc else "", "Shechita" if in_shechita else ""]))
        return "NON_STUN", bodies
    if in_hfa:
        return "STUN_RELIGIOUS", "HFA"
    if row["fsa_religious_flag"]:
        return "MIXED", ""
    return "STANDARD", ""


def upsert(con, rows, hmc, hfa, shechita):
    now    = datetime.utcnow().isoformat()
    counts = {"NON_STUN":0,"STUN_RELIGIOUS":0,"MIXED":0,"STANDARD":0}
    for row in rows:
        status, bodies = classify(row, hmc, hfa, shechita)
        counts[status] += 1
        con.execute("""
            INSERT INTO slaughterhouses
              (approval_number,name,address_line1,address_line2,town,county,
               postcode,country,activities_raw,establishment_type,slaughter_status,certified_by,last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(approval_number) DO UPDATE SET
              name=excluded.name, address_line1=excluded.address_line1,
              address_line2=excluded.address_line2, town=excluded.town,
              county=excluded.county, postcode=excluded.postcode,
              country=excluded.country, activities_raw=excluded.activities_raw,
              establishment_type=excluded.establishment_type,
              slaughter_status=excluded.slaughter_status,
              certified_by=excluded.certified_by, last_updated=excluded.last_updated
        """, (row["approval_number"],row["name"],row["address_line1"],
              row["address_line2"],row["town"],row["county"],row["postcode"],
              row["country"],row["activities_raw"],row["establishment_type"],
              status,bodies,now))
    con.execute("""
        INSERT INTO scrape_log (run_at,fsa_total,non_stun,stun_religious,mixed,standard)
        VALUES (?,?,?,?,?,?)
    """, (now,len(rows),counts["NON_STUN"],counts["STUN_RELIGIOUS"],counts["MIXED"],counts["STANDARD"]))
    con.commit()
    log.info(f"DB updated — NON_STUN={counts['NON_STUN']} STUN_RELIGIOUS={counts['STUN_RELIGIOUS']} MIXED={counts['MIXED']} STANDARD={counts['STANDARD']}")


# ── Entry point ───────────────────────────────────────────────────────────────

def run():
    con      = init_db()
    all_rows = []

    try:
        ew_rows = parse_fsa_csv(get_latest_csv_from_fsa_catalog(FSA_EW_CATALOG))
        log.info(f"England & Wales: {len(ew_rows)} establishments")
        all_rows.extend(ew_rows)
    except Exception as e:
        log.error(f"England & Wales failed: {e}")

    try:
        ni_rows = parse_fsa_csv(get_latest_csv_from_fsa_catalog(FSA_NI_CATALOG), country_override="Northern Ireland")
        log.info(f"Northern Ireland: {len(ni_rows)} establishments")
        all_rows.extend(ni_rows)
    except Exception as e:
        log.error(f"Northern Ireland failed: {e}")

    try:
        scot_rows = parse_scotland_csv(FSS_SCOT_CSV)
        all_rows.extend(scot_rows)
    except Exception as e:
        log.error(f"Scotland failed: {e}")

    log.info(f"Total: {len(all_rows)} establishments")
    hmc      = scrape_hmc()
    hfa      = scrape_hfa()
    shechita = scrape_shechita()
    upsert(con, all_rows, hmc, hfa, shechita)

    try:
        outlets = scrape_hmc_outlets()
        if outlets:
            upsert_outlets(con, outlets)
    except Exception as e:
        log.error(f"HMC outlets failed: {e}")

    try:
        schools = scrape_hmc_schools()
        if schools:
            upsert_schools(con, schools)
    except Exception as e:
        log.error(f"HMC schools failed: {e}")

    con.close()
    log.info("Scrape complete.")


if __name__ == "__main__":
    run()
