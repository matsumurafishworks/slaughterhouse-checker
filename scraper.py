"""
scraper.py  – UK Abattoir Data Scraper (full UK coverage + HMC outlets)
=======================================================================
Data sources:
  England & Wales: FSA open data CSV (monthly)
  Scotland:        FSS open data CSV
  Northern Ireland: FSA/DAERA open data CSV (monthly)

Halal/non-stun certification:
  HMC PDF  – non-stun halal  (halalhmc.org)
  HFA      – stunned halal   (halalfoodauthority.com)
  Shechita – Kosher/non-stun (shechitauk.org)

HMC Outlets:
  WordPress REST API — custom post type 'outlets'
  Individual outlet pages live at halalhmc.org/outlets/{slug}/
  so WordPress exposes them at /wp-json/wp/v2/outlets?per_page=100

Status codes:
  NON_STUN       – HMC or Shechita certified
  STUN_RELIGIOUS – HFA certified
  MIXED          – FSA flags religious activity, no cert body match
  STANDARD       – no religious slaughter evidence found
"""

import csv, io, logging, os, re, sqlite3, time
from datetime import datetime
from io import BytesIO
from html.parser import HTMLParser

import requests
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text_to_fp
from pdfminer.layout import LAParams

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "slaughterhouses.db")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SlaughterhouseChecker/1.0)"}
RELIGIOUS_KEYWORDS = ["halal","kosher","shechita","religious","non-stun","non stun","watok","dhabiha"]
GB_PAT = re.compile(r"\bGB\s*(\d{3,5})\b", re.IGNORECASE)

FSA_EW_CATALOG = "https://data.food.gov.uk/catalog/datasets/1e61736a-2a1a-4c6a-b8b1-e45912ebc8e3"
FSA_NI_CATALOG = "https://data.food.gov.uk/catalog/datasets/dae35822-ca4e-41a2-b2af-b10b6163085a"
FSS_SCOT_CSV   = "https://www.foodstandards.gov.scot/sites/default/files/2025-12/Approved%20Establishments%20in%20Scotland_0.csv"

POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b', re.IGNORECASE)


# ── Helpers ───────────────────────────────────────────────────────────────────

def strip_html(html_str: str) -> str:
    """Strip HTML tags from a string."""
    if not html_str:
        return ""
    return BeautifulSoup(html_str, "html.parser").get_text(" ", strip=True)


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

def parse_fsa_csv(url: str, country_override: str = "") -> list:
    log.info(f"Downloading FSA CSV: {url}")
    r = requests.get(url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    content = r.content.decode("utf-8-sig", errors="replace")
    reader  = csv.DictReader(io.StringIO(content))
    rows = []
    for raw in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in raw.items()}
        if row.get("slaughterhouse","").lower() != "yes" and \
           row.get("game_handling_establishment","").lower() != "yes":
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
            "fsa_religious_flag": any(kw in all_text for kw in RELIGIOUS_KEYWORDS),
        })
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
        log.warning("Scotland CSV: could not find header row, using row 0")
        header_idx = 0

    log.info(f"Scotland CSV: header at row {header_idx}: {lines[header_idx][:120]}")
    reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
    rows   = []

    for raw in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in raw.items()}
        is_slaughterhouse = (
            row.get("slaughterhouse","").lower() == "yes"
            or row.get("game_handling_establishment","").lower() == "yes"
            or "slaughter" in row.get("all activities approved","").lower()
            or "slaughter" in row.get("all_activities","").lower()
        )
        if not is_slaughterhouse:
            continue
        approval = (
            row.get("approval number","")
            or row.get("appno","")
            or row.get("approval no","")
        ).strip().upper()
        if not approval:
            continue
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
            "fsa_religious_flag": False,
        })

    log.info(f"Scotland CSV: {len(rows)} slaughterhouses found")
    return rows


# ── Certification body scrapers ───────────────────────────────────────────────

def scrape_hmc() -> set:
    log.info("Fetching HMC certified PDF…")
    numbers = set()
    sources = [
        ("https://halalhmc.org/wp-content/uploads/certified-outlets/meats.pdf", True),
        ("https://halalhmc.org/suppliers/", False),
    ]
    for url, is_pdf in sources:
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


# ── HMC Outlets via WordPress REST API ───────────────────────────────────────

def _classify_outlet_type(name: str) -> str:
    nl = name.lower()
    if any(w in nl for w in ["restaurant","kitchen","diner","café","cafe","grill",
                              "tandoori","biryani","curry","dining","eatery","lounge"]):
        return "RESTAURANT"
    if any(w in nl for w in ["takeaway","take away","take-away","kebab","pizza",
                              "burger","chicken","chippy","fish & chip","fish and chip"]):
        return "TAKEAWAY"
    if any(w in nl for w in ["butcher","meat","halal shop","grocery","supermarket",
                              "cash & carry","food store","deli","butchery"]):
        return "BUTCHER_SHOP"
    return "OTHER"


def _normalise_postcode(pc: str) -> str:
    pc = pc.upper().strip()
    if pc and " " not in pc and len(pc) > 3:
        pc = pc[:-3].strip() + " " + pc[-3:]
    return pc


def scrape_hmc_outlets_via_api() -> list:
    """
    HMC outlets are WordPress custom post type 'outlets'.
    Individual pages live at /outlets/{slug}/ so WordPress
    exposes them at /wp-json/wp/v2/outlets?per_page=100&page=N
    """
    log.info("Fetching HMC outlets via WordPress REST API…")
    outlets = []
    page    = 1

    while True:
        url = f"https://halalhmc.org/wp-json/wp/v2/outlets?per_page=100&page={page}&_fields=id,title,content,acf,link"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 400 or r.status_code == 404:
                log.info(f"WP API outlets: end at page {page} (status {r.status_code})")
                break
            r.raise_for_status()
            items = r.json()
            if not items:
                break
        except Exception as e:
            log.warning(f"WP API outlets page {page} failed: {e}")
            break

        for item in items:
            name = strip_html(item.get("title", {}).get("rendered", ""))
            if not name:
                continue

            # Try ACF (Advanced Custom Fields) first — common on HMC-style WP sites
            acf     = item.get("acf", {}) or {}
            address = acf.get("address","") or strip_html(item.get("content",{}).get("rendered",""))
            phone   = acf.get("phone","") or acf.get("telephone","") or ""
            town    = acf.get("town","") or acf.get("city","") or ""
            postcode= acf.get("postcode","") or acf.get("post_code","") or ""

            # Extract postcode from address text if not in ACF
            if not postcode and address:
                m = POSTCODE_RE.search(address)
                if m:
                    postcode = m.group(1)

            postcode = _normalise_postcode(postcode)

            # Extract town from address if not in ACF
            if not town and postcode and address:
                pre   = address[:address.upper().find(postcode.replace(" ",""))].strip().rstrip(",")
                parts = [p.strip() for p in pre.split(",") if p.strip()]
                town  = parts[-1] if parts else ""

            outlets.append({
                "name":        name,
                "address":     address,
                "town":        town,
                "postcode":    postcode,
                "phone":       phone,
                "outlet_type": _classify_outlet_type(name),
                "source_url":  item.get("link",""),
                "latitude":    None,
                "longitude":   None,
            })

        log.info(f"WP API outlets page {page}: {len(items)} items, running total {len(outlets)}")

        # Check if there are more pages via headers
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.5)

    log.info(f"WP API outlets: {len(outlets)} total")
    return outlets


def scrape_hmc_outlets_fallback() -> list:
    """
    Fallback: try the 'posts' endpoint filtered to 'outlets' category,
    or try custom post type name variants.
    """
    log.info("Trying WP API fallback post type names…")

    for post_type in ["outlet", "shop", "shops", "certified-outlet", "hmc_outlet"]:
        url = f"https://halalhmc.org/wp-json/wp/v2/{post_type}?per_page=1"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                log.info(f"Found working post type: {post_type}")
                # Now scrape all pages of this post type
                outlets = []
                page = 1
                while True:
                    r2 = requests.get(
                        f"https://halalhmc.org/wp-json/wp/v2/{post_type}?per_page=100&page={page}&_fields=id,title,content,acf,link",
                        headers=HEADERS, timeout=30
                    )
                    if r2.status_code in (400, 404):
                        break
                    items = r2.json()
                    if not items:
                        break
                    for item in items:
                        name = strip_html(item.get("title", {}).get("rendered", ""))
                        if not name:
                            continue
                        acf      = item.get("acf", {}) or {}
                        address  = acf.get("address","") or strip_html(item.get("content",{}).get("rendered",""))
                        phone    = acf.get("phone","") or ""
                        town     = acf.get("town","") or ""
                        postcode = acf.get("postcode","") or ""
                        if not postcode and address:
                            m = POSTCODE_RE.search(address)
                            if m:
                                postcode = m.group(1)
                        postcode = _normalise_postcode(postcode)
                        outlets.append({
                            "name":        name,
                            "address":     address,
                            "town":        town,
                            "postcode":    postcode,
                            "phone":       phone,
                            "outlet_type": _classify_outlet_type(name),
                            "source_url":  item.get("link",""),
                            "latitude":    None,
                            "longitude":   None,
                        })
                    total_pages = int(r2.headers.get("X-WP-TotalPages", 1))
                    if page >= total_pages:
                        break
                    page += 1
                    time.sleep(0.5)
                log.info(f"Fallback post type '{post_type}': {len(outlets)} outlets")
                return outlets
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"Fallback {post_type}: {e}")

    log.warning("All WP API fallbacks failed — no outlet data")
    return []


def geocode_outlets(outlets: list) -> list:
    """Geocode postcodes via postcodes.io bulk API (free, no key)."""
    if not outlets:
        return outlets
    log.info("Geocoding outlet postcodes via postcodes.io…")
    to_geocode = list({o["postcode"] for o in outlets if o["postcode"]})
    log.info(f"Unique postcodes to geocode: {len(to_geocode)}")
    pc_coords: dict = {}
    BATCH = 100
    for i in range(0, len(to_geocode), BATCH):
        batch = to_geocode[i:i + BATCH]
        try:
            resp = requests.post(
                "https://api.postcodes.io/postcodes",
                json={"postcodes": batch},
                headers={"Content-Type": "application/json"},
                timeout=20,
            )
            resp.raise_for_status()
            for item in resp.json().get("result", []):
                query  = item.get("query", "")
                result = item.get("result")
                if result:
                    pc_coords[query.upper().replace(" ", "")] = (result["latitude"], result["longitude"])
        except Exception as e:
            log.warning(f"Geocoding batch {i//BATCH + 1} failed: {e}")
        time.sleep(0.3)
    matched = 0
    for o in outlets:
        key = o["postcode"].upper().replace(" ", "") if o["postcode"] else ""
        if key in pc_coords:
            o["latitude"], o["longitude"] = pc_coords[key]
            matched += 1
    log.info(f"Geocoding: {matched}/{len(outlets)} outlets have coordinates")
    return outlets


def upsert_outlets(con: sqlite3.Connection, outlets: list):
    now = datetime.utcnow().isoformat()
    con.execute("DELETE FROM hmc_outlets")
    for o in outlets:
        con.execute("""
            INSERT INTO hmc_outlets
              (name, address, town, postcode, phone, outlet_type,
               source_url, latitude, longitude, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (o["name"], o["address"], o["town"], o["postcode"],
              o["phone"], o["outlet_type"], o["source_url"],
              o["latitude"], o["longitude"], now))
    con.commit()
    geocoded = sum(1 for o in outlets if o["latitude"])
    log.info(f"hmc_outlets: {len(outlets)} records, {geocoded} geocoded.")


# ── Database ──────────────────────────────────────────────────────────────────

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS slaughterhouses (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            approval_number  TEXT UNIQUE NOT NULL,
            name             TEXT,
            address_line1    TEXT,
            address_line2    TEXT,
            town             TEXT,
            county           TEXT,
            postcode         TEXT,
            country          TEXT,
            activities_raw   TEXT,
            slaughter_status TEXT NOT NULL DEFAULT 'STANDARD',
            certified_by     TEXT,
            last_updated     TEXT
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
    con.commit()
    return con


def classify(row, hmc, hfa, shechita):
    n = row["approval_number"]
    in_hmc      = n in hmc
    in_hfa      = n in hfa
    in_shechita = n in shechita
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
               postcode,country,activities_raw,slaughter_status,certified_by,last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(approval_number) DO UPDATE SET
              name=excluded.name, address_line1=excluded.address_line1,
              address_line2=excluded.address_line2, town=excluded.town,
              county=excluded.county, postcode=excluded.postcode,
              country=excluded.country, activities_raw=excluded.activities_raw,
              slaughter_status=excluded.slaughter_status,
              certified_by=excluded.certified_by, last_updated=excluded.last_updated
        """, (row["approval_number"],row["name"],row["address_line1"],
              row["address_line2"],row["town"],row["county"],row["postcode"],
              row["country"],row["activities_raw"],status,bodies,now))
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

    # England & Wales
    try:
        ew_rows = parse_fsa_csv(get_latest_csv_from_fsa_catalog(FSA_EW_CATALOG))
        log.info(f"England & Wales: {len(ew_rows)} slaughterhouses")
        all_rows.extend(ew_rows)
    except Exception as e:
        log.error(f"England & Wales failed: {e}")

    # Northern Ireland
    try:
        ni_rows = parse_fsa_csv(get_latest_csv_from_fsa_catalog(FSA_NI_CATALOG), country_override="Northern Ireland")
        log.info(f"Northern Ireland: {len(ni_rows)} slaughterhouses")
        all_rows.extend(ni_rows)
    except Exception as e:
        log.error(f"Northern Ireland failed: {e}")

    # Scotland
    try:
        scot_rows = parse_scotland_csv(FSS_SCOT_CSV)
        all_rows.extend(scot_rows)
    except Exception as e:
        log.error(f"Scotland failed: {e}")

    log.info(f"Total: {len(all_rows)} slaughterhouses")

    # Certification bodies
    hmc      = scrape_hmc()
    hfa      = scrape_hfa()
    shechita = scrape_shechita()
    upsert(con, all_rows, hmc, hfa, shechita)

    # HMC Outlets — try WP REST API, then fallback post type variants
    try:
        outlets = scrape_hmc_outlets_via_api()
        if not outlets:
            log.info("Primary WP API endpoint returned nothing, trying fallbacks…")
            outlets = scrape_hmc_outlets_fallback()
        if outlets:
            outlets = geocode_outlets(outlets)
            upsert_outlets(con, outlets)
        else:
            log.warning("No outlet data retrieved — hmc_outlets table will be empty")
    except Exception as e:
        log.error(f"HMC outlets failed: {e}")

    con.close()
    log.info("Scrape complete.")


if __name__ == "__main__":
    run()
