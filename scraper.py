"""
scraper.py  – UK Abattoir Data Scraper (full UK coverage)
==========================================================
Data sources:
  England & Wales: FSA open data CSV (monthly)
  Scotland:        FSS open data CSV
  Northern Ireland: FSA/DAERA open data CSV (monthly)

Halal/non-stun certification:
  HMC PDF  – non-stun halal  (halalhmc.org)
  HFA      – stunned halal   (halalfoodauthority.com)
  Shechita – Kosher/non-stun (shechitauk.org)

Status codes:
  NON_STUN       – HMC or Shechita certified
  STUN_RELIGIOUS – HFA certified
  MIXED          – FSA flags religious activity, no cert body match
  STANDARD       – no religious slaughter evidence found
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
GB_PAT = re.compile(r"\bGB\s*(\d{3,5})\b", re.IGNORECASE)

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
    """
    FSS Scotland CSV has metadata rows before the actual header.
    We scan for the header row by looking for a row containing 'Approval Number'
    or an 'AppNo' style column, then parse from there.
    """
    log.info(f"Downloading Scotland CSV: {url}")
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    content = r.content.decode("utf-8-sig", errors="replace")
    lines   = content.splitlines()

    # Find the header row — it contains a recognisable column name
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
        log.warning("Scotland CSV: could not find header row, trying row 0")
        header_idx = 0

    log.info(f"Scotland CSV: using row {header_idx} as header. First few cols: {lines[header_idx][:120]}")
    clean   = "\n".join(lines[header_idx:])
    reader  = csv.DictReader(io.StringIO(clean))
    rows    = []

    for raw in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in raw.items()}

        is_slaughterhouse = (
            row.get("slaughterhouse","").lower() == "yes"
            or row.get("game_handling_establishment","").lower() == "yes"
            # Some Scotland CSVs use activity columns only
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
            "fsa_religious_flag": False,  # Scotland mandates stunning
        })

    log.info(f"Scotland CSV: {len(rows)} slaughterhouses found")
    return rows


# ── Certification body scrapers ───────────────────────────────────────────────

def scrape_hmc() -> set:
    """HMC (non-stun halal only) — certified abattoir list via PDF."""
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
    """HFA (stunned halal) — try known URL patterns."""
    log.info("Fetching HFA certified list…")
    numbers = set()
    urls = [
        "https://halalfoodauthority.com/certified-slaughterhouses/",
        "https://halalfoodauthority.com/certified-companies/",
        "https://www.halalfoodauthority.com/",
    ]
    for url in urls:
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
    """Shechita UK — Kosher, always non-stun."""
    log.info("Fetching Shechita UK list…")
    numbers = set()
    urls = [
        "https://www.shechitauk.org/",
        "https://www.shechitauk.org/about-shechita/",
        "https://www.shechitauk.org/contact/",
    ]
    for url in urls:
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
        bodies = ",".join(filter(None, [
            "HMC"      if in_hmc      else "",
            "Shechita" if in_shechita else "",
        ]))
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
    log.info(
        f"DB updated — "
        f"NON_STUN={counts['NON_STUN']} "
        f"STUN_RELIGIOUS={counts['STUN_RELIGIOUS']} "
        f"MIXED={counts['MIXED']} "
        f"STANDARD={counts['STANDARD']}"
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def run():
    con      = init_db()
    all_rows = []

    # England & Wales
    try:
        ew_url  = get_latest_csv_from_fsa_catalog(FSA_EW_CATALOG)
        ew_rows = parse_fsa_csv(ew_url)
        log.info(f"England & Wales: {len(ew_rows)} slaughterhouses")
        all_rows.extend(ew_rows)
    except Exception as e:
        log.error(f"England & Wales failed: {e}")

    # Northern Ireland
    try:
        ni_url  = get_latest_csv_from_fsa_catalog(FSA_NI_CATALOG)
        ni_rows = parse_fsa_csv(ni_url, country_override="Northern Ireland")
        log.info(f"Northern Ireland: {len(ni_rows)} slaughterhouses")
        all_rows.extend(ni_rows)
    except Exception as e:
        log.error(f"Northern Ireland failed: {e}")

    # Scotland — use direct CSV URL as fallback if page scrape fails
    try:
        scot_rows = parse_scotland_csv(FSS_SCOT_CSV)
        all_rows.extend(scot_rows)
    except Exception as e:
        log.error(f"Scotland failed: {e}")

    log.info(f"Total across all regions: {len(all_rows)} slaughterhouses")

    # Certification bodies
    hmc      = scrape_hmc()
    hfa      = scrape_hfa()
    shechita = scrape_shechita()
    upsert(con, all_rows, hmc, hfa, shechita)
    con.close()
    log.info("Scrape complete.")


if __name__ == "__main__":
    run()
