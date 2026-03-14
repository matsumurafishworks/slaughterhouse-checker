"""
scraper.py  –  UK Slaughterhouse Data Scraper  (fixed)
=======================================================
Real FSA CSV columns: AppNo, TradingName, Address1, Address2, Address3,
Town, Postcode, Country, All_Activities, Slaughterhouse (Yes/blank),
Game_Handling_Establishment (Yes/blank), Remarks

HMC PDF: https://halalhmc.org/wp-content/uploads/certified-outlets/meats.pdf
  - Lists "GB 4227", "GB 2762" etc  →  extract numeric part to match AppNo
"""

import csv, io, logging, os, re, sqlite3, time
from datetime import datetime
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "slaughterhouses.db")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SlaughterhouseChecker/1.0)"}
FSA_CATALOG = "https://data.food.gov.uk/catalog/datasets/1e61736a-2a1a-4c6a-b8b1-e45912ebc8e3"
RELIGIOUS_KEYWORDS = ["halal","kosher","shechita","religious","non-stun","non stun","watok","dhabiha"]


# ── FSA CSV ───────────────────────────────────────────────────────────────────

def get_latest_fsa_csv_url() -> str:
    log.info("Fetching FSA catalog page…")
    r = requests.get(FSA_CATALOG, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "fsaopendata.blob.core.windows.net" in href and href.endswith(".csv"):
            log.info(f"Latest CSV: {href}")
            return href
    raise RuntimeError("Could not find FSA CSV URL.")


def download_fsa_csv(url: str) -> list:
    log.info("Downloading FSA CSV…")
    r = requests.get(url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    content = r.content.decode("utf-8-sig", errors="replace")
    reader  = csv.DictReader(io.StringIO(content))
    rows = []
    for raw in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in raw.items()}
        # Filter to slaughterhouses only using the dedicated Yes/blank columns
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
            "country":            row.get("country",""),
            "activities_raw":     row.get("all_activities",""),
            "fsa_religious_flag": any(kw in all_text for kw in RELIGIOUS_KEYWORDS),
        })
    log.info(f"FSA: {len(rows)} slaughterhouse rows found.")
    return rows


# ── HMC ───────────────────────────────────────────────────────────────────────

def scrape_hmc() -> set:
    """
    HMC PDF lists entries like 'GB 4227  Manchester Abattoir Ltd'.
    Extract the numeric part (4227) to match FSA AppNo.
    """
    log.info("Fetching HMC certified PDF…")
    numbers = set()
    gb_pat = re.compile(r"\bGB\s+(\d{3,5})\b")
    urls = [
        "https://halalhmc.org/wp-content/uploads/certified-outlets/meats.pdf",
        "https://halalhmc.org/certified-outlets/",
        "https://halalmc.co.uk/certified-outlets/",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            text = r.content.decode("utf-8", errors="ignore")
            found = set(gb_pat.findall(text))
            if found:
                numbers |= found
                log.info(f"HMC: {len(found)} numbers from {url}")
            time.sleep(1)
        except Exception as e:
            log.warning(f"HMC failed for {url}: {e}")
    log.info(f"HMC total: {len(numbers)}")
    return numbers


# ── HFA ───────────────────────────────────────────────────────────────────────

def scrape_hfa() -> set:
    log.info("Fetching HFA certified list…")
    numbers = set()
    gb_pat = re.compile(r"\bGB\s+(\d{3,5})\b")
    urls = [
        "https://www.halalfoodauthority.com/certified-companies",
        "https://www.halalfoodauthority.com/certified-abattoirs",
        "https://halalfoodauthority.com/abattoirs",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                found = set(gb_pat.findall(r.content.decode("utf-8","ignore")))
                numbers |= found
                log.info(f"HFA: {len(found)} from {url}")
            time.sleep(1)
        except Exception as e:
            log.warning(f"HFA failed for {url}: {e}")
    log.info(f"HFA total: {len(numbers)}")
    return numbers


# ── Shechita UK ───────────────────────────────────────────────────────────────

def scrape_shechita() -> set:
    log.info("Fetching Shechita UK list…")
    numbers = set()
    gb_pat = re.compile(r"\bGB\s+(\d{3,5})\b")
    urls = [
        "https://www.shechitauk.org/approved-abattoirs/",
        "https://www.shechitauk.org/abattoirs/",
        "https://www.shechitauk.org/faqs/",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                found = set(gb_pat.findall(r.content.decode("utf-8","ignore")))
                numbers |= found
                log.info(f"Shechita: {len(found)} from {url}")
            time.sleep(1)
        except Exception as e:
            log.warning(f"Shechita failed for {url}: {e}")
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


def run():
    con      = init_db()
    csv_url  = get_latest_fsa_csv_url()
    fsa_rows = download_fsa_csv(csv_url)
    hmc      = scrape_hmc()
    hfa      = scrape_hfa()
    shechita = scrape_shechita()
    upsert(con, fsa_rows, hmc, hfa, shechita)
    con.close()
    log.info("Scrape complete.")

if __name__ == "__main__":
    run()
