"""
scraper.py  –  UK Slaughterhouse Data Scraper
==============================================
Populates slaughterhouses.db with:
  - FSA approved establishment data (England & Wales, monthly CSV)
  - Cross-referenced against public certification body lists

Slaughter status classification:
  NON_STUN       – certified by HMC (non-stun halal) or a Shechita/Kosher body
  STUN_RELIGIOUS – certified by HFA or similar (stunned halal)
  MIXED          – FSA flags religious activity but no cert body found
  STANDARD       – no evidence of religious slaughter

Run manually:   python scraper.py
Monthly cron:   0 3 2 * * /path/to/venv/bin/python /path/to/scraper.py
"""

import requests
import csv
import sqlite3
import io
import re
import time
import logging
import os
from datetime import datetime
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "slaughterhouses.db")

FSA_CATALOG = (
    "https://data.food.gov.uk/catalog/datasets/"
    "1e61736a-2a1a-4c6a-b8b1-e45912ebc8e3"
)

# Activity strings in the FSA CSV that mean this row is a slaughterhouse
SLAUGHTER_KEYWORDS = {
    "slaughterhouse", "slaughter", "red meat slaughterhouse",
    "white meat slaughterhouse", "poultry slaughterhouse",
    "game handling establishment", "farmed game slaughterhouse",
    "wild game handling establishment",
}

# FSA activity strings hinting at religious / non-stun operations
RELIGIOUS_KEYWORDS = ["religious", "non-stun", "watok", "non stun"]

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SlaughterhouseChecker/1.0)"}


# ── FSA ───────────────────────────────────────────────────────────────────────

def get_latest_fsa_csv_url() -> str:
    log.info("Fetching FSA catalog page…")
    r = requests.get(FSA_CATALOG, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "fsaopendata.blob.core.windows.net" in href and href.endswith(".csv"):
            log.info(f"Latest CSV: {href}")
            return href
    raise RuntimeError("Could not find FSA CSV URL on catalog page.")


def download_fsa_csv(url: str) -> list[dict]:
    log.info("Downloading FSA CSV…")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    content = r.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(content))

    rows = []
    for row in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in row.items()}

        activities = (
            row.get("activities", "")
            + " " + row.get("activity", "")
            + " " + row.get("establishment type", "")
            + " " + row.get("type of establishment", "")
        ).lower()

        if not any(kw in activities for kw in SLAUGHTER_KEYWORDS):
            continue

        approval = (
            row.get("approval number")
            or row.get("establishment number")
            or row.get("fsa number")
            or row.get("number")
        ).strip().upper()

        if not approval:
            continue

        rows.append({
            "approval_number":      approval,
            "name":                 row.get("establishment name") or row.get("name", ""),
            "address_line1":        row.get("address line 1") or row.get("addressline1", ""),
            "address_line2":        row.get("address line 2") or row.get("addressline2", ""),
            "town":                 row.get("city") or row.get("town", ""),
            "county":               row.get("county", ""),
            "postcode":             row.get("post code") or row.get("postcode", ""),
            "country":              row.get("country", ""),
            "activities_raw":       activities.strip(),
            "fsa_religious_flag":   any(kw in activities for kw in RELIGIOUS_KEYWORDS),
        })

    log.info(f"FSA: {len(rows)} slaughterhouse rows found.")
    return rows


# ── Certification body scrapers ───────────────────────────────────────────────

def scrape_hmc() -> set[str]:
    """
    HMC (Halal Monitoring Committee) – certifies NON-STUN halal only.
    Scrapes their public abattoir list for FSA approval numbers.
    """
    log.info("Scraping HMC…")
    numbers: set[str] = set()
    fsa_re = re.compile(r"\b([A-Z]{0,2}\d{3,4})\b")
    urls = [
        "https://halalmc.co.uk/consumer/product-search/",
        "https://halalmc.co.uk/abattoirs/",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            matches = fsa_re.findall(BeautifulSoup(r.text, "html.parser").get_text())
            numbers |= set(matches)
            time.sleep(1)
        except Exception as e:
            log.warning(f"HMC scrape failed for {url}: {e}")
    log.info(f"HMC: {len(numbers)} numbers found.")
    return numbers


def scrape_hfa() -> set[str]:
    """
    HFA (Halal Food Authority) – certifies STUNNED halal.
    """
    log.info("Scraping HFA…")
    numbers: set[str] = set()
    fsa_re = re.compile(r"\b([A-Z]{0,2}\d{3,4})\b")
    try:
        r = requests.get(
            "https://www.halalfoodauthority.com/find-halal-certification",
            headers=HEADERS, timeout=30
        )
        r.raise_for_status()
        matches = fsa_re.findall(BeautifulSoup(r.text, "html.parser").get_text())
        numbers = set(matches)
    except Exception as e:
        log.warning(f"HFA scrape failed: {e}")
    log.info(f"HFA: {len(numbers)} numbers found.")
    return numbers


def scrape_shechita_uk() -> set[str]:
    """
    Shechita UK – represents Jewish (Kosher/Shechita) slaughter.
    Shechita is always NON-STUN.
    """
    log.info("Scraping Shechita UK…")
    numbers: set[str] = set()
    fsa_re = re.compile(r"\b([A-Z]{0,2}\d{3,4})\b")
    urls = [
        "https://www.shechitauk.org/faqs/approved-abattoirs/",
        "https://www.shechitauk.org/abattoirs/",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            matches = fsa_re.findall(BeautifulSoup(r.text, "html.parser").get_text())
            numbers |= set(matches)
            time.sleep(1)
        except Exception as e:
            log.warning(f"Shechita UK scrape failed for {url}: {e}")
    log.info(f"Shechita UK: {len(numbers)} numbers found.")
    return numbers


# ── Database ──────────────────────────────────────────────────────────────────

def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
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
            slaughter_status  TEXT NOT NULL DEFAULT 'STANDARD',
            -- STANDARD | NON_STUN | STUN_RELIGIOUS | MIXED
            certified_by      TEXT,
            -- e.g. 'HMC' | 'HFA' | 'SHECHITA' | 'HMC,SHECHITA'
            last_updated      TEXT
        );

        CREATE TABLE IF NOT EXISTS scrape_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          TEXT,
            fsa_total       INTEGER,
            non_stun        INTEGER,
            stun_religious  INTEGER,
            mixed           INTEGER,
            standard        INTEGER
        );
    """)
    con.commit()
    return con


def classify(row: dict, hmc: set, hfa: set, shechita: set) -> tuple[str, str]:
    n = row["approval_number"]
    in_hmc      = n in hmc
    in_hfa      = n in hfa
    in_shechita = n in shechita

    if in_hmc or in_shechita:
        bodies = ",".join(filter(None, [
            "HMC"      if in_hmc      else "",
            "SHECHITA" if in_shechita else "",
        ]))
        return "NON_STUN", bodies

    if in_hfa:
        return "STUN_RELIGIOUS", "HFA"

    if row["fsa_religious_flag"]:
        return "MIXED", ""

    return "STANDARD", ""


def upsert(con: sqlite3.Connection, rows: list[dict],
           hmc: set, hfa: set, shechita: set):
    now = datetime.utcnow().isoformat()
    counts = {"NON_STUN": 0, "STUN_RELIGIOUS": 0, "MIXED": 0, "STANDARD": 0}

    for row in rows:
        status, bodies = classify(row, hmc, hfa, shechita)
        counts[status] += 1
        con.execute("""
            INSERT INTO slaughterhouses
              (approval_number, name, address_line1, address_line2, town, county,
               postcode, country, activities_raw, slaughter_status, certified_by, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(approval_number) DO UPDATE SET
              name=excluded.name, address_line1=excluded.address_line1,
              address_line2=excluded.address_line2, town=excluded.town,
              county=excluded.county, postcode=excluded.postcode,
              country=excluded.country, activities_raw=excluded.activities_raw,
              slaughter_status=excluded.slaughter_status,
              certified_by=excluded.certified_by, last_updated=excluded.last_updated
        """, (
            row["approval_number"], row["name"], row["address_line1"],
            row["address_line2"], row["town"], row["county"], row["postcode"],
            row["country"], row["activities_raw"], status, bodies, now,
        ))

    con.execute("""
        INSERT INTO scrape_log (run_at, fsa_total, non_stun, stun_religious, mixed, standard)
        VALUES (?,?,?,?,?,?)
    """, (now, len(rows), counts["NON_STUN"], counts["STUN_RELIGIOUS"],
          counts["MIXED"], counts["STANDARD"]))
    con.commit()

    log.info(
        f"DB updated — "
        f"NON_STUN={counts['NON_STUN']}  "
        f"STUN_RELIGIOUS={counts['STUN_RELIGIOUS']}  "
        f"MIXED={counts['MIXED']}  "
        f"STANDARD={counts['STANDARD']}"
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def run():
    con = init_db()
    csv_url = get_latest_fsa_csv_url()
    fsa_rows = download_fsa_csv(csv_url)
    hmc      = scrape_hmc()
    hfa      = scrape_hfa()
    shechita = scrape_shechita_uk()
    upsert(con, fsa_rows, hmc, hfa, shechita)
    con.close()
    log.info("Scrape complete.")


if __name__ == "__main__":
    run()
