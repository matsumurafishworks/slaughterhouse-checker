"""
ADDITIONS TO scraper.py — HMC Outlets with geocoding

1. Add HMC_OUTLETS_TABLE_SQL to init_db() executescript
2. Add scrape_hmc_outlets(), geocode_outlets(), upsert_outlets()
3. In run(), add at the end:
       outlets = scrape_hmc_outlets()
       outlets = geocode_outlets(outlets)
       upsert_outlets(con, outlets)
"""

import math

# ─── STEP 1: Add inside init_db() executescript ──────────────────────────────

HMC_OUTLETS_TABLE_SQL = """
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
"""
# Paste into the executescript("...") inside init_db()


# ─── STEP 2a: Scraper ─────────────────────────────────────────────────────────

def scrape_hmc_outlets() -> list:
    """
    Scrape HMC certified outlets (restaurants, takeaways, butchers, shops).
    HMC certifies the full non-stun chain from abattoir to plate.

    Site: halalhmc.org/outlets/ — paginated WordPress.
    Each article card: heading = name, paragraph = address + phone.
    """
    log.info("Fetching HMC outlets...")
    outlets    = []
    seen_names = set()

    postcode_re = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b', re.IGNORECASE)
    phone_re    = re.compile(r'Tel[:\s]+([\d\s\+\(\)]+)', re.IGNORECASE)

    page = 1
    while page <= 300:  # safety cap; ~1100 outlets at ~8-12/page ≈ 100-140 pages
        url = "https://halalhmc.org/outlets/" if page == 1 \
              else f"https://halalhmc.org/outlets/page/{page}/"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 404:
                log.info(f"HMC outlets: end of pages at page {page}")
                break
            r.raise_for_status()
        except Exception as e:
            log.warning(f"HMC outlets page {page} failed: {e}")
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # WordPress <article> tags contain each outlet
        articles = soup.find_all("article")
        if not articles:
            log.info(f"HMC outlets page {page}: no articles found, stopping")
            break

        found_this_page = 0
        for article in articles:
            heading = article.find(["h1", "h2", "h3", "h4"])
            name    = heading.get_text(strip=True) if heading else ""
            if not name or name in seen_names:
                continue
            seen_names.add(name)

            full_text = article.get_text(" ", strip=True)

            # Phone
            phone_m = phone_re.search(full_text)
            phone   = phone_m.group(1).strip() if phone_m else ""

            # Address: strip name + phone + boilerplate
            addr = full_text
            for strip_part in [name, f"Tel: {phone}", f"Tel:{phone}", phone,
                                "View Certificate", "View Cert", "Certificate"]:
                if strip_part:
                    addr = addr.replace(strip_part, "")
            addr = " ".join(addr.split()).strip().strip(",").strip()

            # Postcode
            pc_m     = postcode_re.search(addr)
            postcode = pc_m.group(1).upper().strip() if pc_m else ""
            if postcode and " " not in postcode and len(postcode) > 3:
                postcode = postcode[:-3].strip() + " " + postcode[-3:]

            # Town: last comma-separated segment before the postcode
            town = ""
            if postcode and postcode in addr:
                pre = addr[:addr.find(postcode)].strip().rstrip(",")
                parts = [p.strip() for p in pre.split(",") if p.strip()]
                town  = parts[-1] if parts else ""

            # Outlet type from name keywords
            nl = name.lower()
            if any(w in nl for w in ["restaurant", "kitchen", "diner", "café", "cafe",
                                      "grill", "tandoori", "tandoor", "biryani", "curry",
                                      "dining", "eatery", "lounge", "grill house"]):
                outlet_type = "RESTAURANT"
            elif any(w in nl for w in ["takeaway", "take away", "take-away", "kebab",
                                        "pizza", "burger", "chicken", "chippy",
                                        "fish & chip", "fish and chip", "wraps"]):
                outlet_type = "TAKEAWAY"
            elif any(w in nl for w in ["butcher", "meat", "halal shop", "grocery",
                                        "supermarket", "cash & carry", "cash and carry",
                                        "food store", "deli", "butchery"]):
                outlet_type = "BUTCHER_SHOP"
            else:
                outlet_type = "OTHER"

            link       = article.find("a", href=True)
            source_url = link["href"] if link else ""

            outlets.append({
                "name":        name,
                "address":     addr,
                "town":        town,
                "postcode":    postcode,
                "phone":       phone,
                "outlet_type": outlet_type,
                "source_url":  source_url,
                "latitude":    None,
                "longitude":   None,
            })
            found_this_page += 1

        log.info(f"HMC outlets page {page}: {found_this_page}, running total {len(outlets)}")
        page += 1
        time.sleep(1.5)

    log.info(f"HMC outlets scrape complete: {len(outlets)} total")
    return outlets


# ─── STEP 2b: Geocoder ────────────────────────────────────────────────────────

def geocode_outlets(outlets: list) -> list:
    """
    Geocode postcode for each outlet using postcodes.io bulk API.
    Free, no API key, rate limit ~100 req/s — we batch 100 at a time.
    Sets latitude/longitude on each outlet dict in-place.
    """
    log.info("Geocoding outlet postcodes via postcodes.io...")

    # Collect unique non-empty postcodes
    to_geocode = list({
        o["postcode"] for o in outlets if o["postcode"]
    })
    log.info(f"Unique postcodes to geocode: {len(to_geocode)}")

    pc_coords: dict[str, tuple] = {}  # postcode -> (lat, lng)

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
            data = resp.json()
            for item in data.get("result", []):
                query  = item.get("query", "")
                result = item.get("result")
                if result:
                    pc_coords[query.upper().replace(" ", "")] = (
                        result["latitude"],
                        result["longitude"],
                    )
        except Exception as e:
            log.warning(f"Geocoding batch {i//BATCH + 1} failed: {e}")
        time.sleep(0.3)

    # Apply coords back to outlets
    matched = 0
    for o in outlets:
        key = o["postcode"].upper().replace(" ", "") if o["postcode"] else ""
        if key in pc_coords:
            o["latitude"], o["longitude"] = pc_coords[key]
            matched += 1

    log.info(f"Geocoding complete: {matched}/{len(outlets)} outlets have coordinates")
    return outlets


# ─── STEP 2c: DB upsert ───────────────────────────────────────────────────────

def upsert_outlets(con: sqlite3.Connection, outlets: list):
    """Clear and reload hmc_outlets table."""
    now = datetime.utcnow().isoformat()
    con.execute("DELETE FROM hmc_outlets")
    for o in outlets:
        con.execute("""
            INSERT INTO hmc_outlets
              (name, address, town, postcode, phone, outlet_type,
               source_url, latitude, longitude, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            o["name"], o["address"], o["town"], o["postcode"],
            o["phone"], o["outlet_type"], o["source_url"],
            o["latitude"], o["longitude"], now,
        ))
    con.commit()
    geocoded = sum(1 for o in outlets if o["latitude"])
    log.info(f"hmc_outlets: {len(outlets)} records, {geocoded} geocoded.")


# ─── STEP 3: Add to run() ────────────────────────────────────────────────────
# outlets = scrape_hmc_outlets()
# outlets = geocode_outlets(outlets)
# upsert_outlets(con, outlets)
