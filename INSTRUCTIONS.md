# Restaurants Feature — Integration Instructions

## Files in this package

| File | What to do with it |
|------|-------------------|
| `restaurants.html` | Add as `templates/restaurants.html` |
| `scraper_additions.py` | Merge into existing `scraper.py` |
| `app_additions.py` | Paste into `app.py` before the `if __name__` line |

---

## Changes to scraper.py

### 1. In `init_db()`, add to the executescript string:
```sql
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
```

### 2. Add these imports at the top (math already imported in app.py, add to scraper too):
```python
import math
```

### 3. Paste in the three new functions from scraper_additions.py:
- `scrape_hmc_outlets()`
- `geocode_outlets()`
- `upsert_outlets()`

### 4. In `run()`, add at the end (before `con.close()`):
```python
outlets = scrape_hmc_outlets()
outlets = geocode_outlets(outlets)
upsert_outlets(con, outlets)
```

---

## Changes to app.py

### 1. Add `import math` at the top (if not already there)

### 2. Paste the three routes from `app_additions.py`:
- `_haversine_miles()` helper function
- `/restaurants` route
- `/restaurants/nearby` route  
- `/restaurants/search` route

---

## Changes to nav (index.html, why.html, data.html)

Add this link to the nav-links div in each template:
```html
<a href="/restaurants" class="nav-link">Restaurants</a>
```

---

## How the search works

**Postcode radius search (`/restaurants/nearby`)**:
1. User enters postcode + radius (1/2/5/10/20/50 miles)
2. Flask geocodes the postcode via postcodes.io (free, no key needed)
3. Bounding box pre-filter in SQLite for speed
4. Exact Haversine distance calculated in Python
5. Results sorted by distance, labelled with "X.X mi"

**Text search (`/restaurants/search`)**:
- Fallback for "search by name/town" — paginated, no geocoding

**Geocoding at scrape time**:
- `geocode_outlets()` calls postcodes.io bulk API in batches of 100
- Takes ~5 seconds for 1100 outlets (fast)
- Stored in `latitude`/`longitude` columns — geocoded once, used forever

---

## Expected data volume
~1,100 HMC outlets across the UK.
Scrape takes ~3 minutes (1.5s/page × ~130 pages).
Geocoding takes ~5 seconds.
Runs monthly with the rest of the scraper.
