"""
app.py  – CheckMyMeat.co.uk
"""

import math, os, sqlite3
import urllib.request, json as _json
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)
DB_PATH = os.environ.get("DB_PATH", "slaughterhouses.db")

ESTABLISHMENT_LABELS = {
    "SLAUGHTERHOUSE": "Slaughterhouse",
    "GAME_HANDLER":   "Game Handling Establishment",
    "CUTTING_PLANT":  "Cutting Plant",
    "OTHER":          "Approved Establishment",
}


def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def lookup(code: str):
    clean = code.upper().replace(" ","").replace("-","")
    con   = get_db()
    row   = con.execute(
        "SELECT * FROM slaughterhouses WHERE REPLACE(UPPER(approval_number),'-','') = ?",
        (clean,)
    ).fetchone()
    con.close()
    return dict(row) if row else None


def _haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R    = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a    = (math.sin(dlat/2)**2
            + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
            * math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    con = get_db()
    counts = {}
    for row in con.execute(
        "SELECT slaughter_status, COUNT(*) n FROM slaughterhouses GROUP BY slaughter_status"
    ).fetchall():
        counts[row["slaughter_status"]] = row["n"]

    # Split abattoirs vs cutting plants
    by_type = {}
    for row in con.execute(
        "SELECT establishment_type, COUNT(*) n FROM slaughterhouses GROUP BY establishment_type"
    ).fetchall():
        by_type[row["establishment_type"]] = row["n"]

    last = con.execute("SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1").fetchone()
    con.close()

    abattoirs     = by_type.get("SLAUGHTERHOUSE", 0) + by_type.get("GAME_HANDLER", 0)
    cutting_plants = by_type.get("CUTTING_PLANT", 0)
    non_stun      = counts.get("NON_STUN", 0)

    return render_template("index.html",
        abattoirs=abattoirs,
        cutting_plants=cutting_plants,
        non_stun=non_stun,
        last_updated=(dict(last)["run_at"][:10] if last else "unknown"),
    )


@app.route("/why")
def why():
    return render_template("why.html")


@app.route("/data")
def data():
    con = get_db()
    non_stun = con.execute(
        "SELECT * FROM slaughterhouses WHERE slaughter_status='NON_STUN' ORDER BY name"
    ).fetchall()
    mixed = con.execute(
        "SELECT * FROM slaughterhouses WHERE slaughter_status='MIXED' ORDER BY name"
    ).fetchall()
    standard = con.execute(
        "SELECT * FROM slaughterhouses WHERE slaughter_status='STANDARD' ORDER BY name"
    ).fetchall()
    last = con.execute("SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1").fetchone()

    # Counts split by type for the stats bar
    by_type = {}
    for row in con.execute(
        "SELECT establishment_type, COUNT(*) n FROM slaughterhouses GROUP BY establishment_type"
    ).fetchall():
        by_type[row["establishment_type"]] = row["n"]

    con.close()

    abattoirs      = by_type.get("SLAUGHTERHOUSE", 0) + by_type.get("GAME_HANDLER", 0)
    cutting_plants = by_type.get("CUTTING_PLANT", 0)

    last_scrape = dict(last) if last else None

    return render_template("data.html",
        non_stun=[dict(r) for r in non_stun],
        mixed=[dict(r) for r in mixed],
        standard=[dict(r) for r in standard],
        last_scrape=last_scrape,
        abattoirs=abattoirs,
        cutting_plants=cutting_plants,
    )


@app.route("/check")
def check():
    code = request.args.get("code","").strip()
    if not code:
        return jsonify({"error": "No code provided"}), 400

    result = lookup(code)

    if not result:
        return jsonify({
            "found":   False,
            "code":    code,
            "message": (
                "No establishment found for this approval code. "
                "Check the number from your packaging. "
                "Scottish establishments are on a separate FSS register; "
                "Northern Ireland on a separate DAERA register."
            ),
            "links": [
                {"label": "Scotland FSS register",
                 "url": "https://www.foodstandards.gov.scot/publications-and-research/publications/approved-premises-register"},
                {"label": "Northern Ireland DAERA register",
                 "url": "https://data.food.gov.uk/catalog/datasets/dae35822-ca4e-41a2-b2af-b10b6163085a"},
            ]
        })

    status      = result["slaughter_status"]
    cert_bodies = result.get("certified_by") or ""
    est_type    = result.get("establishment_type","SLAUGHTERHOUSE")
    est_label   = ESTABLISHMENT_LABELS.get(est_type, "Approved Establishment")
    is_cutting  = est_type == "CUTTING_PLANT"

    if status == "NON_STUN":
        resp = {
            "badge":   "non-stun",
            "colour":  "red",
            "icon":    "●",
            "label":   "HMC Certified — Includes Non-Stun Slaughter" if not is_cutting else "HMC Certified — Non-Stun Supply Chain",
            "summary": (
                f"This {est_label.lower()} is certified by {cert_bodies}. "
                + ("HMC certifies non-stun halal slaughter. Note: some HMC-certified abattoirs "
                   "operate both stun and non-stun protocols depending on the order. "
                   "Verify directly with the establishment if certainty is required."
                   if not is_cutting else
                   "HMC certifies non-stun halal supply chains. Meat processed here originates "
                   "from HMC-certified abattoirs. Note: some certified sites operate both stun "
                   "and non-stun protocols — verify with the establishment if certainty is required.")
            ),
        }
    elif status == "STUN_RELIGIOUS":
        resp = {
            "badge":   "stun-religious",
            "colour":  "amber",
            "icon":    "◑",
            "label":   "Stunned Religious Slaughter" if not is_cutting else "Stunned Halal Certified",
            "summary": (
                f"This {est_label.lower()} is certified for religious slaughter ({cert_bodies}) "
                "using pre-stun methods. Animals are stunned before killing."
            ),
        }
    elif status == "MIXED":
        resp = {
            "badge":   "mixed",
            "colour":  "amber",
            "icon":    "◑",
            "label":   "Possible Religious Slaughter — Verify",
            "summary": (
                f"This {est_label.lower()}'s FSA approval record includes religious slaughter "
                "activities, but it does not appear on any major certification body's public list. "
                "Contact the establishment directly to confirm."
            ),
        }
    else:
        resp = {
            "badge":   "standard",
            "colour":  "green",
            "icon":    "○",
            "label":   "No Religious Certification Found",
            "summary": (
                f"No religious slaughter certification has been found for this {est_label.lower()} "
                "in publicly available data. Always verify via product labelling or by contacting "
                "the producer directly."
            ),
        }

    return jsonify({
        "found":              True,
        "code":               result["approval_number"],
        "name":               result["name"],
        "establishment_type": est_label,
        "address":            ", ".join(filter(None, [
                                  result.get("address_line1"),
                                  result.get("address_line2"),
                                  result.get("town"),
                                  result.get("county"),
                                  result.get("postcode"),
                              ])),
        "country":            result.get("country",""),
        "status":             status,
        "certified_by":       cert_bodies,
        "last_updated":       (result.get("last_updated") or "")[:10],
        **resp,
    })


@app.route("/stats")
def stats():
    con = get_db()
    totals = con.execute(
        "SELECT slaughter_status, COUNT(*) n FROM slaughterhouses GROUP BY slaughter_status"
    ).fetchall()
    try:
        by_type = con.execute(
            "SELECT establishment_type, COUNT(*) n FROM slaughterhouses GROUP BY establishment_type"
        ).fetchall()
    except Exception:
        by_type = []
    last = con.execute("SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1").fetchone()
    con.close()
    return jsonify({
        "counts":      [dict(r) for r in totals],
        "by_type":     [dict(r) for r in by_type],
        "last_scrape": dict(last) if last else None,
    })


# ── Restaurants / Outlets ─────────────────────────────────────────────────────

@app.route("/restaurants")
def restaurants():
    con = get_db()
    counts = {}
    for row in con.execute(
        "SELECT outlet_type, COUNT(*) n FROM hmc_outlets GROUP BY outlet_type"
    ).fetchall():
        counts[row["outlet_type"]] = row["n"]

    total    = sum(counts.values())
    geocoded = con.execute(
        "SELECT COUNT(*) n FROM hmc_outlets WHERE latitude IS NOT NULL"
    ).fetchone()["n"]
    last = con.execute(
        "SELECT last_updated FROM hmc_outlets ORDER BY id DESC LIMIT 1"
    ).fetchone()
    last_updated = (last["last_updated"] or "")[:10] if last else "unknown"
    con.close()

    return render_template("restaurants.html",
        total=total,
        geocoded=geocoded,
        counts=counts,
        last_updated=last_updated,
    )


# ── Schools ───────────────────────────────────────────────────────────────────

@app.route("/schools")
def schools():
    con = get_db()
    total = con.execute("SELECT COUNT(*) n FROM hmc_schools").fetchone()["n"]
    geocoded = con.execute(
        "SELECT COUNT(*) n FROM hmc_schools WHERE latitude IS NOT NULL"
    ).fetchone()["n"]
    last = con.execute(
        "SELECT last_updated FROM hmc_schools ORDER BY id DESC LIMIT 1"
    ).fetchone()
    last_updated = (last["last_updated"] or "")[:10] if last else "unknown"
    con.close()

    return render_template("schools.html",
        total=total,
        geocoded=geocoded,
        last_updated=last_updated,
    )


@app.route("/schools/nearby")
def schools_nearby():
    postcode = request.args.get("postcode","").strip().upper()
    radius   = min(float(request.args.get("radius", 5)), 50)

    if not postcode:
        return jsonify({"error": "postcode required"}), 400

    try:
        pc_clean = postcode.replace(" ","").upper()
        with urllib.request.urlopen(f"https://api.postcodes.io/postcodes/{pc_clean}", timeout=5) as resp:
            data = _json.loads(resp.read())
        if data.get("status") != 200:
            return jsonify({"error": "Postcode not found"}), 404
        centre_lat = data["result"]["latitude"]
        centre_lon = data["result"]["longitude"]
    except Exception as e:
        return jsonify({"error": f"Could not geocode postcode: {e}"}), 400

    lat_delta = radius / 69.0
    lon_delta = radius / (69.0 * math.cos(math.radians(centre_lat)))
    con       = get_db()
    rows      = con.execute("""
        SELECT * FROM hmc_schools
        WHERE latitude BETWEEN ? AND ?
          AND longitude BETWEEN ? AND ?
          AND latitude IS NOT NULL
    """, [centre_lat - lat_delta, centre_lat + lat_delta,
          centre_lon - lon_delta, centre_lon + lon_delta]).fetchall()
    con.close()

    results = []
    for r in rows:
        dist = _haversine_miles(centre_lat, centre_lon, r["latitude"], r["longitude"])
        if dist <= radius:
            d = dict(r)
            d["distance_miles"] = round(dist, 1)
            results.append(d)
    results.sort(key=lambda x: x["distance_miles"])
    return jsonify({"postcode": postcode, "radius": radius, "results": results, "total": len(results)})


@app.route("/schools/search")
def schools_search():
    q      = request.args.get("q","").strip()
    limit  = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))

    conds, params = [], []
    if q:
        conds.append("(name LIKE ? OR address LIKE ? OR town LIKE ? OR postcode LIKE ?)")
        params.extend([f"%{q}%"] * 4)

    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    con   = get_db()
    rows  = con.execute(
        f"SELECT * FROM hmc_schools {where} ORDER BY name LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    total = con.execute(f"SELECT COUNT(*) n FROM hmc_schools {where}", params).fetchone()["n"]
    con.close()
    return jsonify({"results": [dict(r) for r in rows], "total": total, "offset": offset, "limit": limit})


# ── Outlets API (restaurants nearby/search — unchanged) ───────────────────────

@app.route("/restaurants/nearby")
def restaurants_nearby():
    postcode = request.args.get("postcode","").strip().upper()
    radius   = min(float(request.args.get("radius", 5)), 50)
    type_f   = request.args.get("type","").strip().upper()

    if not postcode:
        return jsonify({"error": "postcode required"}), 400

    try:
        pc_clean = postcode.replace(" ","").upper()
        with urllib.request.urlopen(f"https://api.postcodes.io/postcodes/{pc_clean}", timeout=5) as resp:
            data = _json.loads(resp.read())
        if data.get("status") != 200:
            return jsonify({"error": "Postcode not found"}), 404
        centre_lat = data["result"]["latitude"]
        centre_lon = data["result"]["longitude"]
    except Exception as e:
        return jsonify({"error": f"Could not geocode postcode: {e}"}), 400

    lat_delta = radius / 69.0
    lon_delta = radius / (69.0 * math.cos(math.radians(centre_lat)))
    con       = get_db()
    conds     = ["latitude BETWEEN ? AND ?", "longitude BETWEEN ? AND ?", "latitude IS NOT NULL"]
    params    = [centre_lat - lat_delta, centre_lat + lat_delta,
                 centre_lon - lon_delta, centre_lon + lon_delta]
    if type_f in ("RESTAURANT","BUTCHER_SHOP","DESSERT","OTHER"):
        conds.append("outlet_type = ?")
        params.append(type_f)

    rows    = con.execute(f"SELECT * FROM hmc_outlets WHERE {' AND '.join(conds)}", params).fetchall()
    con.close()
    results = []
    for r in rows:
        dist = _haversine_miles(centre_lat, centre_lon, r["latitude"], r["longitude"])
        if dist <= radius:
            d = dict(r)
            d["distance_miles"] = round(dist, 1)
            results.append(d)
    results.sort(key=lambda x: x["distance_miles"])
    return jsonify({"postcode": postcode, "radius": radius, "results": results, "total": len(results)})


@app.route("/restaurants/search")
def restaurants_search():
    q      = request.args.get("q","").strip()
    type_f = request.args.get("type","").strip().upper()
    limit  = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))

    conds, params = [], []
    if q:
        conds.append("(name LIKE ? OR address LIKE ? OR town LIKE ? OR postcode LIKE ?)")
        params.extend([f"%{q}%"] * 4)
    if type_f in ("RESTAURANT","BUTCHER_SHOP","DESSERT","OTHER"):
        conds.append("outlet_type = ?")
        params.append(type_f)

    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    con   = get_db()
    rows  = con.execute(
        f"SELECT * FROM hmc_outlets {where} ORDER BY name LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    total = con.execute(f"SELECT COUNT(*) n FROM hmc_outlets {where}", params).fetchone()["n"]
    con.close()
    return jsonify({"results": [dict(r) for r in rows], "total": total, "offset": offset, "limit": limit})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
