"""
app.py  – CheckMyMeat.co.uk Flask app
"""

import math
import os
import sqlite3
from flask import Flask, render_template, request, jsonify
import urllib.request
import json as _json

app = Flask(__name__)
DB_PATH = os.environ.get("DB_PATH", "slaughterhouses.db")


def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def lookup(code: str):
    clean = code.upper().replace(" ", "").replace("-", "")
    con   = get_db()
    row   = con.execute(
        "SELECT * FROM slaughterhouses "
        "WHERE REPLACE(UPPER(approval_number),'-','') = ?",
        (clean,)
    ).fetchone()
    con.close()
    return dict(row) if row else None


def _haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R    = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a    = (math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# ── Abattoir routes ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    con = get_db()
    counts = {}
    for row in con.execute(
        "SELECT slaughter_status, COUNT(*) n FROM slaughterhouses GROUP BY slaughter_status"
    ).fetchall():
        counts[row["slaughter_status"]] = row["n"]
    last = con.execute(
        "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    con.close()
    total    = sum(counts.values())
    non_stun = counts.get("NON_STUN", 0)
    return render_template("index.html",
        total=total,
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
    last = con.execute(
        "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    con.close()
    return render_template("data.html",
        non_stun=[dict(r) for r in non_stun],
        mixed=[dict(r) for r in mixed],
        standard=[dict(r) for r in standard],
        last_scrape=dict(last) if last else None,
    )


@app.route("/check")
def check():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify({"error": "No code provided"}), 400

    result = lookup(code)

    if not result:
        return jsonify({
            "found":   False,
            "code":    code,
            "message": (
                "No abattoir found for this approval code. "
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

    if status == "NON_STUN":
        resp = {
            "badge":   "non-stun",
            "colour":  "red",
            "icon":    "●",
            "label":   "Non-Stun Religious Slaughter",
            "summary": (
                "This establishment is certified for non-stun religious slaughter "
                f"({cert_bodies}). Animals are not rendered unconscious before killing. "
                "This covers both Halal (non-stun dhabiha) and Kosher (Shechita) methods."
            ),
        }
    elif status == "STUN_RELIGIOUS":
        resp = {
            "badge":   "stun-religious",
            "colour":  "amber",
            "icon":    "◑",
            "label":   "Stunned Religious Slaughter",
            "summary": (
                f"This establishment is certified for religious slaughter ({cert_bodies}) "
                "using pre-stun methods. Animals are stunned before killing. "
                "Approximately 88% of UK halal production uses this method."
            ),
        }
    elif status == "MIXED":
        resp = {
            "badge":   "mixed",
            "colour":  "amber",
            "icon":    "◑",
            "label":   "Possible Religious Slaughter — Verify",
            "summary": (
                "This establishment's FSA approval record includes religious slaughter activities, "
                "but it does not appear on any major certification body's public list. "
                "It may carry out some religious slaughter runs alongside standard slaughter. "
                "Contact the establishment directly to confirm."
            ),
        }
    else:
        resp = {
            "badge":   "standard",
            "colour":  "green",
            "icon":    "○",
            "label":   "Standard Slaughter — No Religious Certification Found",
            "summary": (
                "No religious slaughter certification has been found for this establishment "
                "in publicly available data. Based on the FSA register and major certification "
                "body lists, this site appears to operate standard pre-stun slaughter only. "
                "Always verify via product labelling or by contacting the producer directly."
            ),
        }

    updated = (result.get("last_updated") or "")[:10]
    return jsonify({
        "found":        True,
        "code":         result["approval_number"],
        "name":         result["name"],
        "address":      ", ".join(filter(None, [
                            result.get("address_line1"),
                            result.get("address_line2"),
                            result.get("town"),
                            result.get("county"),
                            result.get("postcode"),
                        ])),
        "country":      result.get("country", ""),
        "status":       status,
        "certified_by": cert_bodies,
        "last_updated": updated,
        **resp,
    })


@app.route("/stats")
def stats():
    con = get_db()
    totals = con.execute(
        "SELECT slaughter_status, COUNT(*) n FROM slaughterhouses GROUP BY slaughter_status"
    ).fetchall()
    last = con.execute(
        "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    con.close()
    return jsonify({
        "counts":      [dict(r) for r in totals],
        "last_scrape": dict(last) if last else None,
    })


# ── Restaurant / outlets routes ───────────────────────────────────────────────

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


@app.route("/restaurants/nearby")
def restaurants_nearby():
    """Postcode radius search using postcodes.io for geocoding."""
    postcode = request.args.get("postcode", "").strip().upper()
    radius   = float(request.args.get("radius", 5))
    radius   = min(radius, 50)
    type_f   = request.args.get("type", "").strip().upper()

    if not postcode:
        return jsonify({"error": "postcode required"}), 400

    try:
        pc_clean = postcode.replace(" ", "").upper()
        url_geo  = f"https://api.postcodes.io/postcodes/{pc_clean}"
        with urllib.request.urlopen(url_geo, timeout=5) as resp:
            data = _json.loads(resp.read())
        if data.get("status") != 200:
            return jsonify({"error": "Postcode not found"}), 404
        centre_lat = data["result"]["latitude"]
        centre_lon = data["result"]["longitude"]
    except Exception as e:
        return jsonify({"error": f"Could not geocode postcode: {e}"}), 400

    lat_delta = radius / 69.0
    lon_delta = radius / (69.0 * math.cos(math.radians(centre_lat)))

    con    = get_db()
    conds  = ["latitude BETWEEN ? AND ?", "longitude BETWEEN ? AND ?", "latitude IS NOT NULL"]
    params = [centre_lat - lat_delta, centre_lat + lat_delta,
              centre_lon - lon_delta, centre_lon + lon_delta]

    if type_f in ("RESTAURANT", "TAKEAWAY", "BUTCHER_SHOP", "OTHER"):
        conds.append("outlet_type = ?")
        params.append(type_f)

    rows = con.execute(
        f"SELECT * FROM hmc_outlets WHERE {' AND '.join(conds)}", params
    ).fetchall()
    con.close()

    results = []
    for r in rows:
        dist = _haversine_miles(centre_lat, centre_lon, r["latitude"], r["longitude"])
        if dist <= radius:
            d = dict(r)
            d["distance_miles"] = round(dist, 1)
            results.append(d)

    results.sort(key=lambda x: x["distance_miles"])
    return jsonify({
        "postcode": postcode,
        "centre":   {"lat": centre_lat, "lon": centre_lon},
        "radius":   radius,
        "results":  results,
        "total":    len(results),
    })


@app.route("/restaurants/search")
def restaurants_search():
    """Free-text search fallback."""
    q      = request.args.get("q", "").strip()
    type_f = request.args.get("type", "").strip().upper()
    limit  = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))

    conds, params = [], []
    if q:
        conds.append("(name LIKE ? OR address LIKE ? OR town LIKE ? OR postcode LIKE ?)")
        params.extend([f"%{q}%"] * 4)
    if type_f in ("RESTAURANT", "TAKEAWAY", "BUTCHER_SHOP", "OTHER"):
        conds.append("outlet_type = ?")
        params.append(type_f)

    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    con   = get_db()
    rows  = con.execute(
        f"SELECT * FROM hmc_outlets {where} ORDER BY name LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    total = con.execute(
        f"SELECT COUNT(*) n FROM hmc_outlets {where}", params
    ).fetchone()["n"]
    con.close()

    return jsonify({
        "results": [dict(r) for r in rows],
        "total":   total,
        "offset":  offset,
        "limit":   limit,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
