"""
ADD THESE THREE ROUTES TO app.py
(paste before the `if __name__ == "__main__":` line)
"""

import math


def _haversine_miles(lat1, lon1, lat2, lon2) -> float:
    """Distance between two lat/lng points in miles."""
    R = 3958.8  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


@app.route("/restaurants")
def restaurants():
    con = get_db()
    counts = con.execute(
        "SELECT outlet_type, COUNT(*) n FROM hmc_outlets GROUP BY outlet_type"
    ).fetchall()
    counts_dict  = {r["outlet_type"]: r["n"] for r in counts}
    total        = sum(counts_dict.values())
    geocoded     = con.execute(
        "SELECT COUNT(*) n FROM hmc_outlets WHERE latitude IS NOT NULL"
    ).fetchone()["n"]
    last = con.execute(
        "SELECT last_updated FROM hmc_outlets ORDER BY id DESC LIMIT 1"
    ).fetchone()
    last_updated = (last["last_updated"] or "")[:10] if last else "unknown"
    con.close()
    return render_template(
        "restaurants.html",
        total=total,
        geocoded=geocoded,
        counts=counts_dict,
        last_updated=last_updated,
    )


@app.route("/restaurants/nearby")
def restaurants_nearby():
    """
    Postcode-based radius search.
    Query params: postcode, radius (miles, default 5), type, limit
    Returns JSON list sorted by distance.
    """
    postcode = request.args.get("postcode", "").strip().upper()
    radius   = float(request.args.get("radius", 5))
    radius   = min(radius, 50)  # cap at 50 miles
    type_f   = request.args.get("type", "").strip().upper()

    if not postcode:
        return jsonify({"error": "postcode required"}), 400

    # Geocode the search postcode via postcodes.io
    try:
        import urllib.request, json as _json
        pc_clean = postcode.replace(" ", "").upper()
        url      = f"https://api.postcodes.io/postcodes/{pc_clean}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data    = _json.loads(resp.read())
        if data.get("status") != 200:
            return jsonify({"error": "Postcode not found"}), 404
        centre_lat = data["result"]["latitude"]
        centre_lon = data["result"]["longitude"]
    except Exception as e:
        return jsonify({"error": f"Could not geocode postcode: {e}"}), 400

    # Bounding box (approx) to pre-filter in SQL — 1 degree lat ≈ 69 miles
    lat_delta = radius / 69.0
    lon_delta = radius / (69.0 * math.cos(math.radians(centre_lat)))

    con  = get_db()
    conds = [
        "latitude  BETWEEN ? AND ?",
        "longitude BETWEEN ? AND ?",
        "latitude IS NOT NULL",
    ]
    params = [
        centre_lat - lat_delta, centre_lat + lat_delta,
        centre_lon - lon_delta, centre_lon + lon_delta,
    ]
    if type_f in ("RESTAURANT", "TAKEAWAY", "BUTCHER_SHOP", "OTHER"):
        conds.append("outlet_type = ?")
        params.append(type_f)

    rows = con.execute(
        f"SELECT * FROM hmc_outlets WHERE {' AND '.join(conds)}",
        params
    ).fetchall()
    con.close()

    # Exact Haversine filter + distance annotation
    results = []
    for r in rows:
        dist = _haversine_miles(centre_lat, centre_lon,
                                r["latitude"], r["longitude"])
        if dist <= radius:
            d = dict(r)
            d["distance_miles"] = round(dist, 1)
            results.append(d)

    results.sort(key=lambda x: x["distance_miles"])
    return jsonify({
        "postcode":  postcode,
        "centre":    {"lat": centre_lat, "lon": centre_lon},
        "radius":    radius,
        "results":   results,
        "total":     len(results),
    })


@app.route("/restaurants/search")
def restaurants_search():
    """Free-text search fallback (no postcode)."""
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
