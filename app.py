"""
app.py  –  UK Abattoir Religious Slaughter Checker
"""

import sqlite3
import os
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

DB_PATH = os.environ.get(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "slaughterhouses.db")
)


def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def lookup(code: str):
    """Return a abattoir row or None."""
    clean = code.upper().replace(" ", "").replace("-", "")
    con = get_db()
    row = con.execute(
        "SELECT * FROM slaughterhouses "
        "WHERE REPLACE(UPPER(approval_number),'-','') = ?",
        (clean,)
    ).fetchone()
    con.close()
    return dict(row) if row else None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/why")
def why():
    return render_template("why.html")


@app.route("/check")
def check():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify({"error": "No code provided"}), 400

    result = lookup(code)

    if not result:
        return jsonify({
            "found": False,
            "code": code,
            "message": (
                "No abattoir found for this approval code. "
                "Check the number from your packaging. "
                "Scottish establishments are on a separate FSS register; "
                "Northern Ireland on a separate DAERA register."
            ),
            "links": [
                {
                    "label": "Scotland FSS register",
                    "url": "https://www.foodstandards.gov.scot/publications-and-research/publications/approved-premises-register"
                },
                {
                    "label": "Northern Ireland DAERA register",
                    "url": "https://data.food.gov.uk/catalog/datasets/dae35822-ca4e-41a2-b2af-b10b6163085a"
                }
            ]
        })

    status      = result["slaughter_status"]      # NON_STUN | STUN_RELIGIOUS | MIXED | STANDARD
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
    # Note: STUN_RELIGIOUS currently returns 0 results as HFA does not publish
    # a public abattoir list. This status is reserved for future data sources.
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
    else:  # STANDARD
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


@app.route("/data")
def data():
    con = get_db()
    non_stun = con.execute(
        "SELECT approval_number, name, address_line1, town, postcode, country, certified_by "
        "FROM slaughterhouses WHERE slaughter_status='NON_STUN' ORDER BY name"
    ).fetchall()
    mixed = con.execute(
        "SELECT approval_number, name, address_line1, town, postcode, country, activities_raw "
        "FROM slaughterhouses WHERE slaughter_status='MIXED' ORDER BY name"
    ).fetchall()
    standard = con.execute(
        "SELECT approval_number, name, address_line1, town, postcode, country "
        "FROM slaughterhouses WHERE slaughter_status='STANDARD' ORDER BY name"
    ).fetchall()
    last = con.execute(
        "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    con.close()
    return render_template("data.html",
        non_stun=[dict(r) for r in non_stun],
        mixed=[dict(r) for r in mixed],
        standard=[dict(r) for r in standard],
        last_scrape=dict(last) if last else {}
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
