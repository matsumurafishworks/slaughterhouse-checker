"""
Run this script once from your project root to add the Restaurants nav link
to all three templates.

Usage:
    python add_restaurants_nav.py
"""
import re, sys
from pathlib import Path

TEMPLATES = [
    Path("templates/index.html"),
    Path("templates/why.html"),
    Path("templates/data.html"),
]

# The new link to insert after the first nav-link (Check)
NEW_LINK = '<a href="/restaurants" class="nav-link">Restaurants</a>'

patched = 0
for tmpl in TEMPLATES:
    if not tmpl.exists():
        print(f"SKIP (not found): {tmpl}")
        continue
    html = tmpl.read_text(encoding="utf-8")
    if "/restaurants" in html:
        print(f"ALREADY DONE: {tmpl}")
        continue
    # Insert after the first <a href="/" ... nav-link ... line
    updated = re.sub(
        r'(<a href="/"[^>]*class="nav-link[^"]*"[^>]*>Check</a>)',
        rf'\1\n      {NEW_LINK}',
        html,
        count=1,
    )
    if updated == html:
        # Fallback: insert after the first nav-link element found
        updated = re.sub(
            r'(<div class="nav-links">)',
            rf'\1\n      {NEW_LINK}',
            html,
            count=1,
        )
    tmpl.write_text(updated, encoding="utf-8")
    print(f"PATCHED: {tmpl}")
    patched += 1

print(f"\nDone. {patched} file(s) updated.")
