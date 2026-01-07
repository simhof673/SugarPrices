#!/usr/bin/env python3
import csv
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


URL = "https://www.barchart.com/futures/quotes/SB*0/futures-prices"
OUTFILE = "sugar-futures.csv"

# Expected columns (as on the website)
BASE_HEADER = [
    "Contract", "Latest", "Change", "Open", "High", "Low",
    "Previous", "Volume", "Open Int", "Time"
]


def is_10_am_berlin_now() -> bool:
    now_berlin = datetime.now(ZoneInfo("Europe/Berlin"))
    return now_berlin.hour == 10 and now_berlin.minute == 0


def already_written_today(today_iso: str) -> bool:
    if not os.path.exists(OUTFILE):
        return False
    try:
        with open(OUTFILE, "r", newline="", encoding="utf-8") as f:
            # quick scan: if any row starts with today's date, we assume done
            for line in f:
                if line.startswith(today_iso + ",") or line.startswith(today_iso + ";"):
                    return True
    except Exception:
        return False
    return False


def fetch_html() -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; sugar-futures-bot/1.0; +https://github.com/)"
    }
    r = requests.get(URL, headers=headers, timeout=60)
    r.raise_for_status()
    return r.text


def parse_first_6_rows(html: str):
    soup = BeautifulSoup(html, "lxml")

    # Heuristic: pick the first table that contains the header "Contract" and "Latest"
    tables = soup.find_all("table")
    target_table = None
    for t in tables:
        th_text = " ".join(th.get_text(" ", strip=True) for th in t.find_all("th"))
        if "Contract" in th_text and "Latest" in th_text:
            target_table = t
            break

    if target_table is None:
        raise RuntimeError(
            "Konnte die Tabelle nicht finden. "
            "Hinweis: Falls die Seite die Daten rein per JavaScript lädt, "
            "muss stattdessen ein API-Endpoint genutzt werden."
        )

    tbody = target_table.find("tbody") or target_table
    rows = tbody.find_all("tr")

    data_rows = []
    for tr in rows:
        tds = tr.find_all(["td", "th"])
        # Skip empty / header-like rows
        if not tds:
            continue

        # Extract pure text (no links)
        cells = [td.get_text(" ", strip=True) for td in tds]

        # Some tables may include extra columns; keep the first 10 matching our schema
        if len(cells) >= 10:
            cells = cells[:10]
            # Basic sanity: first column should look like "SBH26 (Mar '26)"
            if cells[0] and (cells[0].startswith("SB") or "SB" in cells[0]):
                data_rows.append(cells)

        if len(data_rows) == 6:
            break

    if len(data_rows) < 6:
        raise RuntimeError(f"Es wurden nur {len(data_rows)} Datenzeilen gefunden (erwartet: 6).")

    return data_rows


def append_to_csv(rows, today_iso: str):
    file_exists = os.path.exists(OUTFILE)

    header = ["Date"] + BASE_HEADER

    # Use comma CSV; if you prefer semicolon for DE locales, say so.
    with open(OUTFILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        if not file_exists:
            w.writerow(header)

        for r in rows:
            w.writerow([today_iso] + r)


def main():
    # Time gating to guarantee 10:00 Europe/Berlin despite UTC-only cron
    if not is_10_am_berlin_now():
        print("Not 10:00 in Europe/Berlin — skipping.")
        return 0

    today_iso = datetime.now(ZoneInfo("Europe/Berlin")).date().isoformat()

    # Idempotency: don't append twice on the same day
    if already_written_today(today_iso):
        print(f"Rows for {today_iso} already exist — skipping.")
        return 0

    html = fetch_html()
    rows = parse_first_6_rows(html)
    append_to_csv(rows, today_iso)

    print(f"Appended 6 rows for {today_iso} to {OUTFILE}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
