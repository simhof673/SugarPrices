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

BASE_HEADER = [
    "Contract", "Latest", "Change", "Open", "High", "Low",
    "Previous", "Volume", "Open Int", "Time"
]

def today_berlin_iso() -> str:
    return datetime.now(ZoneInfo("Europe/Berlin")).date().isoformat()

def already_written_today(today_iso: str) -> bool:
    if not os.path.exists(OUTFILE):
        return False
    with open(OUTFILE, "r", newline="", encoding="utf-8") as f:
        for line in f:
            if line.startswith(today_iso + ",") or line.startswith(today_iso + ";"):
                return True
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
            "Falls die Seite Daten per JavaScript lädt, brauchen wir den API/XHR-Endpoint."
        )

    tbody = target_table.find("tbody") or target_table
    rows = tbody.find_all("tr")

    data_rows = []
    for tr in rows:
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue

        cells = [td.get_text(" ", strip=True) for td in tds]

        if len(cells) >= 10:
            cells = cells[:10]
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

    with open(OUTFILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(header)
        for r in rows:
            w.writerow([today_iso] + r)

def main():
    today_iso = today_berlin_iso()
    print(f"Berlin date: {today_iso}")

    if already_written_today(today_iso):
        print(f"Already written for {today_iso} — skipping.")
        return 0

    html = fetch_html()
    rows = parse_first_6_rows(html)
    append_to_csv(rows, today_iso)

    print(f"Appended 6 rows for {today_iso} to {OUTFILE}.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
