#!/usr/bin/env python3
import csv
import os
import sys
from datetime import datetime
from urllib.parse import unquote

import requests

try:
    from zoneinfo import ZoneInfo  # py3.9+
except ImportError:
    ZoneInfo = None


BARCHART_PAGE = "https://www.barchart.com/futures/quotes/SB*0/futures-prices"
BARCHART_API = "https://www.barchart.com/proxies/core-api/v1/quotes/get"
OUTFILE = "sugar-futures.csv"

FIELDS = [
    "symbol",
    "lastPrice",
    "priceChange",
    "openPrice",
    "highPrice",
    "lowPrice",
    "previousPrice",
    "volume",
    "openInterest",
    "tradeTime",
]


def berlin_now():
    if ZoneInfo is None:
        raise RuntimeError("Python <3.9: zoneinfo nicht verfügbar.")
    return datetime.now(tz=ZoneInfo("Europe/Berlin"))


def should_run_at_10_berlin(now_berlin: datetime) -> bool:
    # Wir lassen den Workflow 08:00 & 09:00 UTC laufen und schreiben nur,
    # wenn es in Berlin genau 10 Uhr ist (DST-sicher).
    return now_berlin.hour == 10


def fetch_rows() -> list[dict]:
    s = requests.Session()

    # 1) Seite einmal aufrufen, um XSRF-Cookie zu bekommen
    get_headers = {
        "user-agent": "Mozilla/5.0",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
    }
    r = s.get(BARCHART_PAGE, headers=get_headers, timeout=30)
    r.raise_for_status()

    xsrf = s.cookies.get("XSRF-TOKEN")
    if not xsrf:
        raise RuntimeError("Kein XSRF-TOKEN Cookie erhalten (Barchart Block/Änderung?).")

    # Laut Praxis muss der Token oft URL-decoded werden (teils doppelt). :contentReference[oaicite:1]{index=1}
    xsrf_decoded = unquote(unquote(xsrf))

    # 2) Interne API abfragen
    api_headers = {
        "user-agent": "Mozilla/5.0",
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "referer": BARCHART_PAGE,
        "x-xsrf-token": xsrf_decoded,
    }

    params = {
        "fields": ",".join(FIELDS),
        "list": "futures.contractInRoot",
        "root": "SB",
        "raw": "1",
    }

    j = s.get(BARCHART_API, params=params, headers=api_headers, timeout=30)
    j.raise_for_status()
    data = j.json()

    results = data.get("results") or []
    if not results:
        raise RuntimeError(f"Keine results erhalten. Antwort keys: {list(data.keys())}")

    # Die Seite zeigt „Nearby“; i.d.R. ist die API bereits passend sortiert.
    # Wir nehmen die ersten 6.
    return results[:6]


def ensure_header(path: str, header: list[str]) -> None:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)


def append_rows(path: str, date_str: str, rows: list[dict]) -> int:
    header = ["Date"] + [
        "Contract",
        "Latest",
        "Change",
        "Open",
        "High",
        "Low",
        "Previous",
        "Volume",
        "Open Int",
        "Time",
    ]
    ensure_header(path, header)

    def norm(v):
        if v is None:
            return ""
        return str(v)

    written = 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        for item in rows:
            # "symbol" entspricht i.d.R. dem Contract-Code (z.B. SBH26).
            # Falls Barchart stattdessen "contractSymbol" liefert, nimm das als Fallback.
            contract = item.get("symbol") or item.get("contractSymbol") or ""

            line = [
                date_str,
                contract,
                norm(item.get("lastPrice")),
                norm(item.get("priceChange")),
                norm(item.get("openPrice")),
                norm(item.get("highPrice")),
                norm(item.get("lowPrice")),
                norm(item.get("previousPrice")),
                norm(item.get("volume")),
                norm(item.get("openInterest")),
                norm(item.get("tradeTime")),
            ]
            w.writerow(line)
            written += 1

    return written


def main() -> int:
    now = berlin_now()
    print(f"Berlin datetime: {now.isoformat()}")

    if not should_run_at_10_berlin(now):
        print("Not 10:00 in Berlin – exiting without writing.")
        return 0

    date_str = now.date().isoformat()
    rows = fetch_rows()
    n = append_rows(OUTFILE, date_str, rows)
    print(f"Appended {n} rows to {OUTFILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
