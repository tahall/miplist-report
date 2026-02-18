"""Scrape the NIST CMVP Modules In Process list."""

import argparse
import re
import requests
from bs4 import BeautifulSoup
import sqlite3
import sys
from collections import Counter
from datetime import datetime


DB_FILE = "nist_modules_in_process.db"
NIST_URL = "https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list"


def parse_page(html):
    """Parse the NIST MIP page HTML and return (publish_date, not_displayed, rows)."""
    soup = BeautifulSoup(html, "html.parser")

    publish_date = None
    not_displayed = 0
    page_text = soup.get_text()
    match = re.search(r"Last Updated:\s*(\d{1,2}/\d{1,2}/\d{4})", page_text)
    if match:
        publish_date = match.group(1)

    table = soup.find("table")
    if not table:
        return publish_date, not_displayed, []

    tfoot = table.find("tfoot")
    if tfoot:
        for tr in tfoot.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) >= 2 and cells[0] == "Not Displayed":
                not_displayed = int(cells[-1])

    rows = []
    tbody = table.find("tbody")
    if not tbody:
        return publish_date, not_displayed, rows

    for tr in tbody.find_all("tr"):
        cells = []
        for td in tr.find_all("td"):
            text = td.get_text(strip=True)
            text = text.replace("View Contacts", "").strip()
            cells.append(text)
        if cells:
            rows.append(cells)

    return publish_date, not_displayed, rows


def print_summary(publish_date, not_displayed, rows):
    """Print a summary of the scraped data."""
    status_counts = Counter()
    for row in rows:
        if len(row) >= 4:
            status = row[3].split("(")[0].strip()
            status_counts[status] += 1

    total = len(rows) + not_displayed
    if not_displayed:
        status_counts["Not Displayed"] = not_displayed

    print(f"Publish Date: {publish_date}\n")
    print(f"Total: {total} modules\n")
    for status, count in status_counts.most_common():
        print(f"  {status:<30} {count}")


def save_to_db(publish_date, rows):
    """Save scraped data to SQLite, replacing any existing data for the same publish date."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publish_date TEXT,
            module_name TEXT,
            vendor_name TEXT,
            standard TEXT,
            status TEXT
        )
    """)
    cur.execute("DELETE FROM modules WHERE publish_date = ?", (publish_date,))
    for row in rows:
        if len(row) >= 4:
            cur.execute(
                "INSERT INTO modules (publish_date, module_name, vendor_name, standard, status) VALUES (?, ?, ?, ?, ?)",
                (publish_date, row[0], row[1], row[2], row[3]),
            )
    conn.commit()
    conn.close()


def get_existing_publish_dates():
    """Return a set of publish dates already in the database."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publish_date TEXT,
            module_name TEXT,
            vendor_name TEXT,
            standard TEXT,
            status TEXT
        )
    """)
    cur.execute("SELECT DISTINCT publish_date FROM modules")
    dates = {row[0] for row in cur.fetchall()}
    conn.close()
    return dates


def parse_from_date(date_str):
    """Parse M/YYYY or M/D/YYYY into a datetime."""
    parts = date_str.split("/")
    if len(parts) == 2:
        return datetime(int(parts[1]), int(parts[0]), 1)
    elif len(parts) == 3:
        return datetime(int(parts[2]), int(parts[0]), int(parts[1]))
    else:
        raise ValueError(f"Invalid date format: {date_str}. Use M/YYYY or M/D/YYYY.")


def fetch_wayback_snapshots(from_date):
    """Fetch Wayback Machine snapshot timestamps for the NIST URL from from_date to today."""
    from_str = from_date.strftime("%Y%m%d")
    to_str = datetime.now().strftime("%Y%m%d")

    cdx_url = (
        f"https://web.archive.org/cdx/search/cdx"
        f"?url={NIST_URL}&output=json&from={from_str}&to={to_str}"
    )
    print(f"Querying Wayback Machine CDX API...")
    try:
        response = requests.get(cdx_url, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to query CDX API: {e}", file=sys.stderr)
        return []

    data = response.json()
    if len(data) <= 1:
        print("No Wayback Machine snapshots found for the given date range.")
        return []

    # First row is the header
    header = data[0]
    timestamp_idx = header.index("timestamp")
    snapshots = [row[timestamp_idx] for row in data[1:]]
    print(f"Found {len(snapshots)} snapshots.")
    return snapshots


def scrape_from_wayback(from_date_str):
    """Fetch and process archived versions of the NIST MIP page."""
    from_date = parse_from_date(from_date_str)
    existing_dates = get_existing_publish_dates()
    print(f"Existing publish dates in DB: {len(existing_dates)}")

    snapshots = fetch_wayback_snapshots(from_date)
    if not snapshots:
        return

    seen_publish_dates = set(existing_dates)
    new_count = 0

    for i, timestamp in enumerate(snapshots):
        wayback_url = f"https://web.archive.org/web/{timestamp}/{NIST_URL}"

        # Fetch the archived page
        try:
            response = requests.get(wayback_url, timeout=60)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"  [{i+1}/{len(snapshots)}] Failed to fetch snapshot {timestamp}: {e}")
            continue

        publish_date, not_displayed, rows = parse_page(response.text)

        if not publish_date:
            print(f"  [{i+1}/{len(snapshots)}] Snapshot {timestamp}: no publish date found, skipping.")
            continue

        if publish_date in seen_publish_dates:
            print(f"  [{i+1}/{len(snapshots)}] Snapshot {timestamp}: publish date {publish_date} already seen, skipping.")
            continue

        seen_publish_dates.add(publish_date)
        new_count += 1

        print(f"\n  [{i+1}/{len(snapshots)}] Snapshot {timestamp}: NEW publish date {publish_date}")
        print_summary(publish_date, not_displayed, rows)
        save_to_db(publish_date, rows)
        print(f"  Saved to {DB_FILE}")

    print(f"\nDone. {new_count} new publish date(s) added.")


def scrape_modules_in_process():
    """Scrape the live NIST page."""
    response = requests.get(NIST_URL, timeout=30)
    response.raise_for_status()

    publish_date, not_displayed, rows = parse_page(response.text)

    if not rows:
        print("No table found on the page.", file=sys.stderr)
        sys.exit(1)

    print_summary(publish_date, not_displayed, rows)
    save_to_db(publish_date, rows)
    print(f"\nSaved to {DB_FILE}")
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape the NIST CMVP Modules In Process list.")
    parser.add_argument("-from", dest="from_date", help="Fetch historical data from Wayback Machine starting at M/YYYY or M/D/YYYY")
    args = parser.parse_args()

    if args.from_date:
        scrape_from_wayback(args.from_date)
    else:
        scrape_modules_in_process()
