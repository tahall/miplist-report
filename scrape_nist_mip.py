"""Scrape the NIST CMVP Modules In Process list."""

import argparse
import csv
import os
import re
import requests
import subprocess
from bs4 import BeautifulSoup
import sqlite3
import sys
from collections import Counter
from datetime import datetime


DB_FILE = "nist_modules_in_process.db"
NIST_URL = "https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list"


def parse_page(html, verbose=False):
    """Parse the NIST MIP page HTML and return (publish_date, not_displayed, rows)."""
    if verbose:
        print("  Parsing HTML...")
    soup = BeautifulSoup(html, "html.parser")

    publish_date = None
    not_displayed = 0
    page_text = soup.get_text()
    match = re.search(r"Last Updated:\s*(\d{1,2}/\d{1,2}/\d{4})", page_text)
    if match:
        publish_date = match.group(1)
        if verbose:
            print(f"  Found publish date: {publish_date}")

    table = soup.find("table")
    if not table:
        if verbose:
            print("  No table found in HTML.")
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

    if verbose:
        print(f"  Parsed {len(rows)} rows from table.")
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


def save_to_db(publish_date, rows, not_displayed=0, verbose=False):
    """Save scraped data to SQLite, replacing any existing data for the same publish date."""
    if verbose:
        print(f"  Saving {len(rows)} rows for publish date {publish_date} to {DB_FILE}...")
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS not_displayed (
            publish_date TEXT PRIMARY KEY,
            count INTEGER NOT NULL DEFAULT 0
        )
    """)
    cur.execute("DELETE FROM modules WHERE publish_date = ?", (publish_date,))
    for row in rows:
        if len(row) >= 4:
            cur.execute(
                "INSERT INTO modules (publish_date, module_name, vendor_name, standard, status) VALUES (?, ?, ?, ?, ?)",
                (publish_date, row[0], row[1], row[2], row[3]),
            )
    cur.execute(
        "INSERT OR REPLACE INTO not_displayed (publish_date, count) VALUES (?, ?)",
        (publish_date, not_displayed),
    )
    conn.commit()
    conn.close()


def get_existing_publish_dates(verbose=False):
    """Return a set of publish dates already in the database."""
    if verbose:
        print(f"  Querying existing publish dates from {DB_FILE}...")
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
    if verbose:
        print(f"  Found {len(dates)} existing publish dates: {sorted(dates)}")
    return dates


def parse_date_arg(date_str, label="date", verbose=False):
    """Parse M/YYYY or M/D/YYYY into a datetime."""
    parts = date_str.split("/")
    if len(parts) == 2:
        result = datetime(int(parts[1]), int(parts[0]), 1)
    elif len(parts) == 3:
        result = datetime(int(parts[2]), int(parts[0]), int(parts[1]))
    else:
        raise ValueError(f"Invalid date format: {date_str}. Use M/YYYY or M/D/YYYY.")
    if verbose:
        print(f"  Parsed {label} '{date_str}' as {result.strftime('%Y-%m-%d')}")
    return result


def fetch_wayback_snapshots(from_date, to_date=None, verbose=False):
    """Fetch Wayback Machine snapshot timestamps for the NIST URL from from_date to to_date (default: today)."""
    from_str = from_date.strftime("%Y%m%d")
    to_str = (to_date if to_date else datetime.now()).strftime("%Y%m%d")

    cdx_url = (
        f"https://web.archive.org/cdx/search/cdx"
        f"?url={NIST_URL}&output=json&from={from_str}&to={to_str}"
    )
    print(f"Querying Wayback Machine CDX API...")
    if verbose:
        print(f"  CDX URL: {cdx_url}")
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


def scrape_from_wayback(from_date_str, to_date_str=None, verbose=False):
    """Fetch and process archived versions of the NIST MIP page."""
    from_date = parse_date_arg(from_date_str, label="from-date", verbose=verbose)
    to_date = parse_date_arg(to_date_str, label="to-date", verbose=verbose) if to_date_str else None
    existing_dates = get_existing_publish_dates(verbose=verbose)
    print(f"Existing publish dates in DB: {len(existing_dates)}")

    snapshots = fetch_wayback_snapshots(from_date, to_date=to_date, verbose=verbose)
    if not snapshots:
        return

    seen_publish_dates = set(existing_dates)
    new_count = 0

    for i, timestamp in enumerate(snapshots):
        wayback_url = f"https://web.archive.org/web/{timestamp}/{NIST_URL}"

        if verbose:
            print(f"  [{i+1}/{len(snapshots)}] Fetching {wayback_url}")

        # Fetch the archived page
        try:
            response = requests.get(wayback_url, timeout=60)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"  [{i+1}/{len(snapshots)}] Failed to fetch snapshot {timestamp}: {e}")
            continue

        publish_date, not_displayed, rows = parse_page(response.text, verbose=verbose)

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
        save_to_db(publish_date, rows, not_displayed=not_displayed, verbose=verbose)
        print(f"  Saved to {DB_FILE}")
        print_changes(publish_date)

    print(f"\nDone. {new_count} new publish date(s) added.")


def scrape_modules_in_process(verbose=False):
    """Scrape the live NIST page."""
    if verbose:
        print(f"Fetching live page: {NIST_URL}")
    response = requests.get(NIST_URL, timeout=30)
    response.raise_for_status()
    if verbose:
        print(f"  Received {len(response.text)} bytes.")

    publish_date, not_displayed, rows = parse_page(response.text, verbose=verbose)

    if not rows:
        print("No table found on the page.", file=sys.stderr)
        sys.exit(1)

    print_summary(publish_date, not_displayed, rows)
    save_to_db(publish_date, rows, not_displayed=not_displayed, verbose=verbose)
    print(f"\nSaved to {DB_FILE}")
    print_changes(publish_date)
    return rows


def export_csv(output_file):
    """Export all DB data to a CSV file ordered by publish_date, module_name."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT publish_date, module_name, vendor_name, standard, status "
        "FROM modules ORDER BY publish_date, module_name"
    )
    rows = cur.fetchall()
    conn.close()

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["publish_date", "module_name", "vendor_name", "standard", "status"])
        writer.writerows(rows)

    print(f"Exported {len(rows)} rows to {output_file}")


def print_changes(publish_date):
    """Print what changed between publish_date and the immediately preceding publish date."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT publish_date FROM modules")
    all_dates = [row[0] for row in cur.fetchall()]

    parsed = sorted(all_dates, key=lambda d: datetime.strptime(d, "%m/%d/%Y"))
    if publish_date not in parsed:
        conn.close()
        return

    idx = parsed.index(publish_date)
    if idx == 0:
        print("First record — no comparison available.")
        conn.close()
        return

    prev_date = parsed[idx - 1]

    def fetch_rows(date):
        cur.execute(
            "SELECT module_name, vendor_name, standard, status FROM modules WHERE publish_date = ?",
            (date,),
        )
        return {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}

    old = fetch_rows(prev_date)
    new = fetch_rows(publish_date)
    conn.close()

    added = [k for k in new if k not in old]
    removed = [k for k in old if k not in new]
    changed = [k for k in new if k in old and new[k].split("(")[0].strip() != old[k].split("(")[0].strip()]

    print(f"\nChanges from {prev_date} to {publish_date}:")
    if not added and not removed and not changed:
        print("  No changes from previous publish date.")
        return
    for k in sorted(added):
        print(f"  ADDED:   {k[0]} / {k[1]} / {k[2]} — {new[k]}")
    for k in sorted(removed):
        print(f"  REMOVED: {k[0]} / {k[1]} / {k[2]} — {old[k]}")
    for k in sorted(changed):
        print(f"  STATUS:  {k[0]} / {k[1]} / {k[2]}: {old[k]} → {new[k]}")


def install_cron():
    """Install a daily 8 AM cron job to run this script."""
    script_path = os.path.abspath(__file__)
    workdir = os.path.dirname(script_path)
    log_file = os.path.join(workdir, "scrape_nist_mip.log")
    cron_line = f"0 8 * * * cd {workdir} && python3 {script_path} >> {log_file} 2>&1"

    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = result.stdout if result.returncode == 0 else ""

    if cron_line in existing.splitlines():
        print("Already scheduled.")
        return

    new_crontab = existing.rstrip("\n") + "\n" + cron_line + "\n"
    subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=True)
    print(f"Installed cron job:\n  {cron_line}")


def remove_cron():
    """Remove the cron job installed by install_cron()."""
    script_path = os.path.abspath(__file__)

    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        print("No cron job found.")
        return

    lines = result.stdout.splitlines(keepends=True)
    filtered = [l for l in lines if script_path not in l]

    if len(filtered) == len(lines):
        print("No cron job found.")
        return

    subprocess.run(["crontab", "-"], input="".join(filtered), text=True, check=True)
    print("Cron job removed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape the NIST CMVP Modules In Process list.")
    parser.add_argument("-from", dest="from_date", help="Fetch historical data from Wayback Machine starting at M/YYYY or M/D/YYYY")
    parser.add_argument("-to", dest="to_date", help="End date for Wayback Machine scraping at M/YYYY or M/D/YYYY (default: today)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print detailed progress information")
    parser.add_argument("--csv", nargs="?", const="nist_modules_in_process.csv", metavar="FILENAME",
                        help="Export all DB data to CSV (default: nist_modules_in_process.csv)")
    parser.add_argument("--schedule", action="store_true", help="Install a daily 8 AM cron job")
    parser.add_argument("--unschedule", action="store_true", help="Remove the cron job installed by --schedule")
    args = parser.parse_args()

    if args.csv:
        export_csv(args.csv)
        sys.exit(0)

    if args.schedule:
        install_cron()
        sys.exit(0)

    if args.unschedule:
        remove_cron()
        sys.exit(0)

    if args.to_date and not args.from_date:
        parser.error("-to requires -from")

    if args.from_date:
        scrape_from_wayback(args.from_date, to_date_str=args.to_date, verbose=args.verbose)
    else:
        scrape_modules_in_process(verbose=args.verbose)
