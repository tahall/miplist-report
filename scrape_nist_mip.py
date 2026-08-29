"""Scrape the NIST CMVP Modules In Process list."""

import argparse
import csv
import os
import re
import requests
import subprocess
import time
from bs4 import BeautifulSoup
import sqlite3
import sys
from collections import Counter
from datetime import datetime


DB_FILE = "nist_modules_in_process.db"
NIST_URL = "https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list"


def normalize_vendor(raw):
    """Normalize vendor name for key construction, collapsing pipe-separator spacing variants.

    NIST's site sometimes renders 'Codan | DTC' as 'Codan DTC' (dropping the pipe) for
    certain statuses. Normalizing ensures both forms map to the same key, matching
    normalize_vendor() in generate_report.py.
    """
    return re.sub(r'\s*\|\s*', ' ', raw).strip()


# Status columns used by the pre-November-2020 MIP layout, which had no Status
# column: a module's status was encoded by a highlighted cell in one of these.
OLD_STATUS_COLUMNS = ("Review Pending", "In Review", "Coordination", "Finalization")

# The pre-November-2020 layout had no Standard column, and every module from that
# era predates FIPS 140-3 submissions.
DEFAULT_OLD_STANDARD = "FIPS 140-2"


def _is_marked(td):
    """True if a cell carries the marker class used by the pre-2020 layout.

    The class name changed from 'highlight' to 'mip-highlight' around August 2020,
    so match on the substring rather than an exact name.
    """
    return any("highlight" in c for c in (td.get("class") or []))


def _parse_footer(table, status_cols=None):
    """Return (not_displayed, {status: displayed_count}) from the table footer.

    Handles the current layout ('Not Displayed' | 28) and the pre-2020 layout
    ('Not Displayed: 10' followed by one count per status column).
    """
    tfoot = table.find("tfoot")
    if not tfoot:
        return 0, {}

    not_displayed = 0
    displayed = {}
    for tr in tfoot.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cells:
            continue
        label = cells[0]
        if label.startswith("Not Displayed"):
            m = re.search(r"(\d+)", label)
            if m:
                not_displayed = int(m.group(1))
            elif len(cells) >= 2 and cells[-1].isdigit():
                not_displayed = int(cells[-1])
        elif label.startswith("Displayed") and status_cols:
            counts = [c for c in cells[1:] if c.isdigit()]
            if len(counts) == len(status_cols):
                displayed = {s: int(c) for s, c in zip(status_cols, counts)}
    return not_displayed, displayed


def _parse_old_rows(table, headers):
    """Return (rows, unmarked) for the pre-2020 layout.

    Rows are normalised to the canonical [module, vendor, standard, status] shape so
    the rest of the pipeline sees no difference between layouts.
    """
    status_idx = {h: i for i, h in enumerate(headers) if h in OLD_STATUS_COLUMNS}
    std_idx = headers.index("Standard") if "Standard" in headers else None

    rows, unmarked = [], 0
    tbody = table.find("tbody")
    if not tbody:
        return rows, unmarked

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        status = next(
            (h for h, i in status_idx.items() if i < len(tds) and _is_marked(tds[i])),
            None,
        )
        if status is None:
            unmarked += 1
            continue
        standard = (
            tds[std_idx].get_text(strip=True)
            if std_idx is not None and std_idx < len(tds)
            else DEFAULT_OLD_STANDARD
        )
        rows.append([
            tds[0].get_text(strip=True),
            tds[1].get_text(strip=True),
            standard or DEFAULT_OLD_STANDARD,
            status,
        ])
    return rows, unmarked


def parse_page(html, verbose=False):
    """Parse the NIST MIP page HTML and return (publish_date, not_displayed, rows, valid).

    Handles the current layout (a dedicated Status column) and the pre-November-2020
    layout (one column per status, the module's status marked by a highlighted cell).

    `valid` is False only when a pre-2020 page's per-status row counts disagree with
    the totals printed in its own table footer, which means the page should not be
    trusted and the caller should skip it.
    """
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
        return publish_date, not_displayed, [], True

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    status_cols = [h for h in headers if h in OLD_STATUS_COLUMNS]

    if status_cols:
        # Pre-November-2020 layout.
        not_displayed, footer_counts = _parse_footer(table, status_cols)
        rows, unmarked = _parse_old_rows(table, headers)

        expected = {s: n for s, n in footer_counts.items() if n}
        actual = dict(Counter(r[3] for r in rows))
        valid = (not expected) or (actual == expected)
        if verbose or not valid:
            print(f"  Legacy layout: {len(rows)} rows, {unmarked} unmarked, "
                  f"not_displayed={not_displayed}")
            if not valid:
                print(f"  Footer mismatch: parsed {actual} vs footer {expected}")
            elif expected:
                print(f"  Footer cross-check OK: {actual}")
        return publish_date, not_displayed, rows, valid

    # Current layout.
    tfoot = table.find("tfoot")
    if tfoot:
        for tr in tfoot.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) >= 2 and cells[0] == "Not Displayed":
                not_displayed = int(cells[-1])

    rows = []
    tbody = table.find("tbody")
    if not tbody:
        return publish_date, not_displayed, rows, True

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
    return publish_date, not_displayed, rows, True


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


def save_to_db(publish_date, rows, not_displayed=0, verbose=False, dry_run=False):
    """Save scraped data to SQLite, replacing any existing data for the same publish date."""
    valid_rows = [row for row in rows if len(row) >= 4]
    if dry_run:
        print(f"[DRY RUN] Would save {len(valid_rows)} module rows for {publish_date}"
              + (f" + not_displayed={not_displayed}" if not_displayed else ""))
        return
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
            count INTEGER NOT NULL DEFAULT 0,
            total_count INTEGER NOT NULL DEFAULT 0
        )
    """)
    try:
        cur.execute("ALTER TABLE not_displayed ADD COLUMN total_count INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # column already exists
    cur.execute("DELETE FROM modules WHERE publish_date = ?", (publish_date,))
    for row in valid_rows:
        cur.execute(
            "INSERT INTO modules (publish_date, module_name, vendor_name, standard, status) VALUES (?, ?, ?, ?, ?)",
            (publish_date, row[0], row[1], row[2], row[3]),
        )
    total_count = len(valid_rows) + not_displayed
    cur.execute(
        "INSERT OR REPLACE INTO not_displayed (publish_date, count, total_count) VALUES (?, ?, ?)",
        (publish_date, not_displayed, total_count),
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


def scrape_from_wayback(from_date_str, to_date_str=None, verbose=False, dry_run=False, delay=2.0):
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

        if i > 0:
            time.sleep(delay)

        if verbose:
            print(f"  [{i+1}/{len(snapshots)}] Fetching {wayback_url}")

        # Fetch the archived page
        try:
            response = requests.get(wayback_url, timeout=60)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"  [{i+1}/{len(snapshots)}] Failed to fetch snapshot {timestamp}: {e}")
            continue

        publish_date, not_displayed, rows, valid = parse_page(response.text, verbose=verbose)

        if not publish_date:
            print(f"  [{i+1}/{len(snapshots)}] Snapshot {timestamp}: no publish date found, skipping.")
            continue

        if not valid:
            print(f"  [{i+1}/{len(snapshots)}] Snapshot {timestamp}: parsed counts disagree "
                  f"with table footer, skipping.")
            continue

        if publish_date in seen_publish_dates:
            print(f"  [{i+1}/{len(snapshots)}] Snapshot {timestamp}: publish date {publish_date} already seen, skipping.")
            continue

        seen_publish_dates.add(publish_date)
        new_count += 1

        print(f"\n  [{i+1}/{len(snapshots)}] Snapshot {timestamp}: NEW publish date {publish_date}")
        print_summary(publish_date, not_displayed, rows)
        save_to_db(publish_date, rows, not_displayed=not_displayed, verbose=verbose, dry_run=dry_run)
        if not dry_run:
            print(f"  Saved to {DB_FILE}")
            print_changes(publish_date)

    print(f"\nDone. {new_count} new publish date(s) added.")


def scrape_modules_in_process(verbose=False, dry_run=False):
    """Scrape the live NIST page."""
    if verbose:
        print(f"Fetching live page: {NIST_URL}")
    response = requests.get(NIST_URL, timeout=30)
    response.raise_for_status()
    if verbose:
        print(f"  Received {len(response.text)} bytes.")

    publish_date, not_displayed, rows, valid = parse_page(response.text, verbose=verbose)

    if not valid:
        print("Parsed counts disagree with the table footer; refusing to save.", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("No table found on the page.", file=sys.stderr)
        sys.exit(1)

    print_summary(publish_date, not_displayed, rows)
    save_to_db(publish_date, rows, not_displayed=not_displayed, verbose=verbose, dry_run=dry_run)
    if not dry_run:
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


def export_csv_history(output_file):
    """Export per-module history to CSV: one row per module per publish date, sorted by module then date."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT module_name, vendor_name, standard, publish_date, status "
        "FROM modules"
    )
    rows = cur.fetchall()
    conn.close()

    rows.sort(key=lambda r: (r[0], r[1], r[2], datetime.strptime(r[3], "%m/%d/%Y")))

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["module_name", "vendor_name", "standard", "publish_date", "status"])
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
        return {(r[0], normalize_vendor(r[1]), r[2]): r[3] for r in cur.fetchall()}

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
    terminal_statuses = {"Finalization"}
    for k in sorted(removed):
        last_status = old[k].split("(")[0].strip()
        prefix = "  REMOVED:" if last_status in terminal_statuses else "  [ALERT] REMOVED:"
        print(f"{prefix} {k[0]} / {k[1]} / {k[2]} — {old[k]}")
    for k in sorted(changed):
        print(f"  STATUS:  {k[0]} / {k[1]} / {k[2]}: {old[k]} → {new[k]}")


def install_cron():
    """Install a daily 4 AM Eastern cron job to run this script."""
    script_path = os.path.abspath(__file__)
    workdir = os.path.dirname(script_path)
    log_file = os.path.join(workdir, "scrape_nist_mip.log")
    cron_line = f"TZ=America/New_York 0 4 * * * cd {workdir} && python3 {script_path} >> {log_file} 2>&1"

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
    parser.add_argument("--backfill", action="store_true", help="Fill gaps in DB via Wayback Machine, starting from the earliest date already in the DB")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print detailed progress information")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Show what would be saved without writing to the database")
    parser.add_argument("--delay", type=float, default=2.0, metavar="SECONDS", help="Seconds to wait between Wayback Machine requests (default: 2.0)")
    parser.add_argument("--csv", nargs="?", const="nist_modules_in_process.csv", metavar="FILENAME",
                        help="Export all DB data to CSV (default: nist_modules_in_process.csv)")
    parser.add_argument("--csv-history", dest="csv_history", nargs="?", const="nist_module_history.csv", metavar="FILENAME",
                        help="Export per-module history CSV (default: nist_module_history.csv)")
    parser.add_argument("--schedule", action="store_true", help="Install a daily 4 AM Eastern cron job")
    parser.add_argument("--unschedule", action="store_true", help="Remove the cron job installed by --schedule")
    args = parser.parse_args()

    if args.csv:
        export_csv(args.csv)
        sys.exit(0)

    if args.csv_history:
        export_csv_history(args.csv_history)
        sys.exit(0)

    if args.schedule:
        install_cron()
        sys.exit(0)

    if args.unschedule:
        remove_cron()
        sys.exit(0)

    if args.backfill and args.from_date:
        parser.error("--backfill and -from are mutually exclusive")

    if args.to_date and not args.from_date and not args.backfill:
        parser.error("-to requires -from")

    if args.backfill:
        existing = get_existing_publish_dates(verbose=args.verbose)
        from_date = (
            min(existing, key=lambda d: datetime.strptime(d, "%m/%d/%Y"))
            if existing else "1/1/2023"
        )
        print(f"Backfilling from {from_date}...")
        scrape_from_wayback(from_date, verbose=args.verbose, dry_run=args.dry_run, delay=args.delay)
    elif args.from_date:
        scrape_from_wayback(args.from_date, to_date_str=args.to_date, verbose=args.verbose, dry_run=args.dry_run, delay=args.delay)
    else:
        scrape_modules_in_process(verbose=args.verbose, dry_run=args.dry_run)
