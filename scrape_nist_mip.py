"""Scrape the NIST CMVP Modules In Process list."""

import re
import requests
from bs4 import BeautifulSoup
import sqlite3
import sys
from collections import Counter


DB_FILE = "nist_modules_in_process.db"


def scrape_modules_in_process():
    url = "https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract "Last Updated" date as publish date
    publish_date = None
    not_displayed = 0
    page_text = soup.get_text()
    match = re.search(r"Last Updated:\s*(\d{1,2}/\d{1,2}/\d{4})", page_text)
    if match:
        publish_date = match.group(1)
    table = soup.find("table")

    if not table:
        print("No table found on the page.", file=sys.stderr)
        sys.exit(1)

    tfoot = table.find("tfoot")
    if tfoot:
        for tr in tfoot.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) >= 2 and cells[0] == "Not Displayed":
                not_displayed = int(cells[-1])

    # Extract rows — remove "View Contacts" link text from vendor column
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        cells = []
        for td in tr.find_all("td"):
            text = td.get_text(strip=True)
            text = text.replace("View Contacts", "").strip()
            cells.append(text)
        if cells:
            rows.append(cells)

    # Print summary: count of modules per status
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

    # Save to SQLite database
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

    print(f"\nSaved to {DB_FILE}")
    return rows


if __name__ == "__main__":
    scrape_modules_in_process()
