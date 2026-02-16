"""Scrape the NIST CMVP Modules In Process list."""

import requests
from bs4 import BeautifulSoup
import csv
import sys


def scrape_modules_in_process():
    url = "https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        print("No table found on the page.", file=sys.stderr)
        sys.exit(1)

    # Extract headers
    headers = [th.get_text(strip=True) for th in table.find_all("th")]

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
    from collections import Counter
    status_counts = Counter()
    for row in rows:
        if len(row) >= 4:
            # Strip the date portion, e.g. "Review Pending  (11/18/2025)" -> "Review Pending"
            status = row[3].split("(")[0].strip()
            status_counts[status] += 1

    from datetime import date
    print(f"Date: {date.today()}\n")
    print(f"Found {len(rows)} modules\n")
    for status, count in status_counts.most_common():
        print(f"  {status:<30} {count}")

    # Save to CSV
    output_file = "nist_modules_in_process.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers if headers else ["Module Name", "Vendor Name", "Standard", "Status"])
        writer.writerows(rows)

    print(f"\nSaved to {output_file}")
    return rows


if __name__ == "__main__":
    scrape_modules_in_process()
