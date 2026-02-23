#!/usr/bin/env python3
"""
merge_db.py — fill missing dates in DB1 from DB2.

Usage:
    python3 merge_db.py <primary.db> <secondary.db>

Any publish_date present in secondary but absent in primary will have its
modules and not_displayed rows copied into primary.
"""

import sqlite3
import sys


def get_dates(cur):
    cur.execute("SELECT DISTINCT publish_date FROM modules")
    dates = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT publish_date FROM not_displayed")
    dates |= {r[0] for r in cur.fetchall()}
    return dates


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <primary.db> <secondary.db>")
        sys.exit(1)

    primary_path, secondary_path = sys.argv[1], sys.argv[2]

    primary = sqlite3.connect(primary_path)
    secondary = sqlite3.connect(secondary_path)

    p = primary.cursor()
    s = secondary.cursor()

    primary_dates = get_dates(p)
    secondary_dates = get_dates(s)

    missing = sorted(secondary_dates - primary_dates)

    if not missing:
        print("No missing dates found. Primary database is already up to date.")
        primary.close()
        secondary.close()
        return

    print(f"Found {len(missing)} date(s) in secondary not present in primary:")
    for d in missing:
        print(f"  {d}")

    for date in missing:
        # Copy module rows
        s.execute(
            "SELECT module_name, vendor_name, standard, status FROM modules WHERE publish_date = ?",
            (date,)
        )
        module_rows = s.fetchall()
        p.executemany(
            "INSERT INTO modules (publish_date, module_name, vendor_name, standard, status) VALUES (?, ?, ?, ?, ?)",
            [(date, r[0], r[1], r[2], r[3]) for r in module_rows]
        )

        # Copy not_displayed row if present
        s.execute(
            "SELECT count, total_count FROM not_displayed WHERE publish_date = ?",
            (date,)
        )
        nd = s.fetchone()
        if nd:
            p.execute(
                "INSERT OR IGNORE INTO not_displayed (publish_date, count, total_count) VALUES (?, ?, ?)",
                (date, nd[0], nd[1])
            )

        print(f"  Copied {len(module_rows)} module rows for {date}"
              + (f" + not_displayed({nd[0]})" if nd else ""))

    primary.commit()
    primary.close()
    secondary.close()
    print("Done.")


if __name__ == "__main__":
    main()
