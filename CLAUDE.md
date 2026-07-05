# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An unofficial scraper and report generator for the [NIST CMVP Modules In Process (MIP) list](https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list). It tracks cryptographic module validation status over time using a SQLite database and produces two static HTML reports.

## Commands

Install dependencies (no virtualenv; install globally or in a venv of your choice):
```bash
pip install requests beautifulsoup4
```

Scrape live NIST page (appends or replaces today's publish date in the DB):
```bash
python scrape_nist_mip.py
```

Backfill historical data via Wayback Machine from the earliest date already in the DB:
```bash
python scrape_nist_mip.py --backfill
```

Scrape historical range from Wayback Machine:
```bash
python scrape_nist_mip.py -from 1/2023 -to 6/2024
```

Preview what the scraper would save without writing to the DB:
```bash
python scrape_nist_mip.py --dry-run
```

Generate HTML reports from the current DB:
```bash
python generate_report.py
# produces index.html and miplist-stats.html
```

Merge missing dates from a secondary DB into the primary:
```bash
python merge_db.py nist_modules_in_process.db latest.db
```

Export all data to CSV:
```bash
python scrape_nist_mip.py --csv
python scrape_nist_mip.py --csv-history
```

## Architecture

**Data pipeline:** `scrape_nist_mip.py` fetches the live NIST page (or archived Wayback Machine snapshots), parses the HTML table, and writes rows to `nist_modules_in_process.db`. `generate_report.py` reads that DB and produces self-contained HTML files with inline Chart.js visualizations.

**Database schema** (`nist_modules_in_process.db`):
- `modules`: one row per (publish_date, module_name, vendor_name, standard, status)
- `not_displayed`: aggregate count of modules NIST omits from the table per publish date

**Module key:** `(module_name, vendor_name, standard)`. Vendor names are normalized via `normalize_vendor()` because NIST sometimes drops the pipe separator (e.g., `Codan | DTC` → `Codan DTC`) in certain statuses.

**Publish date format:** `M/D/YYYY` strings (e.g., `"1/15/2024"`). This is NIST's own format, used as the primary key throughout.

**Status pipeline (current names):**  
`Pending Review` → `Review` → `Comment Resolution - Lab` → `Comment Resolution - CMVP` → `Finalization`  
Legacy names (`Review Pending`, `In Review`, `Coordination`, `On Hold`) appear in historical records and are mapped to current equivalents via `LEGACY_STATUS_MAP` for aggregation. Both names are preserved in the DB as-is.

**Submission splitting:** `_split_into_submissions()` in `generate_report.py` detects when a module key disappears and reappears (date gap) or regresses from a late stage back to an early stage — indicating a new submission reusing the same key. Report logic uses the most recent contiguous segment for per-module stats and history popups.

**Historical Highs and Lows table** (`compute_extremes()` in `generate_report.py`): two module-level dicts control special treatment of statuses in this table:
- `STATUS_START_DATES`: statuses introduced mid-history (e.g., `"Cost Recovery"`, `"Comment Resolution - CMVP/Lab"`, `"Pending Resubmission"` — all introduced 2026-03-06). Dates before the start date are excluded so pre-existence zeros don't skew the median/low. Zero-count days on or after the start date are included.
- `STATUS_RETIRED_DATES`: statuses no longer in use (e.g., `"Coordination"`, last seen 2026-03-04). The Current column renders `—` instead of `0` to signal retirement. Historical high/median/low are still shown for reference.

**Automation:** `.github/workflows/scrape.yml` runs both scripts daily at 5 AM EST and commits `nist_modules_in_process.db`, `index.html`, and `miplist-stats.html` back to `main`.

**`latest.db`** (gitignored): a secondary DB sometimes used locally for merging; not the canonical DB.
