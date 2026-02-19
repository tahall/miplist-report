"""Generate an HTML report from the NIST CMVP Modules In Process database."""

import argparse
import json
import sqlite3
from datetime import datetime

DB_FILE = "nist_modules_in_process.db"
OUTPUT_FILE = "nist_mip_report.html"

STATUS_COLORS = {
    "Review Pending": "#4e79a7",
    "In Review":      "#f28e2b",
    "Coordination":   "#59a14f",
    "Finalization":   "#9467bd",
    "On Hold":        "#e05c5c",
}
DEFAULT_COLOR = "#bab0ac"

ALL_STATUSES = ["Review Pending", "In Review", "Coordination", "Finalization", "On Hold"]


def normalize_status(raw):
    """Strip the trailing date from a status string, e.g. 'Coordination  (10/9/2024)' -> 'Coordination'."""
    return raw.split("(")[0].strip()


def load_data():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # All publish dates sorted chronologically
    cur.execute("SELECT DISTINCT publish_date FROM modules")
    dates = sorted(
        [r[0] for r in cur.fetchall()],
        key=lambda d: datetime.strptime(d, "%m/%d/%Y"),
    )

    # Per-date status counts
    cur.execute("SELECT publish_date, status, COUNT(*) FROM modules GROUP BY publish_date, status")
    raw_counts = cur.fetchall()

    counts = {}  # {date: {status: count}}
    for pub_date, status, n in raw_counts:
        norm = normalize_status(status)
        counts.setdefault(pub_date, {})
        counts[pub_date][norm] = counts[pub_date].get(norm, 0) + n

    # Full rows for change detection
    cur.execute("SELECT publish_date, module_name, vendor_name, standard, status FROM modules")
    all_rows = cur.fetchall()
    conn.close()

    return dates, counts, all_rows


def compute_changes(dates, all_rows):
    """Return (prev_date, new_date, added, removed, changed) for the most recent date."""
    if len(dates) < 2:
        return None, dates[-1] if dates else None, [], [], []

    new_date = dates[-1]
    prev_date = dates[-2]

    def rows_for(date):
        return {
            (r[1], r[2], r[3]): r[4].strip()
            for r in all_rows if r[0] == date
        }

    old = rows_for(prev_date)
    new = rows_for(new_date)

    added = sorted([(k, new[k]) for k in new if k not in old])
    removed = sorted([(k, old[k]) for k in old if k not in new])
    changed = sorted([(k, old[k], new[k]) for k in new if k in old and normalize_status(new[k]) != normalize_status(old[k])])

    return prev_date, new_date, added, removed, changed


def build_chart_data(dates, counts):
    datasets = []
    for status in ALL_STATUSES:
        data = [counts.get(d, {}).get(status, 0) for d in dates]
        datasets.append({
            "label": status,
            "data": data,
            "backgroundColor": STATUS_COLORS.get(status, DEFAULT_COLOR),
        })
    return datasets


def changes_html(prev_date, new_date, added, removed, changed):
    if prev_date is None:
        return "<p>Not enough data for change comparison.</p>"

    total = len(added) + len(removed) + len(changed)
    if total == 0:
        return f"<p>No changes from {prev_date} to {new_date}.</p>"

    parts = []

    def section(title, items, row_fn):
        if not items:
            return ""
        rows = "".join(
            f"<tr>{row_fn(item)}</tr>" for item in items
        )
        return f"""
        <h3>{title} <span class="badge">{len(items)}</span></h3>
        <table>
          <thead><tr>{section_headers(title)}</tr></thead>
          <tbody>{rows}</tbody>
        </table>"""

    def section_headers(title):
        if title.startswith("Status"):
            return "<th>Module</th><th>Vendor</th><th>Standard</th><th>Previous Status</th><th>New Status</th>"
        return "<th>Module</th><th>Vendor</th><th>Standard</th><th>Status</th>"

    def added_row(item):
        k, status = item
        return f"<td>{k[0]}</td><td>{k[1]}</td><td>{k[2]}</td><td>{status}</td>"

    def removed_row(item):
        k, status = item
        return f"<td>{k[0]}</td><td>{k[1]}</td><td>{k[2]}</td><td>{status}</td>"

    def changed_row(item):
        k, old_s, new_s = item
        return f"<td>{k[0]}</td><td>{k[1]}</td><td>{k[2]}</td><td>{old_s}</td><td>{new_s}</td>"

    parts.append(section("Added", added, added_row))
    parts.append(section("Removed", removed, removed_row))
    parts.append(section("Status Changes", changed, changed_row))

    return "".join(p for p in parts if p)


def generate_html(dates, counts, all_rows):
    prev_date, new_date, added, removed, changed = compute_changes(dates, all_rows)
    datasets = build_chart_data(dates, counts)

    chart_labels = json.dumps(dates)
    chart_datasets = json.dumps(datasets)

    totals = [sum(counts.get(d, {}).values()) for d in dates]
    y_max = max(totals) * 1.1 if totals else 100

    changes_section = changes_html(prev_date, new_date, added, removed, changed)
    changes_title = f"Changes: {prev_date} → {new_date}" if prev_date else "Changes"

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NIST CMVP Modules In Process</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    margin: 0; padding: 24px 32px;
    background: #f8f9fa; color: #212529;
  }}
  h1 {{ font-size: 1.6rem; margin: 0 0 4px; }}
  .subtitle {{ color: #6c757d; font-size: 0.9rem; margin-bottom: 28px; }}
  h2 {{ font-size: 1.2rem; margin: 32px 0 12px; border-bottom: 2px solid #dee2e6; padding-bottom: 6px; }}
  h3 {{ font-size: 1rem; margin: 20px 0 8px; color: #495057; }}
  .badge {{
    display: inline-block; background: #dee2e6; color: #495057;
    border-radius: 10px; padding: 1px 8px; font-size: 0.8rem; font-weight: 600;
  }}
  .chart-container {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  canvas {{ max-height: 420px; }}
  .changes {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-top: 8px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; }}
  th {{ background: #f1f3f5; text-align: left; padding: 6px 10px; font-weight: 600; border-bottom: 2px solid #dee2e6; }}
  td {{ padding: 5px 10px; border-bottom: 1px solid #f1f3f5; vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f8f9fa; }}
  .footer {{ color: #adb5bd; font-size: 0.78rem; margin-top: 28px; }}
</style>
</head>
<body>
<h1>NIST CMVP Modules In Process</h1>
<p class="subtitle">Source: <a href="https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list" target="_blank">NIST CSRC</a> &mdash; {len(dates)} publish dates &mdash; most recent: {new_date}</p>

<h2>Status Over Time</h2>
<div class="chart-container">
  <canvas id="mipChart"></canvas>
</div>

<h2>{changes_title}</h2>
<div class="changes">
{changes_section}
</div>

<p class="footer">Generated {generated_at}</p>

<script>
const ctx = document.getElementById('mipChart').getContext('2d');
new Chart(ctx, {{
  type: 'bar',
  data: {{
    labels: {chart_labels},
    datasets: {chart_datasets}
  }},
  options: {{
    plugins: {{
      legend: {{ position: 'top' }},
      tooltip: {{
        mode: 'index',
        callbacks: {{
          footer: (items) => {{
            const total = items.reduce((s, i) => s + i.parsed.y, 0);
            return 'Total: ' + total;
          }}
        }}
      }}
    }},
    responsive: true,
    scales: {{
      x: {{
        stacked: true,
        ticks: {{ maxRotation: 60, minRotation: 45, font: {{ size: 11 }} }}
      }},
      y: {{
        stacked: true,
        max: {y_max:.0f},
        title: {{ display: true, text: 'Modules' }}
      }}
    }}
  }}
}});
</script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description="Generate HTML report from NIST MIP database.")
    parser.add_argument("-o", "--output", default=OUTPUT_FILE, help=f"Output HTML file (default: {OUTPUT_FILE})")
    args = parser.parse_args()

    dates, counts, all_rows = load_data()
    html = generate_html(dates, counts, all_rows)

    with open(args.output, "w") as f:
        f.write(html)

    print(f"Report written to {args.output} ({len(dates)} publish dates)")


if __name__ == "__main__":
    main()
