"""Generate an HTML report from the NIST CMVP Modules In Process database."""

import argparse
import html as html_mod
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime

DB_FILE = "nist_modules_in_process.db"
OUTPUT_FILE = "nist_mip_report.html"
VALIDATED_URL = "https://csrc.nist.gov/projects/cryptographic-module-validation-program/validated-modules/search/all"

STATUS_COLORS = {
    "Review Pending": "#4e79a7",
    "In Review":      "#f28e2b",
    "Coordination":   "#59a14f",
    "Finalization":   "#9467bd",
    "On Hold":        "#e05c5c",
    "Not Displayed":  "#bab0ac",
}
DEFAULT_COLOR = "#bab0ac"

ALL_STATUSES = ["Review Pending", "In Review", "Coordination", "Finalization", "On Hold", "Not Displayed"]


def subtract_months(dt, n):
    """Return dt shifted back by n calendar months."""
    import calendar
    total = dt.year * 12 + (dt.month - 1) - n
    year, month = total // 12, total % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day)


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

    # Not-displayed counts per publish date
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='not_displayed'")
    if cur.fetchone():
        cur.execute("SELECT publish_date, count FROM not_displayed")
        for pub_date, nd_count in cur.fetchall():
            if nd_count:
                counts.setdefault(pub_date, {})
                counts[pub_date]["Not Displayed"] = nd_count

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
        data = [
            {"x": datetime.strptime(d, "%m/%d/%Y").strftime("%Y-%m-%d"),
             "y": counts.get(d, {}).get(status, 0)}
            for d in dates
        ]
        datasets.append({
            "label": status,
            "data": data,
            "backgroundColor": STATUS_COLORS.get(status, DEFAULT_COLOR),
        })
    return datasets


def fetch_validated_modules():
    """Return {module_name_lower: (cert_num, vendor, val_date)} from NIST validated modules list."""
    try:
        import requests
        from bs4 import BeautifulSoup as BS
        resp = requests.get(VALIDATED_URL, timeout=30,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except Exception as e:
        print(f"Warning: could not fetch validated modules list: {e}", file=sys.stderr)
        return {}

    from bs4 import BeautifulSoup as BS
    soup = BS(resp.text, "html.parser")
    table = soup.find("table", id="searchResultsTable")
    if not table:
        print("Warning: could not find validated modules table in NIST response.", file=sys.stderr)
        return {}

    result = {}
    for tr in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) >= 5:
            cert_num, vendor, module_name, _mod_type, val_date = cells[:5]
            result[module_name.lower()] = (cert_num, vendor, val_date)

    return result



def vendor_breakdown_html(all_rows, new_date):
    """Return HTML table of vendors ranked by current module count (top 25)."""
    counts = Counter(vn for pd, _mn, vn, _std, _st in all_rows if pd == new_date)
    if not counts:
        return ""
    trs = "".join(
        f"<tr><td>{vendor}</td><td>{count}</td></tr>"
        for vendor, count in counts.most_common(25)
    )
    return (
        "<table><thead><tr><th>Vendor</th><th>Modules in Process</th></tr></thead>"
        f"<tbody>{trs}</tbody></table>"
    )


def compute_module_stats(all_rows, dates):
    """Return status_since dict keyed by (module_name, vendor_name, standard).

    status_since: earliest publish_date of the current unbroken normalized-status run (as datetime)
    """
    date_dt = {d: datetime.strptime(d, "%m/%d/%Y") for d in dates}

    history = {}
    for pub_date, module_name, vendor_name, standard, status in all_rows:
        key = (module_name, vendor_name, standard)
        history.setdefault(key, []).append((pub_date, normalize_status(status)))

    for key in history:
        history[key].sort(key=lambda x: date_dt.get(x[0], datetime.min))

    status_since = {}
    for key, entries in history.items():
        current_status = entries[-1][1]
        run_start = entries[-1][0]
        for pub_date, norm_status in reversed(entries[:-1]):
            if norm_status == current_status:
                run_start = pub_date
            else:
                break
        status_since[key] = date_dt[run_start]

    return status_since


def build_module_histories(all_rows, dates, keys):
    """Return {key_str: [{date, status, status_date}, ...]} for the given module keys, sorted chronologically.

    status_date is the date embedded in the raw status string (e.g. '9/2/2025' from
    'Review Pending (9/2/2025)'), representing when the module entered that status.
    """
    date_dt = {d: datetime.strptime(d, "%m/%d/%Y") for d in dates}
    key_set = set(keys)
    raw = {}
    status_date_re = re.compile(r'\((\d{1,2}/\d{1,2}/\d{4})\)')
    for pub_date, mn, vn, std, status in all_rows:
        key = (mn, vn, std)
        if key in key_set:
            k_str = f"{mn}||{vn}||{std}"
            m = status_date_re.search(status)
            status_date = m.group(1) if m else None
            raw.setdefault(k_str, []).append((pub_date, normalize_status(status), status_date))
    return {
        k: [{"date": d, "status": s, "status_date": sd}
            for d, s, sd in sorted(v, key=lambda x: date_dt.get(x[0], datetime.min))]
        for k, v in raw.items()
    }


def _key_attr(mn, vn, std):
    """Return an HTML-safe data-key attribute value for a module."""
    return html_mod.escape(f"{mn}||{vn}||{std}", quote=True)


def finalization_html(all_rows, new_date, status_since=None, validated=None):
    """Return (html, count) for modules in Finalization as of new_date, sorted by days in status desc."""
    target = {"Finalization"}
    new_dt = datetime.strptime(new_date, "%m/%d/%Y")
    rows = [(r[1], r[2], r[3], r[4]) for r in all_rows
            if r[0] == new_date and normalize_status(r[4]) in target]
    if not rows:
        return "<p>No modules currently in Finalization.</p>", 0

    def days_ago(dt):
        return (new_dt - dt).days

    if status_since:
        rows.sort(key=lambda r: status_since.get((r[0], r[1], r[2]), new_dt))
    else:
        rows.sort(key=lambda r: (normalize_status(r[3]), r[0]))

    if status_since:
        def row_html(r):
            key = (r[0], r[1], r[2])
            ds = days_ago(status_since.get(key, new_dt))
            cert = ""
            if validated is not None:
                v = validated.get(r[0].lower())
                cert = f"<td><a href='https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/{v[0]}' target='_blank'>#{v[0]}</a></td>" if v else "<td></td>"
            ka = _key_attr(r[0], r[1], r[2])
            return (f"<tr><td class='module-name' data-key='{ka}'>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td><td>{r[3]}</td>"
                    f"<td>{ds}</td>{cert}</tr>")

        cert_header = "<th>Certificate</th>" if validated is not None else ""
        header = f"<th>Module</th><th>Vendor</th><th>Standard</th><th>Status</th><th>Days in Status</th>{cert_header}"
        trs = "".join(row_html(r) for r in rows)
    else:
        trs = "".join(
            f"<tr><td class='module-name' data-key='{_key_attr(r[0], r[1], r[2])}'>{r[0]}</td>"
            f"<td>{r[1]}</td><td>{r[2]}</td><td>{r[3]}</td></tr>"
            for r in rows
        )
        header = "<th>Module</th><th>Vendor</th><th>Standard</th><th>Status</th>"

    return (
        f"<table><thead><tr>{header}</tr></thead>"
        f"<tbody>{trs}</tbody></table>"
    ), len(rows)


def disappearances_html(all_rows, dates):
    """Return (html, count) for modules that dropped from a non-terminal status (most recent disappearance first)."""
    terminal = {"Finalization"}
    date_dt = {d: datetime.strptime(d, "%m/%d/%Y") for d in dates}
    sorted_dates = sorted(dates, key=lambda d: date_dt[d])

    last_status = {}  # key -> (last_publish_date, normalized_status)
    for pub_date, module_name, vendor_name, standard, status in all_rows:
        key = (module_name, vendor_name, standard)
        if key not in last_status or date_dt[pub_date] > date_dt[last_status[key][0]]:
            last_status[key] = (pub_date, normalize_status(status))

    most_recent = sorted_dates[-1]
    disappeared = [
        (key, last_date, last_norm)
        for key, (last_date, last_norm) in last_status.items()
        if last_date != most_recent and last_norm not in terminal
    ]

    if not disappeared:
        return "<p>No modules have disappeared without reaching Finalization.</p>", 0

    disappeared.sort(key=lambda x: date_dt[x[1]], reverse=True)

    trs = "".join(
        f"<tr><td>{k[0]}</td><td>{k[1]}</td><td>{k[2]}</td>"
        f"<td>{last_norm}</td><td>{last_date}</td></tr>"
        for k, last_date, last_norm in disappeared
    )
    html = (
        f"<table><thead><tr>"
        f"<th>Module</th><th>Vendor</th><th>Standard</th><th>Last Status</th><th>Last Seen</th>"
        f"</tr></thead><tbody>{trs}</tbody></table>"
    )
    return html, len(disappeared)


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
        return f"<td class='module-name' data-key='{_key_attr(*k)}'>{k[0]}</td><td>{k[1]}</td><td>{k[2]}</td><td>{status}</td>"

    def removed_row(item):
        k, status = item
        return f"<td class='module-name' data-key='{_key_attr(*k)}'>{k[0]}</td><td>{k[1]}</td><td>{k[2]}</td><td>{status}</td>"

    def changed_row(item):
        k, old_s, new_s = item
        return f"<td class='module-name' data-key='{_key_attr(*k)}'>{k[0]}</td><td>{k[1]}</td><td>{k[2]}</td><td>{old_s}</td><td>{new_s}</td>"

    parts.append(section("Added", added, added_row))
    parts.append(section("Removed", removed, removed_row))
    parts.append(section("Status Changes", changed, changed_row))

    return "".join(p for p in parts if p)


def generate_html(dates, counts, all_rows, chart_dates=None, check_validated=False, show_vendors=False):
    if chart_dates is None:
        chart_dates = dates

    prev_date, new_date, added, removed, changed = compute_changes(dates, all_rows)
    datasets = build_chart_data(chart_dates, counts)

    chart_datasets = json.dumps(datasets)

    totals = [sum(counts.get(d, {}).values()) for d in chart_dates]
    y_max = max(totals) * 1.1 if totals else 100

    if len(chart_dates) < len(dates):
        chart_note = f"showing {len(chart_dates)} of {len(dates)} publish dates (last 12 months)"
    else:
        chart_note = f"{len(dates)} publish dates"

    status_since = compute_module_stats(all_rows, dates)
    validated = fetch_validated_modules() if check_validated else None
    fin_html, fin_count = finalization_html(all_rows, new_date, status_since=status_since, validated=validated)

    # Build module histories for all modules visible in the report
    history_keys = (
        {k for k, _ in added} | {k for k, _ in removed} | {k for k, _, _ in changed}
        | {(r[1], r[2], r[3]) for r in all_rows if r[0] == new_date and normalize_status(r[4]) == "Finalization"}
    )
    histories_json = json.dumps(build_module_histories(all_rows, dates, history_keys))

    vendor_section = (
        f"<h2>Top Vendors by Modules in Process as of {new_date}</h2>"
        f"<div class=\"changes\">{vendor_breakdown_html(all_rows, new_date)}</div>"
    ) if show_vendors else ""

    # Current-day summary panel
    day_counts = counts.get(new_date, {})
    day_total = sum(day_counts.values())
    summary_rows = "".join(
        f"<tr><td><span class='swatch' style='background:{STATUS_COLORS.get(s, DEFAULT_COLOR)}'></span>{s}</td>"
        f"<td class='sum-n'>{day_counts[s]:,}</td></tr>"
        for s in ALL_STATUSES if day_counts.get(s, 0) > 0
    )
    summary_html = (
        f"<div class='chart-summary'>"
        f"<div class='sum-title'>Today's Summary</div>"
        f"<div class='sum-date'>{new_date}</div>"
        f"<table class='sum-table'><tbody>{summary_rows}"
        f"<tr class='sum-total'><td>Total</td><td class='sum-n'>{day_total:,}</td></tr>"
        f"</tbody></table></div>"
    )

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
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3"></script>
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
  .chart-row {{ display: flex; gap: 16px; align-items: stretch; }}
  .chart-container {{ flex: 1; min-width: 0; background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  canvas {{ max-height: 420px; }}
  .chart-summary {{
    width: 250px; flex-shrink: 0;
    background: #fff; border-radius: 8px; padding: 16px 28px;
    box-shadow: 0 1px 4px rgba(0,0,0,.08);
    display: flex; flex-direction: column; justify-content: center;
  }}
  .sum-title {{ font-size: 0.95rem; font-weight: 700; color: #212529; margin-bottom: 2px; text-align: center; }}
  .sum-date {{ font-size: 0.8rem; color: #6c757d; margin-bottom: 12px; text-align: center; }}
  .sum-table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; }}
  .sum-table td {{ padding: 4px 6px; border: none; }}
  .sum-table tr:hover td {{ background: #f8f9fa; }}
  .sum-n {{ text-align: right; font-variant-numeric: tabular-nums; font-weight: 500; }}
  .sum-total td {{ border-top: 2px solid #dee2e6; font-weight: 700; padding-top: 6px; }}
  .swatch {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 6px; vertical-align: middle; }}
  .changes {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-top: 8px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; }}
  th {{ background: #f1f3f5; text-align: left; padding: 6px 10px; font-weight: 600; border-bottom: 2px solid #dee2e6; }}
  td {{ padding: 5px 10px; border-bottom: 1px solid #f1f3f5; vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f8f9fa; }}
  .footer {{ color: #adb5bd; font-size: 0.78rem; margin-top: 28px; }}
  .module-name {{ cursor: pointer; color: #4e79a7; text-decoration: underline dotted; }}
  .module-name:hover {{ color: #2c5282; }}
  .modal-overlay {{
    display: none; position: fixed; inset: 0;
    background: rgba(0,0,0,.45); z-index: 100;
    align-items: center; justify-content: center;
  }}
  .modal-overlay.open {{ display: flex; }}
  .modal-box {{
    background: #fff; border-radius: 10px; padding: 28px 32px;
    max-width: 700px; width: 90%; max-height: 80vh;
    display: flex; flex-direction: column; box-shadow: 0 8px 32px rgba(0,0,0,.2);
  }}
  .modal-header {{ display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; }}
  .modal-header h3 {{ margin: 0; font-size: 1rem; color: #212529; max-width: 90%; }}
  .modal-close {{
    background: none; border: none; font-size: 1.4rem; cursor: pointer;
    color: #6c757d; line-height: 1; padding: 0 4px;
  }}
  .modal-close:hover {{ color: #212529; }}
  .modal-body {{ overflow-y: auto; }}
</style>
</head>
<body>
<div class="modal-overlay" id="historyModal">
  <div class="modal-box">
    <div class="modal-header">
      <h3 id="modalTitle"></h3>
      <button class="modal-close" id="modalClose">&#x2715;</button>
    </div>
    <div class="modal-body">
      <table>
        <thead><tr><th>Date Range</th><th>Status</th></tr></thead>
        <tbody id="historyBody"></tbody>
      </table>
    </div>
  </div>
</div>
<h1>NIST CMVP Modules In Process</h1>
<p class="subtitle">Source: <a href="https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list" target="_blank">NIST CSRC</a> &mdash; {chart_note} &mdash; most recent: {new_date}</p>

<h2>Status Over Time</h2>
<div class="chart-row">
  <div class="chart-container">
    <canvas id="mipChart"></canvas>
  </div>
{summary_html}
</div>

<h2>{changes_title}</h2>
<div class="changes">
{changes_section}
</div>

<h2 id="fin-heading">Finalization as of {new_date} <span class="badge">{fin_count}</span></h2>
<div class="changes" id="fin-section">
{fin_html}
</div>

{vendor_section}

<p class="footer">Generated {generated_at}</p>

<script>
const ctx = document.getElementById('mipChart').getContext('2d');
new Chart(ctx, {{
  type: 'bar',
  data: {{
    datasets: {chart_datasets}
  }},
  options: {{
    plugins: {{
      legend: {{ position: 'top' }},
      tooltip: {{
        mode: 'index',
        callbacks: {{
          title: (items) => items[0]?.label ?? '',
          footer: (items) => {{
            const total = items.reduce((s, i) => s + i.parsed.y, 0);
            return 'Total: ' + total;
          }}
        }}
      }}
    }},
    responsive: true,
    barThickness: 6,
    scales: {{
      x: {{
        type: 'time',
        time: {{ unit: 'month', tooltipFormat: 'M/d/yyyy', displayFormats: {{ month: 'MMM yyyy' }} }},
        stacked: true,
        ticks: {{ maxRotation: 45, minRotation: 45, font: {{ size: 11 }} }}
      }},
      y: {{
        stacked: true,
        max: {y_max:.0f},
        title: {{ display: true, text: 'Modules' }}
      }}
    }}
  }}
}});


const moduleHistories = {histories_json};

function showHistory(key) {{
  const history = moduleHistories[key];
  if (!history) return;
  // Collapse into status runs; use embedded status_date as start when available
  const runs = [];
  for (const h of history) {{
    if (runs.length && runs[runs.length - 1].status === h.status) {{
      runs[runs.length - 1].end = h.date;
    }} else {{
      runs.push({{ start: h.status_date || h.date, end: h.date, status: h.status }});
    }}
  }}
  // If a status appears more than once, keep only the latest run
  const latest = {{}};
  runs.forEach(r => {{ latest[r.status] = r; }});
  const dedupedRuns = runs.filter(r => latest[r.status] === r);
  const parts = key.split('||');
  document.getElementById('modalTitle').textContent = parts[0] + ' — ' + parts[1];
  document.getElementById('historyBody').innerHTML = dedupedRuns.map(r =>
    `<tr><td>${{r.start === r.end ? r.start : r.start + ' – ' + r.end}}</td><td>${{r.status}}</td></tr>`
  ).join('');
  document.getElementById('historyModal').classList.add('open');
}}

document.getElementById('modalClose').addEventListener('click', () =>
  document.getElementById('historyModal').classList.remove('open'));
document.getElementById('historyModal').addEventListener('click', e => {{
  if (e.target === e.currentTarget)
    e.currentTarget.classList.remove('open');
}});
document.addEventListener('keydown', e => {{
  if (e.key === 'Escape') document.getElementById('historyModal').classList.remove('open');
}});
document.querySelectorAll('.module-name').forEach(td =>
  td.addEventListener('click', () => showHistory(td.dataset.key)));
</script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description="Generate HTML report from NIST MIP database.")
    parser.add_argument("-o", "--output", default=OUTPUT_FILE, help=f"Output HTML file (default: {OUTPUT_FILE})")
    parser.add_argument("--all", dest="all_dates", action="store_true", help="Chart all dates in the database (default: last 18 months)")
    parser.add_argument("--check-validated", dest="check_validated", action="store_true",
                        help="Cross-reference Finalization modules against the NIST validated list (requires network)")
    parser.add_argument("--vendors", dest="show_vendors", action="store_true",
                        help="Include top vendors by module count table")
    args = parser.parse_args()

    dates, counts, all_rows = load_data()

    if args.all_dates:
        chart_dates = dates
    else:
        most_recent = datetime.strptime(dates[-1], "%m/%d/%Y")
        cutoff = subtract_months(most_recent, 12)
        chart_dates = [d for d in dates if datetime.strptime(d, "%m/%d/%Y") >= cutoff]

    html = generate_html(dates, counts, all_rows, chart_dates=chart_dates,
                         check_validated=args.check_validated, show_vendors=args.show_vendors)

    with open(args.output, "w") as f:
        f.write(html)

    print(f"Report written to {args.output} ({len(chart_dates)} of {len(dates)} publish dates charted)")


if __name__ == "__main__":
    main()
