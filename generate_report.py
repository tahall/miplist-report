"""Generate an HTML report from the NIST CMVP Modules In Process database."""

import argparse
import html as html_mod
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from statistics import mean, median

DB_FILE = "nist_modules_in_process.db"
OUTPUT_FILE = "index.html"
STATS_OUTPUT_FILE = "miplist-stats.html"
VALIDATED_URL = "https://csrc.nist.gov/projects/cryptographic-module-validation-program/validated-modules/search/all"

STATUS_COLORS = {
    # Current status names
    "Pending Review":            "#4e79a7",
    "Review":                    "#f28e2b",
    "Comment Resolution - CMVP": "#3a7d35",
    "Comment Resolution - Lab":  "#76c068",
    "Hold":                      "#e05c5c",
    "Cost Recovery":             "#c47d0e",
    "Pending Resubmission":      "#d4799a",
    "Finalization":              "#9467bd",
    "Not Displayed":             "#bab0ac",
    # Legacy status names (historical data)
    "Review Pending":            "#4e79a7",
    "In Review":                 "#f28e2b",
    "Coordination":              "#59a14f",
    "On Hold":                   "#e05c5c",
}
DEFAULT_COLOR = "#bab0ac"

ALL_STATUSES = [
    # Review group
    "Review Pending", "Pending Review",
    # In Review group
    "In Review", "Review",
    # Coordination / Comment Resolution group
    "Coordination", "Comment Resolution - CMVP", "Comment Resolution - Lab",
    # Hold group
    "On Hold", "Hold", "Cost Recovery", "Pending Resubmission",
    # Terminal / other
    "Finalization",
    "Not Displayed",
]

# Chart legend groups: (display_label, [source_statuses_to_sum], color)
# Combined entries merge legacy + current names into one bar series.
CHART_STATUS_GROUPS = [
    ("Review Pending / Pending Review", ["Review Pending", "Pending Review"],    "#4e79a7"),
    ("In Review / Review",              ["In Review", "Review"],                  "#f28e2b"),
    ("Coordination",                    ["Coordination"],                          "#59a14f"),
    ("Comment Resolution - CMVP",       ["Comment Resolution - CMVP"],             "#3a7d35"),
    ("Comment Resolution - Lab",        ["Comment Resolution - Lab"],              "#76c068"),
    ("On Hold / Hold",                  ["On Hold", "Hold"],                       "#e05c5c"),
    ("Cost Recovery",                   ["Cost Recovery"],                         "#c47d0e"),
    ("Pending Resubmission",            ["Pending Resubmission"],                  "#d4799a"),
    ("Finalization",                    ["Finalization"],                           "#9467bd"),
    ("Not Displayed",                   ["Not Displayed"],                          "#bab0ac"),
]

# Map legacy status names to current equivalents for stats aggregation
LEGACY_STATUS_MAP = {
    "Review Pending": "Pending Review",
    "In Review":      "Review",
    "On Hold":        "Hold",
}

# Ordered list of statuses to show in the stats page duration table
STATS_STATUSES = [
    "Pending Review", "Review", "Coordination",
    "Comment Resolution - CMVP", "Comment Resolution - Lab",
    "Hold", "Cost Recovery", "Pending Resubmission", "Finalization",
]


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
    for label, source_statuses, color in CHART_STATUS_GROUPS:
        data = [
            {"x": datetime.strptime(d, "%m/%d/%Y").strftime("%Y-%m-%d"),
             "y": sum(counts.get(d, {}).get(s, 0) for s in source_statuses)}
            for d in dates
        ]
        datasets.append({"label": label, "data": data, "backgroundColor": color})
    return datasets


def compute_status_durations(all_rows, dates):
    """Return {quarter_str: {status: [duration_days]}} for completed status runs.

    Durations are computed snapshot-to-snapshot. Legacy status names are mapped
    to their current equivalents before grouping.
    """
    date_dt = {d: datetime.strptime(d, "%m/%d/%Y") for d in dates}

    def norm(raw):
        s = normalize_status(raw)
        return LEGACY_STATUS_MAP.get(s, s)

    def quarter_str(dt):
        return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"

    history = {}
    for pub_date, mn, vn, std, status in all_rows:
        key = (mn, vn, std)
        history.setdefault(key, []).append((date_dt[pub_date], norm(status)))
    for key in history:
        history[key].sort()

    result = {}
    for entries in history.values():
        runs = []
        for dt, status in entries:
            if runs and runs[-1][2] == status:
                runs[-1][1] = dt
            else:
                runs.append([dt, dt, status])
        for i in range(len(runs) - 1):
            days = (runs[i + 1][0] - runs[i][0]).days
            status = runs[i][2]
            q = quarter_str(runs[i + 1][0])
            result.setdefault(q, {}).setdefault(status, []).append(days)

    return result


def compute_quarterly_changes(all_rows, dates):
    """Return list of (quarter_str, added, removed) tuples sorted chronologically."""
    date_dt = {d: datetime.strptime(d, "%m/%d/%Y") for d in dates}
    sorted_dates = sorted(dates, key=lambda d: date_dt[d])

    keys_by_date = {}
    for pub_date, mn, vn, std, _status in all_rows:
        keys_by_date.setdefault(pub_date, set()).add((mn, vn, std))

    quarterly = {}
    for i in range(1, len(sorted_dates)):
        prev, curr = sorted_dates[i - 1], sorted_dates[i]
        added = len(keys_by_date.get(curr, set()) - keys_by_date.get(prev, set()))
        removed = len(keys_by_date.get(prev, set()) - keys_by_date.get(curr, set()))
        q = (date_dt[curr].year, (date_dt[curr].month - 1) // 3 + 1)
        quarterly.setdefault(q, {"added": 0, "removed": 0})
        quarterly[q]["added"] += added
        quarterly[q]["removed"] += removed

    return [
        (f"Q{q[1]} {q[0]}", v["added"], v["removed"])
        for q, v in sorted(quarterly.items())
    ]


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
        norm = normalize_status(status)
        mapped = LEGACY_STATUS_MAP.get(norm, norm)
        history.setdefault(key, []).append((pub_date, mapped))

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



def compute_forecasts(dates, counts):
    """Return list of {label, color, current, trend: [+30d,+91d], ewma: value}.

    trend: weighted least squares on data from Mar 6 2026 onward (up to 3 months back),
           with exponential decay weights (decay=0.97/day) so recent data counts more.
    ewma:  exponential weighted moving average (alpha=0.05), flat projection.
    """
    DECAY = 0.95
    HORIZONS = [30, 91]

    most_recent_dt = datetime.strptime(dates[-1], "%m/%d/%Y")
    cutoff = max(subtract_months(most_recent_dt, 3), datetime(2026, 3, 6))
    recent_dates = [d for d in dates if datetime.strptime(d, "%m/%d/%Y") >= cutoff]
    new_date = dates[-1]

    groups = list(CHART_STATUS_GROUPS) + [("Total", None, None)]
    result = []

    for label, source_statuses, color in groups:
        def val_for(d, ss=source_statuses):
            if ss is None:
                return sum(counts.get(d, {}).values())
            return sum(counts.get(d, {}).get(s, 0) for s in ss)

        current = val_for(new_date)

        # WLS: exponential decay weights, most recent point has weight 1
        origin_dt = datetime.strptime(recent_dates[0], "%m/%d/%Y")
        pts = [((datetime.strptime(d, "%m/%d/%Y") - origin_dt).days, val_for(d))
               for d in recent_dates]
        last_x = pts[-1][0]
        weights = [DECAY ** (last_x - x) for x, _ in pts]

        n = len(pts)
        if n >= 2:
            W   = sum(weights)
            Wx  = sum(w * x     for w, (x, _) in zip(weights, pts))
            Wy  = sum(w * y     for w, (_, y) in zip(weights, pts))
            Wxx = sum(w * x * x for w, (x, _) in zip(weights, pts))
            Wxy = sum(w * x * y for w, (x, y) in zip(weights, pts))
            denom = W * Wxx - Wx ** 2
            if denom:
                slope     = (W * Wxy - Wx * Wy) / denom
                intercept = (Wy - slope * Wx) / W
                trend = [max(0, round(intercept + slope * (last_x + h))) for h in HORIZONS]
            else:
                trend = [current] * len(HORIZONS)
        else:
            trend = [current] * len(HORIZONS)

        result.append({"label": label, "color": color, "current": current,
                        "trend": trend})

    # Replace Total's trend with the sum of individual group trends so the
    # table adds up correctly.
    non_total = [r for r in result if r["label"] != "Total"]
    total = next(r for r in result if r["label"] == "Total")
    total["trend"] = [sum(r["trend"][i] for r in non_total) for i in range(len(HORIZONS))]

    return result


def compute_extremes(dates, counts):
    """Return list of {label, color, current, recent, alltime} dicts using CHART_STATUS_GROUPS.

    Each period value is (high_val, [high_dates], low_val, [low_dates]) or None.
    High/low exclude zero-count dates for individual statuses; Total includes all.
    """
    most_recent_dt = datetime.strptime(dates[-1], "%m/%d/%Y")
    cutoff = subtract_months(most_recent_dt, 12)
    recent_dates = [d for d in dates if datetime.strptime(d, "%m/%d/%Y") >= cutoff]
    new_date = dates[-1]

    groups = list(CHART_STATUS_GROUPS) + [("Total", None, None)]

    result = []
    for label, source_statuses, color in groups:
        def val_for(d):
            if source_statuses is None:
                return sum(counts.get(d, {}).values())
            return sum(counts.get(d, {}).get(s, 0) for s in source_statuses)

        current = val_for(new_date)
        row = {"label": label, "color": color, "current": current}

        for period_label, period_dates in (("recent", recent_dates), ("alltime", dates)):
            pairs = [(val_for(d), d) for d in period_dates]
            if source_statuses is not None:
                pairs = [(v, d) for v, d in pairs if v > 0]
            if not pairs:
                row[period_label] = None
            else:
                vals = sorted(v for v, _ in pairs)
                high_v = vals[-1]
                low_v = vals[0]
                med_v = median(vals)
                row[period_label] = (
                    high_v, [d for v, d in pairs if v == high_v],
                    med_v,
                    low_v, [d for v, d in pairs if v == low_v],
                )
        result.append(row)
    return result


def compute_aging(all_rows, dates, status_since):
    """Return list of {label, color, count, median, p75, p90, max} for current modules.

    Groups statuses by CHART_STATUS_GROUPS. Not Displayed modules are excluded
    (they are tracked as aggregate counts only, not individual records).
    """
    new_date = dates[-1]
    new_dt = datetime.strptime(new_date, "%m/%d/%Y")

    status_to_group = {}
    for label, source_statuses, color in CHART_STATUS_GROUPS:
        if source_statuses:
            for s in source_statuses:
                status_to_group[s] = (label, color)

    group_days = {}
    for pub_date, mn, vn, std, status in all_rows:
        if pub_date != new_date:
            continue
        norm = normalize_status(status)
        key = (mn, vn, std)
        since_dt = status_since.get(key)
        if since_dt is None:
            continue
        days = (new_dt - since_dt).days
        group_label, color = status_to_group.get(norm, (norm, DEFAULT_COLOR))
        group_days.setdefault(group_label, {"days": [], "color": color})["days"].append(days)

    def pct(sorted_vals, p):
        idx = (len(sorted_vals) - 1) * p / 100
        lo = int(idx)
        hi = min(lo + 1, len(sorted_vals) - 1)
        return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (idx - lo)

    result = []
    for label, source_statuses, color in CHART_STATUS_GROUPS:
        if label not in group_days:
            continue
        days = sorted(group_days[label]["days"])
        result.append({
            "label": label, "color": color, "count": len(days),
            "p25": pct(days, 25), "median": pct(days, 50),
            "p75": pct(days, 75), "p90": pct(days, 90), "max": days[-1],
        })
    return result


def generate_stats_html(dates, counts, all_rows):
    """Generate HTML for the statistics page (miplist-stats.html)."""
    # Extremes table
    extremes = compute_extremes(dates, counts)

    def ext_cell(e, divider=False):
        div = " divider" if divider else ""
        if e is None:
            return f"<td class='no-data{div}' colspan='3'>—</td>"
        high_v, high_ds, med_v, low_v, low_ds = e
        return (f"<td class='num{div}'><span>{high_v:,}</span></td>"
                f"<td class='num'><span>{med_v:,.1f}</span></td>"
                f"<td class='num'><span>{low_v:,}</span></td>")

    ext_rows = ""
    for row in extremes:
        label, color, current = row["label"], row["color"], row["current"]
        swatch = f"<span class='swatch' style='background:{color}'></span>" if color else ""
        sep = " class='sep-row'" if label == "Total" else ""
        ext_rows += (
            f"<tr{sep}><td>{swatch}{label}</td>"
            f"<td class='num'><span>{current:,}</span></td>"
            f"{ext_cell(row['recent'], divider=True)}"
            f"{ext_cell(row['alltime'], divider=True)}</tr>"
        )

    ext_table = f"""<table>
  <thead>
    <tr>
      <th rowspan='2'>Status</th>
      <th class='num' rowspan='2'>Current</th>
      <th colspan='3' class='divider'>Last 12 Months</th>
      <th colspan='3' class='divider'>All Time</th>
    </tr>
    <tr>
      <th class='num divider'>High</th><th class='num'>Median</th><th class='num'>Low</th>
      <th class='num divider'>High</th><th class='num'>Median</th><th class='num'>Low</th>
    </tr>
  </thead>
  <tbody>{ext_rows}</tbody>
</table>"""

    # Aging analysis table
    status_since = compute_module_stats(all_rows, dates)
    aging = compute_aging(all_rows, dates, status_since)
    aging_rows = ""
    for row in aging:
        swatch = f"<span class='swatch' style='background:{row['color']}'></span>"
        aging_rows += (
            f"<tr><td>{swatch}{row['label']}</td>"
            f"<td class='num'><span>{row['count']:,}</span></td>"
            f"<td class='num'><span>{round(row['median'])}</span></td>"
            f"<td class='num'><span>{round(row['p75'])}</span></td>"
            f"<td class='num'><span>{round(row['p90'])}</span></td></tr>"
        )
    aging_table = f"""<table>
  <thead>
    <tr>
      <th>Status</th>
      <th class='num'>Count</th>
      <th class='num'>Median days</th>
      <th class='num'>75th %ile</th>
      <th class='num'>90th %ile</th>
    </tr>
  </thead>
  <tbody>{aging_rows}</tbody>
</table>"""

    # Forecast table
    forecasts = compute_forecasts(dates, counts)
    fcast_rows = ""
    for row in [r for r in forecasts if r["label"] != "Coordination"]:
        label, color, current = row["label"], row["color"], row["current"]
        swatch = f"<span class='swatch' style='background:{color}'></span>" if color else ""
        sep = " class='sep-row'" if label == "Total" else ""
        t1, t3 = row["trend"]

        def delta_span(pred, cur=current):
            d = pred - cur
            if d > 0:
                return f"<span class='dlt'><span class='delta pos'>(+{d})</span></span>"
            if d < 0:
                return f"<span class='dlt'><span class='delta neg'>({d})</span></span>"
            return "<span class='dlt'></span>"

        fcast_rows += (
            f"<tr{sep}><td>{swatch}{label}</td>"
            f"<td class='fnum'><span class='val'>{current:,}</span><span class='dlt'></span></td>"
            f"<td class='fnum'><span class='val'>{t1:,}</span>{delta_span(t1)}</td>"
            f"<td class='fnum'><span class='val'>{t3:,}</span>{delta_span(t3)}</td></tr>"
        )

    fcast_table = f"""<table>
  <thead>
    <tr>
      <th>Status</th><th class='num'>Current</th>
      <th class='num'>+1 Month</th><th class='num'>+3 Months</th>
    </tr>
  </thead>
  <tbody>{fcast_rows}</tbody>
</table>"""

    # Chart (full timeline)
    datasets = build_chart_data(dates, counts)
    chart_datasets_json = json.dumps(datasets)
    totals = [sum(counts.get(d, {}).values()) for d in dates]
    y_max = max(totals) * 1.1 if totals else 100

    # Today's summary panel
    new_date = dates[-1]
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

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NIST CMVP MIP Statistics</title>
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
  h2 {{ font-size: 1.2rem; margin: 32px 0 8px; border-bottom: 2px solid #dee2e6; padding-bottom: 6px; }}
  .footer {{ color: #adb5bd; font-size: 0.78rem; margin-top: 28px; }}
  .note {{ font-size: 0.82rem; color: #6c757d; margin-bottom: 10px; }}
  .card {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); overflow-x: auto; margin-bottom: 8px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.82rem; }}
  th {{ background: #f1f3f5; text-align: left; padding: 6px 10px; font-weight: 600; border-bottom: 2px solid #dee2e6; white-space: nowrap; }}
  th[colspan] {{ text-align: center; }}
  td {{ padding: 5px 10px; border-bottom: 1px solid #f1f3f5; vertical-align: top; white-space: nowrap; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f8f9fa; }}
  td.no-data {{ color: #adb5bd; text-align: center; }}
  th.num {{ text-align: center; }}
  td.num {{ text-align: center; }}
  td.num span {{ display: inline-block; text-align: right; min-width: 5ch; font-variant-numeric: tabular-nums; }}
  small {{ color: #6c757d; }}
  tr.sep-row td {{ border-top: 2px solid #dee2e6; font-weight: 700; }}
  .divider {{ border-left: 2px solid #dee2e6; }}
  .delta {{ font-size: 0.78rem; font-weight: 500; }}
  .delta.pos {{ color: #2c7a2c; }}
  .delta.neg {{ color: #c0392b; }}
  td.fnum {{ text-align: center; }}
  td.fnum span.val {{ display: inline-block; text-align: right; min-width: 5ch; font-variant-numeric: tabular-nums; }}
  td.fnum span.dlt {{ display: inline-block; min-width: 7ch; text-align: left; }}
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
  .sum-table td {{ padding: 4px 6px; border: none; white-space: normal; }}
  .sum-table tr:hover td {{ background: #f8f9fa; }}
  .sum-n {{ text-align: right; font-variant-numeric: tabular-nums; font-weight: 500; }}
  .sum-total td {{ border-top: 2px solid #dee2e6; font-weight: 700; padding-top: 6px; }}
  .swatch {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 6px; vertical-align: middle; }}
</style>
</head>
<body>
<h1>NIST CMVP MIP Statistics</h1>
<p class="subtitle"><a href="index.html">&larr; Back to report</a></p>

<h2>Status Over Time (Full History)</h2>
<div class="chart-row">
  <div class="chart-container">
    <canvas id="mipChart"></canvas>
  </div>
{summary_html}
</div>

<script>
const ctx = document.getElementById('mipChart').getContext('2d');
new Chart(ctx, {{
  type: 'bar',
  data: {{
    datasets: {chart_datasets_json}
  }},
  options: {{
    plugins: {{
      legend: {{ position: 'top' }},
      tooltip: {{
        mode: 'index',
        filter: (item) => item.parsed.y > 0,
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
</script>

<h2>Current Aging</h2>
<p class="note">Days each module has been continuously in its current status as of {new_date}. "Not Displayed" modules are excluded (tracked as aggregate counts only).</p>
<div class="card">
{aging_table}
</div>

<h2>Status Forecast</h2>
<p class="note">+1 and +3 month projections using weighted least squares (exponential decay, half-life ≈ 14 days) on data from Mar 6 2026 onward. Deltas vs. current in parentheses.</p>
<div class="card">
{fcast_table}
</div>

<h2>Historical Highs and Lows</h2>
<div class="card">
{ext_table}
</div>

<p class="footer">Generated {generated_at}</p>
<p class="footer">This is an unofficial tool and is not affiliated with or endorsed by NIST or the Internet Archive. Data is scraped from the <a href="https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list" target="_blank">NIST CMVP Modules In Process list</a>; historical data is sourced from the <a href="https://web.archive.org" target="_blank">Wayback Machine</a>. This data may not be complete or accurate. Always refer to the official NIST source for authoritative information.</p>
</body>
</html>
"""


def generate_html(dates, counts, all_rows, chart_dates=None, check_validated=False, show_vendors=False):
    if chart_dates is None:
        chart_dates = dates

    prev_date, new_date, added, removed, changed = compute_changes(dates, all_rows)
    datasets = build_chart_data(chart_dates, counts)

    chart_datasets = json.dumps(datasets)

    totals = [sum(counts.get(d, {}).values()) for d in chart_dates]
    y_max = max(totals) * 1.1 if totals else 100

    chart_note = f"most recent: {new_date}"

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
<p class="subtitle">Source: <a href="https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list" target="_blank">NIST CSRC</a> &mdash; {chart_note} &mdash; <a href="miplist-stats.html">Statistics &rarr;</a></p>

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
<p class="footer">This is an unofficial tool and is not affiliated with or endorsed by NIST or the Internet Archive. Data is scraped from the <a href="https://csrc.nist.gov/projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list" target="_blank">NIST CMVP Modules In Process list</a>; historical data is sourced from the <a href="https://web.archive.org" target="_blank">Wayback Machine</a>. This data may not be complete or accurate. Always refer to the official NIST source for authoritative information.</p>

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
        filter: (item) => item.parsed.y > 0,
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

    stats_html = generate_stats_html(dates, counts, all_rows)
    with open(STATS_OUTPUT_FILE, "w") as f:
        f.write(stats_html)
    print(f"Stats written to {STATS_OUTPUT_FILE}")


if __name__ == "__main__":
    main()
