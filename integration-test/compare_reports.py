#!/usr/bin/env python3
"""
compare_reports.py  <report_dir>

Reads <scenario>_before.json and <scenario>_after.json from the report
directory and produces a before/after comparison table.
"""

import json
import os
import sys

SCENARIOS = [
    "skew",
    "cache",
    "join_broadcast",
    "join_shuffles",
    "config_aqe",
    "config_serializer",
    "plan_cartesian",
    "plan_window",
    "driver_collect",
]


def load_report(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as e:
            print(f"  [WARN] Could not parse {path}: {e}", file=sys.stderr)
            return None


def summarise_issues(issues):
    """Return a short string listing (category, severity) pairs."""
    if not issues:
        return "—"
    parts = []
    for i in issues:
        cat = i.get("category", "?")
        sev = i.get("severity", "?")
        title = i.get("title", "")[:50]
        parts.append(f"{cat}/{sev}: {title}")
    return "; ".join(parts)


def compare_scenario(report_dir, scenario):
    before_path = os.path.join(report_dir, f"{scenario}_before.json")
    after_path  = os.path.join(report_dir, f"{scenario}_after.json")

    before = load_report(before_path)
    after  = load_report(after_path)

    if before is None and after is None:
        return {
            "scenario":         scenario,
            "health_before":    "N/A",
            "health_after":     "N/A",
            "delta":            "N/A",
            "issues_before":    "N/A",
            "issues_after":     "N/A",
            "resolved":         "N/A",
            "remaining":        "N/A",
            "new_issues":       "N/A",
            "notes":            "Both reports missing",
        }

    if before is None:
        return {
            "scenario":         scenario,
            "health_before":    "N/A",
            "health_after":     after.get("health_score", "?"),
            "delta":            "N/A",
            "issues_before":    "N/A",
            "issues_after":     str(after.get("issue_count", "?")),
            "resolved":         "N/A",
            "remaining":        summarise_issues(after.get("issues", [])),
            "new_issues":       "N/A",
            "notes":            "before report missing",
        }

    if after is None:
        return {
            "scenario":         scenario,
            "health_before":    before.get("health_score", "?"),
            "health_after":     "N/A",
            "delta":            "N/A",
            "issues_before":    str(before.get("issue_count", "?")),
            "issues_after":     "N/A",
            "resolved":         "N/A",
            "remaining":        "N/A",
            "new_issues":       "N/A",
            "notes":            "after report missing",
        }

    score_before = before.get("health_score", 0)
    score_after  = after.get("health_score", 0)
    delta        = score_after - score_before

    issues_before = before.get("issues", [])
    issues_after  = after.get("issues", [])

    # Match issues by category (and rough title prefix) to detect resolved / remaining / new
    before_cats = {(i["category"], i.get("id", "")[:20]) for i in issues_before}
    after_cats  = {(i["category"], i.get("id", "")[:20]) for i in issues_after}

    # Simpler: compare by id prefix (strip stage/exec suffix)
    def id_prefix(issue_id):
        """Strip trailing -<number> to get a stable category key."""
        parts = issue_id.rsplit("-", 1)
        if len(parts) == 2 and parts[1].lstrip("-").isdigit():
            return parts[0]
        return issue_id

    before_ids = {id_prefix(i.get("id", "")) for i in issues_before}
    after_ids  = {id_prefix(i.get("id", "")) for i in issues_after}

    resolved_ids  = before_ids - after_ids
    remaining_ids = before_ids & after_ids
    new_ids       = after_ids  - before_ids

    def fmt_ids(ids):
        return ", ".join(sorted(ids)) if ids else "—"

    # Build human-readable resolved list from before issues
    resolved_titles = []
    for i in issues_before:
        if id_prefix(i.get("id", "")) in resolved_ids:
            resolved_titles.append(f"{i.get('category','?')}/{i.get('severity','?')}: {i.get('title','')[:60]}")

    remaining_titles = []
    for i in issues_after:
        if id_prefix(i.get("id", "")) in remaining_ids:
            remaining_titles.append(f"{i.get('category','?')}/{i.get('severity','?')}: {i.get('title','')[:60]}")

    new_titles = []
    for i in issues_after:
        if id_prefix(i.get("id", "")) in new_ids:
            new_titles.append(f"{i.get('category','?')}/{i.get('severity','?')}: {i.get('title','')[:60]}")

    notes = []
    if new_ids:
        notes.append(f"New issues introduced: {len(new_ids)}")
    if delta == 0 and resolved_ids:
        notes.append("Score unchanged despite resolving issues (other issues added or remained)")
    if not resolved_ids and not new_ids:
        notes.append("No change in issue set")

    return {
        "scenario":         scenario,
        "health_before":    score_before,
        "health_after":     score_after,
        "delta":            f"+{delta}" if delta > 0 else str(delta),
        "issues_before":    len(issues_before),
        "issues_after":     len(issues_after),
        "resolved":         "; ".join(resolved_titles) if resolved_titles else "—",
        "remaining":        "; ".join(remaining_titles) if remaining_titles else "—",
        "new_issues":       "; ".join(new_titles) if new_titles else "—",
        "notes":            "; ".join(notes) if notes else "—",
    }


def print_table(rows):
    """Print a markdown table."""
    headers = [
        "Scenario", "Health Before", "Health After", "Δ Health",
        "Issues Before", "Issues After",
        "Resolved", "Remaining", "New Issues", "Notes",
    ]
    keys = [
        "scenario", "health_before", "health_after", "delta",
        "issues_before", "issues_after",
        "resolved", "remaining", "new_issues", "notes",
    ]

    # Compute column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, k in enumerate(keys):
            widths[i] = max(widths[i], len(str(row.get(k, ""))))

    def fmt_row(values):
        return "| " + " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(values)) + " |"

    print(fmt_row(headers))
    print("|" + "|".join("-" * (w + 2) for w in widths) + "|")
    for row in rows:
        print(fmt_row([row.get(k, "") for k in keys]))


def print_detail(rows):
    """Print detailed per-scenario breakdown."""
    for row in rows:
        print(f"\n{'='*70}")
        print(f"Scenario: {row['scenario']}")
        print(f"  Health:   {row['health_before']} → {row['health_after']}  (Δ {row['delta']})")
        print(f"  Issues:   {row['issues_before']} before → {row['issues_after']} after")
        print(f"  Resolved: {row['resolved']}")
        print(f"  Remaining:{row['remaining']}")
        print(f"  New:      {row['new_issues']}")
        if row.get("notes") and row["notes"] != "—":
            print(f"  Notes:    {row['notes']}")


def main():
    if len(sys.argv) < 2:
        print("Usage: compare_reports.py <report_dir>", file=sys.stderr)
        sys.exit(1)

    report_dir = sys.argv[1]
    if not os.path.isdir(report_dir):
        print(f"Report directory not found: {report_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"\nLoading reports from: {report_dir}")
    print(f"Available files: {sorted(os.listdir(report_dir))}\n")

    rows = [compare_scenario(report_dir, s) for s in SCENARIOS]

    print("\n## spark-lens Before/After Comparison\n")
    print_table(rows)
    print()
    print_detail(rows)


if __name__ == "__main__":
    main()
