#!/usr/bin/env python3
"""
Compare test weights and generate PR description with change analysis.

This script compares old and new test weight files to:
1. Calculate percentage changes in total time
2. Identify tests with significant duration changes (>10%)
3. Find new and removed tests
4. Generate recommendations for batch count adjustments if total time changes >20%
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional


def load_weights(file_path: str, test_id_key: str) -> Dict[str, float]:
    """Load test weights from JSON file into a dict."""
    if not Path(file_path).exists():
        return {}

    with open(file_path) as f:
        data = json.load(f)

    return {item[test_id_key]: float(item["duration"].rstrip("s")) for item in data}


def calculate_changes(
    old_weights: Dict[str, float], new_weights: Dict[str, float]
) -> Dict:
    """Calculate comprehensive change statistics."""

    # New and removed tests (calculate first to exclude from significant changes)
    new_test_ids = set(new_weights.keys()) - set(old_weights.keys())
    removed_test_ids = set(old_weights.keys()) - set(new_weights.keys())

    new_tests = {test: new_weights[test] for test in new_test_ids}
    removed_tests = {test: old_weights[test] for test in removed_test_ids}

    # Overall stats
    old_total = sum(old_weights.values())
    new_total = sum(new_weights.values())
    total_change_pct = (
        ((new_total - old_total) / old_total * 100) if old_total > 0 else 0
    )

    # Individual test changes (ONLY for tests that exist in both old and new)
    significant_changes = []
    for test_id, new_time in new_weights.items():
        # Skip new tests - they shouldn't appear in significant changes
        if test_id in old_weights:
            old_time = old_weights[test_id]
            diff = new_time - old_time
            pct_change = (diff / old_time * 100) if old_time > 0 else 0

            # Only report significant changes for tests with meaningful durations (>5s)
            # This filters out noise from very fast tests
            if (abs(pct_change) > 10 or abs(diff) > 10) and (
                old_time >= 5.0 or new_time >= 5.0
            ):
                significant_changes.append(
                    {
                        "test": test_id,
                        "old": old_time,
                        "new": new_time,
                        "diff": diff,
                        "pct": pct_change,
                    }
                )

    # Sort by absolute percentage change
    significant_changes.sort(key=lambda x: abs(x["pct"]), reverse=True)

    return {
        "old_total": old_total,
        "new_total": new_total,
        "total_change_pct": total_change_pct,
        "old_count": len(old_weights),
        "new_count": len(new_weights),
        "significant_changes": significant_changes,
        "new_tests": new_tests,
        "removed_tests": removed_tests,
        "new_tests_total": sum(new_tests.values()),
        "removed_tests_total": sum(removed_tests.values()),
    }


def generate_pr_body(
    pytest_changes: Dict, cypress_changes: Optional[Dict] = None
) -> str:
    """Generate markdown PR body with change analysis."""

    lines = []
    lines.append("## 🤖 Automated Test Weight Update")
    lines.append("")
    lines.append(
        "This PR updates test weights based on recent CI runs to improve batch balancing."
    )
    lines.append("")

    # Overall summary
    lines.append("## 📊 Summary")
    lines.append("")
    lines.append("| Test Type | Old Total | New Total | Change | # Tests |")
    lines.append("|-----------|-----------|-----------|--------|---------|")

    summary_rows: List[tuple[str, Dict]] = [("Pytest", pytest_changes)]
    if cypress_changes is not None:
        summary_rows.insert(0, ("Cypress", cypress_changes))

    for name, changes in summary_rows:
        old_min = changes["old_total"] / 60
        new_min = changes["new_total"] / 60
        change_sign = "+" if changes["total_change_pct"] > 0 else ""
        lines.append(
            f"| {name} | {old_min:.1f} min | {new_min:.1f} min | "
            f"{change_sign}{changes['total_change_pct']:.1f}% | "
            f"{changes['old_count']} → {changes['new_count']} |"
        )

    lines.append("")

    # Warnings for large changes
    warnings = []
    for name, changes in summary_rows:
        if abs(changes["total_change_pct"]) > 20:
            warnings.append(
                f"⚠️ **{name} total time changed by {changes['total_change_pct']:+.1f}%** - "
                f"Consider reviewing batch count configuration!"
            )

    if warnings:
        lines.append("## ⚠️ Warnings")
        lines.append("")
        for warning in warnings:
            lines.append(warning)
        lines.append("")
        lines.append("<details>")
        lines.append("<summary>Batch count recommendations</summary>")
        lines.append("")
        lines.append("Current configuration:")
        if cypress_changes is not None:
            lines.append("- Cypress: 11 batches (Depot) / 5 batches (GitHub runners)")
        lines.append("- Pytest: 7 batches (GitHub runners)")
        lines.append("")
        lines.append(
            "If total time increased >20%, consider increasing batch count to maintain CI speed."
        )
        lines.append(
            "If total time decreased >20%, consider decreasing batch count to save runner costs."
        )
        lines.append("")
        lines.append("Update batch counts in `.github/workflows/docker-unified.yml`")
        lines.append("</details>")
        lines.append("")

    # Significant changes
    def format_significant_changes(changes: Dict, name: str, max_display: int = 15):
        if not changes["significant_changes"]:
            return []

        section = []
        section.append(f"## 🔍 {name} - Significant Changes (>10% or >10s)")
        section.append("")
        section.append("<details>")
        section.append(
            f"<summary>{len(changes['significant_changes'])} tests with significant duration changes</summary>"
        )
        section.append("")

        for item in changes["significant_changes"][:max_display]:
            sign = "+" if item["diff"] > 0 else ""
            emoji = "🔴" if item["diff"] > 0 else "🟢"
            section.append(f"**{emoji} `{item['test']}`**")
            section.append(
                f"- Old: {item['old']:.1f}s → New: {item['new']:.1f}s ({sign}{item['diff']:.1f}s, {sign}{item['pct']:.1f}%)"
            )
            section.append("")

        if len(changes["significant_changes"]) > max_display:
            section.append(
                f"*... and {len(changes['significant_changes']) - max_display} more*"
            )
            section.append("")

        section.append("</details>")
        section.append("")
        return section

    for name, changes in summary_rows:
        lines.extend(format_significant_changes(changes, name))

    # New and removed tests - show summary instead of listing all
    def format_test_changes(changes: Dict, name: str):
        new_tests = changes["new_tests"]
        removed_tests = changes["removed_tests"]
        new_tests_total = changes["new_tests_total"]
        removed_tests_total = changes["removed_tests_total"]

        if not new_tests and not removed_tests:
            return []

        section = []
        section.append(f"## ✨ {name} - Test Changes")
        section.append("")

        if new_tests:
            section.append(
                f"**➕ Added: {len(new_tests)} tests** ({new_tests_total / 60:.1f} min total)"
            )
            section.append("<details>")
            section.append("<summary>View new tests</summary>")
            section.append("")
            for test, duration in sorted(list(new_tests.items())[:20]):
                section.append(f"- `{test}`: {duration:.1f}s")
            if len(new_tests) > 20:
                section.append(f"- *... and {len(new_tests) - 20} more*")
            section.append("")
            section.append("</details>")
            section.append("")

        if removed_tests:
            section.append(
                f"**➖ Removed: {len(removed_tests)} tests** ({removed_tests_total / 60:.1f} min total)"
            )
            section.append("<details>")
            section.append("<summary>View removed tests</summary>")
            section.append("")
            for test, duration in sorted(list(removed_tests.items())[:20]):
                section.append(f"- `{test}`: {duration:.1f}s")
            if len(removed_tests) > 20:
                section.append(f"- *... and {len(removed_tests) - 20} more*")
            section.append("")
            section.append("</details>")
            section.append("")

        return section

    for name, changes in summary_rows:
        lines.extend(format_test_changes(changes, name))

    # Footer
    lines.append("---")
    lines.append("")
    lines.append("*Generated by automated test weight update workflow*")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Compare test weights and generate PR description"
    )
    parser.add_argument(
        "--old-cypress",
        required=False,
        help="Path to old Cypress weights JSON",
    )
    parser.add_argument(
        "--new-cypress",
        required=False,
        help="Path to new Cypress weights JSON",
    )
    parser.add_argument(
        "--old-pytest", required=True, help="Path to old Pytest weights JSON"
    )
    parser.add_argument(
        "--new-pytest", required=True, help="Path to new Pytest weights JSON"
    )
    parser.add_argument(
        "--output", required=True, help="Output file for PR body markdown"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="Minimum total change percentage to trigger PR (default: 5.0)",
    )

    args = parser.parse_args()

    if (args.old_cypress is None) != (args.new_cypress is None):
        print("Error: --old-cypress and --new-cypress must be provided together")
        sys.exit(1)

    old_pytest = load_weights(args.old_pytest, "testId")
    new_pytest = load_weights(args.new_pytest, "testId")
    pytest_changes = calculate_changes(old_pytest, new_pytest)

    cypress_changes = None
    change_pcts = [abs(pytest_changes["total_change_pct"])]
    if args.old_cypress and args.new_cypress:
        old_cypress = load_weights(args.old_cypress, "filePath")
        new_cypress = load_weights(args.new_cypress, "filePath")
        cypress_changes = calculate_changes(old_cypress, new_cypress)
        change_pcts.append(abs(cypress_changes["total_change_pct"]))
        print(f"Cypress total change: {cypress_changes['total_change_pct']:+.2f}%")

    max_change = max(change_pcts)

    print(f"Pytest total change: {pytest_changes['total_change_pct']:+.2f}%")
    print(f"Max change: {max_change:.2f}%")
    print(f"Threshold: {args.threshold}%")

    if max_change < args.threshold:
        print(f"\n✓ Changes below threshold ({args.threshold}%). No PR needed.")
        with open(args.output, "w") as f:
            f.write("")
        sys.exit(0)

    print("\n✓ Changes exceed threshold. Generating PR body...")

    pr_body = generate_pr_body(pytest_changes, cypress_changes)

    with open(args.output, "w") as f:
        f.write(pr_body)

    print(f"✓ PR body written to {args.output}")
    if cypress_changes is not None:
        print(
            "✓ Significant changes: "
            f"Cypress={len(cypress_changes['significant_changes'])}, "
            f"Pytest={len(pytest_changes['significant_changes'])}"
        )
    else:
        print(
            f"✓ Significant changes: Pytest={len(pytest_changes['significant_changes'])}"
        )

    sys.exit(0)


if __name__ == "__main__":
    main()
