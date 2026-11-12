#!/usr/bin/env python3
"""
Compare test weight JSON files and assemble a per-file PR description.

Compare mode writes a stats sidecar (always) and optional GitHub Actions
outputs for PR-create gates. Assemble mode turns one or more sidecars into a
single markdown PR body, grouped by JSON path.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

SIGNIFICANT_DISPLAY_CAP = 15
ADDED_REMOVED_DISPLAY_CAP = 20
WARNING_TOTAL_CHANGE_PCT = 20.0


def load_weights(file_path: str, test_id_key: str) -> Dict[str, float]:
    """Load test weights from JSON file into a dict. Missing file → {}."""
    path = Path(file_path)
    if not path.exists():
        return {}

    with open(path) as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{file_path} must be a JSON array of weight objects")

    return {item[test_id_key]: float(item["duration"].rstrip("s")) for item in data}


def calculate_changes(
    old_weights: Dict[str, float], new_weights: Dict[str, float]
) -> Dict[str, Any]:
    """Calculate comprehensive change statistics."""
    new_test_ids = set(new_weights.keys()) - set(old_weights.keys())
    removed_test_ids = set(old_weights.keys()) - set(new_weights.keys())

    new_tests = {test: new_weights[test] for test in new_test_ids}
    removed_tests = {test: old_weights[test] for test in removed_test_ids}

    old_total = sum(old_weights.values())
    new_total = sum(new_weights.values())
    if old_total > 0:
        total_change_pct = (new_total - old_total) / old_total * 100
    elif new_total > 0:
        total_change_pct = 100.0
    else:
        total_change_pct = 0.0

    significant_changes: List[Dict[str, Any]] = []
    for test_id, new_time in new_weights.items():
        if test_id not in old_weights:
            continue
        old_time = old_weights[test_id]
        diff = new_time - old_time
        pct_change = (diff / old_time * 100) if old_time > 0 else 0.0
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


def build_stats(
    *,
    old_weights: Dict[str, float],
    new_weights: Dict[str, float],
    json_path: str,
    framework: str,
    workflow: str,
    entry_noun: str,
    batch_hint: str,
    threshold: float,
) -> Dict[str, Any]:
    changes = calculate_changes(old_weights, new_weights)
    file_changed = old_weights != new_weights
    exceeds_threshold = abs(changes["total_change_pct"]) >= threshold
    return {
        "file": json_path,
        "framework": framework,
        "workflow": workflow,
        "entry_noun": entry_noun,
        "batch_hint": batch_hint,
        "threshold": threshold,
        "exceeds_threshold": exceeds_threshold,
        "file_changed": file_changed,
        **changes,
    }


def write_stats(stats: Mapping[str, Any], path: str) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(stats, f, indent=2)
        f.write("\n")


def _format_change_pct(pct: float) -> str:
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"


def _format_minutes(seconds: float) -> str:
    return f"{seconds / 60:.1f} min"


def _has_section_details(stats: Mapping[str, Any]) -> bool:
    return bool(
        stats["significant_changes"] or stats["new_tests"] or stats["removed_tests"]
    )


def assemble_pr_body(stats_list: Sequence[Mapping[str, Any]]) -> str:
    """Build one PR body from per-file stats. Unchanged files are omitted."""
    changed = [s for s in stats_list if s.get("file_changed")]
    if not changed:
        return ""

    lines: List[str] = [
        "## Test weight update",
        "",
        "This PR updates committed test-duration weights from recent CI runs so "
        "shard/batch balancing tracks current test times. Each row is one JSON file.",
        "",
        "## Summary",
        "",
        "| File | Framework | Workflow | Old total | New total | Change | Entries |",
        "|------|-----------|----------|-----------|-----------|--------|---------|",
    ]

    for stats in changed:
        lines.append(
            f"| `{stats['file']}` | {stats['framework']} | `{stats['workflow']}` | "
            f"{_format_minutes(stats['old_total'])} | {_format_minutes(stats['new_total'])} | "
            f"{_format_change_pct(stats['total_change_pct'])} | "
            f"{stats['old_count']} → {stats['new_count']} |"
        )

    lines.append("")

    warnings = [
        s for s in changed if abs(s["total_change_pct"]) > WARNING_TOTAL_CHANGE_PCT
    ]
    if warnings:
        lines.append("## Warnings")
        lines.append("")
        for stats in warnings:
            lines.append(
                f"**`{stats['file']}` total time changed by "
                f"{_format_change_pct(stats['total_change_pct'])}** — "
                f"{stats['batch_hint']}"
            )
            lines.append("")
        lines.append("<details>")
        lines.append("<summary>Batch count recommendations</summary>")
        lines.append("")
        lines.append(
            "If total time increased >20%, consider increasing batch/shard count "
            "to maintain CI speed. If it decreased >20%, consider decreasing count "
            "to save runner costs."
        )
        lines.append("")
        lines.append("</details>")
        lines.append("")

    for stats in changed:
        lines.extend(_format_file_section(stats))

    lines.append("---")
    lines.append("")
    lines.append("*Generated by automated test weight update workflow*")
    lines.append("")
    return "\n".join(lines)


def _format_file_section(stats: Mapping[str, Any]) -> List[str]:
    noun = stats["entry_noun"]
    section = [
        f"## `{stats['file']}`",
        "",
        f"{stats['framework']} · `{stats['workflow']}`",
        "",
    ]

    if not _has_section_details(stats):
        section.append(
            f"No {noun} added/removed; no changes above the significant threshold."
        )
        section.append("")
        return section

    significant = stats["significant_changes"]
    if significant:
        section.append("### Significant duration changes (>10% or >10s)")
        section.append("")
        section.append("<details>")
        section.append(
            f"<summary>{len(significant)} {noun} with significant duration changes</summary>"
        )
        section.append("")
        for item in significant[:SIGNIFICANT_DISPLAY_CAP]:
            sign = "+" if item["diff"] > 0 else ""
            section.append(f"**`{item['test']}`**")
            section.append(
                f"- Old: {item['old']:.1f}s → New: {item['new']:.1f}s "
                f"({sign}{item['diff']:.1f}s, {sign}{item['pct']:.1f}%)"
            )
            section.append("")
        extra = len(significant) - SIGNIFICANT_DISPLAY_CAP
        if extra > 0:
            section.append(f"*... and {extra} more*")
            section.append("")
        section.append("</details>")
        section.append("")

    new_tests: Dict[str, float] = stats["new_tests"]
    removed_tests: Dict[str, float] = stats["removed_tests"]

    if new_tests:
        section.append(
            f"**Added: {len(new_tests)} {noun}** "
            f"({_format_minutes(stats['new_tests_total'])} total)"
        )
        section.append("<details>")
        section.append(f"<summary>View new {noun}</summary>")
        section.append("")
        for test, duration in sorted(new_tests.items())[:ADDED_REMOVED_DISPLAY_CAP]:
            section.append(f"- `{test}`: {duration:.1f}s")
        extra = len(new_tests) - ADDED_REMOVED_DISPLAY_CAP
        if extra > 0:
            section.append(f"- *... and {extra} more*")
        section.append("")
        section.append("</details>")
        section.append("")

    if removed_tests:
        section.append(
            f"**Removed: {len(removed_tests)} {noun}** "
            f"({_format_minutes(stats['removed_tests_total'])} total)"
        )
        section.append("<details>")
        section.append(f"<summary>View removed {noun}</summary>")
        section.append("")
        for test, duration in sorted(removed_tests.items())[:ADDED_REMOVED_DISPLAY_CAP]:
            section.append(f"- `{test}`: {duration:.1f}s")
        extra = len(removed_tests) - ADDED_REMOVED_DISPLAY_CAP
        if extra > 0:
            section.append(f"- *... and {extra} more*")
        section.append("")
        section.append("</details>")
        section.append("")

    return section


def _append_github_output(key: str, value: bool) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a") as f:
        f.write(f"{key}={'true' if value else 'false'}\n")


def _run_compare(args: argparse.Namespace) -> int:
    old_weights = load_weights(args.old, args.test_id_key)
    new_weights = load_weights(args.new, args.test_id_key)
    stats = build_stats(
        old_weights=old_weights,
        new_weights=new_weights,
        json_path=args.file,
        framework=args.framework,
        workflow=args.workflow,
        entry_noun=args.entry_noun,
        batch_hint=args.batch_hint,
        threshold=args.threshold,
    )
    write_stats(stats, args.stats_out)

    pr_gate = (
        stats["exceeds_threshold"]
        if args.pr_gate == "threshold"
        else stats["file_changed"]
    )
    if args.github_output_key:
        _append_github_output(args.github_output_key, pr_gate)

    print(f"File: {stats['file']}")
    print(f"Total change: {stats['total_change_pct']:+.2f}%")
    print(f"File changed: {stats['file_changed']}")
    print(f"Exceeds threshold ({args.threshold}%): {stats['exceeds_threshold']}")
    print(f"PR gate ({args.pr_gate}): {pr_gate}")
    print(f"Stats written to {args.stats_out}")
    return 0


def _run_assemble(args: argparse.Namespace) -> int:
    stats_list = []
    for path in args.assemble:
        with open(path) as f:
            stats_list.append(json.load(f))
    body = assemble_pr_body(stats_list)
    with open(args.output, "w") as f:
        f.write(body)
    print(f"Assembled PR body ({len(stats_list)} sidecar(s)) → {args.output}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare test weights and assemble a per-file PR description"
    )
    parser.add_argument(
        "--assemble",
        nargs="+",
        metavar="STATS.json",
        help="Assemble a PR body from one or more stats sidecars",
    )
    parser.add_argument(
        "--old",
        "--old-pytest",
        dest="old",
        help="Path to previous weights JSON",
    )
    parser.add_argument(
        "--new",
        "--new-pytest",
        dest="new",
        help="Path to new weights JSON",
    )
    parser.add_argument(
        "--output",
        help="Output markdown path (assemble mode)",
    )
    parser.add_argument(
        "--stats-out",
        help="Write per-file stats JSON (compare mode; always written)",
    )
    parser.add_argument(
        "--file",
        help="Repo-relative JSON path shown in the PR body",
    )
    parser.add_argument(
        "--framework",
        default="Pytest",
        help="Framework label for the summary table",
    )
    parser.add_argument(
        "--workflow",
        default="",
        help="Workflow filename that produced the artifacts",
    )
    parser.add_argument(
        "--entry-noun",
        default="tests",
        help="Noun for entries (tests, classes, spec files)",
    )
    parser.add_argument(
        "--batch-hint",
        default="",
        help="Shown when |total change| is greater than 20 percent",
    )
    parser.add_argument(
        "--test-id-key",
        default="testId",
        choices=["testId", "filePath"],
        help="JSON key containing the test identifier",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="Minimum |total change %%| for the threshold PR gate (default: 5.0)",
    )
    parser.add_argument(
        "--pr-gate",
        choices=["threshold", "changed"],
        default="threshold",
        help="threshold: pytest 5%% gate. changed: any map difference (Gradle/Playwright).",
    )
    parser.add_argument(
        "--github-output-key",
        help="If set, write this boolean key to GITHUB_OUTPUT using --pr-gate",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.assemble:
        if not args.output:
            parser.error("--output is required with --assemble")
        return _run_assemble(args)

    missing = [
        name
        for name, value in (
            ("--old", args.old),
            ("--new", args.new),
            ("--stats-out", args.stats_out),
            ("--file", args.file),
        )
        if not value
    ]
    if missing:
        parser.error(
            "compare mode requires --old, --new, --stats-out, and --file "
            f"(missing: {', '.join(missing)})"
        )
    return _run_compare(args)


if __name__ == "__main__":
    sys.exit(main())
