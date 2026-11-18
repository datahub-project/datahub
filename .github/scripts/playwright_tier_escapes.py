#!/usr/bin/env python3
"""Report failing Playwright tests that the PR P0 tier would have skipped.

Shadow mode runs the full suite while plan_playwright_tier.py works out what
the tier would have run. When the tier would have narrowed the run (plan
``selectionMode == "p0"``) and this run still ran everything (``runMode == "full"``),
any test that failed here but falls outside the selection is an escape: a
regression the tier would have let merge. This prints those as Markdown for
the job summary; it never fails the job.

A test is in the selection when its spec file runs in full (``wholeFile``:
changed, affected by the change, or seeding from changed data), or when the
file runs only its P0 tests and the test carries ``@p0``.

Input: the plan JSON (playwright-tier-plan artifact) and the run's merged JSON
report (``playwright merge-reports --reporter json``), whose tests carry their
real tags and final status after retries.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

P0_TAG = "p0"  # the JSON report lists tags without their leading '@'


def _tests(
    suite: dict[str, Any], titles: list[str]
) -> Iterator[tuple[str, str, list[str], str]]:
    """(file, title path, tags, status) for every test under a JSON-report suite."""
    for spec in suite.get("specs", []):
        title = " › ".join([*titles, spec["title"]])
        for test in spec.get("tests", []):
            yield (
                spec.get("file", ""),
                title,
                spec.get("tags", []),
                test.get("status", ""),
            )
    for child in suite.get("suites", []):
        yield from _tests(child, [*titles, child["title"]])


def selected(file: str, tags: list[str], plan_specs: dict[str, dict[str, Any]]) -> bool:
    spec = plan_specs.get(file)
    if spec is None:
        return False
    return spec["wholeFile"] or P0_TAG in tags


def escapes(plan: dict[str, Any], report: dict[str, Any]) -> tuple[int, list[str]]:
    """(failing test count, the failing tests outside the tier's selection)."""
    plan_specs = {spec["path"]: spec for spec in plan.get("specs", [])}
    failing, missed = 0, []
    for file_suite in report.get("suites", []):
        for file, title, tags, status in _tests(file_suite, []):
            if status != "unexpected":
                continue
            failing += 1
            if not selected(file, tags, plan_specs):
                missed.append(f"{file} › {title}")
    return failing, missed


def summary_markdown(plan: dict[str, Any], report: dict[str, Any]) -> str:
    if plan.get("runMode") != "full" or plan.get("selectionMode") != "p0":
        return ""
    failing, missed = escapes(plan, report)
    lines = ["## Playwright P0 tier: shadow check", ""]
    if not failing:
        lines.append("No failing tests, so nothing the tier would have missed.")
    elif not missed:
        lines.append(
            f"All {failing} failing test(s) are in the tier's selection; it would have caught them."
        )
    else:
        lines += [
            (
                f"**{len(missed)} of {failing} failing test(s) are outside the tier's selection** "
                "and would not have run on this PR with the tier on:"
            ),
            "",
            *[f"- `{test}`" for test in missed],
        ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Report failing tests the Playwright P0 tier would have skipped."
    )
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        plan = json.loads(args.plan.read_text(encoding="utf-8"))
        report = json.loads(args.report.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"playwright_tier_escapes: skipped ({exc})", file=sys.stderr)
        return 0
    print(summary_markdown(plan, report), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
