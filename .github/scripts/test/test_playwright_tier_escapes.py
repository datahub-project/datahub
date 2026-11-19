"""Tests for the Playwright P0 tier shadow escape report."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import playwright_tier_escapes as esc  # noqa: E402

PLAN = {
    "runMode": "full",
    "selectionMode": "p0",
    "specs": [
        {"path": "ingest/sources.spec.ts", "reasons": ["p0"], "wholeFile": False},
        {"path": "search/search.spec.ts", "reasons": ["changed"], "wholeFile": True},
    ],
}


def _report(*tests: tuple[str, str, list[str], str]) -> dict:
    """A merged JSON report: one file suite > one describe > the given specs."""
    suites = []
    for file, title, tags, status in tests:
        suites.append(
            {
                "title": file,
                "specs": [],
                "suites": [
                    {
                        "title": "Suite",
                        "specs": [
                            {
                                "title": title,
                                "file": file,
                                "tags": tags,
                                "tests": [{"status": status}],
                            }
                        ],
                        "suites": [],
                    }
                ],
            }
        )
    return {"suites": suites}


def test_failures_outside_the_selection_are_escapes() -> None:
    report = _report(
        (
            "ingest/sources.spec.ts",
            "create",
            ["p0", "domain:ingestion"],
            "unexpected",
        ),  # P0 test: selected
        (
            "ingest/sources.spec.ts",
            "search",
            ["domain:ingestion"],
            "unexpected",
        ),  # non-P0 in a P0-only file
        (
            "search/search.spec.ts",
            "finds",
            ["domain:catalog"],
            "unexpected",
        ),  # changed file: selected
        (
            "lineage/graph.spec.ts",
            "draws",
            ["domain:catalog"],
            "unexpected",
        ),  # file not selected
        (
            "lineage/graph.spec.ts",
            "zooms",
            ["domain:catalog"],
            "flaky",
        ),  # passed on retry: not a failure
    )
    failing, missed = esc.escapes(PLAN, report)
    assert failing == 4
    assert missed == [
        "ingest/sources.spec.ts › Suite › search",
        "lineage/graph.spec.ts › Suite › draws",
    ]
    assert "2 of 4 failing test(s) are outside" in esc.summary_markdown(PLAN, report)


def test_all_failures_inside_the_selection() -> None:
    report = _report(("search/search.spec.ts", "finds", [], "unexpected"))
    assert "would have caught them" in esc.summary_markdown(PLAN, report)


@pytest.mark.parametrize("plan_change", [{"runMode": "p0"}, {"selectionMode": "full"}])
def test_nothing_to_report_unless_a_shadow_run_the_tier_would_narrow(
    plan_change: dict,
) -> None:
    report = _report(("lineage/graph.spec.ts", "draws", [], "unexpected"))
    assert esc.summary_markdown({**PLAN, **plan_change}, report) == ""


def test_main_tolerates_missing_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps(PLAN))
    assert (
        esc.main(["--plan", str(plan), "--report", str(tmp_path / "missing.json")]) == 0
    )
    assert capsys.readouterr().out == ""


SHADOW_PLAN = {
    **PLAN,
    "runMode": "p0",
    "smartShadow": {
        "selectionMode": "smart",
        "specs": ["ingest/sources.spec.ts", "search/search.spec.ts"],
        "p0OnlySpecs": ["ingest/sources.spec.ts"],
    },
}
SHADOW_REPORT = _report(
    ("search/search.spec.ts", "finds", [], "unexpected"),  # tagged spec: selected
    ("ingest/sources.spec.ts", "create", ["p0"], "unexpected"),  # @p0 in a P0-only file
    ("ingest/sources.spec.ts", "search", [], "unexpected"),  # not @p0: skipped
    ("lineage/graph.spec.ts", "draws", [], "unexpected"),  # spec not selected
    ("lineage/graph.spec.ts", "zooms", [], "flaky"),  # passed on retry
)


def test_failures_outside_the_shadow_smart_selection_are_false_negatives() -> None:
    assert esc.smart_shadow_misses(SHADOW_PLAN, SHADOW_REPORT) == (
        4,
        [
            "ingest/sources.spec.ts › Suite › search",
            "lineage/graph.spec.ts › Suite › draws",
        ],
    )


@pytest.mark.parametrize(
    "plan",
    [PLAN, {**PLAN, "smartShadow": {"selectionMode": "full", "specs": []}}],
)
def test_no_shadow_check_without_a_narrowing_shadow_selection(plan: dict) -> None:
    assert esc.smart_shadow_misses(plan, SHADOW_REPORT) is None


def test_main_writes_the_summary_and_warns_on_false_negatives(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps(SHADOW_PLAN))
    report = tmp_path / "report.json"
    report.write_text(json.dumps(SHADOW_REPORT))
    summary = tmp_path / "summary.md"
    assert (
        esc.main(
            [
                "--plan",
                str(plan),
                "--report",
                str(report),
                "--summary-out",
                str(summary),
            ]
        )
        == 0
    )
    assert "2 of 4 failing test(s) are outside the smart selection" in summary.read_text()
    out = capsys.readouterr().out
    assert out.startswith("::warning title=Playwright smart selection::")
    assert "lineage/graph.spec.ts › Suite › draws" in out
