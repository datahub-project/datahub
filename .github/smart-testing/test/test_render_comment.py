"""Tests for render_comment.py."""

import json
from pathlib import Path

import render_comment as render
from smart_testing_constants import FAILSAFE_TEXT, PYTEST_OPT_OUT_LABEL, FailSafeReason

PLAN = {
    "batch_count": 2,
    "test_count": 40,
    "candidate_test_count": 400,
    "max_predicted_wall": 540.0,
    "batches": [
        {"scopes": ["tests/e2e/a.py", "tests/e2e/b.py"]},
        {"scopes": ["tests/e2e/b.py", "tests/e2e/c.py"]},
    ],
}

BASE_ENV = {
    "SMART_MODE": "shadow",
    "SMART_CONFIDENCE": "HIGH",
    "FAILSAFE_REASON": "",
    "SMART_TAGS": json.dumps(
        {"confidence": "HIGH", "feature-user": ["lineage", "glossary"]}
    ),
    "SMART_REASONING": "Touches lineage resolvers.",
    "CHANGED_FILES": "4",
    "SMART_SELECTION": "smart",
    "SELECTION_APPLIED": "p0",
    "PLAN_OUTCOME": "success",
}


def with_plan(tmp_path: Path, env: dict) -> dict:
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(PLAN))
    return {**env, "PLAN_FILE": str(plan_file)}


def test_shadow_comment_says_selection_is_not_applied(tmp_path: Path) -> None:
    body = render.render_pytest_section(with_plan(tmp_path, BASE_ENV))
    assert body.startswith("### pytest smoke tests")
    assert "Mode: shadow" in body
    assert "not applied" in body
    assert "40 of 400 tests in 2 batches" in body
    assert "~9 min" in body
    assert "`lineage`, `glossary`" in body
    assert PYTEST_OPT_OUT_LABEL in body


def test_active_comment_reports_applied_selection(tmp_path: Path) -> None:
    body = render.render_pytest_section(
        with_plan(
            tmp_path, {**BASE_ENV, "SMART_MODE": "active", "SELECTION_APPLIED": "smart"}
        )
    )
    assert "Mode: active" in body
    assert "not applied" not in body


def test_failsafe_reason_is_explained() -> None:
    body = render.render_pytest_section(
        {
            **BASE_ENV,
            "SMART_TAGS": "",
            "FAILSAFE_REASON": "low_confidence",
            "SMART_SELECTION": "p0",
            "PLAN_OUTCOME": "failure",
        }
    )
    assert "Fell back to the fail-safe" in body
    assert "low confidence" in body
    assert "- **Tags:** none" in body
    assert "Size:" not in body


def test_missing_plan_and_classifier_outputs_still_render() -> None:
    body = render.render_pytest_section({"SMART_MODE": "shadow"})
    assert "plan did not complete" in body


def test_reasoning_from_the_diff_is_defanged() -> None:
    body = render.render_pytest_section(
        {
            **BASE_ENV,
            "SMART_REASONING": "ping @someone\n<script>x</script> " + "a" * 500,
        }
    )
    assert "@someone" not in body
    assert "<script>" not in body
    reasoning_line = next(line for line in body.splitlines() if "reasoning" in line)
    assert len(reasoning_line) < 400


def test_selected_scopes_are_listed_once_and_sorted(tmp_path: Path) -> None:
    body = render.render_pytest_section(with_plan(tmp_path, BASE_ENV))
    assert "Selected scopes (3 files or classes, 40 tests)" in body
    assert body.count("`tests/e2e/b.py`") == 1
    assert body.index("a.py") < body.index("c.py")


def test_long_module_list_is_capped(tmp_path: Path) -> None:
    plan = {**PLAN, "batches": [{"scopes": [f"tests/t{i:03}.py" for i in range(150)]}]}
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan))
    body = render.render_pytest_section({**BASE_ENV, "PLAN_FILE": str(plan_file)})
    assert "and 50 more" in body


def test_medium_confidence_asks_for_a_check(tmp_path: Path) -> None:
    medium = render.render_pytest_section(
        with_plan(tmp_path, {**BASE_ENV, "SMART_CONFIDENCE": "MEDIUM"})
    )
    assert "Medium confidence" in medium
    assert "Medium confidence" not in render.render_pytest_section(
        with_plan(tmp_path, BASE_ENV)
    )


P0_PLAN = {
    "runMode": "p0",
    "runReason": "P0 tier applied",
    "tierRequested": True,
    "selectionMode": "p0",
    "selectionReason": "3 files changed, all mapped to tests",
    "totalShards": 2,
    "predictedSeconds": 600.0,
    "specs": [
        {
            "path": "search/search.spec.ts",
            "reasons": ["changed", "@p0"],
            "selectedTests": 4,
            "totalTests": 9,
        },
        {
            "path": "lineage/lineage.spec.ts",
            "reasons": ["p0"],
            "selectedTests": 2,
            "totalTests": 2,
        },
    ],
}


def test_playwright_p0_section_lists_specs() -> None:
    body = render.render_playwright_section(P0_PLAN)
    assert "**This run:** `p0`" in body
    assert "6 tests in 2 spec files, ~10 min predicted on 2 shard(s)" in body
    assert "Specs selected because of this PR (1)" in body
    assert "| `search/search.spec.ts` | 4/9 | `changed`, `@p0` |" in body
    assert "lineage/lineage.spec.ts" not in body
    assert "**Baseline:** 1 spec file run for every PR" in body
    assert "run-all-tests:playwright" in body


def test_playwright_shadow_and_full_suite_sections() -> None:
    shadow = render.render_playwright_section(
        {**P0_PLAN, "tierRequested": False, "runMode": "full"}
    )
    assert "Shadow" in shadow
    assert "still runs the full Playwright suite" in shadow
    full = render.render_playwright_section(
        {
            **P0_PLAN,
            "selectionMode": "full",
            "selectionReason": "package.json changed",
            "specs": [],
        }
    )
    assert "package.json changed" in full
    assert "Selected specs" not in full


def test_compose_handles_missing_sections() -> None:
    env = {"SHORT_SHA": "abc1234", "RUN_URL": "https://example.test/run/1"}
    both = render.compose("### pytest smoke tests\n", P0_PLAN, env)
    assert both.startswith(render.MARKER)
    assert "### pytest smoke tests" in both and "### Playwright E2E tests" in both
    assert "commit `abc1234`" in both

    only_playwright = render.compose("", P0_PLAN, env)
    assert "pytest smoke tests" not in only_playwright

    neither = render.compose("", None, env)
    assert "No test selection was computed" in neither


def test_playwright_baseline_only_plan_has_no_table() -> None:
    plan = {**P0_PLAN, "specs": [P0_PLAN["specs"][1]]}
    body = render.render_playwright_section(plan)
    assert "No spec was selected because of this PR" in body
    assert "<details>" not in body


def test_every_failsafe_reason_has_comment_text() -> None:
    assert set(FAILSAFE_TEXT) == set(FailSafeReason)
