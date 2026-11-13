"""Tests for send_smart_selection_to_posthog.py."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

import send_smart_selection_to_posthog as sender

BASE_ENV = {
    "TEST_FRAMEWORK": "pytest",
    "GITHUB_REPOSITORY": "org/repo",
    "GITHUB_RUN_ID": "123",
    "GITHUB_RUN_ATTEMPT": "2",
    "HEAD_BRANCH": "feature",
    "BASE_BRANCH": "main",
    "HEAD_SHA": "abc123",
    "PR_NUMBER": "7",
    "SMART_MODE": "active",
    "SMART_CONFIDENCE": "HIGH",
    "FAILSAFE_REASON": "",
    "SMART_TAGS": '{"confidence": "HIGH", "feature-user": ["lineage"]}',
    "PROPOSED_TAGS": '{"confidence": "HIGH", "feature-user": ["lineage"]}',
    "SMART_MODEL": "claude-haiku-5-5",
    "CLASSIFIER_DURATION_S": "4.2",
    "SMART_SELECTION": "smart",
    "SELECTION_APPLIED": "canary",
    "PLAN_OUTCOME": "success",
}

PLAN = {
    "batch_count": 2,
    "test_count": 40,
    "candidate_test_count": 400,
    "scope_count": 3,
    "candidate_scope_count": 30,
    "batches": [{"scopes": ["tests/e2e/a.py"]}],
}


def test_properties_from_successful_plan() -> None:
    props = sender.build_properties(BASE_ENV, PLAN)
    assert props["framework"] == "pytest"
    assert props["tests_selected"] == 40 and props["tests_total"] == 400
    assert props["modules_selected"] == 3 and props["modules_total"] == 30
    assert props["batch_count"] == 2 and props["plan_ok"] is True
    assert props["run_id"] == 123 and props["run_attempt"] == 2
    assert props["pr_number"] == 7
    assert props["classifier_duration_s"] == 4.2
    assert props["mode"] == "active" and props["confidence"] == "HIGH"
    assert props["model"] == "claude-haiku-5-5"
    assert props["selection"] == "smart" and props["selection_applied"] == "canary"
    assert props["failsafe_reason"] == ""
    assert "selected_scopes" not in props and "batches" not in props


def test_properties_carry_proposed_tags_on_fallback() -> None:
    props = sender.build_properties(
        BASE_ENV
        | {
            "FAILSAFE_REASON": "low_confidence",
            "SMART_CONFIDENCE": "LOW",
            "SMART_TAGS": "",
            "PROPOSED_TAGS": '{"confidence": "LOW", "feature-dev": ["executor"]}',
        },
        PLAN,
    )
    assert props["tags"] == ""
    assert props["failsafe_reason"] == "low_confidence"
    assert json.loads(props["proposed_tags"])["feature-dev"] == ["executor"]


def test_properties_without_plan_have_null_counts() -> None:
    props = sender.build_properties(BASE_ENV | {"PLAN_OUTCOME": "failure"}, None)
    assert props["plan_ok"] is False
    assert props["tests_selected"] is None and props["batch_count"] is None


def test_missing_classifier_outputs_default_to_empty() -> None:
    env = {
        k: v
        for k, v in BASE_ENV.items()
        if k not in ("FAILSAFE_REASON", "PROPOSED_TAGS", "CLASSIFIER_DURATION_S")
    }
    props = sender.build_properties(env, None)
    assert props["failsafe_reason"] == ""
    assert props["proposed_tags"] == ""
    assert props["classifier_duration_s"] is None


def test_load_plan_returns_none_for_missing_or_bad_json(tmp_path: Path) -> None:
    assert sender.load_plan(tmp_path / "missing.json") is None
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    assert sender.load_plan(bad) is None
    good = tmp_path / "plan.json"
    good.write_text(json.dumps(PLAN))
    assert sender.load_plan(good) == PLAN


@pytest.mark.parametrize("missing", ["POSTHOG_API_KEY", "TEST_FRAMEWORK"])
def test_main_without_required_env_sends_nothing(
    monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    for name, value in (BASE_ENV | {"POSTHOG_API_KEY": "phc_test"}).items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv(missing)
    with patch.object(sender, "capture_event") as capture:
        sender.main()
    capture.assert_not_called()


def test_main_sends_event_with_plan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps(PLAN))
    for name, value in (
        BASE_ENV | {"POSTHOG_API_KEY": "phc_test", "PLAN_FILE": str(plan)}
    ).items():
        monkeypatch.setenv(name, value)
    with patch.object(sender, "capture_event", return_value=True) as capture:
        sender.main()
    _, event, properties, distinct_id = capture.call_args.args
    assert event == "ci_smart_selection"
    assert distinct_id == "github-actions-org/repo"
    assert properties["tests_selected"] == 40


def test_counts_from_a_failed_plan_are_not_sent() -> None:
    props = sender.build_properties(BASE_ENV | {"PLAN_OUTCOME": "failure"}, PLAN)
    assert props["plan_ok"] is False
    assert props["tests_selected"] is None and props["batch_count"] is None


def test_classifier_crash_without_outputs_is_reported_as_error() -> None:
    env = BASE_ENV | {"SMART_TAGS": "", "FAILSAFE_REASON": "", "SMART_CONFIDENCE": ""}
    assert sender.build_properties(env, None)["failsafe_reason"] == "error"


def test_main_without_repository_uses_unknown_distinct_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in (BASE_ENV | {"POSTHOG_API_KEY": "phc_test"}).items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("GITHUB_REPOSITORY")
    with patch.object(sender, "capture_event", return_value=True) as capture:
        sender.main()
    assert capture.call_args.args[3] == "github-actions-unknown"
