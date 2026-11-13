"""Tests for send_smart_selection_escapes_to_posthog.py."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

import send_smart_selection_escapes_to_posthog as sender

ENV = {
    "RUN_ID": "100",
    "RUN_ATTEMPT": "2",
    "REPOSITORY": "org/repo",
    "TEST_FRAMEWORK": "pytest",
    "HEAD_BRANCH": "main",
    "HEAD_SHA": "merge123",
    "PR_NUMBER": "7",
    "PR_HEAD_SHA": "head456",
    "PR_RUN_ID": "90",
}

RESULT = {
    "post_merge_failures": ["tests.e2e.a::test_a", "tests.e2e.b::test_b"],
    "escapes": ["tests.e2e.b::test_b"],
    "smart_tags_applied": False,
    "tests_selected": 40,
}


def test_properties_from_result() -> None:
    props = sender.build_properties(ENV, RESULT)
    assert props["run_id"] == 100 and props["run_attempt"] == 2
    assert props["framework"] == "pytest"
    assert props["head_branch"] == "main" and props["head_sha"] == "merge123"
    assert props["pr_number"] == 7 and props["pr_run_id"] == 90
    assert props["pr_head_sha"] == "head456"
    assert props["pr_smart_tags_applied"] is False
    assert props["pr_tests_selected"] == 40
    assert props["post_merge_failure_count"] == 2
    assert props["escape_count"] == 1
    assert json.loads(props["escapes"]) == ["tests.e2e.b::test_b"]


def test_clean_check_still_sends(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result_file = tmp_path / "escape-check.json"
    result_file.write_text(json.dumps(RESULT | {"post_merge_failures": [], "escapes": []}))
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("POSTHOG_API_KEY", "key")
    monkeypatch.setenv("RESULT_FILE", str(result_file))

    with patch.object(sender, "capture_event", return_value=True) as capture:
        sender.main()

    _, event, props, distinct_id = capture.call_args.args
    assert event == sender.EVENT and distinct_id == "github-actions-org/repo"
    assert props["escape_count"] == 0 and props["post_merge_failure_count"] == 0


@pytest.mark.parametrize(
    "api_key, framework, has_result",
    [("", "pytest", True), ("key", "", True), ("key", "pytest", False)],
)
def test_sends_nothing_without_key_framework_or_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    api_key: str,
    framework: str,
    has_result: bool,
) -> None:
    result_file = tmp_path / "escape-check.json"
    if has_result:
        result_file.write_text(json.dumps(RESULT))
    monkeypatch.setenv("POSTHOG_API_KEY", api_key)
    monkeypatch.setenv("TEST_FRAMEWORK", framework)
    monkeypatch.setenv("RESULT_FILE", str(result_file))

    with patch.object(sender, "capture_event") as capture:
        sender.main()

    capture.assert_not_called()
