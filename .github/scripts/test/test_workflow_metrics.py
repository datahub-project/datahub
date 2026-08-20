"""Tests for workflow_metrics.py"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.github.workflow_metrics import JobMetrics, WorkflowMetrics


def test_parse_matrix_normal_job():
    assert JobMetrics._parse_matrix("Build (ubuntu)") == ("Build", ["ubuntu"])


def test_parse_matrix_no_matrix():
    assert JobMetrics._parse_matrix("lint") == ("lint", [])


def test_parse_matrix_reusable_workflow_static_inner_name():
    # Inner job has no trailing parens (SaaS): matrix must still be parsed from
    # the caller segment, not left unparsed because the string lacks a final ")".
    assert JobMetrics._parse_matrix(
        "Playwright E2E Tests (2, 5) / Playwright E2E Tests"
    ) == ("Playwright E2E Tests", ["2", "5"])


def test_parse_matrix_reusable_workflow_dynamic_inner_name():
    # Inner job name itself ends in parens (OSS "(Shard 5/5)"): must not be
    # mistaken for the matrix.
    assert JobMetrics._parse_matrix(
        "Playwright E2E Tests (5, 5) / Playwright E2E Tests (Shard 5/5)"
    ) == ("Playwright E2E Tests", ["5", "5"])


def test_workflow_metrics_base_branch_from_pull_request():
    metrics = WorkflowMetrics.from_api(
        {
            "workflow_id": 1,
            "name": "lint",
            "event": "pull_request",
            "actor": {"login": "alice"},
            "triggering_actor": {"login": "alice"},
            "pull_requests": [
                {
                    "number": 42,
                    "url": "https://api.github.com/repos/org/repo/pulls/42",
                    "base": {"ref": "main"},
                }
            ],
            "head_branch": "feature/foo",
            "head_sha": "abc123",
            "conclusion": "success",
            "created_at": "2026-01-01T00:00:00Z",
            "run_started_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:01:00Z",
        },
        run_id=99,
        attempt=1,
        rerun_type="initial",
    )
    assert metrics.base_branch == "main"
    assert metrics.pull_request_number == 42


def test_workflow_metrics_base_branch_from_pull_request_target():
    metrics = WorkflowMetrics.from_api(
        {
            "workflow_id": 1,
            "name": "lint",
            "event": "pull_request_target",
            "actor": {"login": "alice"},
            "triggering_actor": {"login": "alice"},
            "pull_requests": [
                {
                    "number": 42,
                    "url": "https://api.github.com/repos/org/repo/pulls/42",
                    "base": {"ref": "main"},
                }
            ],
            "head_branch": "feature/foo",
            "head_sha": "abc123",
            "conclusion": "success",
            "created_at": "2026-01-01T00:00:00Z",
            "run_started_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:01:00Z",
        },
        run_id=99,
        attempt=1,
        rerun_type="initial",
    )
    assert metrics.base_branch == "main"


def test_workflow_metrics_base_branch_absent_without_pull_request():
    metrics = WorkflowMetrics.from_api(
        {
            "workflow_id": 1,
            "name": "lint",
            "event": "push",
            "actor": {"login": "alice"},
            "triggering_actor": {"login": "alice"},
            "pull_requests": [],
            "head_branch": "main",
            "head_sha": "abc123",
            "conclusion": "success",
            "created_at": "2026-01-01T00:00:00Z",
            "run_started_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:01:00Z",
        },
        run_id=99,
        attempt=1,
        rerun_type="initial",
    )
    assert metrics.base_branch is None
    assert metrics.pull_request_number is None


def test_workflow_metrics_base_branch_ignored_for_non_pull_request_event():
    # GitHub may associate an open PR with a push run; base_branch stays null.
    metrics = WorkflowMetrics.from_api(
        {
            "workflow_id": 1,
            "name": "lint",
            "event": "push",
            "actor": {"login": "alice"},
            "triggering_actor": {"login": "alice"},
            "pull_requests": [
                {
                    "number": 42,
                    "url": "https://api.github.com/repos/org/repo/pulls/42",
                    "base": {"ref": "main"},
                }
            ],
            "head_branch": "feature/foo",
            "head_sha": "abc123",
            "conclusion": "success",
            "created_at": "2026-01-01T00:00:00Z",
            "run_started_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:01:00Z",
        },
        run_id=99,
        attempt=1,
        rerun_type="initial",
    )
    assert metrics.base_branch is None
    assert metrics.pull_request_number == 42
