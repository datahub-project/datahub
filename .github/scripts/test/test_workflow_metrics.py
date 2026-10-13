"""Tests for workflow_metrics.py"""

import sys
from pathlib import Path

import pytest

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


# Shared GitHub "workflow run attempt" API payload. Individual tests override only
# the keys they exercise, so a change to the API shape lands in one place.
_REPOSITORY = "org/repo"

_PR_ASSOCIATION = [
    {
        "number": 42,
        "url": "https://api.github.com/repos/org/repo/pulls/42",
        "base": {"ref": "main"},
    }
]

_BASE_RUN = {
    "workflow_id": 1,
    "name": "lint",
    "event": "pull_request",
    "actor": {"login": "alice"},
    "triggering_actor": {"login": "alice"},
    "pull_requests": _PR_ASSOCIATION,
    "head_repository": {"full_name": _REPOSITORY},
    "head_branch": "feature/foo",
    "head_sha": "abc123",
    "conclusion": "success",
    "created_at": "2026-01-01T00:00:00Z",
    "run_started_at": "2026-01-01T00:00:00Z",
    "updated_at": "2026-01-01T00:01:00Z",
}


def _make_metrics(**overrides):
    """Build WorkflowMetrics from the shared run payload with the given overrides."""
    return WorkflowMetrics.from_api(
        {**_BASE_RUN, **overrides},
        run_id=99,
        attempt=1,
        rerun_type="initial",
        repository=_REPOSITORY,
    )


@pytest.mark.parametrize(
    "event,pull_requests,expected_base_branch,expected_pr_number",
    [
        # PR triggers with an associated open PR: base.ref is the PR base branch.
        ("pull_request", _PR_ASSOCIATION, "main", 42),
        ("pull_request_target", _PR_ASSOCIATION, "main", 42),
        # GitHub returns an empty pull_requests array for fork PRs, and for PRs
        # already closed/merged by the time metrics are collected — the array
        # only ever lists *open*, same-repository PRs. Both yield a null base.
        ("pull_request", [], None, None),
        ("pull_request_target", [], None, None),
        # GitHub may associate an open PR with a push run; base_branch stays null
        # because the run was not PR-triggered.
        ("push", _PR_ASSOCIATION, None, 42),
        ("push", [], None, None),
    ],
)
def test_workflow_metrics_base_branch(
    event, pull_requests, expected_base_branch, expected_pr_number
):
    metrics = _make_metrics(event=event, pull_requests=pull_requests)
    assert metrics.base_branch == expected_base_branch
    assert metrics.pull_request_number == expected_pr_number


@pytest.mark.parametrize(
    "event,head_repository,expected",
    [
        # PR-triggered run whose head repo differs from the workflow repo: fork PR.
        ("pull_request", {"full_name": "contributor/repo"}, True),
        ("pull_request_target", {"full_name": "contributor/repo"}, True),
        # Same-repo PR branch.
        ("pull_request", {"full_name": _REPOSITORY}, False),
        # Not PR-triggered, so never a community contribution.
        ("push", {"full_name": "contributor/repo"}, False),
        # head_repository can be absent (e.g. the fork was deleted).
        ("pull_request", None, False),
    ],
)
def test_workflow_metrics_is_community_contribution(event, head_repository, expected):
    metrics = _make_metrics(event=event, head_repository=head_repository)
    assert metrics.is_community_contribution is expected
