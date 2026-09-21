#!/usr/bin/env python3
"""Unit tests for rerun_failed_product_update_cta."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import rerun_failed_product_update_cta as rerun  # noqa: E402


def test_rerun_failed_open_prs_reruns_matching_heads() -> None:
    with (
        patch.object(
            rerun,
            "open_pr_head_shas",
            return_value={"abc123"},
        ),
        patch.object(
            rerun,
            "failed_cta_runs",
            return_value=[
                {
                    "databaseId": 11,
                    "headSha": "abc123",
                    "conclusion": "failure",
                    "ctaJobId": 99,
                },
                {
                    "databaseId": 22,
                    "headSha": "closedsha",
                    "conclusion": "failure",
                    "ctaJobId": 100,
                },
            ],
        ),
        patch.object(rerun, "rerun") as rerun_run,
    ):
        assert rerun.rerun_failed_open_prs() == 0
    rerun_run.assert_called_once_with(99)


def test_cta_failed_job_id_ignores_other_lint_jobs() -> None:
    with patch.object(
        rerun,
        "gh_json",
        return_value={
            "jobs": [
                {
                    "name": "python-lint",
                    "conclusion": "failure",
                    "databaseId": 1,
                },
                {
                    "name": "product_update_release_sync",
                    "conclusion": "success",
                    "databaseId": 2,
                },
            ]
        },
    ):
        assert rerun.cta_failed_job_id(11) is None


def test_cta_failed_job_id_returns_sync_job() -> None:
    with patch.object(
        rerun,
        "gh_json",
        return_value={
            "jobs": [
                {
                    "name": "product_update_release_sync",
                    "conclusion": "failure",
                    "databaseId": 7,
                }
            ]
        },
    ):
        assert rerun.cta_failed_job_id(11) == 7
