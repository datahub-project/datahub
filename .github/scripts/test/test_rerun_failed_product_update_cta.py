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
                },
                {
                    "databaseId": 22,
                    "headSha": "closedsha",
                    "conclusion": "failure",
                },
            ],
        ),
        patch.object(rerun, "rerun") as rerun_run,
    ):
        assert rerun.rerun_failed_open_prs() == 0
    rerun_run.assert_called_once_with(11)
