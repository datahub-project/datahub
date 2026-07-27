"""Tests for workflow_metrics.py"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.github.workflow_metrics import JobMetrics


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
