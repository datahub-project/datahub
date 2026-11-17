"""Names and wording shared by the classifier, the batch planner and the PR comment.

Standard library only, so the comment job can import it without installing the
classifier's requirements.
"""

from enum import Enum, unique
from typing import Dict, Tuple

RUN_ALL_TESTS_LABEL = "run-all-tests"
PYTEST_OPT_OUT_LABEL = "run-all-tests:pytest"
PLAYWRIGHT_OPT_OUT_LABEL = "run-all-tests:playwright"

# Dimensions whose tags select tests. The others only describe or narrow.
FEATURE_DIMENSIONS: Tuple[str, ...] = ("feature-user", "feature-dev", "compound")
# PR-only dimension for non-product changes, and the tags with special handling.
CHANGE_TYPE = "change-type"
PYTEST_PLUMBING = "pytest-plumbing"
TEST_INFRA = "test-infra"


@unique
class FailSafeReason(Enum):
    """Why the classifier produced no trusted tags (empty ``smart_tags``).

    Active mode then runs the p0 fail-safe; shadow mode runs its default tier.
    """

    RUN_ALL_TESTS_LABEL = "run_all_tests_label"
    NO_CHANGED_FILES = "no_changed_files"
    COMPARE_FILE_CAP = "compare_file_cap"
    DRY_RUN = "dry_run"
    LOW_CONFIDENCE = "low_confidence"
    UNKNOWN_FEATURE_TAGS = "unknown_feature_tags"
    LARGE_PR_NO_FEATURE_TAGS = "large_pr_no_feature_tags"
    ERROR = "error"


FAILSAFE_TEXT: Dict[FailSafeReason, str] = {
    FailSafeReason.RUN_ALL_TESTS_LABEL: "the `run-all-tests` label is set",
    FailSafeReason.NO_CHANGED_FILES: "the compare API reported no changed files",
    FailSafeReason.COMPARE_FILE_CAP: "the PR is large enough to hit the compare API's file cap, so its file list may be incomplete",
    FailSafeReason.DRY_RUN: "the classifier ran in dry-run mode",
    FailSafeReason.LOW_CONFIDENCE: "the classifier had low confidence in its tags",
    FailSafeReason.UNKNOWN_FEATURE_TAGS: "the classifier only returned tags that are not in the taxonomy",
    FailSafeReason.LARGE_PR_NO_FEATURE_TAGS: "the PR is large and no product feature was identified, so its blast radius is unknown",
    FailSafeReason.ERROR: "the classifier failed",
}

# The selection names come from selection.py, which the "Plan pytest batches"
# step in docker-unified.yml runs.
SELECTION_TEXT: Dict[str, str] = {
    "smart": "tests for the tagged features, plus always-run modules, this PR's changed tests, and the plumbing sample if the PR changes the pytest harness",
    "plumbing": "always-run modules, this PR's changed tests and the plumbing sample (the PR changes the pytest harness; no product feature identified)",
    "canary": "always-run modules plus this PR's changed tests (no product feature identified)",
    "p0": "the p0 tier (fail-safe)",
    "full": "the full smoke suite",
}
