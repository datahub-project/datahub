"""Name the pytest smoke selection that a classifier result plans.

Reads ``SMOKE_SMART_TAGS`` (``classify_pr.py``'s ``smart_tags`` output) and
prints one of:

    p0        no tags: the classifier failed, had LOW confidence, or the PR is
              large without feature tags. The p0 fail-safe.
    smart     feature tags: tag-matched modules from the full tier.
    plumbing  no feature tags, but the PR changes the shared pytest harness
              (change-type: pytest-plumbing): always-run modules, the PR's
              changed tests and the plumbing sample.
    canary    no feature tags: always-run modules plus the PR's changed tests.

``plan_pytest_batches.sh`` runs it in CI. Standard library only.
"""

import json
import os

from smart_testing_constants import CHANGE_TYPE, FEATURE_DIMENSIONS, PYTEST_PLUMBING


def selection_for(smart_tags: str) -> str:
    if not smart_tags.strip():
        return "p0"
    tags = json.loads(smart_tags)
    if not isinstance(tags, dict):
        raise ValueError(f"smart_tags must be a JSON object, got {smart_tags!r}")
    if any(tags.get(dim) for dim in FEATURE_DIMENSIONS):
        return "smart"
    if PYTEST_PLUMBING in (tags.get(CHANGE_TYPE) or []):
        return "plumbing"
    return "canary"


def main() -> None:
    print(selection_for(os.environ.get("SMOKE_SMART_TAGS", "")))


if __name__ == "__main__":
    main()
