#!/usr/bin/env python3
"""
Send one ``ci_smart_selection`` event to PostHog for a PR's smart test selection.

Run by each test framework's planner job (docker-unified ``smoke_test_matrix``
for pytest) after the classifier and the plan. The classifier fields are the
same for every framework; selection and counts come from ``PLAN_FILE``, the
framework's own plan. Selection and plan outcome come from the planner job
(``SMART_SELECTION``, ``SELECTION_APPLIED``, ``PLAN_OUTCOME``); only the counts
come from ``PLAN_FILE``, the framework's own plan, and only when the plan step
succeeded. Everything is read from the environment (see ``build_properties``),
so a missing classifier output or plan yields empty or null properties rather
than no event.
"""

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, TypeVar

from send_failed_tests_to_posthog import PostHogConfig, capture_event

EVENT = "ci_smart_selection"

# Plan file key -> event property. A framework's plan supplies whichever of
# these it can; the rest are sent as null.
PLAN_COUNTS = {
    "test_count": "tests_selected",
    "candidate_test_count": "tests_total",
    "scope_count": "modules_selected",
    "candidate_scope_count": "modules_total",
    "batch_count": "batch_count",
}


_N = TypeVar("_N", int, float)


def _number(value: str, parse: Callable[[str], _N]) -> Optional[_N]:
    try:
        return parse(value)
    except ValueError:
        return None


def load_plan(path: Path) -> Optional[Dict[str, Any]]:
    """The framework's plan, or None when it is missing or unreadable."""
    try:
        plan = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        print(f"ℹ️ No usable plan at {path}: {e}")
        return None
    return plan if isinstance(plan, dict) else None


def build_properties(
    env: Mapping[str, str], plan: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Event properties from the job environment and the framework's plan."""

    def get(name: str) -> str:
        return env.get(name, "").strip()

    tags = get("SMART_TAGS")
    failsafe_reason = get("FAILSAFE_REASON")
    # The classifier writes failsafe_reason on every path it controls. No tags
    # and no reason means it died before writing outputs (failed import, OOM);
    # the classify step does not fail then, since its output goes through tee.
    if not tags and not failsafe_reason:
        failsafe_reason = "error"
    plan_ok = get("PLAN_OUTCOME") == "success"

    properties: Dict[str, Any] = {
        "run_id": _number(get("GITHUB_RUN_ID"), int),
        "run_attempt": _number(get("GITHUB_RUN_ATTEMPT"), int),
        "repository": get("GITHUB_REPOSITORY"),
        "head_branch": get("HEAD_BRANCH"),
        "base_branch": get("BASE_BRANCH"),
        "head_sha": get("HEAD_SHA"),
        "pr_number": _number(get("PR_NUMBER"), int),
        "framework": get("TEST_FRAMEWORK"),
        "mode": get("SMART_MODE"),
        "confidence": get("SMART_CONFIDENCE"),
        "failsafe_reason": failsafe_reason,
        "tags": tags,
        "proposed_tags": get("PROPOSED_TAGS"),
        "model": get("SMART_MODEL"),
        "classifier_duration_s": _number(get("CLASSIFIER_DURATION_S"), float),
        "selection": get("SMART_SELECTION"),
        "selection_applied": get("SELECTION_APPLIED"),
        "plan_ok": plan_ok,
    }
    # A plan file can exist after a failed plan step (written, then a later
    # command failed); its batches never ran, so its counts are not sent.
    for key, prop in PLAN_COUNTS.items():
        value = plan.get(key) if plan and plan_ok else None
        properties[prop] = value if isinstance(value, int) else None
    return properties


def main() -> None:
    api_key = os.environ.get("POSTHOG_API_KEY", "")
    if not api_key:
        print("⚠️ POSTHOG_API_KEY not configured, skipping smart selection event")
        return
    if not os.environ.get("TEST_FRAMEWORK", "").strip():
        print("⚠️ TEST_FRAMEWORK not set, skipping smart selection event")
        return

    plan_file = os.environ.get("PLAN_FILE", "").strip()
    plan = load_plan(Path(plan_file)) if plan_file else None
    properties = build_properties(os.environ, plan)
    config = PostHogConfig(
        api_key=api_key,
        host=os.environ.get("POSTHOG_HOST") or "https://app.posthog.com",
    )
    print(json.dumps(properties, indent=2, sort_keys=True))
    if capture_event(
        config,
        EVENT,
        properties,
        f"github-actions-{properties['repository'] or 'unknown'}",
    ):
        print(f"✅ Sent {EVENT}")


if __name__ == "__main__":
    main()
