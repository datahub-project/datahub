"""
Send one ``ci_smart_selection_escape_check`` event to PostHog.

Run by post-workflow-actions after ``smart_testing.escape_report`` compared a
post-merge run with the merged PR's last run (``RESULT_FILE``). One event per
check, also when nothing escaped, so the latest ``run_attempt`` per ``run_id``
is that commit's result: a later clean attempt supersedes an earlier flaky one.
Run and PR identifiers are read from the environment (see ``build_properties``).
``TEST_FRAMEWORK`` names the framework checked (``pytest``; later others), the
same value ``ci_smart_selection`` sends as ``framework``.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from send_failed_tests_to_posthog import PostHogConfig, capture_event

EVENT = "ci_smart_selection_escape_check"


def _int(value: str) -> Optional[int]:
    return int(value) if value.isdigit() else None


def build_properties(env: Mapping[str, str], result: Dict[str, Any]) -> Dict[str, Any]:
    """Event properties from the job environment and the escape_report result."""

    def get(name: str) -> str:
        return env.get(name, "").strip()

    escapes = result["escapes"]
    return {
        "run_id": _int(get("RUN_ID")),
        "run_attempt": _int(get("RUN_ATTEMPT")),
        "repository": get("REPOSITORY"),
        "framework": get("TEST_FRAMEWORK"),
        "head_branch": get("HEAD_BRANCH"),
        "head_sha": get("HEAD_SHA"),
        "pr_number": _int(get("PR_NUMBER")),
        "pr_head_sha": get("PR_HEAD_SHA"),
        "pr_run_id": _int(get("PR_RUN_ID")),
        "pr_smart_tags_applied": result["smart_tags_applied"],
        "pr_tests_selected": result["tests_selected"],
        "post_merge_failure_count": len(result["post_merge_failures"]),
        "escape_count": len(escapes),
        "escapes": json.dumps(escapes),
    }


def main() -> None:
    api_key = os.environ.get("POSTHOG_API_KEY", "")
    if not api_key:
        print("⚠️ POSTHOG_API_KEY not configured, skipping escape check event")
        return
    if not os.environ.get("TEST_FRAMEWORK", "").strip():
        print("⚠️ TEST_FRAMEWORK not set, skipping escape check event")
        return
    result_file = Path(os.environ.get("RESULT_FILE", "escape-check.json"))
    if not result_file.is_file():
        print(f"ℹ️ No escape check result at {result_file}, nothing to send")
        return

    properties = build_properties(os.environ, json.loads(result_file.read_text()))
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
