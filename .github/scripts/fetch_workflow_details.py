#!/usr/bin/env python3
"""Fetch GitHub Actions workflow run details and write them to a JSON file.

Used by notify-slack-status.yml to gather all data needed for a Slack
notification in one place, without mixing API calls and context expressions.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from utils.datetime_utils import duration_seconds, parse_dt
from utils.github.gh_client import GitHubAPIClient

_FAILED_CONCLUSIONS = {"failure", "timed_out"}


def _build_details(
    run_data: dict[str, Any],
    jobs: list[dict[str, Any]],
    repo: str,
    conclusion_override: str | None,
) -> dict[str, Any]:
    started = parse_dt(run_data.get("run_started_at"))
    completed = parse_dt(run_data.get("updated_at"))
    actor: dict[str, Any] = run_data.get("actor") or {}
    conclusion = conclusion_override or run_data.get("conclusion") or ""
    failed_jobs = [j["name"] for j in jobs if j.get("conclusion") in _FAILED_CONCLUSIONS]

    # display_title is provided by the GitHub API and automatically resolves to:
    # - first line of the commit message for push/PR events
    # - workflow run name for schedule events
    # - release name for release events
    head_commit: dict[str, Any] = run_data.get("head_commit") or {}
    display_title: str = run_data.get("display_title") or head_commit.get("message", "").split("\n")[0]

    return {
        "workflow_name": run_data.get("name", ""),
        "head_branch": run_data.get("head_branch", ""),
        "head_sha": run_data.get("head_sha", ""),
        "actor_login": actor.get("login", ""),
        "run_url": run_data.get("html_url", ""),
        "event": run_data.get("event", ""),
        "conclusion": conclusion,
        "repo": repo,
        "duration_seconds": duration_seconds(started, completed),
        "failed_jobs": failed_jobs,
        "display_title": display_title,
    }


def main() -> int:
    args = _parse_args()

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("ERROR: GITHUB_TOKEN environment variable is required.", file=sys.stderr)
        return 2

    gh = GitHubAPIClient(token)
    run_data = gh.get_run_attempt(args.repo, args.run_id, args.attempt)
    jobs = gh.get_run_attempt_jobs(args.repo, args.run_id, args.attempt)
    details = _build_details(run_data, jobs, args.repo, args.conclusion)

    Path(args.output).write_text(json.dumps(details, indent=2))
    print(f"Workflow details written to {args.output}")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="Workflow run ID")
    parser.add_argument("--attempt", required=True, type=int, help="Run attempt number")
    parser.add_argument("--repo", required=True, help="Repository in owner/repo format")
    parser.add_argument("--output", required=True, help="Path to write the JSON output")
    parser.add_argument(
        "--conclusion",
        help="Override the conclusion from the API (e.g. when a manual input is provided)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
