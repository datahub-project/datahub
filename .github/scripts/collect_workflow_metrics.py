#!/usr/bin/env python3
"""Collect GitHub Actions workflow metrics.

Fetches workflow run and job data from the GitHub REST API,
computes timing metrics, and writes the results as JSON.
"""

import argparse
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils.datetime_utils import parse_dt
from utils.github.gh_client import GitHubAPIClient
from utils.github.workflow_metrics import JobMetrics, Metrics, WorkflowMetrics


def classify_rerun(
    attempt: int, attempt_started_at: str, jobs: list[dict[str, Any]]
) -> str:
    """Classify the rerun type based on job start times vs attempt start time.

    - attempt 1 → "initial"
    - attempt > 1, all runner jobs started at or after attempt_started_at → "full_rerun"
    - attempt > 1, any runner job started before attempt_started_at → "partial_rerun"

    Jobs without a runner (skipped) are excluded — they don't have reliable start times.
    Carry-over jobs from prior attempts retain their original started_at, which predates
    the current attempt's start time, making them the correct signal for partial reruns.
    """
    if attempt == 1:
        return "initial"
    attempt_start = parse_dt(attempt_started_at)
    if attempt_start is None:
        return "full_rerun"
    ran_jobs = [
        j for j in jobs if j.get("runner_name") and j.get("started_at") is not None
    ]
    if not ran_jobs or all(
        parse_dt(j["started_at"]) >= attempt_start  # type: ignore[operator]
        for j in ran_jobs
    ):
        return "full_rerun"
    return "partial_rerun"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect GitHub Actions workflow metrics"
    )
    parser.add_argument("--run-id", required=True, help="Workflow run ID")
    parser.add_argument("--attempt", required=True, type=int, help="Run attempt number")
    parser.add_argument("--repo", required=True, help="Repository in owner/repo format")
    parser.add_argument(
        "--output", default="metrics.json", help="Output JSON file path"
    )
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("Error: GITHUB_TOKEN environment variable is not set", file=sys.stderr)
        sys.exit(1)

    collected_at = datetime.now(timezone.utc)
    gh = GitHubAPIClient(token)

    # Step 1: Fetch workflow run attempt
    run_data = gh.get_run_attempt(args.repo, args.run_id, args.attempt)

    # Step 2: Fetch jobs for this attempt — carry-over jobs from prior attempts retain
    # their original started_at, which predates the attempt's run_started_at and is
    # the signal used to classify partial vs full reruns.
    attempt_jobs = gh.get_run_attempt_jobs(args.repo, args.run_id, args.attempt)

    # Step 3: Classify rerun type
    rerun_type = classify_rerun(
        args.attempt, run_data.get("run_started_at", ""), attempt_jobs
    )

    # Step 4: Build metrics dataclasses
    metrics = Metrics(
        collected_at=collected_at.isoformat(),
        repository=args.repo,
        workflow=WorkflowMetrics.from_api(
            run_data, int(args.run_id), args.attempt, rerun_type
        ),
        jobs=[JobMetrics.from_api(j) for j in attempt_jobs],
    )

    # Step 5: Write output
    with Path(args.output).open("w") as f:
        json.dump(asdict(metrics), f, indent=2)

    print(f"Metrics written to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
