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

from utils.clients.gh_client import GitHubAPIClient
from utils.workflow_metrics import JobMetrics, Metrics, WorkflowMetrics


def classify_rerun(attempt: int, jobs: list[dict[str, Any]]) -> str:
    """Classify the rerun type based on the full job list.

    - attempt 1 → "initial"
    - attempt > 1, all jobs have run_attempt == attempt → "full_rerun"
    - attempt > 1, only some jobs have run_attempt == attempt → "partial_rerun"
    """
    if attempt == 1:
        return "initial"
    if all(job.get("run_attempt") == attempt for job in jobs):
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

    # Step 2: Fetch all jobs for this run (filter=latest gives latest version of each job)
    all_jobs = gh.get_run_jobs(args.repo, args.run_id)

    # Step 3: Classify rerun type
    rerun_type = classify_rerun(args.attempt, all_jobs)

    # Step 4: Filter to only jobs that ran in this attempt
    attempt_jobs = [j for j in all_jobs if j.get("run_attempt") == args.attempt]

    # Step 5: Build metrics dataclasses
    metrics = Metrics(
        collected_at=collected_at.isoformat(),
        repository=args.repo,
        workflow=WorkflowMetrics.from_api(run_data, int(args.run_id), args.attempt, rerun_type),
        jobs=[JobMetrics.from_api(j) for j in attempt_jobs],
    )

    # Step 6: Write output
    with Path(args.output).open("w") as f:
        json.dump(asdict(metrics), f, indent=2)

    print(f"Metrics written to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
