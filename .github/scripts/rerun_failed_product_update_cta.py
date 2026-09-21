#!/usr/bin/env python3
"""Re-run failed product_update_release_sync pull_request jobs for open PRs.

Used by the 15-minute cron so a CTA that 404'd (unpublished blog) turns green
shortly after the URL starts returning 2xx, without a human clicking Re-run.

CTA probing lives in lint's product_update_release_sync job. Only that job is
re-run — a failed python-lint / spotless-check lint run is left alone.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any, Iterable

WORKFLOW_FILE = "lint-jobs.yml"
EVENT = "pull_request"
CTA_JOB_NAME = "product_update_release_sync"


def gh_json(args: list[str]) -> Any:
    result = subprocess.run(
        ["gh", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    if not result.stdout.strip():
        return None
    return json.loads(result.stdout)


def open_pr_head_shas() -> set[str]:
    prs = gh_json(
        [
            "pr",
            "list",
            "--state",
            "open",
            "--limit",
            "200",
            "--json",
            "headRefOid",
        ]
    )
    if not isinstance(prs, list):
        return set()
    return {str(pr["headRefOid"]) for pr in prs if pr.get("headRefOid")}


def cta_failed_job_id(run_id: int) -> int | None:
    data = gh_json(["run", "view", str(run_id), "--json", "jobs"])
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(jobs, list):
        return None
    for job in jobs:
        if job.get("name") != CTA_JOB_NAME or job.get("conclusion") != "failure":
            continue
        job_id = job.get("databaseId")
        if job_id is not None:
            return int(job_id)
    return None


def failed_cta_runs() -> list[dict[str, Any]]:
    runs = gh_json(
        [
            "run",
            "list",
            "--workflow",
            WORKFLOW_FILE,
            "--event",
            EVENT,
            "--status",
            "failure",
            "--limit",
            "50",
            "--json",
            "databaseId,headSha,status,conclusion",
        ]
    )
    if not isinstance(runs, list):
        return []
    matched: list[dict[str, Any]] = []
    for run in runs:
        if run.get("conclusion") != "failure":
            continue
        run_id = run.get("databaseId")
        if run_id is None:
            continue
        job_id = cta_failed_job_id(int(run_id))
        if job_id is None:
            continue
        entry = dict(run)
        entry["ctaJobId"] = job_id
        matched.append(entry)
    return matched


def rerun(job_id: int) -> None:
    subprocess.run(
        ["gh", "run", "rerun", "--job", str(job_id)],
        check=True,
    )


def rerun_failed_open_prs() -> int:
    heads = open_pr_head_shas()
    reran = 0
    skipped = 0
    for run in failed_cta_runs():
        sha = str(run.get("headSha") or "")
        job_id = run.get("ctaJobId")
        if job_id is None:
            continue
        if sha not in heads:
            skipped += 1
            continue
        print(
            f"Re-running failed {CTA_JOB_NAME} job {job_id} "
            f"(run {run.get('databaseId')}) for {sha}"
        )
        rerun(int(job_id))
        reran += 1
    print(f"Re-ran {reran} failed CTA check(s); skipped {skipped} closed-PR run(s).")
    return 0


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    parse_args(argv)
    try:
        return rerun_failed_open_prs()
    except (subprocess.CalledProcessError, json.JSONDecodeError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            print(exc.stderr, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
