#!/usr/bin/env python3
"""Scan connector-tests runs for one dispatched with a specific version input.

Called by check-connector-tests.sh. Reads a JSON array of workflow runs (from
`gh run list --json databaseId,url,status,conclusion,createdAt`) on stdin,
walks them newest-first, fetches each run's set-version job log via `gh api`,
and prints the first run whose executed 'Testing version: <rc-tag>' echo
matches. Prints one JSON object on stdout, or nothing if no match found.

Progress messages go to stderr.

Exits 0 whether a match was found or not — the caller inspects stdout to
decide. Non-zero only on invalid usage.
"""

import json
import re
import subprocess
import sys


USAGE = "Usage: _find_connector_run.py <rc-tag> <repo>"

# Anchor on 'v' or 'http' to skip the script-source echo that contains the
# literal '$VERSION' before shell substitution. We only want executed output.
VERSION_PATTERN = re.compile(r"Testing version: (v[0-9]\S*|https?://\S+)")


def _set_version_job_id(repo: str, run_id: int) -> str:
    """Return the databaseId of the run's set-version job, or '' if absent."""
    res = subprocess.run(
        [
            "gh",
            "api",
            f"/repos/{repo}/actions/runs/{run_id}/jobs",
            "--jq",
            '.jobs[] | select(.name == "set-version") | .id',
        ],
        capture_output=True,
        text=True,
    )
    return res.stdout.strip().split("\n")[0] if res.stdout.strip() else ""


def _job_log(repo: str, job_id: str) -> str | None:
    """Return the job's log as text, or None if gh refuses to fetch it."""
    res = subprocess.run(
        ["gh", "api", f"/repos/{repo}/actions/jobs/{job_id}/logs"],
        capture_output=True,
        text=True,
    )
    return res.stdout if res.returncode == 0 else None


def _find(rc_tag: str, repo: str, runs: list[dict]) -> dict | None:
    """Newest-first scan for a run dispatched with version=<rc_tag>."""
    runs.sort(key=lambda r: r.get("createdAt", ""), reverse=True)
    for idx, run in enumerate(runs, 1):
        run_id = run["databaseId"]
        print(
            f"  [{idx}/{len(runs)}] scanning run {run_id} ...",
            file=sys.stderr,
            flush=True,
        )
        job_id = _set_version_job_id(repo, run_id)
        if not job_id:
            continue
        log = _job_log(repo, job_id)
        if log is None:
            continue
        for line in log.splitlines():
            m = VERSION_PATTERN.search(line)
            if m and m.group(1) == rc_tag:
                return {
                    "run_id": run_id,
                    "url": run["url"],
                    "status": run.get("status", ""),
                    "conclusion": run.get("conclusion") or "",
                    "created_at": run.get("createdAt", ""),
                    "scan_position": idx,
                }
    return None


def main() -> int:
    if len(sys.argv) != 3:
        print(USAGE, file=sys.stderr)
        return 2
    rc_tag, repo = sys.argv[1], sys.argv[2]
    runs = json.loads(sys.stdin.read() or "[]")
    match = _find(rc_tag, repo, runs)
    if match is not None:
        print(json.dumps(match))
    return 0


if __name__ == "__main__":
    sys.exit(main())
