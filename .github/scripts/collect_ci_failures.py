#!/usr/bin/env python3
"""
Collect CI failures from recent workflow runs.

This script:
1. Loads previous check state
2. Queries GitHub API for workflow runs since last check
3. Calculates analysis window: max(5, runs_since_last_check)
4. Downloads test artifacts from runs (successful and failed)
5. Saves updated state for next run
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Configuration thresholds - adjust these to tune the analysis
FAILED_RUNS_PER_WORKFLOW = 10  # Analyze last N failed runs per workflow (all branches)


def run_gh_command(args: List[str]) -> str:
    """Run a GitHub CLI command and return stdout."""
    try:
        result = subprocess.run(
            ["gh"] + args,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running gh command: {e}", file=sys.stderr)
        print(f"stderr: {e.stderr}", file=sys.stderr)
        raise


def get_state_filename(repository: str) -> str:
    """Get state filename based on repository."""
    # Convert repository name to safe filename
    repo_slug = repository.replace("/", "_").replace("-", "_")
    return f".last_check_state_{repo_slug}.json"


def load_previous_state(repository: str) -> Dict[str, Any]:
    """Load the previous check state from file."""
    state_file = Path(get_state_filename(repository))
    if state_file.exists():
        print("Loading previous state...")
        with open(state_file, "r") as f:
            state = json.load(f)
            print(f"  Last check: {state.get('last_check_timestamp', 'unknown')}")
            return state
    else:
        print("No previous state found. This is the first run.")
        return {}


def save_state(state: Dict[str, Any], repository: str) -> None:
    """Save the check state to file."""
    state_file = Path(get_state_filename(repository))
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)
    print(f"State saved to {state_file}")


def get_workflow_runs(
    repository: str,
    workflow_name: str,
    since_timestamp: Optional[str] = None,
    limit: int = 100,
    failed_only: bool = False,
    branch: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get workflow runs from GitHub API (all branches or specific branch, optionally filtered by status)."""
    print(f"Fetching runs for {workflow_name}...")

    # Query GitHub API for workflow runs
    api_path = f"repos/{repository}/actions/workflows/{workflow_name}/runs"
    params = f"per_page={limit}"
    if branch:
        params += f"&branch={branch}"

    output = run_gh_command(["api", f"{api_path}?{params}"])
    data = json.loads(output)

    runs = data.get("workflow_runs", [])

    # Filter by timestamp if provided
    if since_timestamp:
        since_dt = datetime.fromisoformat(since_timestamp.replace("Z", "+00:00"))
        filtered_runs = []
        for run in runs:
            run_dt = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
            if run_dt > since_dt:
                filtered_runs.append(run)
        runs = filtered_runs

    # Filter for failed runs only if requested
    if failed_only:
        runs = [r for r in runs if r.get("conclusion") == "failure"]

    print(f"  Found {len(runs)} runs since last check")
    return runs


def get_job_details(repository: str, run_id: int) -> List[Dict[str, Any]]:
    """Get job details for a workflow run."""
    api_path = f"repos/{repository}/actions/runs/{run_id}/jobs"
    output = run_gh_command(["api", api_path])
    data = json.loads(output)
    return data.get("jobs", [])


def artifact_matches_failed_job(artifact_name: str, failed_jobs: List[Dict]) -> bool:
    """Check if artifact belongs to a failed job."""
    # Extract key identifiers from artifact name
    artifact_lower = artifact_name.lower()

    for job in failed_jobs:
        job_name_lower = job["name"].lower()

        # Match cypress batch artifacts
        if "cypress" in artifact_lower and "cypress" in job_name_lower:
            batch_num = extract_batch_number(artifact_name, "cypress")
            if f"batch {batch_num}" in job_name_lower or f"{batch_num}/" in job_name_lower:
                return True

        # Match pytest batch artifacts
        if "pytests" in artifact_lower and "pytest" in job_name_lower:
            batch_num = extract_batch_number(artifact_name, "pytests")
            if f"batch {batch_num}" in job_name_lower or f"{batch_num}/" in job_name_lower:
                return True

        # Match metadata-io artifacts (artifact contains "metadata-io", job name is "build")
        if "metadata-io" in artifact_lower and "build" in job_name_lower:
            return True

        # Match build artifacts (build-and-test.yml and other workflows)
        if "build" in artifact_lower and "build" in job_name_lower:
            return True

    return False


def download_artifacts(
    repository: str, run_id: int, output_dir: Path, jobs: List[Dict[str, Any]]
) -> Dict[str, Path]:
    """Download test artifacts only from failed jobs."""
    run_dir = output_dir / f"run-{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Find failed jobs
    failed_jobs = [job for job in jobs if job.get("conclusion") == "failure"]

    if not failed_jobs:
        print(f"    No failed jobs in run {run_id}")
        return {}

    # Get list of artifacts
    api_path = f"repos/{repository}/actions/runs/{run_id}/artifacts"
    try:
        output = run_gh_command(["api", api_path])
        data = json.loads(output)
        artifacts = data.get("artifacts", [])
    except Exception as e:
        print(f"    ✗ Failed to get artifacts list: {e}")
        return {}

    # Filter for test result artifacts from failed jobs that haven't expired
    test_artifacts = [
        a
        for a in artifacts
        if ("Test Results" in a["name"] or "junit" in a["name"].lower())
        and not a.get("expired", False)
        and artifact_matches_failed_job(a["name"], failed_jobs)
    ]

    if not test_artifacts:
        expired_count = len([a for a in artifacts if a.get("expired", False)])
        if expired_count > 0:
            print(f"    ⚠ All {expired_count} test artifacts have expired for run {run_id}")
        else:
            print(f"    No test artifacts from failed jobs for run {run_id}")
        return {}

    downloaded = {}
    for artifact in test_artifacts:
        artifact_name = artifact["name"]
        artifact_id = artifact["id"]

        # Determine subdirectory based on artifact name
        if "pytests" in artifact_name:
            batch = extract_batch_number(artifact_name, "pytests")
            artifact_dir = run_dir / f"pytests-{batch}"
        elif "cypress" in artifact_name:
            batch = extract_batch_number(artifact_name, "cypress")
            artifact_dir = run_dir / f"cypress-{batch}"
        elif "metadata-io" in artifact_name:
            artifact_dir = run_dir / "metadata-io"
        elif "build" in artifact_name:
            # Extract command and timezone from build artifact name
            # Format: "Test Results (build) - <command>-<timezone>"
            parts = artifact_name.replace("Test Results (build) - ", "")
            artifact_dir = run_dir / f"build-{parts}"
        else:
            artifact_dir = run_dir / "other"

        artifact_dir.mkdir(parents=True, exist_ok=True)

        # Download artifact
        zip_path = artifact_dir / "artifact.zip"
        try:
            api_path = f"repos/{repository}/actions/artifacts/{artifact_id}/zip"

            # gh api returns binary data, need to write it properly
            result = subprocess.run(
                ["gh", "api", api_path],
                check=False,  # Don't raise on non-zero exit
                capture_output=True,
            )

            # Check if artifact expired (HTTP 410)
            if result.returncode != 0:
                stderr = result.stderr.decode("utf-8", errors="ignore")
                if "410" in stderr or "expired" in stderr.lower():
                    print(f"    ⚠ Artifact expired: {artifact_name}")
                    continue
                else:
                    print(f"    ✗ Failed to download: {artifact_name} - {stderr.strip()}")
                    continue

            with open(zip_path, "wb") as f:
                f.write(result.stdout)

            # Unzip only XML files to save space and time
            # Use unzip with wildcard to extract only *.xml files
            unzip_result = subprocess.run(
                ["unzip", "-q", "-o", str(zip_path), "*.xml", "-d", str(artifact_dir)],
                check=False,
                capture_output=True,
            )

            # unzip returns exit code 11 if no files match the pattern, which is acceptable
            if unzip_result.returncode == 0 or unzip_result.returncode == 11:
                zip_path.unlink()

                # Check if any XML files were extracted
                xml_files = list(artifact_dir.rglob("*.xml"))

                if xml_files:
                    downloaded[artifact_name] = artifact_dir
                    print(f"    ✓ Downloaded: {artifact_name} ({len(xml_files)} XML files)")
                else:
                    print(f"    ⚠ No XML files found in: {artifact_name}")
            else:
                print(f"    ✗ Failed to extract: {artifact_name}")
                if zip_path.exists():
                    zip_path.unlink()

        except Exception as e:
            print(f"    ✗ Error downloading {artifact_name}: {e}")
            if zip_path.exists():
                zip_path.unlink()

    return downloaded


def extract_batch_number(artifact_name: str, test_type: str) -> int:
    """Extract batch number from artifact name."""
    import re

    pattern = rf"{test_type}\s+(\d+)"
    match = re.search(pattern, artifact_name)
    if match:
        return int(match.group(1))
    return 0


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Collect CI failures from workflow runs")
    parser.add_argument(
        "--repository",
        default=os.environ.get("GITHUB_REPOSITORY", "acryldata/datahub-fork"),
        help="Repository in owner/name format (default: acryldata/datahub-fork)",
    )
    parser.add_argument(
        "--branch",
        default="acryl-main",
        help="Branch to check for new runs (default: acryl-main)",
    )
    args = parser.parse_args()

    repository = args.repository
    main_branch = args.branch
    force_analysis = os.environ.get("FORCE_ANALYSIS", "false").lower() == "true"

    print(f"Monitoring repository: {repository}")
    print(f"Main branch: {main_branch}")

    # Workflows to monitor
    workflows = [
        "docker-unified.yml",
        "metadata-io.yml",
        "build-and-test.yml",
    ]

    # Load previous state
    previous_state = load_previous_state(repository)
    last_check_timestamp = previous_state.get("last_check_timestamp")

    # Current timestamp
    current_timestamp = datetime.now(timezone.utc).isoformat()

    # Collect runs for each workflow
    all_runs: Dict[str, List[Dict[str, Any]]] = {}
    total_new_runs = 0

    for workflow in workflows:
        runs = get_workflow_runs(
            repository=repository,
            workflow_name=workflow,
            branch=main_branch,
            since_timestamp=last_check_timestamp,
        )
        all_runs[workflow] = runs
        total_new_runs += len(runs)

        print(f"  {workflow}: {len(runs)} new runs")

    # Check if we have new runs
    if total_new_runs == 0 and not force_analysis:
        print(f"\n✓ No new runs on {main_branch} since last check. Exiting.")
        # Set output for GitHub Actions
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write("has_new_runs=false\n")
        sys.exit(0)

    print(f"\n✓ Found {total_new_runs} new runs on {main_branch}")

    # Analysis window: fetch last N FAILED runs across ALL branches
    print(f"\nAnalysis window: {FAILED_RUNS_PER_WORKFLOW} failed runs per workflow (all branches)")

    # Download artifacts
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)

    run_details: Dict[str, List[Dict[str, Any]]] = {}

    for workflow in workflows:
        print(f"\nProcessing {workflow}...")

        # Get last N FAILED runs across ALL branches
        workflow_runs = get_workflow_runs(
            repository=repository,
            workflow_name=workflow,
            limit=200,  # Fetch enough to find N failed runs
            failed_only=True,
        )[:FAILED_RUNS_PER_WORKFLOW]

        run_details[workflow] = []

        for run in workflow_runs:
            run_id = run["id"]
            conclusion = run["conclusion"]
            created_at = run["created_at"]
            branch = run.get("head_branch", "unknown")

            print(f"  Run {run_id} ({branch}) - {created_at}")

            # Get job details
            jobs = get_job_details(repository, run_id)

            # Download artifacts only from failed jobs
            downloaded = download_artifacts(repository, run_id, artifacts_dir, jobs)

            run_info = {
                "id": run_id,
                "branch": branch,  # Store branch for analysis
                "conclusion": conclusion,
                "created_at": created_at,
                "html_url": run["html_url"],
                "jobs": [
                    {
                        "name": job["name"],
                        "conclusion": job["conclusion"],
                        "started_at": job["started_at"],
                        "completed_at": job["completed_at"],
                    }
                    for job in jobs
                ],
                "artifacts": list(downloaded.keys()),
            }
            run_details[workflow].append(run_info)

    # Save run details
    run_data = {
        "analysis_window": FAILED_RUNS_PER_WORKFLOW,
        "current_timestamp": current_timestamp,
        "last_check_timestamp": last_check_timestamp,
        "workflows": run_details,
    }

    with open("ci_runs.json", "w") as f:
        json.dump(run_data, f, indent=2)

    print(f"\n✓ Run details saved to ci_runs.json")

    # Update and save state
    new_state = {
        "last_check_timestamp": current_timestamp,
        "last_analyzed_run_ids": {
            workflow: [r["id"] for r in runs]
            for workflow, runs in run_details.items()
        },
        "run_counts_analyzed": {
            workflow: len(runs) for workflow, runs in run_details.items()
        },
    }

    save_state(new_state, repository)

    # Set outputs for GitHub Actions
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write("has_new_runs=true\n")
            f.write(f"analysis_window={FAILED_RUNS_PER_WORKFLOW}\n")

    print("\n✓ Collection complete!")


if __name__ == "__main__":
    main()
