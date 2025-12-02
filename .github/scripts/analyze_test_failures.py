#!/usr/bin/env python3
"""
Analyze test failures from CI runs.

This script:
1. Parses JUnit XML files from downloaded artifacts
2. Tracks each test across runs (newest to oldest)
3. Categorizes failures:
   - Build failures: Jobs that failed at build step (not test) in 2+ runs
   - Regressions: Tests where latest run failed AND 3+ consecutive failures
   - Flakes: Tests with failures on 3+ branches not meeting regression criteria
   - Tests failing on only 1-2 branches are filtered out (not reported)
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Configuration thresholds - adjust these to tune the analysis
REGRESSION_MIN_BRANCHES = 3  # Test must fail on acryl-main + 2+ other branches
FLAKE_MIN_BRANCHES = 2  # Test must fail on 2+ branches to be reported as flake
BUILD_FAILURE_CONSECUTIVE_MIN = 3  # Job must fail N+ consecutive times
COMPILATION_FAILURE_MIN_BRANCHES = 2  # Compilation must fail on main + 1+ other branches


def parse_junit_xml(xml_file: Path) -> List[Dict]:
    """
    Parse a JUnit XML file and extract test results.

    Returns:
        List of test results with name, status, and failure message
    """
    results = []

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Find all testcase elements
        for testcase in root.findall(".//testcase"):
            classname = testcase.get("classname", "")
            name = testcase.get("name", "")
            time_str = testcase.get("time", "0")

            # Build test ID
            if classname and name:
                test_id = f"{classname}::{name}"
            elif name:
                test_id = name
            else:
                continue

            # Check for failure or error
            failure = testcase.find("failure")
            error = testcase.find("error")
            skipped = testcase.find("skipped")

            if failure is not None:
                status = "FAILED"
                failure_message = failure.get("message", "")
                failure_type = failure.get("type", "")
                failure_text = failure.text or ""
            elif error is not None:
                status = "ERROR"
                failure_message = error.get("message", "")
                failure_type = error.get("type", "")
                failure_text = error.text or ""
            elif skipped is not None:
                status = "SKIPPED"
                failure_message = ""
                failure_type = ""
                failure_text = ""
            else:
                status = "PASSED"
                failure_message = ""
                failure_type = ""
                failure_text = ""

            results.append(
                {
                    "test_id": test_id,
                    "status": status,
                    "time": float(time_str),
                    "failure_message": failure_message,
                    "failure_type": failure_type,
                    "failure_text": failure_text[:500],  # Truncate long stack traces
                }
            )

    except ET.ParseError as e:
        print(f"Warning: Failed to parse {xml_file}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Error processing {xml_file}: {e}", file=sys.stderr)

    return results


def collect_test_results(artifacts_dir: Path, run_info: List[Dict]) -> Dict:
    """
    Collect test results from failed runs only.

    Since we only analyze failed runs and download artifacts from failed jobs,
    we only track actual test failures seen in the artifacts.

    Args:
        artifacts_dir: Directory containing run-* subdirectories
        run_info: List of run metadata (ordered newest to oldest)

    Returns:
        Dictionary with test results organized by job/batch and test ID
    """
    test_results = defaultdict(lambda: defaultdict(list))

    for run_idx, run in enumerate(run_info):
        run_id = run["id"]
        run_branch = run.get("branch", "unknown")
        run_url = run["html_url"]
        run_dir = artifacts_dir / f"run-{run_id}"

        if not run_dir.exists():
            continue

        # Find all JUnit XML files in this run
        xml_files = list(run_dir.rglob("*.xml"))

        for xml_file in xml_files:
            # Determine job/batch from directory structure
            rel_path = xml_file.relative_to(run_dir)
            job_batch = rel_path.parts[0] if rel_path.parts else "unknown"

            # Parse XML
            test_cases = parse_junit_xml(xml_file)

            for test_case in test_cases:
                test_id = test_case["test_id"]

                # Store result with run index, branch, and status
                test_results[job_batch][test_id].append(
                    {
                        "run_idx": run_idx,
                        "run_id": run_id,
                        "branch": run_branch,
                        "run_url": run_url,
                        "status": test_case["status"],
                        "failure_message": test_case["failure_message"],
                        "failure_type": test_case["failure_type"],
                    }
                )

    return test_results


def analyze_build_failures(
    workflow_name: str, workflow_runs: List[Dict], main_branch: str = "acryl-main"
) -> List[Dict]:
    """
    Identify jobs that are currently broken at build step.

    Workflow-specific logic:
    - docker-unified.yml: Only report base_build job (build-only step)
      - Requires failures on main branch + 1+ other branches (COMPILATION_FAILURE_MIN_BRANCHES)
    - metadata-io.yml: Report jobs that failed without producing JUnit artifacts (build failures)
      - Requires failures on main branch + 1+ other branches (COMPILATION_FAILURE_MIN_BRANCHES)
    - build-and-test.yml: Report jobs that failed without producing JUnit artifacts (build failures)
      - Requires failures on main branch + 1+ other branches (COMPILATION_FAILURE_MIN_BRANCHES)

    All workflows use branch-based detection: main branch + 1+ other branches (2+ total).

    Args:
        workflow_name: Name of the workflow being analyzed
        workflow_runs: List of run metadata (ordered newest to oldest)
        main_branch: Main branch name (acryl-main or master)

    Returns:
        List of build failures requiring action
    """
    # Track job status across all runs
    job_history = defaultdict(list)

    for run_idx, run in enumerate(workflow_runs):
        run_id = run["id"]
        run_url = run["html_url"]
        conclusion = run["conclusion"]
        artifacts = run.get("artifacts", [])
        branch = run.get("branch", "unknown")

        # Track all jobs, not just failures
        for job in run["jobs"]:
            job_name = job["name"]
            job_conclusion = job["conclusion"]

            # Determine if this job has test artifacts
            has_test_artifacts = any(
                _job_matches_artifact(job_name, artifact_name)
                for artifact_name in artifacts
            )

            job_history[job_name].append(
                {
                    "run_idx": run_idx,
                    "run_id": run_id,
                    "run_url": run_url,
                    "branch": branch,
                    "job_name": job_name,
                    "status": job_conclusion,
                    "has_test_artifacts": has_test_artifacts,
                }
            )

    # Filter to jobs that should be reported as build/compilation failures
    build_failures = []
    for job_name, history in job_history.items():
        # Sort by run_idx to ensure newest to oldest
        history = sorted(history, key=lambda x: x["run_idx"])

        if not history:
            continue

        # Check if latest run failed
        latest = history[0]
        if latest["status"] != "failure":
            continue

        # Apply workflow-specific filtering
        should_report = False
        failure_type = "build"

        failed_runs = [h for h in history if h["status"] == "failure"]

        if workflow_name == "docker-unified.yml":
            # Only report base_build job (build-only step)
            # Requires main branch + 1+ other branches (COMPILATION_FAILURE_MIN_BRANCHES)
            if job_name == "base_build":
                # Get unique branches that failed
                failed_branches = list(set(h["branch"] for h in failed_runs))

                # Check if main branch is in failed branches
                if main_branch in failed_branches and len(failed_branches) >= COMPILATION_FAILURE_MIN_BRANCHES:
                    should_report = True
                    failure_type = "build"

        elif workflow_name == "metadata-io.yml":
            # Only report if job failed without producing test artifacts (build failure)
            # Requires main branch + 1+ other branches (COMPILATION_FAILURE_MIN_BRANCHES)
            if not latest.get("has_test_artifacts", False):
                # Get unique branches that failed
                failed_branches = list(set(h["branch"] for h in failed_runs))

                # Check if main branch is in failed branches
                if main_branch in failed_branches and len(failed_branches) >= COMPILATION_FAILURE_MIN_BRANCHES:
                    should_report = True
                    failure_type = "build"

        elif workflow_name == "build-and-test.yml":
            # Only report if job failed without producing test artifacts (build failure)
            # Requires main branch + 1+ other branches (COMPILATION_FAILURE_MIN_BRANCHES)
            if not latest.get("has_test_artifacts", False):
                # Get unique branches that failed
                failed_branches = list(set(h["branch"] for h in failed_runs))

                # Check if main branch is in failed branches
                if main_branch in failed_branches and len(failed_branches) >= COMPILATION_FAILURE_MIN_BRANCHES:
                    should_report = True
                    failure_type = "build"

        if should_report:
            # Calculate consecutive failures (for display purposes)
            consecutive_failures = 0
            for entry in history:
                if entry["status"] == "failure":
                    consecutive_failures += 1
                else:
                    break

            # Get unique branches
            failed_branches = list(set(h["branch"] for h in failed_runs))

            build_failures.append(
                {
                    "job_name": job_name,
                    "workflow": workflow_name,
                    "failure_type": failure_type,
                    "failure_count": len(failed_runs),
                    "consecutive_failures": consecutive_failures,
                    "total_runs": len(history),
                    "unique_branch_count": len(failed_branches),
                    "failed_branches": sorted(failed_branches),
                    "latest_run_url": latest["run_url"],
                    "failures": failed_runs[:5],  # Show up to 5 recent failures
                }
            )

    return build_failures


def _job_matches_artifact(job_name: str, artifact_name: str) -> bool:
    """
    Check if an artifact belongs to a specific job.

    This matches the logic from collect_ci_failures.py's artifact_matches_failed_job().
    """
    job_lower = job_name.lower()
    artifact_lower = artifact_name.lower()

    # Match cypress batch artifacts
    if "cypress" in artifact_lower and "cypress" in job_lower:
        return True

    # Match pytest batch artifacts
    if "pytests" in artifact_lower and "pytest" in job_lower:
        return True

    # Match metadata-io artifacts (artifact contains "metadata-io", job name is "build")
    if "metadata-io" in artifact_lower and "build" in job_lower:
        return True

    # Match build artifacts (build-and-test.yml and other workflows)
    if "build" in artifact_lower and "build" in job_lower:
        return True

    return False


def categorize_test(test_id: str, results: List[Dict], main_branch: str = "acryl-main") -> Tuple[str, Dict]:
    """
    Categorize a test as regression or flake based on branch failures.

    Regression: Test fails on latest main branch run + 2+ other unique branches (3+ total)
    Flake: Test fails on 2+ unique branches but doesn't meet regression criteria
    Passing: Test with failures on only 1 branch (not reported)

    Args:
        test_id: Test identifier
        results: List of test results from different runs and branches
        main_branch: Main branch name (acryl-main or master)

    Returns:
        Tuple of (category, details)
    """
    # Sort by run_idx to ensure newest to oldest
    results = sorted(results, key=lambda x: x["run_idx"])

    if not results:
        return "UNKNOWN", {}

    # Separate results by branch
    main_results = [r for r in results if r.get("branch") == main_branch]
    other_branch_results = [r for r in results if r.get("branch") != main_branch]

    # Get all failed runs
    failed_runs = [r for r in results if r["status"] in ("FAILED", "ERROR")]

    if not failed_runs:
        return "PASSING", {"total_runs": len(results), "failure_count": 0}

    # Check if latest main branch run failed
    latest_main_failed = False
    if main_results:
        latest_main = min(main_results, key=lambda x: x["run_idx"])
        if latest_main["status"] in ("FAILED", "ERROR"):
            latest_main_failed = True

    # Count unique branches with failures
    failed_branches: Set[str] = {r.get("branch", "unknown") for r in failed_runs}
    unique_branch_count = len(failed_branches)

    # Categorize based on branch count and main branch status
    # REGRESSION: Latest main branch failed + 3+ unique branches total
    if latest_main_failed and unique_branch_count >= REGRESSION_MIN_BRANCHES:
        category = "REGRESSION"
    elif failed_runs and unique_branch_count >= FLAKE_MIN_BRANCHES:
        category = "FLAKE"
    else:
        return "PASSING", {"total_runs": len(results), "failure_count": 0}

    # Get latest failure for reporting
    latest_failure = failed_runs[0] if failed_runs else results[0]

    details = {
        "total_runs": len(results),
        "failure_count": len(failed_runs),
        "unique_branch_count": unique_branch_count,
        "failed_branches": sorted(list(failed_branches)),
        "latest_main_failed": latest_main_failed,
        "failure_rate": len(failed_runs) / len(results) if results else 0,
        "latest_failure_message": latest_failure.get("failure_message", ""),
        "latest_failure_type": latest_failure.get("failure_type", ""),
        "latest_run_url": latest_failure.get("run_url", ""),
        "failure_pattern": [f"{r['branch']}:{r['status']}" for r in results],
        "failed_in_runs": [r["run_id"] for r in failed_runs],
        "failed_run_urls": [r["run_url"] for r in failed_runs],
    }

    return category, details


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Analyze test failures from CI runs")
    parser.add_argument(
        "--repository-name",
        default="acryldata/datahub-fork",
        help="Repository name for display purposes (default: acryldata/datahub-fork)",
    )
    args = parser.parse_args()

    repository_name = args.repository_name

    # Determine main branch based on repository
    main_branch = "master" if "datahub-project/datahub" in repository_name else "acryl-main"

    # Load run data
    run_data_file = Path("ci_runs.json")
    if not run_data_file.exists():
        print("Error: ci_runs.json not found", file=sys.stderr)
        sys.exit(1)

    with open(run_data_file, "r") as f:
        run_data = json.load(f)

    analysis_window = run_data["analysis_window"]
    workflows = run_data["workflows"]

    print(f"Analyzing test failures for {repository_name}...")
    print(f"Analysis window: {analysis_window} runs")
    print()

    artifacts_dir = Path("artifacts")

    all_analysis = {}

    for workflow_name, workflow_runs in workflows.items():
        print(f"Processing {workflow_name}...")
        print(f"  Runs: {len(workflow_runs)}")

        # Analyze build failures (workflow-specific logic)
        build_failures = analyze_build_failures(workflow_name, workflow_runs, main_branch)
        print(f"  Build failures: {len(build_failures)}")

        # Collect test results
        test_results = collect_test_results(artifacts_dir, workflow_runs)

        # Categorize tests
        regressions = []
        flakes = []

        for job_batch, tests in test_results.items():
            for test_id, results in tests.items():
                category, details = categorize_test(test_id, results, main_branch)

                if category == "REGRESSION":
                    regressions.append(
                        {
                            "test_id": test_id,
                            "job_batch": job_batch,
                            "workflow": workflow_name,
                            **details,
                        }
                    )
                elif category == "FLAKE":
                    flakes.append(
                        {
                            "test_id": test_id,
                            "job_batch": job_batch,
                            "workflow": workflow_name,
                            **details,
                        }
                    )

        print(f"  Regressions: {len(regressions)}")
        print(f"  Flakes: {len(flakes)}")
        print()

        all_analysis[workflow_name] = {
            "build_failures": build_failures,
            "regressions": regressions,
            "flakes": flakes,
        }

    # Save analysis
    analysis_output = {
        "repository": repository_name,
        "main_branch": main_branch,
        "analysis_window": analysis_window,
        "configuration": {
            "REGRESSION_MIN_BRANCHES": REGRESSION_MIN_BRANCHES,
            "FLAKE_MIN_BRANCHES": FLAKE_MIN_BRANCHES,
            "BUILD_FAILURE_CONSECUTIVE_MIN": BUILD_FAILURE_CONSECUTIVE_MIN,
            "COMPILATION_FAILURE_MIN_BRANCHES": COMPILATION_FAILURE_MIN_BRANCHES,
        },
        "workflows": all_analysis,
        "summary": {
            "total_build_failures": sum(
                len(w["build_failures"]) for w in all_analysis.values()
            ),
            "total_regressions": sum(
                len(w["regressions"]) for w in all_analysis.values()
            ),
            "total_flakes": sum(len(w["flakes"]) for w in all_analysis.values()),
        },
    }

    with open("failure_analysis.json", "w") as f:
        json.dump(analysis_output, f, indent=2)

    print("✓ Analysis complete!")
    print(f"  Total build failures: {analysis_output['summary']['total_build_failures']}")
    print(f"  Total regressions: {analysis_output['summary']['total_regressions']}")
    print(f"  Total flakes: {analysis_output['summary']['total_flakes']}")


if __name__ == "__main__":
    main()
