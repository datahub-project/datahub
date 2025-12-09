#!/usr/bin/env python3
"""
Generate failure report from analysis.

This script:
1. Loads failure analysis JSON
2. Generates markdown report with:
   - Summary statistics
   - Build failures
   - Regressions (currently broken tests)
   - Flakes (intermittent failures)
3. Outputs report to stdout (for workflow logs)

Phase 2 additions (commented out):
- Save report as artifact
- Generate Slack notification payload
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


def format_failure_pattern(pattern: List[str]) -> str:
    """Format failure pattern for display."""
    # Use emojis for visual clarity
    symbols = {
        "PASSED": "✓",
        "FAILED": "✗",
        "ERROR": "✗",
        "SKIPPED": "○",
    }
    return "[" + ", ".join(symbols.get(s, s) for s in pattern) + "]"


def truncate_message(message: str, max_length: int = 150) -> str:
    """Truncate long failure messages."""
    if len(message) <= max_length:
        return message
    return message[:max_length] + "..."


def generate_summary_section(analysis: Dict[str, Any], run_data: Dict[str, Any]) -> str:
    """Generate summary section of report."""
    lines = []
    lines.append("# CI Health Report")
    lines.append("")
    lines.append(f"**Repository**: {analysis.get('repository', 'acryldata/datahub-fork')}")
    lines.append(f"**Generated**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append(f"**Analysis Window**: Last {analysis['analysis_window']} failed runs per workflow (all branches)")
    lines.append("")

    # Calculate runs analyzed per workflow
    total_runs = 0
    for workflow_name, workflow_data in run_data["workflows"].items():
        total_runs += len(workflow_data)

    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Workflows Analyzed**: {len(run_data['workflows'])} workflows")
    lines.append(f"- **Total Runs Analyzed**: {total_runs} failed runs across all branches")
    lines.append(
        f"- **Build Failures**: {analysis['summary']['total_build_failures']} jobs"
    )
    lines.append(
        f"- **Test Regressions**: {analysis['summary']['total_regressions']} unique tests"
    )
    lines.append(f"- **Test Flakes**: {analysis['summary']['total_flakes']} unique tests")
    lines.append("")

    # Add configuration thresholds
    config = analysis.get('configuration', {})
    main_branch = analysis.get('main_branch', 'acryl-main')
    if config:
        lines.append("## Configuration")
        lines.append("")
        lines.append(f"- **Main Branch**: {main_branch}")
        lines.append(f"- **Regression Threshold**: {main_branch} failed + {config.get('REGRESSION_MIN_BRANCHES', 3) - 1}+ other branches ({config.get('REGRESSION_MIN_BRANCHES', 3)}+ total)")
        lines.append(f"- **Flake Threshold**: {config.get('FLAKE_MIN_BRANCHES', 2)}+ unique branches")
        lines.append(f"- **Build Failure Threshold** (all workflows): {main_branch} + {config.get('COMPILATION_FAILURE_MIN_BRANCHES', 2) - 1}+ other branches ({config.get('COMPILATION_FAILURE_MIN_BRANCHES', 2)}+ total)")
        lines.append("")

    # Has failures indicator
    has_failures = (
        analysis["summary"]["total_build_failures"] > 0
        or analysis["summary"]["total_regressions"] > 0
    )
    if has_failures:
        lines.append("⚠️ **Action Required**: Build failures or regressions detected")
    else:
        lines.append("✅ **All Clear**: No build failures or regressions detected")

    lines.append("")
    return "\n".join(lines)


def generate_build_failures_section(analysis: Dict[str, Any]) -> str:
    """Generate build failures section of report."""
    lines = []
    lines.append("## Build Failures")
    lines.append("")

    all_build_failures = []
    for workflow_name, workflow_data in analysis["workflows"].items():
        all_build_failures.extend(workflow_data["build_failures"])

    if not all_build_failures:
        lines.append("✅ No build failures detected")
        lines.append("")
        return "\n".join(lines)

    # Group by workflow for clearer reporting
    docker_failures = [f for f in all_build_failures if f.get("workflow") == "docker-unified.yml"]
    other_failures = [
        f for f in all_build_failures
        if f.get("workflow") in ["metadata-io.yml", "build-and-test.yml"]
    ]

    # Show docker-unified build failures
    if docker_failures:
        lines.append(
            f"### docker-unified.yml Build Failures"
        )
        lines.append("")
        lines.append(
            f"Found {len(docker_failures)} build job(s) currently broken:"
        )
        lines.append("")

        for failure in docker_failures:
            _append_failure_details(lines, failure)

    # Show metadata-io and build-and-test build failures
    if other_failures:
        if docker_failures:
            lines.append("")  # Add spacing between sections
        lines.append(
            f"### metadata-io.yml & build-and-test.yml Build Failures"
        )
        lines.append("")
        lines.append(
            f"Found {len(other_failures)} job(s) failing at build step (no test artifacts produced):"
        )
        lines.append("")

        for failure in other_failures:
            _append_failure_details(lines, failure)

    return "\n".join(lines)


def _append_failure_details(lines: List[str], failure: Dict[str, Any]) -> None:
    """Append failure details to the lines list."""
    workflow = failure["workflow"]
    job_name = failure["job_name"]
    failure_count = failure["failure_count"]
    consecutive = failure["consecutive_failures"]
    total_runs = failure["total_runs"]
    failure_rate = (failure_count / total_runs * 100) if total_runs > 0 else 0
    run_url = failure.get("latest_run_url", "")
    unique_branch_count = failure.get("unique_branch_count", 0)
    failed_branches = failure.get("failed_branches", [])

    lines.append(f"#### {workflow} - {job_name}")
    lines.append("")

    if unique_branch_count > 0:
        lines.append(
            f"- **Failed Branches**: {unique_branch_count} branches - {', '.join(failed_branches)}"
        )

    lines.append(f"- **Consecutive Failures**: {consecutive}/{total_runs} runs")
    lines.append(
        f"- **Total Failure Rate**: {failure_count}/{total_runs} runs ({failure_rate:.1f}%)"
    )

    if run_url:
        lines.append(f"- **Latest Failed Run**: {run_url}")

    lines.append(f"- **Recent Failures**:")
    for fail_detail in failure["failures"][:5]:  # Show up to 5 recent failures
        run_id = fail_detail["run_id"]
        lines.append(f"  - Run #{run_id}")

    lines.append("")


def generate_regressions_section(analysis: Dict[str, Any]) -> str:
    """Generate regressions section of report."""
    lines = []
    lines.append("## Regressions (Currently Broken)")
    lines.append("")

    all_regressions = []
    for workflow_name, workflow_data in analysis["workflows"].items():
        all_regressions.extend(workflow_data["regressions"])

    if not all_regressions:
        lines.append("✅ No test regressions detected")
        lines.append("")
        return "\n".join(lines)

    # Sort by unique branch count (most widespread first)
    all_regressions.sort(key=lambda x: x.get("unique_branch_count", 0), reverse=True)

    lines.append(
        f"Found {len(all_regressions)} unique tests that failed on acryl-main + 2+ other branches:"
    )
    lines.append("")

    for idx, regression in enumerate(all_regressions, 1):
        test_id = regression["test_id"]
        workflow = regression["workflow"]
        job_batch = regression["job_batch"]
        total = regression["total_runs"]
        failure_count = regression["failure_count"]
        failure_rate = regression["failure_rate"] * 100
        unique_branch_count = regression.get("unique_branch_count", 0)
        failed_branches = regression.get("failed_branches", [])
        message = regression.get("latest_failure_message", "")
        run_url = regression.get("latest_run_url", "")

        lines.append(f"### {idx}. {test_id}")
        lines.append("")
        lines.append(f"- **Workflow**: {workflow}")
        lines.append(f"- **Job/Batch**: {job_batch}")
        lines.append(
            f"- **Failed Branches**: {unique_branch_count} branches - {', '.join(failed_branches)}"
        )
        lines.append(
            f"- **Failure Rate**: {failure_count}/{total} runs ({failure_rate:.1f}%)"
        )

        if run_url:
            lines.append(f"- **Failed Run**: {run_url}")

        if message:
            lines.append(f"- **Latest Error**: `{truncate_message(message)}`")

        lines.append("")

    return "\n".join(lines)


def generate_flakes_section(analysis: Dict[str, Any], max_flakes: int = 20) -> str:
    """Generate flakes section of report."""
    lines = []
    lines.append("## Flakes (Intermittent Failures)")
    lines.append("")

    all_flakes = []
    for workflow_name, workflow_data in analysis["workflows"].items():
        all_flakes.extend(workflow_data["flakes"])

    if not all_flakes:
        lines.append("✅ No test flakes detected")
        lines.append("")
        return "\n".join(lines)

    # Sort by failure count (most failures first)
    all_flakes.sort(key=lambda x: x["failure_count"], reverse=True)

    lines.append(
        f"Found {len(all_flakes)} unique tests with intermittent failures (showing top {min(max_flakes, len(all_flakes))}):"
    )
    lines.append("")

    for idx, flake in enumerate(all_flakes[:max_flakes], 1):
        test_id = flake["test_id"]
        workflow = flake["workflow"]
        job_batch = flake["job_batch"]
        failure_count = flake["failure_count"]
        total = flake["total_runs"]
        failure_rate = flake["failure_rate"] * 100
        unique_branch_count = flake.get("unique_branch_count", 0)
        failed_branches = flake.get("failed_branches", [])
        run_url = flake.get("latest_run_url", "")

        lines.append(f"### {idx}. {test_id}")
        lines.append("")
        lines.append(f"- **Workflow**: {workflow}")
        lines.append(f"- **Job/Batch**: {job_batch}")
        lines.append(
            f"- **Failed Branches**: {unique_branch_count} branches - {', '.join(failed_branches)}"
        )
        lines.append(
            f"- **Failure Rate**: {failure_count}/{total} runs ({failure_rate:.1f}%)"
        )

        if run_url:
            lines.append(f"- **Failed Run**: {run_url}")

        lines.append("")

    if len(all_flakes) > max_flakes:
        lines.append(f"*...and {len(all_flakes) - max_flakes} more flaky tests*")
        lines.append("")

    return "\n".join(lines)


def generate_report(analysis: Dict[str, Any], run_data: Dict[str, Any]) -> str:
    """Generate complete failure report."""
    sections = [
        generate_summary_section(analysis, run_data),
        generate_build_failures_section(analysis),
        generate_regressions_section(analysis),
        generate_flakes_section(analysis),
    ]

    report = "\n".join(sections)
    return report


def format_slack_section_text(analysis: Dict[str, Any]) -> str:
    """
    Format build failures, regressions, and flakes for Slack message.

    Returns formatted text sections for Slack attachment.
    """
    sections = []

    # Build Failures Section
    build_failures_text = []
    all_build_failures = []
    for workflow_name, workflow_data in analysis["workflows"].items():
        all_build_failures.extend(workflow_data["build_failures"])

    if all_build_failures:
        build_failures_text.append("*Build Failures*")
        build_failures_text.append("")
        for failure in all_build_failures[:3]:  # Show top 3
            workflow = failure["workflow"]
            job_name = failure["job_name"]
            unique_branch_count = failure.get("unique_branch_count", 0)
            failed_branches = failure.get("failed_branches", [])
            run_url = failure.get("latest_run_url", "")

            build_failures_text.append(f"• *{workflow} - {job_name}*")
            if unique_branch_count > 0:
                branch_list = ", ".join(failed_branches[:5])
                if len(failed_branches) > 5:
                    branch_list += f", +{len(failed_branches) - 5} more"
                build_failures_text.append(f"  Failed Branches: {unique_branch_count} branches - {branch_list}")
            if run_url:
                build_failures_text.append(f"  <{run_url}|Latest Failed Run>")

        if len(all_build_failures) > 3:
            build_failures_text.append(f"_...and {len(all_build_failures) - 3} more build failures_")
    else:
        build_failures_text.append("*Build Failures*")
        build_failures_text.append("✅ No build failures detected")

    sections.append("\n".join(build_failures_text))

    # Regressions Section
    regressions_text = []
    all_regressions = []
    for workflow_name, workflow_data in analysis["workflows"].items():
        all_regressions.extend(workflow_data["regressions"])

    if all_regressions:
        # Sort by unique branch count
        all_regressions.sort(key=lambda x: x.get("unique_branch_count", 0), reverse=True)

        regressions_text.append("*Regressions (Currently Broken)*")
        regressions_text.append("")
        for idx, regression in enumerate(all_regressions[:3], 1):  # Show top 3
            test_id = regression["test_id"]
            workflow = regression["workflow"]
            job_batch = regression["job_batch"]
            unique_branch_count = regression.get("unique_branch_count", 0)
            failed_branches = regression.get("failed_branches", [])
            failure_rate = regression["failure_rate"] * 100
            run_url = regression.get("latest_run_url", "")

            regressions_text.append(f"{idx}. *{test_id}*")
            regressions_text.append(f"  Workflow: {workflow} - {job_batch}")
            branch_list = ", ".join(failed_branches[:5])
            if len(failed_branches) > 5:
                branch_list += f", +{len(failed_branches) - 5} more"
            regressions_text.append(f"  Failed Branches: {unique_branch_count} branches - {branch_list}")
            regressions_text.append(f"  Failure Rate: {regression['failure_count']}/{regression['total_runs']} runs ({failure_rate:.1f}%)")
            if run_url:
                regressions_text.append(f"  <{run_url}|Failed Run>")
            regressions_text.append("")

        if len(all_regressions) > 3:
            regressions_text.append(f"_...and {len(all_regressions) - 3} more regressions_")
    else:
        regressions_text.append("*Regressions (Currently Broken)*")
        regressions_text.append("✅ No test regressions detected")

    sections.append("\n".join(regressions_text))

    # Flakes Section
    flakes_text = []
    all_flakes = []
    for workflow_name, workflow_data in analysis["workflows"].items():
        all_flakes.extend(workflow_data["flakes"])

    if all_flakes:
        # Sort by failure count
        all_flakes.sort(key=lambda x: x["failure_count"], reverse=True)

        flakes_text.append("*Flakes (Intermittent Failures)*")
        flakes_text.append("")
        flakes_text.append(f"Found {len(all_flakes)} unique tests with intermittent failures (showing top 1):")
        flakes_text.append("")

        for idx, flake in enumerate(all_flakes[:1], 1):  # Show top 1
            test_id = flake["test_id"]
            workflow = flake["workflow"]
            job_batch = flake["job_batch"]
            unique_branch_count = flake.get("unique_branch_count", 0)
            failed_branches = flake.get("failed_branches", [])
            failure_rate = flake["failure_rate"] * 100
            run_url = flake.get("latest_run_url", "")

            flakes_text.append(f"{idx}. *{test_id}*")
            flakes_text.append(f"  Workflow: {workflow} - {job_batch}")
            branch_list = ", ".join(failed_branches[:5])
            if len(failed_branches) > 5:
                branch_list += f", +{len(failed_branches) - 5} more"
            flakes_text.append(f"  Failed Branches: {unique_branch_count} branches - {branch_list}")
            flakes_text.append(f"  Failure Rate: {flake['failure_count']}/{flake['total_runs']} runs ({failure_rate:.1f}%)")
            if run_url:
                flakes_text.append(f"  <{run_url}|Failed Run>")
    else:
        flakes_text.append("*Flakes (Intermittent Failures)*")
        flakes_text.append("✅ No test flakes detected")

    sections.append("\n".join(flakes_text))

    return "\n\n".join(sections)


def generate_slack_payload(
    analysis: Dict[str, Any], workflow_run_url: str = ""
) -> Dict[str, Any]:
    """
    Generate Slack webhook payload with detailed sections.

    Args:
        analysis: The failure analysis dictionary
        workflow_run_url: URL to the GitHub Actions workflow run (optional)
    """
    summary = analysis["summary"]
    repository = analysis.get("repository", "Unknown")
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

    # Determine color based on severity
    if summary["total_build_failures"] > 0:
        color = "#d9534f"  # Red
    elif summary["total_regressions"] > 0:
        color = "#f0ad4e"  # Orange
    else:
        color = "#5cb85c"  # Green

    # Build the detailed text sections
    detailed_text = format_slack_section_text(analysis)

    # Add timestamp at the end
    footer_text = f"Generated: {timestamp}"
    if workflow_run_url:
        footer_text = f"<{workflow_run_url}|View Full Report> • {timestamp}"

    payload = {
        "attachments": [
            {
                "color": color,
                "title": f"CI Health Report - {repository}",
                "text": detailed_text,
                "footer": footer_text,
                "mrkdwn_in": ["text", "footer"],
            }
        ]
    }

    return payload


def main() -> None:
    """Main entry point."""
    # Get workflow run URL from environment
    workflow_run_url = os.environ.get("WORKFLOW_RUN_URL", "")

    # Load analysis data
    analysis_file = Path("failure_analysis.json")
    if not analysis_file.exists():
        print("Error: failure_analysis.json not found", file=sys.stderr)
        sys.exit(1)

    with open(analysis_file, "r") as f:
        analysis = json.load(f)

    # Load run data for additional context
    run_data_file = Path("ci_runs.json")
    if not run_data_file.exists():
        print("Error: ci_runs.json not found", file=sys.stderr)
        sys.exit(1)

    with open(run_data_file, "r") as f:
        run_data = json.load(f)

    # Generate report
    report = generate_report(analysis, run_data)

    # Output to stdout (visible in workflow logs)
    print(report)

    # Phase 2: Save report as artifact (commented out for Phase 1)
    # timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    # report_file = Path(f"failure_report.md")
    # with open(report_file, 'w') as f:
    #     f.write(report)
    # print(f"\n✓ Report saved to {report_file}")

    # Phase 2: Generate Slack payload
    slack_payload = generate_slack_payload(analysis, workflow_run_url)
    with open("slack_payload.json", "w") as f:
        json.dump(slack_payload, f, indent=2)
    print(f"\n✓ Slack payload saved to slack_payload.json")


if __name__ == "__main__":
    main()
