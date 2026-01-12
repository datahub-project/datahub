#!/usr/bin/env python3
"""
Fetch flaky test data from PostHog and generate reports.

This script:
1. Fetches data from PostHog insight via API
2. Parses CSV/JSON response to extract test failures
3. Generates markdown report with summary and details sections
4. Constructs workflow run URLs from run IDs
5. Saves raw data as artifacts

Exit Codes:
    0: Success (always, for graceful degradation)
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class PostHogConfig:
    """Configuration for PostHog API."""
    api_key: str
    host: str
    project_id: str
    timeout: int = 30


@dataclass
class InsightConfig:
    """Configuration for a single insight."""
    insight_id: str
    label: str
    repository: str


@dataclass
class FlakyTest:
    """Represents a flaky test with its failure data."""
    workflow_name: str
    test_name: str
    runs_failed: int
    branches: List[str]
    run_ids: List[str]

    @property
    def failure_rate_display(self) -> str:
        """Display string for failure rate."""
        return f"{self.runs_failed} failures"

    def get_workflow_urls(self, repository: str) -> List[str]:
        """Generate GitHub workflow URLs from run IDs pointing to first attempt."""
        return [
            f"https://github.com/{repository}/actions/runs/{run_id}/attempts/1"
            for run_id in self.run_ids if run_id
        ]


def fetch_posthog_insight(config: PostHogConfig, insight_id: str) -> bytes:
    """
    Fetch data from PostHog insight API with cache refresh.

    Args:
        config: PostHog configuration
        insight_id: Insight short_id to fetch

    Returns:
        Raw response data (JSON)

    Raises:
        urllib.error.HTTPError: If API request fails
        urllib.error.URLError: If network error occurs
    """
    # Use insights list endpoint with short_id filter and refresh parameter
    # refresh=blocking: calculate synchronously unless there are very fresh results
    endpoint = (
        f"{config.host.rstrip('/')}/api/projects/{config.project_id}/"
        f"insights/?short_id={insight_id}&refresh=blocking"
    )

    print(f"Fetching data from PostHog: {endpoint}")

    request = urllib.request.Request(
        endpoint,
        headers={
            "Authorization": f"Bearer {config.api_key}"
        }
    )

    with urllib.request.urlopen(request, timeout=config.timeout) as response:
        data = response.read()
        print(f"✅ Fetched {len(data)} bytes from PostHog")

        # Parse JSON response to get the insight result
        import json
        result = json.loads(data.decode('utf-8'))

        # Get the first (and should be only) insight
        if 'results' in result and len(result['results']) > 0:
            insight = result['results'][0]
            print(f"✅ Found insight: {insight.get('name', 'Unnamed')}")
            return data
        else:
            raise ValueError("No insight found with the given short_id")



def fetch_posthog_insight_with_retry(
    config: PostHogConfig,
    insight_id: str,
    max_retries: int = 2,
    retry_delay: int = 60
) -> bytes:
    """
    Fetch PostHog insight with retry logic for null results.

    Args:
        config: PostHog configuration
        insight_id: Insight short_id to fetch
        max_retries: Maximum number of retry attempts if result is null
        retry_delay: Seconds to wait between retries (default: 60)

    Returns:
        Raw response data (JSON)

    Raises:
        urllib.error.HTTPError: If API request fails
        urllib.error.URLError: If network error occurs
    """
    for attempt in range(max_retries + 1):
        json_data = fetch_posthog_insight(config, insight_id)

        # Check if we got actual result data
        data = json.loads(json_data.decode('utf-8'))
        if 'results' in data and len(data['results']) > 0:
            insight = data['results'][0]
            result = insight.get('result')

            if result is not None:
                # Success - we have data
                return json_data

            # Result is null - check if we should retry
            if attempt < max_retries:
                is_cached = insight.get('is_cached', False)
                last_refresh = insight.get('last_refresh')
                print(f"⚠️  Attempt {attempt + 1}/{max_retries + 1}: Insight returned null result")
                print(f"   (cached: {is_cached}, last_refresh: {last_refresh})")
                print(f"   Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                # Final attempt failed, return the null result
                print(f"⚠️  All {max_retries + 1} attempts returned null result")
                return json_data
        else:
            # No results at all - return immediately
            return json_data

    return json_data


def parse_posthog_json(json_data: bytes) -> List[FlakyTest]:
    """
    Parse PostHog insight JSON data into FlakyTest objects.

    Args:
        json_data: Raw JSON bytes from PostHog insight API

    Returns:
        List of FlakyTest objects sorted by runs_failed (descending)
    """
    data = json.loads(json_data.decode('utf-8'))

    tests = []

    # Get the first result (should be our insight)
    if 'results' not in data or len(data['results']) == 0:
        return tests

    insight = data['results'][0]

    # Get the result data - array of arrays
    # Format: [workflow_name, test_name, runs_failed, branches_array, run_ids_array]
    # Handle case where result is explicitly None (PostHog query returned no data)
    result_data = insight.get('result')

    if result_data is None:
        # Insight hasn't been refreshed/cached in PostHog yet
        is_cached = insight.get('is_cached', False)
        last_refresh = insight.get('last_refresh')
        print(f"⚠️  Warning: Insight has no result data (cached: {is_cached}, last_refresh: {last_refresh})")
        print(f"   This usually means PostHog hasn't calculated this insight yet or the query failed.")
        return tests

    for row in result_data:
        if len(row) < 5:
            continue

        workflow_name = row[0] if row[0] else ''
        test_name = row[1] if row[1] else ''
        runs_failed = int(row[2]) if row[2] else 0
        branches = row[3] if isinstance(row[3], list) else []
        run_ids = [str(rid) for rid in row[4]] if isinstance(row[4], list) else []

        if workflow_name and test_name and runs_failed > 0:
            tests.append(FlakyTest(
                workflow_name=workflow_name,
                test_name=test_name,
                runs_failed=runs_failed,
                branches=branches,
                run_ids=run_ids
            ))

    # Sort by runs_failed descending (most flaky first)
    tests.sort(key=lambda t: t.runs_failed, reverse=True)

    return tests


def generate_markdown_report(
    insights_data: Dict[str, List[FlakyTest]],
    insight_repositories: Dict[str, str]
) -> str:
    """
    Generate markdown report for GitHub Step Summary with summary and detail sections.

    Args:
        insights_data: Dictionary mapping insight label to list of flaky tests
        insight_repositories: Dictionary mapping insight label to repository (org/repo)

    Returns:
        Markdown formatted report
    """
    # Calculate total tests across all insights
    total_tests = sum(len(tests) for tests in insights_data.values())

    lines = [
        "# Flaky Test Report",
        "",
        f"**Period**: Last 7 days",
        f"**Total flaky tests found**: {total_tests}",
        "",
        "## Summary"
    ]

    # Generate all summary tables first
    for label, tests in insights_data.items():
        if not tests:
            continue

        lines.extend([
            "",
            f"### {label} Flaky Tests",
            "",
            f"**Count**: {len(tests)} flaky tests",
            f"**Repository**: {insight_repositories[label]}",
            "",
            "| Test | Workflow | Failures |",
            "|------|----------|----------|"
        ])

        # Summary section - just test name, workflow, and failure count
        for test in tests:
            # Truncate test name if too long
            test_name = test.test_name
            if len(test_name) > 80:
                test_name = test_name[:77] + "..."

            lines.append(
                f"| `{test_name}` | {test.workflow_name} | {test.runs_failed} |"
            )

    # Now generate all detail sections
    lines.extend([
        "",
        "---",
        "",
        "## Details"
    ])

    for label, tests in insights_data.items():
        if not tests:
            continue

        repository = insight_repositories[label]

        lines.extend([
            "",
            f"### {label} Flaky Tests",
            ""
        ])

        for test in tests:
            # Test header
            lines.append(f"#### {test.test_name}")
            lines.append("")
            lines.append(f"**Workflow**: {test.workflow_name}")
            lines.append(f"**Failures**: {test.runs_failed}")
            lines.append("")

            # Show unique affected branches
            unique_branches = list(dict.fromkeys(test.branches))
            if unique_branches:
                lines.append(f"**Affected Branches**: {', '.join(unique_branches)}")
                lines.append("")

            # List ALL failed runs
            lines.append(f"**Failed Runs**:")
            run_urls = test.get_workflow_urls(repository)
            for i, url in enumerate(run_urls, 1):
                # Extract run ID from URL (before /attempts/1)
                run_id = url.split('/')[-3]
                lines.append(f"{i}. [{run_id}]({url})")
            lines.append("")

    lines.extend([
        "---",
        "",
        "_Report generated from PostHog insight data_"
    ])

    return "\n".join(lines)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Fetch PostHog flaky test data and generate reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Single insight
  %(prog)s --insight-id "nkKYhNp7:Fork:acryldata/datahub-fork" ...

  # Multiple insights
  %(prog)s --insight-id "nkKYhNp7:Fork:acryldata/datahub-fork" \\
           --insight-id "BumyM8n4:OSS:datahub-project/datahub" ...
        '''
    )

    # Required arguments
    parser.add_argument('--posthog-api-key', required=True, help='PostHog API key')
    parser.add_argument('--posthog-host', required=True, help='PostHog host URL')
    parser.add_argument('--project-id', required=True, help='PostHog project ID')
    parser.add_argument(
        '--insight-id',
        action='append',
        required=True,
        help='PostHog insight ID in format "id:label:repository" (can be specified multiple times)'
    )
    parser.add_argument('--output-dir', required=True, type=Path, help='Output directory')

    args = parser.parse_args()

    # Parse insight IDs, labels, and repositories
    insights = []
    for insight_spec in args.insight_id:
        parts = insight_spec.split(':', 2)
        if len(parts) != 3:
            print(f"⚠️ Invalid insight spec '{insight_spec}' - expected format 'id:label:repository'", file=sys.stderr)
            sys.exit(1)

        insight_id, label, repository = parts
        insights.append(InsightConfig(
            insight_id=insight_id.strip(),
            label=label.strip(),
            repository=repository.strip()
        ))

    if not insights:
        print("⚠️ No insights specified", file=sys.stderr)
        sys.exit(1)

    try:
        # Create output directory
        args.output_dir.mkdir(parents=True, exist_ok=True)

        # Configure PostHog
        config = PostHogConfig(
            api_key=args.posthog_api_key,
            host=args.posthog_host,
            project_id=args.project_id
        )

        print("=" * 60)
        print("Flaky Test Report Generator")
        print("=" * 60)
        print(f"Insights:")
        for i in insights:
            print(f"  - {i.label}: {i.repository} ({i.insight_id})")
        print()

        # Fetch and parse data for each insight
        insights_data: Dict[str, List[FlakyTest]] = {}
        insight_repositories: Dict[str, str] = {}
        all_tests_json = {}

        for insight in insights:
            print(f"\n📊 Fetching {insight.label} data...")
            print(f"PostHog: {args.posthog_host}/project/{args.project_id}/insights/{insight.insight_id}")
            print(f"Repository: {insight.repository}")

            # Fetch data from PostHog with retry logic
            json_data = fetch_posthog_insight_with_retry(config, insight.insight_id)

            # Save raw JSON
            json_file = args.output_dir / f"flaky-tests-raw-{insight.label.lower().replace(' ', '-')}.json"
            json_file.write_bytes(json_data)
            print(f"💾 Saved raw JSON to: {json_file}")

            # Parse JSON data
            print(f"Parsing {insight.label} test data...")
            tests = parse_posthog_json(json_data)
            print(f"✅ Parsed {len(tests)} flaky tests")

            insights_data[insight.label] = tests
            insight_repositories[insight.label] = insight.repository

            # Collect structured JSON for this insight
            all_tests_json[insight.label] = [
                {
                    "workflow_name": t.workflow_name,
                    "test_name": t.test_name,
                    "runs_failed": t.runs_failed,
                    "branches": t.branches,
                    "run_ids": t.run_ids,
                    "workflow_urls": t.get_workflow_urls(insight.repository)
                }
                for t in tests
            ]

        # Check if we have any tests
        total_tests = sum(len(tests) for tests in insights_data.values())
        if total_tests == 0:
            print("\nℹ️ No flaky tests found across all insights")
            sys.exit(0)

        # Generate markdown report
        print("\n\nGenerating combined markdown report...")
        markdown = generate_markdown_report(insights_data, insight_repositories)

        md_file = args.output_dir / "flaky-tests-report.md"
        md_file.write_text(markdown)
        print(f"✅ Saved markdown report to: {md_file}")

        # Write to GitHub Step Summary
        github_step_summary = os.environ.get('GITHUB_STEP_SUMMARY')
        if github_step_summary:
            Path(github_step_summary).write_text(markdown)
            print(f"✅ Wrote report to GitHub Step Summary")

        # Save structured JSON for further analysis
        json_file = args.output_dir / "flaky-tests-structured.json"
        json_file.write_text(json.dumps(all_tests_json, indent=2))
        print(f"✅ Saved structured JSON to: {json_file}")

        # Summary
        print("\n" + "=" * 60)
        print("📊 Summary")
        print("=" * 60)
        for label, tests in insights_data.items():
            print(f"\n{label}: {len(tests)} flaky tests")
            if tests:
                print(f"  Top 3 most flaky:")
                for i, test in enumerate(tests[:3], 1):
                    print(f"    {i}. {test.test_name[:60]}... ({test.runs_failed} failures)")

        print(f"\nTotal flaky tests: {total_tests}")
        print("\n✅ Report generation complete!")
        sys.exit(0)

    except urllib.error.HTTPError as e:
        print(f"⚠️ HTTP error fetching PostHog data: {e.code} {e.reason}", file=sys.stderr)
        print(f"Response: {e.read().decode('utf-8')[:200]}", file=sys.stderr)
        sys.exit(0)
    except urllib.error.URLError as e:
        print(f"⚠️ Network error: {e.reason}", file=sys.stderr)
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"⚠️ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(0)


if __name__ == '__main__':
    main()
