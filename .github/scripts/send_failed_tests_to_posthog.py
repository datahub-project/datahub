#!/usr/bin/env python3
"""
Send failed test results to PostHog for CI metrics tracking.

This script parses JUnit XML files to extract failed tests and sends
individual failure events to PostHog for analysis and monitoring.

Exit Codes:
    0: Success (events sent or no failures found or graceful skip)
"""

import argparse
import json
import sys
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass


@dataclass
class FailedTest:
    """Represents a failed test with its details."""
    name: str
    test_type: str
    error_message: Optional[str] = None


@dataclass
class PostHogConfig:
    """Configuration for PostHog API."""
    api_key: str
    host: str
    timeout: int = 10


def detect_xml_type(xml_file: Path) -> str:
    """
    Detect the type of JUnit XML file.

    Returns: 'cypress', 'pytest', 'java', or 'unknown'
    """
    filename = xml_file.name

    # Check filename patterns
    if filename.startswith('cypress-test-'):
        return 'cypress'
    elif filename.startswith('junit') and filename.endswith('.xml'):
        return 'pytest'
    elif filename.startswith('TEST-') and filename.endswith('.xml'):
        return 'java'

    return 'unknown'


def parse_cypress_failures(xml_file: Path) -> List[FailedTest]:
    """
    Parse Cypress JUnit XML to extract failed tests.

    Cypress reports at the spec file level, not individual test level.
    """
    failed_tests = []

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Find testsuite with file attribute
        file_testsuite = root.find('.//testsuite[@file]')
        if file_testsuite is None:
            return failed_tests

        file_path = file_testsuite.get('file', '')
        if not file_path:
            return failed_tests

        # Check if any testcase has failures
        has_failures = False
        error_msg = None

        for testcase in root.findall('.//testcase'):
            failure = testcase.find('failure')
            error = testcase.find('error')

            if failure is not None or error is not None:
                has_failures = True
                if error_msg is None:  # Get first error message
                    if failure is not None:
                        error_msg = failure.get('message', failure.text or '')
                    elif error is not None:
                        error_msg = error.get('message', error.text or '')
                break

        if has_failures:
            # Strip 'cypress/e2e/' prefix
            if file_path.startswith('cypress/e2e/'):
                file_path = file_path.replace('cypress/e2e/', '')

            failed_tests.append(FailedTest(
                name=file_path,
                test_type='cypress',
                error_message=error_msg[:200] if error_msg else None
            ))

    except ET.ParseError as e:
        print(f"⚠️ Failed to parse {xml_file}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"⚠️ Error processing {xml_file}: {e}", file=sys.stderr)

    return failed_tests


def parse_pytest_failures(xml_file: Path) -> List[FailedTest]:
    """
    Parse pytest JUnit XML to extract failed tests.

    Pytest reports individual test cases with classname in dot notation.
    """
    failed_tests = []

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for testcase in root.findall('.//testcase'):
            failure = testcase.find('failure')
            error = testcase.find('error')

            if failure is not None or error is not None:
                classname = testcase.get('classname', '')
                testname = testcase.get('name', '')

                if not classname:
                    continue

                # Convert classname from dotted format to file path
                # Example: tests.structured_properties.test_structured_properties
                #       -> tests/structured_properties/test_structured_properties.py
                module_path = classname.replace('.', '/') + '.py'

                # Format as pytest convention: module::test_name
                test_identifier = f"{module_path}::{testname}" if testname else module_path

                # Get error message
                error_msg = None
                if failure is not None:
                    error_msg = failure.get('message', failure.text or '')
                elif error is not None:
                    error_msg = error.get('message', error.text or '')

                failed_tests.append(FailedTest(
                    name=test_identifier,
                    test_type='pytest',
                    error_message=error_msg[:200] if error_msg else None
                ))

    except ET.ParseError as e:
        print(f"⚠️ Failed to parse {xml_file}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"⚠️ Error processing {xml_file}: {e}", file=sys.stderr)

    return failed_tests


def parse_java_failures(xml_file: Path) -> List[FailedTest]:
    """
    Parse Java/Gradle JUnit XML to extract failed tests.

    Standard JUnit format with classname as fully-qualified class names.
    """
    failed_tests = []

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for testcase in root.findall('.//testcase'):
            failure = testcase.find('failure')
            error = testcase.find('error')

            if failure is not None or error is not None:
                classname = testcase.get('classname', '')
                testname = testcase.get('name', '')

                if not classname:
                    continue

                # Format as ClassName.methodName
                test_identifier = f"{classname}.{testname}" if testname else classname

                # Get error message
                error_msg = None
                if failure is not None:
                    error_msg = failure.get('message', failure.text or '')
                elif error is not None:
                    error_msg = error.get('message', error.text or '')

                failed_tests.append(FailedTest(
                    name=test_identifier,
                    test_type='java',
                    error_message=error_msg[:200] if error_msg else None
                ))

    except ET.ParseError as e:
        print(f"⚠️ Failed to parse {xml_file}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"⚠️ Error processing {xml_file}: {e}", file=sys.stderr)

    return failed_tests


def parse_all_failures(input_dir: Path) -> List[FailedTest]:
    """
    Parse all JUnit XML files in directory and extract failed tests.

    Args:
        input_dir: Directory containing test result XML files

    Returns:
        List of FailedTest objects
    """
    all_failures = []
    xml_files = list(input_dir.rglob('*.xml'))

    if not xml_files:
        print("ℹ️ No XML files found in directory")
        return all_failures

    print(f"Found {len(xml_files)} XML file(s) to parse")

    for xml_file in xml_files:
        xml_type = detect_xml_type(xml_file)

        if xml_type == 'cypress':
            failures = parse_cypress_failures(xml_file)
        elif xml_type == 'pytest':
            failures = parse_pytest_failures(xml_file)
        elif xml_type == 'java':
            failures = parse_java_failures(xml_file)
        else:
            # Try all parsers for unknown types
            failures = (
                parse_cypress_failures(xml_file) or
                parse_pytest_failures(xml_file) or
                parse_java_failures(xml_file)
            )

        all_failures.extend(failures)

    return all_failures


def send_posthog_event(
    config: PostHogConfig,
    test: FailedTest,
    metadata: Dict[str, Any]
) -> bool:
    """
    Send a single test failure event to PostHog.

    Args:
        config: PostHog configuration
        test: Failed test details
        metadata: Workflow metadata (repository, branch, run_id, etc.)

    Returns:
        True if event sent successfully, False otherwise
    """
    try:
        endpoint = f"{config.host.rstrip('/')}/capture/"

        # Build event properties
        properties = {
            "test_name": test.name,
            "test_type": test.test_type,
            **metadata
        }

        # Build event payload
        payload = {
            "api_key": config.api_key,
            "event": "ci-test-failure",
            "properties": properties,
            "distinct_id": f"github-actions-{metadata.get('github_repository', 'unknown')}"
        }

        # Send request
        data = json.dumps(payload).encode('utf-8')
        request = urllib.request.Request(
            endpoint,
            data=data,
            headers={'Content-Type': 'application/json'}
        )

        with urllib.request.urlopen(request, timeout=config.timeout) as response:
            status_code = response.getcode()
            if status_code == 200:
                print(f"✅ Sent event for {test.test_type} test: {test.name}")
                return True
            else:
                print(f"⚠️ PostHog returned HTTP {status_code} for test: {test.name}", file=sys.stderr)
                return False

    except urllib.error.HTTPError as e:
        print(f"⚠️ HTTP error sending event for {test.name}: {e.code} {e.reason}", file=sys.stderr)
        return False
    except urllib.error.URLError as e:
        print(f"⚠️ Network error sending event for {test.name}: {e.reason}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"⚠️ Unexpected error sending event for {test.name}: {e}", file=sys.stderr)
        return False


def process_and_send_failures(
    input_dir: Path,
    config: PostHogConfig,
    workflow_metadata: Dict[str, Any]
) -> None:
    """
    Parse XML files and send events for each failure.

    Args:
        input_dir: Directory containing test result XML files
        config: PostHog configuration
        workflow_metadata: Workflow metadata to include in events
    """
    print(f"Parsing test results from: {input_dir}")

    # Parse all XML files
    failed_tests = parse_all_failures(input_dir)

    if not failed_tests:
        print("ℹ️ No test failures found")
        return

    print(f"\nFound {len(failed_tests)} failed test(s)")

    # Group by type for summary
    by_type = {}
    for test in failed_tests:
        by_type.setdefault(test.test_type, []).append(test)

    for test_type, tests in by_type.items():
        print(f"  - {test_type}: {len(tests)} failure(s)")

    # Send events for each failed test
    success_count = 0
    failure_count = 0

    print("\nSending events to PostHog:")
    for test in failed_tests:
        if send_posthog_event(config, test, workflow_metadata):
            success_count += 1
        else:
            failure_count += 1

    print(f"\n📊 Summary: {success_count} events sent, {failure_count} failed")

    if failure_count > 0:
        print(f"⚠️ Warning: {failure_count} events failed to send", file=sys.stderr)


def build_workflow_metadata(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Build workflow metadata dictionary from command line arguments.

    Args:
        args: Parsed command line arguments

    Returns:
        Dictionary of workflow metadata
    """
    metadata: Dict[str, Any] = {
        "github_repository": args.repository,
        "workflow_name": args.workflow_name,
        "head_branch": args.branch,
        "run_id": args.run_id,
    }

    # Add optional fields if provided
    if args.run_attempt:
        metadata["run_attempt"] = args.run_attempt

    # docker-unified matrix context
    if args.batch is not None:
        metadata["batch"] = args.batch
    if args.batch_count is not None:
        metadata["batch_count"] = args.batch_count
    if args.test_strategy:
        metadata["test_strategy"] = args.test_strategy

    # build-and-test matrix context
    if args.command:
        metadata["command"] = args.command
    if args.timezone:
        metadata["timezone"] = args.timezone

    return metadata


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Send failed test results to PostHog for CI metrics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0 - Success (events sent or no failures found)

Examples:
  # metadata-io workflow
  python3 send_failed_tests_to_posthog.py \\
    --input-dir ./test-results \\
    --posthog-api-key $POSTHOG_API_KEY \\
    --repository datahub-project/datahub \\
    --workflow-name "metadata-io" \\
    --branch main \\
    --run-id 12345

  # docker-unified workflow with matrix
  python3 send_failed_tests_to_posthog.py \\
    --input-dir ./test-results \\
    --posthog-api-key $POSTHOG_API_KEY \\
    --repository datahub-project/datahub \\
    --workflow-name "Docker Build, Scan, Test" \\
    --branch main \\
    --run-id 12345 \\
    --batch 0 \\
    --batch-count 8 \\
    --test-strategy pytests
        """
    )

    # Required arguments
    parser.add_argument(
        '--input-dir',
        required=True,
        type=Path,
        help='Directory containing test result XML files (searched recursively)'
    )
    parser.add_argument(
        '--posthog-api-key',
        required=True,
        help='PostHog API key'
    )
    parser.add_argument(
        '--repository',
        required=True,
        help='GitHub repository (org/repo)'
    )
    parser.add_argument(
        '--workflow-name',
        required=True,
        help='GitHub workflow name'
    )
    parser.add_argument(
        '--branch',
        required=True,
        help='Git branch name'
    )
    parser.add_argument(
        '--run-id',
        required=True,
        help='GitHub workflow run ID'
    )

    # Optional arguments
    parser.add_argument(
        '--posthog-host',
        default='https://app.posthog.com',
        help='PostHog host URL (default: https://app.posthog.com)'
    )
    parser.add_argument(
        '--run-attempt',
        help='GitHub workflow run attempt number'
    )
    parser.add_argument(
        '--batch',
        type=int,
        help='Batch number (for docker-unified matrix)'
    )
    parser.add_argument(
        '--batch-count',
        type=int,
        help='Total batch count (for docker-unified matrix)'
    )
    parser.add_argument(
        '--test-strategy',
        help='Test strategy: pytests or cypress (for docker-unified matrix)'
    )
    parser.add_argument(
        '--command',
        help='Build command (for build-and-test matrix)'
    )
    parser.add_argument(
        '--timezone',
        help='Timezone (for build-and-test matrix)'
    )

    args = parser.parse_args()

    try:
        # Validate input directory exists
        if not args.input_dir.exists():
            print(f"ℹ️ Input directory does not exist: {args.input_dir}")
            print("No test results to process")
            sys.exit(0)

        if not args.input_dir.is_dir():
            print(f"⚠️ Input path is not a directory: {args.input_dir}", file=sys.stderr)
            sys.exit(0)  # Exit 0 to not block workflow

        # Build PostHog configuration
        config = PostHogConfig(
            api_key=args.posthog_api_key,
            host=args.posthog_host
        )

        # Build workflow metadata
        metadata = build_workflow_metadata(args)

        print(f"Workflow: {args.workflow_name}")
        print(f"Branch: {args.branch}")
        print(f"Run ID: {args.run_id}")
        if args.batch is not None:
            print(f"Batch: {args.batch}/{args.batch_count} ({args.test_strategy})")
        print()

        # Process and send failures
        process_and_send_failures(args.input_dir, config, metadata)

        # Always exit 0 (graceful degradation)
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"⚠️ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        # Exit 0 to not block workflow
        sys.exit(0)


if __name__ == '__main__':
    main()
