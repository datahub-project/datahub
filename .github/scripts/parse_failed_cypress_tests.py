#!/usr/bin/env python3
"""
Parse failed Cypress tests from JUnit XML files.

This script extracts the file paths of failed tests from Cypress JUnit XML
test results, used to retry only failed tests in CI/CD workflows.

Exit Codes:
    0: Failed tests found (success - list written to output file)
    1: Error during processing (fall back to full test run)
    2: No failures found (all passed - skip batch)
    3: No test results (missing artifacts - run all tests)
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Set


def parse_failed_tests(input_dir: Path) -> Optional[Set[str]]:
    """
    Extract failed test file paths from Cypress JUnit XML files.

    Args:
        input_dir: Directory containing cypress-test-*.xml files

    Returns:
        Set of relative test file paths (e.g., 'mutations/dataset_ownership.js')
        None if no XML files found
        Empty set if all tests passed
    """
    failed_tests: Set[str] = set()
    xml_files = list(input_dir.rglob("cypress-test-*.xml"))

    if not xml_files:
        return None

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            # Find testsuite with file attribute to get the test file path
            file_testsuite = root.find('.//testsuite[@file]')
            if file_testsuite is None:
                continue

            file_path = file_testsuite.get('file')
            if not file_path:
                continue

            # Check if ANY testcase in the entire XML has failures or errors
            # (testcases are in sibling testsuites, not children of the file testsuite)
            has_failures = False
            for testcase in root.findall('.//testcase'):
                if testcase.find('failure') is not None or testcase.find('error') is not None:
                    has_failures = True
                    break

            if has_failures:
                # Strip 'cypress/e2e/' prefix to match expected format
                if file_path.startswith('cypress/e2e/'):
                    relative_path = file_path.replace('cypress/e2e/', '')
                else:
                    relative_path = file_path
                failed_tests.add(relative_path)

        except ET.ParseError as e:
            print(f"Warning: Failed to parse {xml_file}: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"Warning: Error processing {xml_file}: {e}", file=sys.stderr)
            continue

    return failed_tests


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Parse failed Cypress tests from JUnit XML',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0 - Failed tests found (list written to output file)
  1 - Error during processing
  2 - No failures found (all tests passed)
  3 - No test results found (missing artifacts)
        """
    )
    parser.add_argument(
        '--input-dir',
        required=True,
        type=Path,
        help='Directory containing cypress-test-*.xml files'
    )
    parser.add_argument(
        '--output',
        required=True,
        type=Path,
        help='Output file path for failed test list'
    )
    args = parser.parse_args()

    try:
        failed_tests = parse_failed_tests(args.input_dir)

        if failed_tests is None:
            print("No test results found", file=sys.stderr)
            sys.exit(3)

        if not failed_tests:
            print("All tests passed - no failures to retry")
            args.output.write_text("")
            sys.exit(2)

        # Write failed tests (one per line)
        args.output.write_text('\n'.join(sorted(failed_tests)) + '\n')
        print(f"Found {len(failed_tests)} failed test(s):")
        for test in sorted(failed_tests):
            print(f"  - {test}")
        sys.exit(0)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
