#!/usr/bin/env python3
"""
Generate test weight files from historical CI test results.

This script parses JUnit XML files from multiple CI runs, calculates median
test durations, and generates JSON weight files for both Cypress and Pytest tests.
"""

import argparse
import json
import statistics
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List


def parse_cypress_results(artifact_dir: Path) -> Dict[str, List[float]]:
    """
    Parse Cypress JUnit XML files from multiple runs.

    Args:
        artifact_dir: Root directory containing run-* subdirectories

    Returns:
        Dictionary mapping test file paths to lists of durations across runs
        Example: {"glossaryV2/v2_glossary_navigation.js": [94.8, 95.2, 94.5]}
    """
    test_durations = {}

    # Find all cypress-test-*.xml files
    xml_files = list(artifact_dir.rglob("cypress-test-*.xml"))

    print(f"Found {len(xml_files)} Cypress XML files")

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            # Find the root suite with file attribute
            root_suite = root.find('.//testsuite[@file]')
            if root_suite is None:
                continue

            file_path = root_suite.get('file')

            # Strip "cypress/e2e/" prefix to get relative path
            if file_path.startswith('cypress/e2e/'):
                relative_path = file_path.replace('cypress/e2e/', '')
            else:
                relative_path = file_path

            # Find all other testsuites (not the root suite) to get actual test durations
            all_testsuites = root.findall('.//testsuite')
            for testsuite in all_testsuites:
                # Skip if this is the root suite with file attribute
                if testsuite.get('file'):
                    continue

                time_str = testsuite.get('time', '0')
                try:
                    duration = float(time_str)

                    # Only add if duration is non-zero
                    if duration > 0:
                        if relative_path not in test_durations:
                            test_durations[relative_path] = []
                        test_durations[relative_path].append(duration)
                        # Only take the first non-zero duration per file
                        break
                except ValueError:
                    print(f"Warning: Invalid duration '{time_str}' in {xml_file}")

        except ET.ParseError as e:
            print(f"Warning: Failed to parse {xml_file}: {e}")
        except Exception as e:
            print(f"Warning: Error processing {xml_file}: {e}")

    return test_durations


def parse_pytest_results(artifact_dir: Path) -> Dict[str, List[float]]:
    """
    Parse Pytest JUnit XML files from multiple runs.

    Args:
        artifact_dir: Root directory containing run-* subdirectories

    Returns:
        Dictionary mapping test IDs to lists of durations across runs
        Example: {"test_e2e::test_gms_get_dataset": [262.8, 265.3, 260.1]}
    """
    test_durations = {}

    # Find all junit.*.xml files (exclude cypress ones)
    xml_files = []
    for xml_file in artifact_dir.rglob("junit*.xml"):
        # Exclude Cypress JUnit files
        if "cypress" not in xml_file.name:
            xml_files.append(xml_file)

    print(f"Found {len(xml_files)} Pytest XML files")

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            # Find all testcase elements
            for testcase in root.findall('.//testcase'):
                classname = testcase.get('classname', '')
                name = testcase.get('name', '')
                time_str = testcase.get('time', '0')

                # Build test ID
                if classname and name:
                    test_id = f"{classname}::{name}"
                elif name:
                    test_id = name
                else:
                    continue

                try:
                    duration = float(time_str)

                    # Only add if duration is non-zero
                    if duration > 0:
                        if test_id not in test_durations:
                            test_durations[test_id] = []
                        test_durations[test_id].append(duration)
                except ValueError:
                    print(f"Warning: Invalid duration '{time_str}' in {xml_file}")

        except ET.ParseError as e:
            print(f"Warning: Failed to parse {xml_file}: {e}")
        except Exception as e:
            print(f"Warning: Error processing {xml_file}: {e}")

    return test_durations


def calculate_median_weights(
    test_durations: Dict[str, List[float]],
    key_name: str = "filePath"
) -> List[Dict]:
    """
    Calculate median duration for each test.

    Args:
        test_durations: Dictionary mapping test IDs to duration lists
        key_name: Key name to use in output ("filePath" or "testId")

    Returns:
        List of dictionaries with test IDs and median durations
        Example: [{"filePath": "test1", "duration": "10.000s"}, ...]
    """
    results = []

    for test_id, durations in test_durations.items():
        if not durations:
            continue

        median = statistics.median(durations)
        results.append({
            key_name: test_id,
            "duration": f"{median:.3f}s"
        })

    # Sort by duration descending
    results.sort(key=lambda x: float(x["duration"][:-1]), reverse=True)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Generate test weight files from CI test results"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing test artifacts (organized by run ID)"
    )
    parser.add_argument(
        "--cypress-output",
        type=Path,
        required=True,
        help="Output path for Cypress test weights JSON"
    )
    parser.add_argument(
        "--pytest-output",
        type=Path,
        required=True,
        help="Output path for Pytest test weights JSON"
    )

    args = parser.parse_args()

    if not args.input_dir.exists():
        print(f"Error: Input directory does not exist: {args.input_dir}")
        sys.exit(1)

    print("=" * 60)
    print("Parsing Cypress test results...")
    print("=" * 60)
    cypress_durations = parse_cypress_results(args.input_dir)
    print(f"Found {len(cypress_durations)} unique Cypress tests")

    print("\n" + "=" * 60)
    print("Parsing Pytest test results...")
    print("=" * 60)
    pytest_durations = parse_pytest_results(args.input_dir)
    print(f"Found {len(pytest_durations)} unique Pytest tests")

    print("\n" + "=" * 60)
    print("Calculating median weights...")
    print("=" * 60)

    cypress_weights = calculate_median_weights(cypress_durations, key_name="filePath")
    pytest_weights = calculate_median_weights(pytest_durations, key_name="testId")

    print(f"Generated {len(cypress_weights)} Cypress weights")
    print(f"Generated {len(pytest_weights)} Pytest weights")

    # Create output directories if they don't exist
    args.cypress_output.parent.mkdir(parents=True, exist_ok=True)
    args.pytest_output.parent.mkdir(parents=True, exist_ok=True)

    # Write output files
    print("\n" + "=" * 60)
    print("Writing output files...")
    print("=" * 60)

    with open(args.cypress_output, 'w') as f:
        json.dump(cypress_weights, f, indent=2)
    print(f"Wrote Cypress weights to: {args.cypress_output}")

    with open(args.pytest_output, 'w') as f:
        json.dump(pytest_weights, f, indent=2)
    print(f"Wrote Pytest weights to: {args.pytest_output}")

    # Print top 5 longest tests for each type
    if cypress_weights:
        print("\n" + "=" * 60)
        print("Top 5 longest Cypress tests:")
        print("=" * 60)
        for i, test in enumerate(cypress_weights[:5], 1):
            print(f"{i}. {test['filePath']}: {test['duration']}")

    if pytest_weights:
        print("\n" + "=" * 60)
        print("Top 5 longest Pytest tests:")
        print("=" * 60)
        for i, test in enumerate(pytest_weights[:5], 1):
            print(f"{i}. {test['testId']}: {test['duration']}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
