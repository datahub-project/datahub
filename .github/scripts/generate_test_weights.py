#!/usr/bin/env python3
"""
Generate test weight files from historical CI test results.

This script parses JUnit XML files from multiple CI runs, calculates median
test durations, and generates JSON weight files for Pytest, Gradle, and Playwright tests.
"""

import argparse
import json
import math
import statistics
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional


def _safe_parse_root(xml_file: Path) -> Optional[ET.Element]:
    """Parse xml_file, printing a warning and returning None on failure instead of raising.
    Shared by every parser below so a malformed report from any framework fails the same way."""
    try:
        return ET.parse(xml_file).getroot()
    except ET.ParseError as e:
        print(f"Warning: Failed to parse {xml_file}: {e}")
    except Exception as e:
        print(f"Warning: Error processing {xml_file}: {e}")
    return None


def _parse_finite_duration(time_str: str, xml_file: Path) -> Optional[float]:
    """Float-convert a duration string, rejecting non-finite/negative values (float() accepts
    nan/inf, and inf would otherwise pass a later ">0" filter and poison a median)."""
    try:
        duration = float(time_str)
    except ValueError:
        print(f"Warning: Invalid duration '{time_str}' in {xml_file}")
        return None
    if not math.isfinite(duration) or duration < 0:
        return None
    return duration


def parse_pytest_results(artifact_dir: Path) -> Dict[str, List[float]]:
    """
    Parse Pytest JUnit XML files from multiple runs.

    Args:
        artifact_dir: Root directory containing run-* subdirectories

    Returns:
        Dictionary mapping test IDs to lists of durations across runs
        Example: {"test_e2e::test_gms_get_dataset": [262.8, 265.3, 260.1]}
    """
    test_durations: Dict[str, List[float]] = {}

    xml_files = list(artifact_dir.rglob("junit*.xml"))

    print(f"Found {len(xml_files)} Pytest XML files")

    for xml_file in xml_files:
        root = _safe_parse_root(xml_file)
        if root is None:
            continue

        for testcase in root.findall(".//testcase"):
            classname = testcase.get("classname", "")
            name = testcase.get("name", "")

            # Build test ID
            if classname and name:
                test_id = f"{classname}::{name}"
            elif name:
                test_id = name
            else:
                continue

            duration = _parse_finite_duration(testcase.get("time", "0"), xml_file)
            if duration is not None and duration > 0:
                test_durations.setdefault(test_id, []).append(duration)

    return test_durations


def parse_gradle_results(artifact_dir: Path) -> Dict[str, List[float]]:
    """
    Parse Gradle JUnit XML files (TEST-*.xml) from multiple runs.

    Keyed by fully-qualified class name only (not module): the class->module mapping is
    resolved by the sharder at repo root, which avoids trying to recover the module from
    download-prefixed artifact paths here. Times are summed per class per report (one sample
    per run), then medianed across runs by calculate_median_weights.

    Returns:
        Dictionary mapping FQCN -> list of per-run durations.
        Example: {"com.linkedin.datahub.graphql.GraphQLEngineTest": [12.1, 11.8]}
    """
    test_durations: Dict[str, List[float]] = {}

    xml_files = list(artifact_dir.rglob("TEST-*.xml"))
    print(f"Found {len(xml_files)} Gradle XML files")

    for xml_file in xml_files:
        root = _safe_parse_root(xml_file)
        if root is None:
            continue

        per_class: Dict[str, float] = {}
        for testcase in root.findall(".//testcase"):
            classname = testcase.get("classname", "")
            if not classname:
                continue
            duration = _parse_finite_duration(testcase.get("time", "0"), xml_file)
            if duration is None:
                continue
            per_class[classname] = per_class.get(classname, 0.0) + duration
        for classname, total in per_class.items():
            if total > 0:
                test_durations.setdefault(classname, []).append(total)

    return test_durations


def _run_dir(xml_file: Path) -> Path:
    """Nearest ancestor directory named run-<id> (download_test_artifacts.sh's convention),
    falling back to the file's own parent if none is found."""
    for parent in xml_file.parents:
        if parent.name.startswith("run-"):
            return parent
    return xml_file.parent


def parse_playwright_results(artifact_dir: Path) -> Dict[str, List[float]]:
    """
    Parse Playwright JUnit XML files from multiple runs.

    Playwright's junit reporter emits one flat <testsuite name="path/to/file.spec.ts"
    time="22.9"> per spec file, relative to testDir -- no nested root suite to unwrap.
    But Playwright's --shard=N/M splits by test *count*, not by file,
    so one file's tests can land in two different shards' junit.xml within the same run.
    Sum a file's duration across all of a run's shard XMLs before treating it as one
    sample -- otherwise each fragment is counted as an independent (much smaller) sample,
    silently halving that file's median weight. Mirrors parse_gradle_results()'s per-class,
    per-run summing for the same reason.

    Returns:
        Dictionary mapping spec file paths to lists of durations across runs
        Example: {"analytics/analytics.spec.ts": [22.9, 21.4, 23.1]}
    """
    per_run_totals: Dict[Path, Dict[str, float]] = {}

    xml_files = list(artifact_dir.rglob("junit.xml"))
    print(f"Found {len(xml_files)} Playwright XML files")

    for xml_file in xml_files:
        root = _safe_parse_root(xml_file)
        if root is None:
            continue

        run_dir = _run_dir(xml_file)
        for testsuite in root.findall(".//testsuite"):
            file_path = testsuite.get("name", "")
            if not file_path:
                continue
            duration = _parse_finite_duration(testsuite.get("time", "0"), xml_file)
            if duration is None:
                continue
            totals = per_run_totals.setdefault(run_dir, {})
            totals[file_path] = totals.get(file_path, 0.0) + duration

    test_durations: Dict[str, List[float]] = {}
    for totals in per_run_totals.values():
        for file_path, total in totals.items():
            if total > 0:
                test_durations.setdefault(file_path, []).append(total)

    return test_durations


def calculate_median_weights(
    test_durations: Dict[str, List[float]], key_name: str = "filePath"
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
        results.append({key_name: test_id, "duration": f"{median:.3f}s"})

    # Sort alphabetically by test identifier for stable, reviewable diffs.
    results.sort(key=lambda x: x[key_name])

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Generate test weight files from CI test results"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing test artifacts (organized by run ID)",
    )
    parser.add_argument(
        "--pytest-output",
        type=Path,
        required=False,
        help="Output path for Pytest test weights JSON",
    )
    parser.add_argument(
        "--gradle-output",
        type=Path,
        required=False,
        help="Output path for Gradle test weights JSON (keyed by FQCN)",
    )
    parser.add_argument(
        "--playwright-output",
        type=Path,
        required=False,
        help="Output path for Playwright test weights JSON (keyed by spec file path)",
    )

    args = parser.parse_args()

    if not (args.pytest_output or args.gradle_output or args.playwright_output):
        parser.error(
            "at least one of --pytest-output/--gradle-output/--playwright-output is required"
        )

    if not args.input_dir.exists():
        print(f"Error: Input directory does not exist: {args.input_dir}")
        sys.exit(1)

    pytest_durations = {}
    if args.pytest_output:
        print("\n" + "=" * 60)
        print("Parsing Pytest test results...")
        print("=" * 60)
        pytest_durations = parse_pytest_results(args.input_dir)
        print(f"Found {len(pytest_durations)} unique Pytest tests")

    gradle_durations = {}
    if args.gradle_output:
        print("\n" + "=" * 60)
        print("Parsing Gradle test results...")
        print("=" * 60)
        gradle_durations = parse_gradle_results(args.input_dir)
        print(f"Found {len(gradle_durations)} unique Gradle tests")

    playwright_durations = {}
    if args.playwright_output:
        print("\n" + "=" * 60)
        print("Parsing Playwright test results...")
        print("=" * 60)
        playwright_durations = parse_playwright_results(args.input_dir)
        print(f"Found {len(playwright_durations)} unique Playwright spec files")

    print("\n" + "=" * 60)
    print("Calculating median weights...")
    print("=" * 60)

    pytest_weights = (
        calculate_median_weights(pytest_durations, key_name="testId")
        if args.pytest_output
        else []
    )
    gradle_weights = (
        calculate_median_weights(gradle_durations, key_name="testId")
        if args.gradle_output
        else []
    )
    playwright_weights = (
        calculate_median_weights(playwright_durations, key_name="filePath")
        if args.playwright_output
        else []
    )

    # Write output files
    print("\n" + "=" * 60)
    print("Writing output files...")
    print("=" * 60)

    for output_path, weights, label in (
        (args.pytest_output, pytest_weights, "Pytest"),
        (args.gradle_output, gradle_weights, "Gradle"),
        (args.playwright_output, playwright_weights, "Playwright"),
    ):
        if output_path is None:
            continue
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(weights, f, indent=2)
            f.write("\n")
        print(f"Wrote {len(weights)} {label} weights to: {output_path}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
