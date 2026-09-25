#!/usr/bin/env python3
"""
Parse failed pytest tests from JUnit XML files.

This script extracts the module paths (test files) of failed tests from pytest
JUnit XML test results, used to retry only failed modules in CI/CD workflows.

Exit Codes:
    0: Failed modules found (success - list written to output file)
    1: Error during processing (fall back to full test run)
    2: No failures found (all passed - skip batch)
    3: No test results (missing artifacts - run all tests)
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Set


class UnmappedFailuresError(Exception):
    """JUnit reports errors/failures that could not be mapped to test modules."""


def _module_path_from_nodeid(name: str) -> Optional[str]:
    """Map a pytest nodeid (testcase name) to a .py path, or None if unusable."""
    if not name:
        return None
    nodeid = name.split("::", 1)[0].strip().lstrip("./")
    if nodeid.endswith(".py"):
        return nodeid
    return None


def _module_path_from_testcase(testcase: ET.Element) -> Optional[str]:
    classname = testcase.get("classname") or ""
    name = testcase.get("name") or ""

    if classname:
        parts = classname.split(".")
        if parts and parts[-1] and parts[-1][0].isupper():
            parts = parts[:-1]
        if parts:
            return "/".join(parts) + ".py"

    # Collection errors: classname is empty; name is the nodeid (file path).
    return _module_path_from_nodeid(name)


def _int_attr(element: ET.Element, attr: str) -> int:
    try:
        return int(element.get(attr) or 0)
    except ValueError:
        return 0


def _suite_reported_problems(root: ET.Element) -> bool:
    suites = root.findall(".//testsuite")
    if root.tag == "testsuite":
        suites = [root] + suites
    for suite in suites:
        if _int_attr(suite, "errors") > 0 or _int_attr(suite, "failures") > 0:
            return True
    return False


def parse_failed_modules(input_dir: Path) -> Optional[Set[str]]:
    """
    Extract failed test module paths from pytest JUnit XML files.

    Args:
        input_dir: Directory containing junit.*.xml files

    Returns:
        Set of relative test file paths (e.g., 'tests/structured_properties/test_structured_properties.py')
        None if no XML files found or none parsed
        Empty set if all tests passed

    Raises:
        UnmappedFailuresError: Suite reported errors/failures but no module paths
            could be extracted. Callers must not treat this as all-passed.
    """
    failed_modules: Set[str] = set()
    xml_files = list(input_dir.rglob("junit*.xml"))

    if not xml_files:
        return None

    parsed_any = False
    unmapped_failures = False

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
        except ET.ParseError as e:
            print(f"Warning: Failed to parse {xml_file}: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"Warning: Error processing {xml_file}: {e}", file=sys.stderr)
            continue

        parsed_any = True
        mapped_from_this_file = False

        for testcase in root.findall(".//testcase"):
            has_failure = testcase.find("failure") is not None
            has_error = testcase.find("error") is not None
            if not (has_failure or has_error):
                continue

            module_path = _module_path_from_testcase(testcase)
            if module_path:
                failed_modules.add(module_path)
                mapped_from_this_file = True
            else:
                unmapped_failures = True

        if _suite_reported_problems(root) and not mapped_from_this_file:
            unmapped_failures = True

    if not parsed_any:
        return None

    if not failed_modules and unmapped_failures:
        raise UnmappedFailuresError(
            "JUnit XML reported errors or failures that could not be mapped "
            "to test modules; refusing to treat results as all-passed"
        )

    return failed_modules


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Parse failed pytest modules from JUnit XML",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0 - Failed modules found (list written to output file)
  1 - Error during processing
  2 - No failures found (all tests passed)
  3 - No test results found (missing artifacts)
        """,
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        type=Path,
        help="Directory containing junit*.xml files",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output file path for failed module list",
    )
    args = parser.parse_args()

    try:
        failed_modules = parse_failed_modules(args.input_dir)

        if failed_modules is None:
            print("No test results found", file=sys.stderr)
            sys.exit(3)

        if not failed_modules:
            print("All tests passed - no failures to retry")
            args.output.write_text("")
            sys.exit(2)

        # Write failed modules (one per line)
        args.output.write_text("\n".join(sorted(failed_modules)) + "\n")
        print(f"Found {len(failed_modules)} failed module(s):")
        for module in sorted(failed_modules):
            print(f"  - {module}")
        sys.exit(0)

    except UnmappedFailuresError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
