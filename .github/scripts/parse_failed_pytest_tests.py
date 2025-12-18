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


def parse_failed_modules(input_dir: Path) -> Optional[Set[str]]:
    """
    Extract failed test module paths from pytest JUnit XML files.

    Args:
        input_dir: Directory containing junit.*.xml files

    Returns:
        Set of relative test file paths (e.g., 'tests/structured_properties/test_structured_properties.py')
        None if no XML files found
        Empty set if all tests passed
    """
    failed_modules: Set[str] = set()
    xml_files = list(input_dir.rglob("junit*.xml"))

    if not xml_files:
        return None

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            # Find all testcase elements with failures or errors
            for testcase in root.findall('.//testcase'):
                # Check if this testcase has a failure or error
                has_failure = testcase.find('failure') is not None
                has_error = testcase.find('error') is not None

                if has_failure or has_error:
                    classname = testcase.get('classname', '')
                    if not classname:
                        continue

                    # Convert classname from dotted format to file path
                    # Example: tests.structured_properties.test_structured_properties
                    #       -> tests/structured_properties/test_structured_properties.py
                    module_path = classname.replace('.', '/') + '.py'
                    failed_modules.add(module_path)

        except ET.ParseError as e:
            print(f"Warning: Failed to parse {xml_file}: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"Warning: Error processing {xml_file}: {e}", file=sys.stderr)
            continue

    return failed_modules


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Parse failed pytest modules from JUnit XML',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0 - Failed modules found (list written to output file)
  1 - Error during processing
  2 - No failures found (all tests passed)
  3 - No test results found (missing artifacts)
        """
    )
    parser.add_argument(
        '--input-dir',
        required=True,
        type=Path,
        help='Directory containing junit*.xml files'
    )
    parser.add_argument(
        '--output',
        required=True,
        type=Path,
        help='Output file path for failed module list'
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
        args.output.write_text('\n'.join(sorted(failed_modules)) + '\n')
        print(f"Found {len(failed_modules)} failed module(s):")
        for module in sorted(failed_modules):
            print(f"  - {module}")
        sys.exit(0)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
