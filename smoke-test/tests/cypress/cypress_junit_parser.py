"""Parse Cypress JUnit XML test results for pytest reporting integration."""

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class CypressTestResult:
    """Represents a single Cypress test result."""

    spec_file: str
    test_name: str
    status: str  # "passed", "failed", or "skipped"
    duration: float
    failure_message: str | None = None


def parse_cypress_junit_results(results_dir: str) -> List[CypressTestResult]:
    """
    Parse Cypress JUnit XML results from the specified directory.

    Args:
        results_dir: Path to directory containing cypress-test-*.xml files

    Returns:
        List of CypressTestResult objects
    """
    results_path = Path(results_dir)

    if not results_path.exists():
        logger.warning(f"Results directory does not exist: {results_dir}")
        return []

    xml_files = list(results_path.glob("cypress-test-*.xml"))

    if not xml_files:
        logger.warning(f"No Cypress XML result files found in: {results_dir}")
        return []

    logger.info(f"Found {len(xml_files)} Cypress XML result file(s)")

    all_results = []

    for xml_file in xml_files:
        try:
            results = _parse_xml_file(xml_file)
            all_results.extend(results)
            logger.info(f"Parsed {len(results)} test results from {xml_file.name}")
        except Exception as e:
            logger.error(f"Error parsing {xml_file.name}: {e}")

    return all_results


def _parse_xml_file(xml_file: Path) -> List[CypressTestResult]:
    """Parse a single JUnit XML file."""
    tree = ET.parse(xml_file)
    root = tree.getroot()

    results = []

    # JUnit XML structure:
    # <testsuites>
    #   <testsuite name="..." tests="..." failures="..." skipped="...">
    #     <testcase name="..." classname="..." time="...">
    #       <failure message="...">...</failure>  (if failed)
    #       <skipped/>  (if skipped)
    #     </testcase>
    #   </testsuite>
    # </testsuites>

    # Handle both <testsuites> root and direct <testsuite> root
    testsuites = root.findall(".//testsuite")

    for testsuite in testsuites:
        # Cypress creates one testsuite per it() test
        # The testsuite name is the describe block name
        testsuite_name = testsuite.get("name", "")

        # Skip the "Root Suite" testsuite (it has no tests)
        if testsuite_name == "Root Suite":
            continue

        for testcase in testsuite.findall("testcase"):
            # The testcase name is the filepath
            # The testcase classname is "describe block + filepath"
            # We need to extract the actual test name from somewhere else

            testcase_name = testcase.get("name", "unknown")
            time_str = testcase.get("time", "0")

            # Extract spec file from testcase name (which is the filepath)
            spec_file = testcase_name

            # The testsuite name is the describe block
            # We need to get the individual it() test name
            # Unfortunately, cypress-junit-reporter doesn't include it!
            test_name = testsuite_name

            logger.info(f"Cypress test: {test_name} in {spec_file} ({time_str}s)")

            try:
                duration = float(time_str)
            except ValueError:
                duration = 0.0

            # Determine test status
            failure = testcase.find("failure")
            error = testcase.find("error")
            skipped = testcase.find("skipped")

            if failure is not None or error is not None:
                status = "failed"
                failure_elem = failure if failure is not None else error
                failure_message = ""
                if failure_elem is not None:
                    failure_message = failure_elem.get("message", "")
                    if not failure_message and failure_elem.text:
                        failure_message = failure_elem.text.strip()
            elif skipped is not None:
                status = "skipped"
                failure_message = None
            else:
                status = "passed"
                failure_message = None

            result = CypressTestResult(
                spec_file=spec_file,
                test_name=test_name,
                status=status,
                duration=duration,
                failure_message=failure_message,
            )

            results.append(result)

    return results
