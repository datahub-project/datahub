# ABOUTME: Pytest wrapper for running Cypress tests against remote instances.
# ABOUTME: Executes tests from directory specified by CYPRESS_TEST_DIR environment variable.
import logging
import os
import subprocess
import threading

import pytest

from tests.cypress.cypress_junit_parser import parse_cypress_junit_results
from tests.test_result_msg import get_module_tracker
from tests.utils import get_cypress_credentials

logger = logging.getLogger(__name__)


def process_and_report_cypress_results(results_dir: str, tracker) -> None:
    """
    Parse Cypress JUnit XML results and report them to the tracker.

    Deduplicates results by spec_file and aggregates outcomes.
    Reports as failed if ANY test in a spec failed, otherwise passed if ALL passed.
    """
    logger.info(f"Parsing Cypress test results from: {results_dir}")

    results = parse_cypress_junit_results(results_dir)
    logger.info(f"Found {len(results)} Cypress test results")

    # Deduplicate by spec_file: aggregate outcomes
    # cypress-junit-reporter creates one <testsuite> per it() test, but doesn't include
    # individual it() test names in the XML. This means we get multiple results with the
    # same spec_file and test_name (describe block). We deduplicate by spec_file and report
    # as failed if ANY test in that spec failed, otherwise passed if ALL passed.
    # Key: spec_file, Value: {"test_name": str, "has_failure": bool, "has_skip": bool}
    spec_aggregation: dict[str, dict[str, bool | str]] = {}
    for test_result in results:
        spec_file = test_result.spec_file
        if spec_file not in spec_aggregation:
            spec_aggregation[spec_file] = {
                "test_name": test_result.test_name,
                "has_failure": False,
                "has_skip": False,
            }

        if test_result.status == "failed":
            spec_aggregation[spec_file]["has_failure"] = True
        elif test_result.status == "skipped":
            spec_aggregation[spec_file]["has_skip"] = True

    logger.info(
        f"Deduplicated to {len(spec_aggregation)} unique spec files from {len(results)} results"
    )

    # Report each spec file once with aggregated status
    for spec_file, agg_data in spec_aggregation.items():
        test_name = str(agg_data["test_name"])
        nodeid = f"{spec_file}::{test_name}"

        if agg_data["has_failure"]:
            logger.info(f"Recording Cypress test failure: {nodeid}")
            tracker.record_failure(nodeid, test_name)
        elif agg_data["has_skip"]:
            logger.info(f"Recording Cypress test skip: {nodeid}")
            tracker.record_outcome(nodeid, "skipped")
        else:  # all passed
            logger.info(f"Recording Cypress test pass: {nodeid}")
            tracker.record_outcome(nodeid, "passed")


@pytest.mark.remote_tests
def test_run_cypress_remote():
    """
    Run Cypress tests from a directory specified by CYPRESS_TEST_DIR.
    This test is designed to run against remote DataHub instances without data ingestion.

    Environment variables:
        CYPRESS_TEST_DIR: Directory containing tests (relative to tests/cypress/cypress/), e.g., "cypress/customers/figma"
        CYPRESS_BASE_URL: Base URL of the remote instance (default: http://localhost:4173)
        CYPRESS_ADMIN_USERNAME: Admin username (default: admin)
        CYPRESS_ADMIN_PASSWORD: Admin password (required)
    """
    cypress_test_dir = os.getenv("CYPRESS_TEST_DIR", "")

    if not cypress_test_dir:
        pytest.skip("CYPRESS_TEST_DIR environment variable not set")

    base_url = os.getenv("CYPRESS_BASE_URL", "http://localhost:4173")
    username, password = get_cypress_credentials()

    if not password:
        message = "CYPRESS_ADMIN_PASSWORD environment variable not set"
        logger.info(message)
        pytest.skip(message)

    spec_pattern = f"{cypress_test_dir}/**/*.{{js,jsx,ts,tsx}}"

    logger.info(f"Running Cypress tests from: {cypress_test_dir}")
    logger.info(f"Base URL: {base_url}")
    logger.info(f"Username: {username}")

    command = (
        f'CYPRESS_SPEC_PATTERN="{spec_pattern}" '
        f"npx cypress run "
        f'--env "ADMIN_USERNAME={username},ADMIN_PASSWORD={password}" '
        f"--config numTestsKeptInMemory=2"
    )

    redacted_command = command.replace(password, "***REDACTED***")
    logger.info(f"Executing command: {redacted_command}")

    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="tests/cypress",
        text=True,
        bufsize=1,  # Line buffered
    )

    assert proc.stdout is not None
    assert proc.stderr is not None

    def read_and_print(pipe, prefix=""):
        for line in pipe:
            logger.info(f"{prefix}{line.rstrip()}")

    stdout_thread = threading.Thread(target=read_and_print, args=(proc.stdout,))
    stderr_thread = threading.Thread(
        target=read_and_print, args=(proc.stderr, "stderr: ")
    )

    stdout_thread.daemon = True
    stderr_thread.daemon = True

    stdout_thread.start()
    stderr_thread.start()

    return_code = proc.wait()

    stdout_thread.join()
    stderr_thread.join()

    logger.info(f"Return code: {return_code}")

    # Parse Cypress JUnit XML results and report individual tests
    results_dir = "tests/cypress/build/smoke-test-results"
    tracker = get_module_tracker()
    process_and_report_cypress_results(results_dir, tracker)

    assert return_code == 0, f"Cypress tests failed with return code {return_code}"
