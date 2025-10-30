# ABOUTME: Pytest wrapper for running Cypress tests against remote instances.
# ABOUTME: Executes tests from directory specified by CYPRESS_TEST_DIR environment variable.
import logging
import os
import subprocess
import threading

import pytest

from tests.utils import get_cypress_credentials

logger = logging.getLogger(__name__)


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

    assert return_code == 0, f"Cypress tests failed with return code {return_code}"
