"""
Integration tests for library examples by executing them as scripts.

These tests validate that library examples work correctly against a running DataHub instance
by executing them as standalone scripts with appropriate environment variables set.

The test execution order is defined in example_manifest.py to ensure:
- CREATE operations run before READ/UPDATE operations
- Dependencies between examples are respected
- Examples can be run sequentially without conflicts

Run with: cd smoke-test && source venv/bin/activate && pytest tests/library_examples/ -v
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from .example_manifest import EXAMPLE_DEPENDENCIES, EXAMPLE_MANIFEST

# Path to metadata-ingestion examples
EXAMPLES_DIR = (
    Path(__file__).parent.parent.parent.parent
    / "metadata-ingestion"
    / "examples"
    / "library"
)


def run_example_script(script_path: Path, env: dict) -> subprocess.CompletedProcess:
    """
    Execute an example script with given environment variables.

    Args:
        script_path: Path to the Python script to execute
        env: Environment variables to set (includes auth credentials)

    Returns:
        CompletedProcess with returncode, stdout, stderr
    """
    result = subprocess.run(
        [sys.executable, str(script_path)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result


@pytest.fixture
def datahub_env(auth_session):
    """
    Create environment dict with DataHub credentials from auth_session.

    This allows example scripts to authenticate using their standard
    DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN environment variables.
    """
    env = os.environ.copy()
    env["DATAHUB_GMS_URL"] = auth_session.gms_url()
    env["DATAHUB_GMS_TOKEN"] = auth_session.gms_token()
    return env


def pytest_generate_tests(metafunc):
    """
    Custom test generation to add dependency markers.

    This function is called during test collection and adds pytest.mark.dependency
    markers to tests that have dependencies defined in EXAMPLE_DEPENDENCIES.
    """
    if "example_script" in metafunc.fixturenames:
        # Get the dependency info for each test
        params = []
        for script in EXAMPLE_MANIFEST:
            marks = []
            # Check if this test has dependencies
            if script in EXAMPLE_DEPENDENCIES:
                # Add dependency marker - depends on prerequisite tests
                depends = [
                    f"test_library_example[{dep}]"
                    for dep in EXAMPLE_DEPENDENCIES[script]
                ]
                marks.append(pytest.mark.dependency(depends=depends))
            else:
                # Add dependency marker with name only (allows others to depend on it)
                marks.append(
                    pytest.mark.dependency(name=f"test_library_example[{script}]")
                )

            params.append(pytest.param(script, marks=marks))

        metafunc.parametrize("example_script", params)


def test_library_example(example_script: str, datahub_env):
    """
    Parameterized test that executes each library example script.

    This test runs each example from EXAMPLE_MANIFEST in order, validating:
    1. The script executes without errors (exit code 0)
    2. The script can authenticate and communicate with DataHub
    3. The example produces the expected side effects in DataHub

    Args:
        example_script: Relative path to the example script (from EXAMPLE_MANIFEST)
        datahub_env: Environment dict with DataHub credentials
    """
    script_path = EXAMPLES_DIR / example_script

    # Validate the script file exists
    assert script_path.exists(), f"Example script not found: {script_path}"

    # Execute the example script
    result = run_example_script(script_path, datahub_env)

    # Validate successful execution
    assert result.returncode == 0, (
        f"Example script '{example_script}' failed with exit code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
