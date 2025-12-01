import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import pytest
from freezegun import freeze_time

from datahub.testing import mce_helpers
from tests.integration.airbyte.airbyte_test_setup import (  # type: ignore[import-untyped]
    AIRBYTE_API_PORT,
    BASIC_AUTH_PASSWORD,
    BASIC_AUTH_USERNAME,
    cleanup_airbyte,
    complete_airbyte_onboarding,
    get_airbyte_credentials,
    init_test_data,
    install_abctl,
    is_mysql_ready,
    is_postgres_ready,
    setup_airbyte_connections,
    wait_for_airbyte_ready,
)
from tests.test_helpers.click_helpers import run_datahub_cmd

pytestmark = pytest.mark.integration_batch_5

FROZEN_TIME = "2023-10-15 07:00:00"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: Any) -> Path:
    """Return the path to the test resources directory."""
    return pytestconfig.rootpath / "tests/integration/airbyte"


@pytest.fixture(scope="module")
def test_databases(
    test_resources_dir: Path, docker_compose_runner: Any
) -> Generator[Any, None, None]:
    """Start PostgreSQL and MySQL test databases."""
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "airbyte-test-dbs"
    ) as docker_services:
        # Wait for databases
        max_attempts = 30
        for _ in range(max_attempts):
            if is_postgres_ready("test-postgres"):
                break
            time.sleep(2)
        else:
            raise RuntimeError("PostgreSQL test database failed to start")

        for _ in range(max_attempts):
            if is_mysql_ready("test-mysql"):
                break
            time.sleep(2)
        else:
            raise RuntimeError("MySQL test database failed to start")

        init_test_data(test_resources_dir)
        yield docker_services


@pytest.fixture(scope="module")
def set_docker_env_vars() -> Generator[None, None, None]:
    """Set environment variables needed for abctl setup."""
    env_vars = {
        "BASIC_AUTH_USERNAME": BASIC_AUTH_USERNAME,
        "BASIC_AUTH_PASSWORD": BASIC_AUTH_PASSWORD,
    }
    original_vars: Dict[str, Optional[str]] = {
        key: os.environ.get(key) for key in env_vars
    }

    for key, env_value in env_vars.items():
        os.environ[key] = env_value

    yield

    for key, optional_value in original_vars.items():
        if optional_value is None:
            if key in os.environ:
                del os.environ[key]
        else:
            os.environ[key] = optional_value


@pytest.fixture(scope="module")
def airbyte_service(
    test_resources_dir: Path,
    test_databases: Any,
    set_docker_env_vars: None,
) -> Generator[None, None, None]:
    """Set up Airbyte using abctl (Kubernetes-based)."""
    print("\n" + "=" * 80)
    print("AIRBYTE INTEGRATION TEST SETUP")
    print("=" * 80)

    abctl_path = install_abctl(test_resources_dir)

    # Clean up any existing Airbyte installation
    cleanup_airbyte(abctl_path, test_resources_dir)

    # Check if Airbyte is still running and clean up again if needed
    try:
        status_check = subprocess.run(
            [str(abctl_path), "local", "status"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if "Status: deployed" in status_check.stdout:
            print("Airbyte still running, uninstalling again...")
            cleanup_airbyte(abctl_path, test_resources_dir)
            time.sleep(5)
    except Exception as e:
        print(f"Status check failed: {e}, proceeding with install...")

    # Install Airbyte with abctl
    print("\nInstalling Airbyte with abctl...")
    install_cmd = [
        str(abctl_path),
        "local",
        "install",
        "--port",
        str(AIRBYTE_API_PORT),
        "--no-browser",
        "--insecure-cookies",
    ]

    try:
        result = subprocess.run(
            install_cmd,
            cwd=test_resources_dir,
            capture_output=True,
            text=True,
            timeout=600,
        )

        if result.returncode != 0:
            print(f"abctl install failed: {result.stderr}")
            raise RuntimeError("Failed to install Airbyte")

        print("Airbyte installation completed")

    except subprocess.TimeoutExpired as e:
        raise RuntimeError("abctl install timed out") from e

    # Get credentials BEFORE onboarding (needed for authentication)
    get_airbyte_credentials(abctl_path, test_resources_dir)

    # Complete onboarding (this also verifies API is working)
    onboarding_succeeded = complete_airbyte_onboarding()

    # If onboarding succeeded, API is ready; otherwise check readiness
    if onboarding_succeeded:
        print("Onboarding completed - API is ready")
    elif not wait_for_airbyte_ready(timeout=300):
        raise RuntimeError("Airbyte failed to become ready")

    # Set up sources, destinations, and connections
    setup_airbyte_connections(test_resources_dir)

    print("\n" + "=" * 80)
    print("AIRBYTE SETUP COMPLETE - STARTING TESTS")
    print("=" * 80 + "\n")

    yield

    # Cleanup
    print("\n" + "=" * 80)
    print("CLEANING UP AIRBYTE")
    print("=" * 80)
    cleanup_airbyte(abctl_path, test_resources_dir)


def update_config_file_with_api_url(
    config_file: Path,
) -> Path:
    """Return the config file path directly since API URL is consistent."""
    return config_file


@freeze_time(FROZEN_TIME)
def test_airbyte_ingest(
    airbyte_service: None, pytestconfig: Any, tmp_path: Path, mock_time: Any
) -> None:
    """Test basic Airbyte metadata ingestion."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/airbyte"

    # Basic ingestion test
    config_file = test_resources_dir / "airbyte_to_file.yml"
    config_file_to_use = update_config_file_with_api_url(config_file)

    run_datahub_cmd(
        [
            "ingest",
            "-c",
            str(config_file_to_use),
        ],
        tmp_path=tmp_path,
    )

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "airbyte_mces.json",
        golden_path=f"{test_resources_dir}/airbyte_mces_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_airbyte_platform_instance_urns(
    airbyte_service: None, pytestconfig: Any, tmp_path: Path, mock_time: Any
) -> None:
    """Test Airbyte platform instance URN generation."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/airbyte"

    # Test platform instance URN
    config_file = test_resources_dir / "airbyte_platform_instance_to_file.yml"
    config_file_to_use = update_config_file_with_api_url(config_file)

    run_datahub_cmd(
        [
            "ingest",
            "-c",
            str(config_file_to_use),
        ],
        tmp_path=tmp_path,
    )

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "airbyte_platform_instance_mces.json",
        golden_path=f"{test_resources_dir}/airbyte_platform_instance_mces_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_airbyte_schema_filter(
    airbyte_service: None, pytestconfig: Any, tmp_path: Path, mock_time: Any
) -> None:
    """Test Airbyte schema filtering."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/airbyte"

    # Test schema filtering
    config_file = test_resources_dir / "airbyte_schema_filter_to_file.yml"
    config_file_to_use = update_config_file_with_api_url(config_file)

    run_datahub_cmd(
        [
            "ingest",
            "-c",
            str(config_file_to_use),
        ],
        tmp_path=tmp_path,
    )

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "airbyte_schema_filter_mces.json",
        golden_path=f"{test_resources_dir}/airbyte_schema_filter_mces_golden.json",
    )
