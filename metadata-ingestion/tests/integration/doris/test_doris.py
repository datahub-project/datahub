import os
import subprocess
import time

import pytest
from freezegun import freeze_time

from datahub.ingestion.source.sql.doris import DorisSource
from datahub.testing import mce_helpers
from tests.test_helpers import test_connection_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"
DORIS_PORT = 9030  # Doris MySQL protocol port

# Note: Doris FE 3.0.8 uses Java 17 which has cgroup v2 incompatibility issues in CI
# Workaround: JAVA_OPTS=-XX:-UseContainerSupport is explicitly exported in entrypoint scripts
pytestmark = pytest.mark.integration_batch_4


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/doris"


def is_doris_up(container_name: str) -> bool:
    """Check if Doris FE is responsive via MySQL protocol connection"""
    # The most reliable way to check if Doris is ready is to try connecting via MySQL protocol
    # If we can execute a query, FE is fully operational
    mysql_cmd = f"docker exec {container_name}-fe mysql -h 127.0.0.1 -P 9030 -u root -e 'SELECT 1' 2>/dev/null"
    result = subprocess.run(mysql_cmd, shell=True)
    return result.returncode == 0


@pytest.fixture(scope="module")
def doris_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "doris"
    ) as docker_services:
        # Wait for Doris FE to be ready
        print("Waiting for Doris FE to start...")
        try:
            wait_for_port(
                docker_services,
                "testdoris-fe",
                DORIS_PORT,
                timeout=400,  # Longer timeout for CI (includes image pull)
                checker=lambda: is_doris_up("testdoris"),
            )
            print("Doris FE is ready!")
        except Exception:
            # Print logs for debugging
            print("ERROR: Doris FE failed to start. Container logs:")
            subprocess.run("docker logs testdoris-fe 2>&1 | tail -50", shell=True)
            raise

        # Wait for BE to register with FE by polling SHOW BACKENDS
        max_wait = 120 if os.getenv("CI") == "true" else 60
        print("Waiting for BE to register with FE...")
        be_ready = False
        for i in range(max_wait):
            check_be_cmd = f"docker exec testdoris-fe mysql -h 127.0.0.1 -P {DORIS_PORT} -u root -e 'SHOW BACKENDS' 2>/dev/null"
            result = subprocess.run(
                check_be_cmd, shell=True, capture_output=True, text=True
            )
            if (
                result.returncode == 0
                and "Alive" in result.stdout
                and "true" in result.stdout
            ):
                print(f"BE is registered and alive (took {i + 1}s)")
                be_ready = True
                break
            time.sleep(1)

        if not be_ready:
            print(
                f"WARNING: BE registration not confirmed after {max_wait}s, proceeding anyway"
            )

        # Give a few extra seconds for cluster to fully stabilize
        time.sleep(5)

        # Run setup script to create database and tables
        setup_sql = test_resources_dir / "setup" / "setup.sql"
        setup_cmd = f"docker exec -i testdoris-fe mysql -h 127.0.0.1 -P {DORIS_PORT} -u root < {setup_sql}"

        # Retry setup a few times as BE might still be registering
        setup_success = False
        for attempt in range(5):
            result = subprocess.run(
                setup_cmd, shell=True, capture_output=True, text=True
            )
            if result.returncode == 0:
                print("Setup script executed successfully")
                setup_success = True
                break
            print(f"Setup attempt {attempt + 1}/5 failed: {result.stderr}")
            if attempt < 4:
                time.sleep(15)

        if not setup_success:
            print("ERROR: Setup script failed after 5 attempts")
            subprocess.run("docker logs testdoris-be 2>&1 | tail -30", shell=True)
            raise Exception("Failed to execute Doris setup script after 5 attempts")

        yield docker_services


@pytest.mark.parametrize(
    "config_file,golden_file",
    [
        ("doris_to_file.yml", "doris_mces_golden.json"),
        # NOTE: Profiling tests are currently commented out due to a known issue:
        # Great Expectations (acryl_great_expectations 0.15.50.1) has a metaclass conflict
        # with pydantic v2 in Python 3.10: "TypeError: metaclass conflict at ConstrainedDate"
        # This affects ALL profiling tests, not just Doris
        # The test configs are ready and will work once GE is updated for pydantic v2
        # pytest.param(
        #     "doris_profile_table_level_only.yml",
        #     "doris_profile_table_level_golden.json",
        #     marks=pytest.mark.skip(
        #         reason="Great Expectations pydantic v2 incompatibility (Python 3.10)"
        #     ),
        # ),
        # pytest.param(
        #     "doris_profile_with_filtering.yml",
        #     "doris_profile_with_filtering_golden.json",
        #     marks=pytest.mark.skip(
        #         reason="Great Expectations pydantic v2 incompatibility (Python 3.10)"
        #     ),
        # ),
        ("doris_multi_db.yml", "doris_multi_db_golden.json"),
    ],
)
@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_doris_ingest(
    doris_runner,
    pytestconfig,
    test_resources_dir,
    tmp_path,
    mock_time,
    config_file,
    golden_file,
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / config_file).resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "doris_mces.json",
        golden_path=test_resources_dir / golden_file,
    )


@pytest.mark.parametrize(
    "config_dict, is_success, expected_error",
    [
        # Success case
        (
            {
                "host_port": "localhost:59030",
                "database": "dorisdb",
                "username": "root",
                "password": "",
            },
            True,
            None,
        ),
        # Wrong port (connection refused)
        (
            {
                "host_port": "localhost:59999",
                "database": "dorisdb",
                "username": "root",
                "password": "",
            },
            False,
            "Connection refused",
        ),
        # Wrong credentials (access denied)
        (
            {
                "host_port": "localhost:59030",
                "database": "dorisdb",
                "username": "wrong_user",
                "password": "wrong_pass",
            },
            False,
            "Access denied",
        ),
        # Non-existent database
        (
            {
                "host_port": "localhost:59030",
                "database": "nonexistent_db",
                "username": "root",
                "password": "",
            },
            False,
            "Unknown database",
        ),
    ],
)
@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_doris_test_connection(doris_runner, config_dict, is_success, expected_error):
    report = test_connection_helpers.run_test_connection(DorisSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, expected_error
        )
