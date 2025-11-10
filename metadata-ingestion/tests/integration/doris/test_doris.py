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

pytestmark = pytest.mark.integration_batch_4

FROZEN_TIME = "2020-04-14 07:00:00"
DORIS_PORT = 9030  # Doris MySQL protocol port


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/doris"


def is_doris_up(container_name: str, port: int) -> bool:
    """Check if Doris FE is responsive via MySQL protocol"""

    # Try to connect via MySQL protocol to Doris FE
    cmd = f"docker exec {container_name}-fe mysql -h 127.0.0.1 -P {port} -u root -e 'SELECT 1' 2>/dev/null"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def doris_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "doris"
    ) as docker_services:
        # Wait for Doris FE to be ready
        # Increased timeout for CI where image pulls + startup can take 3-5 minutes
        wait_for_port(
            docker_services,
            "testdoris-fe",
            DORIS_PORT,
            timeout=600,  # 10 minutes to account for image download + startup
            checker=lambda: is_doris_up("testdoris", DORIS_PORT),
        )

        # Give BE time to register with FE (longer in CI environments)
        # BE must connect to FE after FE is healthy
        be_wait = 60 if os.getenv("CI") == "true" else 30
        print(f"Waiting {be_wait}s for BE to register with FE...")
        time.sleep(be_wait)

        # Run the setup script
        setup_sql = test_resources_dir / "setup" / "setup.sql"
        setup_cmd = f"docker exec -i testdoris-fe mysql -h 127.0.0.1 -P {DORIS_PORT} -u root < {setup_sql}"
        result = subprocess.run(setup_cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Setup script failed: {result.stderr}")
            # Don't fail the test if setup fails, let it proceed

        yield docker_services


@pytest.mark.parametrize(
    "config_file,golden_file",
    [
        ("doris_to_file.yml", "doris_mces_golden.json"),
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
    "config_dict, is_success",
    [
        (
            {
                "host_port": "localhost:59030",
                "database": "dorisdb",
                "username": "root",
                "password": "",
            },
            True,
        ),
        (
            {
                "host_port": "localhost:59999",
                "database": "wrong_db",
                "username": "wrong_user",
                "password": "wrong_pass",
            },
            False,
        ),
    ],
)
@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_doris_test_connection(doris_runner, config_dict, is_success):
    report = test_connection_helpers.run_test_connection(DorisSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, "Connection refused"
        )
