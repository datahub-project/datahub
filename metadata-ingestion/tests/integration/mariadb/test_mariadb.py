import subprocess
from typing import List

import pytest
from freezegun import freeze_time

from datahub.ingestion.source.sql.mariadb import MariaDBSource
from tests.test_helpers import mce_helpers, test_connection_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2024-04-14 07:00:00"
MARIADB_PORT = 3306


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/mariadb"


def is_mariadb_up(container_name: str, port: int) -> bool:
    """A cheap way to figure out if mariadb is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep 'port: {port}  mariadb.org binary distribution'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def mariadb_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mariadb"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmariadb",
            MARIADB_PORT,
            timeout=120,
            checker=lambda: is_mariadb_up("testmariadb", MARIADB_PORT),
        )
        yield docker_services


@pytest.mark.parametrize(
    "config_file,golden_file",
    [
        ("mariadb_to_file.yml", "mariadb_mces_golden.json"),
    ],
)
@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mariadb_ingest_no_db(
    mariadb_runner,
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

    # These paths change from one instance run of the clickhouse docker to the other, and the FROZEN_TIME does not apply to these.
    ignore_paths: List[str] = [
        r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['created'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['last_altered'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['distinctValueFrequencies'\]\[\d+\]\['value'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['max'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['min'\]",
        r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]\[\d+\]",
    ]

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        ignore_paths=ignore_paths,
        output_path=tmp_path / "mariadb_mces.json",
        golden_path=test_resources_dir / golden_file,
    )


@pytest.mark.parametrize(
    "config_dict, is_success",
    [
        (
            {
                "host_port": "localhost:53300",
                "database": "test_db",
                "username": "root",
                "password": "password",
            },
            True,
        ),
        (
            {
                "host_port": "localhost:5330",
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
def test_mariadb_test_connection(mariadb_runner, config_dict, is_success):
    report = test_connection_helpers.run_test_connection(MariaDBSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, "Connection refused"
        )
