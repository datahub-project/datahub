import subprocess

import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"
MYSQL_PORT = 3306


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/mysql"


def is_mysql_up(container_name: str, port: int) -> bool:
    """A cheap way to figure out if mysql is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep '/usr/sbin/mysqld: ready for connections.' | grep {port}"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def mysql_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mysql"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmysql",
            MYSQL_PORT,
            timeout=120,
            checker=lambda: is_mysql_up("testmysql", MYSQL_PORT),
        )
        yield docker_services


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mysql_ingest_no_db(
    mysql_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "mysql_to_file_no_db.yml").resolve()
    run_datahub_cmd(
        ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
    )

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mysql_mces.json",
        golden_path=test_resources_dir / "mysql_mces_no_db_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mysql_ingest_with_db(
    mysql_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "mysql_to_file_with_db.yml").resolve()
    run_datahub_cmd(
        ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
    )

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mysql_mces.json",
        golden_path=test_resources_dir / "mysql_mces_with_db_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mysql_ingest_with_db_alias(
    mysql_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "mysql_to_file_dbalias.yml").resolve()
    run_datahub_cmd(
        ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
    )

    # Verify the output.
    # Assert that all events generated have instance specific urns
    import re

    urn_pattern = "^" + re.escape(
        "urn:li:dataset:(urn:li:dataPlatform:mysql,foogalaxy."
    )
    mce_helpers.assert_mcp_entity_urn(
        filter="ALL",
        entity_type="dataset",
        regex_pattern=urn_pattern,
        file=tmp_path / "mysql_mces_dbalias.json",
    )
