import subprocess

import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2022-03-06 14:00:00"
POSTGRES_PORT = 5432


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/postgres"


def is_postgres_up(container_name: str) -> bool:
    """A cheap way to figure out if postgres is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep 'PostgreSQL init process complete; ready for start up.'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def postgres_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "postgres"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testpostgres",
            POSTGRES_PORT,
            timeout=120,
            checker=lambda: is_postgres_up("testpostgres"),
        )
        yield docker_services


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_postgres_ingest_with_db(
    postgres_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    # Run the metadata ingestion pipeline.
    config_file = (
        test_resources_dir / "postgres_to_file_with_db_estimate_row_count.yml"
    ).resolve()
    print("Config file: {config_file}")

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "postgres_mces.json",
        golden_path=test_resources_dir / "postgres_mces_with_db_golden.json",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_postgres_ingest_with_all_db(
    postgres_runner, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    # Run the metadata ingestion pipeline.
    config_file = (
        test_resources_dir / "postgres_all_db_to_file_with_db_estimate_row_count.yml"
    ).resolve()
    print("Config file: {config_file}")

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "postgres_all_db_mces.json",
        golden_path=test_resources_dir / "postgres_all_db_mces_with_db_golden.json",
    )
