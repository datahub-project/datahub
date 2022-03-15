import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mysql_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mysql"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mysql"
    ) as docker_services:
        wait_for_port(docker_services, "testmysql", 3306)

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "mysql_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "mysql_mces.json",
            golden_path=test_resources_dir / "mysql_mces_golden.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mysql_ingest_with_db_alias(
    docker_compose_runner, pytestconfig, tmp_path, mock_time
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mysql"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mysql"
    ) as docker_services:
        wait_for_port(docker_services, "testmysql", 3306)

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
