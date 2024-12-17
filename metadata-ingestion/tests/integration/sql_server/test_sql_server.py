import os
import subprocess
import time

import pytest
import yaml

from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import cleanup_image, wait_for_port


@pytest.fixture(scope="module")
def mssql_runner(docker_compose_runner, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sql_server"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "sql-server"
    ) as docker_services:
        # Wait for SQL Server to be ready. We wait an extra couple seconds, as the port being available
        # does not mean the server is accepting connections.
        # TODO: find a better way to check for liveness.
        wait_for_port(docker_services, "testsqlserver", 1433)
        time.sleep(5)

        # Run the setup.sql file to populate the database.
        command = "docker exec testsqlserver /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'test!Password' -d master -i /setup/setup.sql"
        ret = subprocess.run(command, shell=True, capture_output=True)
        assert ret.returncode == 0
        yield docker_services

    # The image is pretty large, so we remove it after the test.
    cleanup_image("mcr.microsoft.com/mssql/server")


SOURCE_FILES_PATH = "./tests/integration/sql_server/source_files"
config_file = os.listdir(SOURCE_FILES_PATH)
config_file = [cf for cf in config_file if cf != "mssql_extended_properties.yml"]


@pytest.mark.parametrize("config_file", config_file)
@pytest.mark.integration
def test_mssql_ingest(mssql_runner, pytestconfig, tmp_path, mock_time, config_file):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sql_server"
    # Run the metadata ingestion pipeline.
    config_file_path = (test_resources_dir / f"source_files/{config_file}").resolve()
    run_datahub_cmd(
        ["ingest", "-c", f"{config_file_path}"], tmp_path=tmp_path, check_result=True
    )

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mssql_mces.json",
        golden_path=test_resources_dir
        / f"golden_files/golden_mces_{config_file.replace('yml','json')}",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['job_id'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['date_created'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['date_modified'\]",
        ],
    )


config_file = ["mssql_extended_properties.yml"]


@pytest.mark.parametrize("config_file", config_file)
@pytest.mark.integration
def test_mssql_extended_properties(
    mssql_runner, mock_datahub_graph, pytestconfig, tmp_path, mock_time, config_file
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sql_server"
    config_file_path = (test_resources_dir / f"source_files/{config_file}").resolve()
    config = yaml.load(open(config_file_path).read(), Loader=yaml.FullLoader)

    sink_filename = str(tmp_path / config["sink"]["config"]["filename"])
    config["sink"]["config"]["filename"] = sink_filename

    pipeline = Pipeline.create(config)

    pipeline.ctx.graph = mock_datahub_graph(DatahubClientConfig())
    pipeline.ctx.graph.list_all_entity_urns.return_value = ["urn:li:corpuser:john_doe@datahub.com", "urn:li:corpuser:baby_doe@datahub.com"]  # type: ignore[attr-defined, union-attr]
    pipeline.ctx.graph.get_ownership.return_value = None  # type: ignore[attr-defined, union-attr]

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "mssql_mces.json",
        golden_path=test_resources_dir
        / f"golden_files/golden_mces_{config_file.replace('yml','json')}",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['job_id'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['date_created'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['date_modified'\]",
        ],
    )
