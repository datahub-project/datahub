import os
import pathlib
import subprocess
import time
from pathlib import Path

import pytest

from datahub.ingestion.source.sql.mssql.job_models import StoredProcedure
from datahub.ingestion.source.sql.mssql.stored_procedure_lineage import (
    generate_procedure_lineage,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
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
        / f"golden_files/golden_mces_{config_file.replace('yml', 'json')}",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['job_id'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['date_created'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['date_modified'\]",
        ],
    )


PROCEDURE_SQLS_DIR = pathlib.Path(__file__).parent / "procedures"
PROCEDURES_GOLDEN_DIR = pathlib.Path(__file__).parent / "golden_files/procedures/"
procedure_sqls = [sql_file.name for sql_file in PROCEDURE_SQLS_DIR.iterdir()]


@pytest.mark.parametrize("procedure_sql_file", procedure_sqls)
@pytest.mark.integration
def test_stored_procedure_lineage(procedure_sql_file: str) -> None:
    sql_file_path = PROCEDURE_SQLS_DIR / procedure_sql_file
    procedure_code = sql_file_path.read_text()

    # Procedure file is named as <db>.<schema>.<procedure_name>
    splits = procedure_sql_file.split(".")
    db = splits[0]
    schema = splits[1]
    name = splits[2]

    procedure = StoredProcedure(
        db=db,
        schema=schema,
        name=name,
        flow=None,  # type: ignore # flow is not used in this test
        code=procedure_code,
    )
    data_job_urn = f"urn:li:dataJob:(urn:li:dataFlow:(mssql,{db}.{schema}.stored_procedures,PROD),{name})"

    schema_resolver = SchemaResolver(platform="mssql")

    mcps = list(
        generate_procedure_lineage(
            schema_resolver=schema_resolver,
            procedure=procedure,
            procedure_job_urn=data_job_urn,
            is_temp_table=lambda name: "temp" in name.lower(),
        )
    )
    mce_helpers.check_goldens_stream(
        outputs=mcps,
        golden_path=(
            PROCEDURES_GOLDEN_DIR / Path(procedure_sql_file).with_suffix(".json")
        ),
    )
