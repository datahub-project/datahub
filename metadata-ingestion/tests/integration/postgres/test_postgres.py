import json
import subprocess
import time
from pathlib import Path
from typing import Iterable, List, Optional, Set, Union

import pytest
import time_machine
from sqlalchemy import create_engine, text
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource, SqlWorkUnit
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.testing import mce_helpers
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


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_postgres_ingest_with_db(
    postgres_runner, pytestconfig, test_resources_dir, tmp_path
):
    # Run the metadata ingestion pipeline.
    config_file = (
        test_resources_dir / "postgres_to_file_with_db_estimate_row_count.yml"
    ).resolve()
    print(f"Config file: {config_file}")

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        fail_on_output_mces=True,
        output_path=tmp_path / "postgres_mces.json",
        golden_path=test_resources_dir / "postgres_with_db_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_postgres_ingest_with_all_db(
    postgres_runner, pytestconfig, test_resources_dir, tmp_path
):
    # Run the metadata ingestion pipeline.
    config_file = (
        test_resources_dir / "postgres_all_db_to_file_with_db_estimate_row_count.yml"
    ).resolve()
    print(f"Config file: {config_file}")

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        fail_on_output_mces=True,
        output_path=tmp_path / "postgres_all_db_mces.json",
        golden_path=test_resources_dir / "postgres_all_db_with_db_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_postgres_ingest_skip_table_partitions(
    postgres_runner, pytestconfig, test_resources_dir, tmp_path
):
    config_file = (
        test_resources_dir / "postgres_skip_table_partitions_to_file.yml"
    ).resolve()

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        fail_on_output_mces=True,
        output_path=tmp_path / "postgres_skip_table_partitions_mces.json",
        golden_path=test_resources_dir / "postgres_skip_table_partitions_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_postgres_test_connection(
    postgres_runner, pytestconfig, test_resources_dir, tmp_path
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "postgres_test_connection.yml").resolve()

    run_datahub_cmd(
        [
            "ingest",
            "-c",
            f"{config_file}",
            "--test-source-connection",
            "--report-to",
            "postgres_test_connection.json",
        ],
        tmp_path=tmp_path,
    )
    with open(tmp_path / "postgres_test_connection.json") as f:
        expected = json.load(f)
    assert expected == {"basic_connectivity": {"capable": True}}


def _drop_ingestion_connections() -> None:
    """Kill every other session on the test database, as a database restart or an
    idle-session or firewall timeout would (Oracle reports the same as ORA-03113).

    pg_terminate_backend only signals the backend, so wait until the sessions are
    gone; otherwise the next query can still run before the drop lands.
    """
    engine = create_engine(
        f"postgresql+psycopg2://postgres:example@localhost:{POSTGRES_PORT}/postgres",
        isolation_level="AUTOCOMMIT",
    )
    others = (
        "FROM pg_stat_activity "
        "WHERE datname = 'postgrestest' AND pid <> pg_backend_pid()"
    )
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SELECT pg_terminate_backend(pid) {others}"))
            deadline = time.monotonic() + 10
            while conn.execute(text(f"SELECT count(*) {others}")).scalar():
                assert time.monotonic() < deadline, "sessions were not terminated"
                time.sleep(0.1)
    finally:
        engine.dispose()


def _datasets_with_schema(output_file: Path) -> Set[str]:
    return {
        record["entityUrn"]
        for record in json.loads(output_file.read_text())
        if record.get("aspectName") == "schemaMetadata"
    }


@pytest.mark.integration
def test_postgres_ingestion_survives_dropped_connection(
    postgres_runner: object, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_file = tmp_path / "postgres_mces.json"
    original_process_table = SQLAlchemySource._process_table
    tables_seen: List[str] = []
    drop_errors: List[BaseException] = []

    def process_table_dropping_connection(
        self: SQLAlchemySource,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        tables_seen.append(dataset_name)
        if len(tables_seen) == 2:
            # Raised here, an error would be swallowed by loop_tables as a table
            # warning and the test would fail later for the wrong reason.
            try:
                _drop_ingestion_connections()
            except Exception as e:
                drop_errors.append(e)
        yield from original_process_table(
            self, dataset_name, inspector, schema, table, sql_config, data_reader
        )

    monkeypatch.setattr(
        SQLAlchemySource, "_process_table", process_table_dropping_connection
    )

    pipeline = Pipeline.create(
        {
            "run_id": "postgres-dropped-connection",
            "source": {
                "type": "postgres",
                "config": {
                    "host_port": f"localhost:{POSTGRES_PORT}",
                    "database": "postgrestest",
                    "username": "postgres",
                    "password": "example",
                    "include_views": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_file)}},
        }
    )
    pipeline.run()
    report = pipeline.source.get_report()

    assert not drop_errors, drop_errors
    assert len(tables_seen) >= 3, "need at least one table after the drop"
    dropped_table = tables_seen[1].split(".", 1)[-1]

    # The drop is a failure against the table being read, so the run doesn't look
    # complete and stale-entity removal doesn't delete that table.
    assert [f.title for f in report.failures] == ["Database connection lost"]
    assert [c.split(" ", 1)[0] for c in report.failures[0].context] == [tables_seen[1]]

    # The table being read when the connection dropped is lost and reported...
    assert any(
        dropped_table in context
        for warning in report.warnings
        for context in warning.context
    )
    # ...but every table after it, and the view, are read over the re-opened
    # connection. Before the fix each of them failed with PendingRollbackError.
    ingested = _datasets_with_schema(output_file)
    for table in tables_seen[2:]:
        assert any(table in urn for urn in ingested), f"{table} was not ingested"
    assert any("metadata_aspect_view" in urn for urn in ingested)
    assert not [
        context
        for warning in report.warnings
        for context in warning.context
        if "PendingRollbackError" in context and dropped_table not in context
    ]
