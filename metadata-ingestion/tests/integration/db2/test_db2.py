import logging
import os
import subprocess

import pytest
import sqlalchemy
import sqlglot
import yaml

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration_batch_2
DB2_PORT = 50000


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/db2"


def is_db2_up(container_name: str) -> bool:
    cmd = f"docker logs {container_name} 2>&1 | grep 'Setup has completed.'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


def _split_statements(sql):
    # Split a SQL script into individual statements that can be executed.
    # The Db2 Docker image does not have a built-in way to run a SQL script
    # upon startup, so the script must be split into statements manually.
    # Statements are usually separated by semicolons, except that BEGIN/END
    # blocks (as for stored procedure definitions) must be kept as a single item.

    result = []
    tokens = sqlglot.tokenize(sql, dialect=sqlglot.Dialect.get("db2"))
    current_statement_start = 0
    needed_end_tokens = 0
    for t in tokens:
        if t.token_type in (sqlglot.TokenType.BEGIN, sqlglot.TokenType.CASE):
            needed_end_tokens += 1
        elif t.token_type == sqlglot.TokenType.END:
            needed_end_tokens -= 1
        elif needed_end_tokens == 0 and t.token_type == sqlglot.TokenType.SEMICOLON:
            result.append(sql[current_statement_start : t.start])
            current_statement_start = t.end + 1

    result.append(sql[current_statement_start:])

    return [statement for statement in result if statement.strip()]


@pytest.fixture(scope="module")
def db2_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "db2"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testdb2",
            DB2_PORT,
            timeout=600,
            checker=lambda: is_db2_up("testdb2"),
        )

        setup_filename = test_resources_dir / "setup" / "setup.sql"
        statements = _split_statements(open(setup_filename).read())

        engine = sqlalchemy.create_engine(
            f"db2+ibm_db://db2inst1:password@localhost:{DB2_PORT}/testdb"
        )
        with engine.begin() as conn:
            for statement in statements:
                logger.info("Executing SQL: " + statement)
                conn.execute(statement)

        yield docker_services


@pytest.mark.parametrize(
    "config_filename",
    [
        "db2_basic.yml",
        "db2_case_sensitivity.yml",
        "db2_comments.yml",
        "db2_procedures.yml",
        "db2_view_qualifier.yml",
    ],
)
@pytest.mark.integration
def test_db2_ingest(
    db2_runner,
    config_filename,
    pytestconfig,
    test_resources_dir,
    tmp_path,
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / config_filename).resolve()
    print(f"Config file: {config_file}")

    output_path = str(tmp_path / (os.path.splitext(config_filename)[0] + "_mces.json"))
    golden_path = str(
        test_resources_dir
        / (os.path.splitext(config_filename)[0] + "_mces_golden.json")
    )

    source = yaml.safe_load(open(config_file))
    source.setdefault("config", {}).update(
        {
            "host_port": f"localhost:{DB2_PORT}",
            "database": "testdb",
            "username": "db2inst1",
            "password": "password",
        }
    )
    config_dict = {
        "source": source,
        "sink": {
            "type": "file",
            "config": {
                "filename": output_path,
            },
        },
    }

    pipeline = Pipeline.create(config_dict)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
            r"root\[\d+\]\['aspect'\]\['json'\].+\[\d+\]\['auditStamp'\]\['time'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
        ],
    )
