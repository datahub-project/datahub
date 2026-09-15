import logging
import os
import platform
import queue
import subprocess
import threading

import pytest
import sqlalchemy
import sqlglot
import yaml

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration_batch_4
DB2_PORT = 50000
DB2_URL = f"db2+ibm_db://db2inst1:password@localhost:{DB2_PORT}/testdb"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/db2"


def _shell(cmd: str) -> str:
    return subprocess.run(
        cmd, shell=True, capture_output=True, text=True
    ).stdout.strip()


def _db2_setup_failure() -> str | None:
    """The container's setup script is fail-open: on CREATE DATABASE failure it
    logs "(!) Failed to create ..." / an SQL error and still reports setup as
    completed, so the server comes up without the database and readiness would
    poll out the full timeout. Detect the failure to abort immediately."""
    logs = _shell("docker logs testdb2 2>&1")
    for line in logs.splitlines():
        if "(!) Failed to create" in line or "SQL0293N" in line:
            return line.strip()
    return None


def _db2_environment_diagnostics() -> str:
    """Facts needed to diagnose a CREATE DATABASE failure: what filesystem the
    database path actually sits on, kernel async-I/O headroom, and the DB2
    diagnostic log entries with the underlying OS error."""
    return "\n".join(
        [
            "--- df -T /database ---",
            _shell("docker exec testdb2 df -T /database /database/data 2>&1"),
            "--- mount options ---",
            _shell(
                "docker exec testdb2 sh -c 'mount | grep -E \"/database| / \"' 2>&1"
            ),
            "--- aio limits (aio-nr / aio-max-nr) ---",
            _shell(
                "docker exec testdb2 sh -c 'cat /proc/sys/fs/aio-nr /proc/sys/fs/aio-max-nr' 2>&1"
            ),
            "--- device sector sizes ---",
            _shell(
                "docker exec testdb2 sh -c 'cat /sys/block/*/queue/logical_block_size /sys/block/*/queue/physical_block_size 2>/dev/null; blockdev --getss --getpbsz /dev/mapper/root 2>&1'"
            ),
            "--- db2diag root errors (first Severe/Error entries with OS errno) ---",
            _shell(
                "docker exec testdb2 sh -c \"grep -B3 -A22 -E 'LEVEL: (Severe|Error)' /database/config/db2inst1/sqllib/db2dump/DIAG0000/db2diag.log | head -220\" 2>&1"
            ),
        ]
    )


def _attempt_db2_connection() -> bool:
    engine = sqlalchemy.create_engine(DB2_URL)
    try:
        with engine.connect():
            return True
    except Exception:
        return False
    finally:
        engine.dispose()


def is_db2_up() -> bool:
    """Readiness = the test database accepts a connection. The container log
    line "Setup has completed." is printed even when database creation failed
    (the setup script only warns and keeps going), so log-grepping cannot
    signal readiness.

    The attempt runs in a daemon thread with a hard bound: the poll loop's
    overall timeout only ticks between checker calls, so a stalled handshake
    inside connect() must neither block the loop nor - were the thread
    non-daemon - the interpreter exit."""
    setup_failure = _db2_setup_failure()
    if setup_failure is not None:
        raise RuntimeError(
            f"DB2 container setup failed: {setup_failure!r}.\n"
            f"{_db2_environment_diagnostics()}"
        )

    outcome: "queue.Queue[bool]" = queue.Queue(maxsize=1)
    threading.Thread(
        target=lambda: outcome.put(_attempt_db2_connection()), daemon=True
    ).start()
    try:
        return outcome.get(timeout=30)
    except queue.Empty:
        return False


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
            checker=is_db2_up,
        )

        setup_filename = test_resources_dir / "setup" / "setup.sql"
        statements = _split_statements(open(setup_filename).read())

        engine = sqlalchemy.create_engine(DB2_URL)
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
@pytest.mark.skipif(
    not (platform.machine() == "x86_64" or platform.system() == "Darwin"),
    reason="ibm_db is not available for Linux ARM",
)
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
