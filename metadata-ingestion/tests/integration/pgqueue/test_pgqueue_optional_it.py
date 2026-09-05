"""Live delivery and recovery coverage for the Python pgQueue repository."""

from __future__ import annotations

import re
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from typing import Callable, ContextManager, Dict, Iterator, List

import pytest
import pytest_docker.plugin
from psycopg2.extensions import connection as PGConnection, quote_ident

from datahub.pgqueue.config import PgQueueConnectionConfig
from datahub.pgqueue.connection import create_pgqueue_connection
from datahub.pgqueue.repository import PgQueueReceivedMessage, PgQueueRepository
from tests.test_helpers.docker_helpers import wait_for_port

POSTGRES_SERVICE = "pgqueue-postgres"
POSTGRES_CONTAINER = "pgqueue-it-postgres"
QUEUE_SCHEMA = "queue"
TABLE_PREFIX = "metadata_queue"
TOPIC_NAME = "MetadataChangeLog_Versioned_v1"
RETENTION_BATCH_DELETE_LIMIT = "5000"
UNRESOLVED_TOKEN = re.compile(r"__[A-Z0-9_]+__")
DockerComposeRunner = Callable[
    [Path, str], ContextManager[pytest_docker.plugin.Services]
]


def _render_migration(path: Path, replacements: Dict[str, str]) -> str:
    sql = path.read_text(encoding="utf-8")
    for token, value in replacements.items():
        sql = sql.replace(token, value)
    assert not UNRESOLVED_TOKEN.findall(sql)
    return sql


def _apply_pgqueue_sqlsetup(connection: PGConnection, repository_root: Path) -> None:
    migrations = (
        repository_root / "metadata-io/src/main/resources/sqlsetup/pgqueue/migrations"
    )
    schema_sql = _render_migration(
        migrations / "V001__schema.sql",
        {
            "__PGQUEUE_PREFIX__": TABLE_PREFIX,
            "__PGQUEUE_SCHEMA__": QUEUE_SCHEMA,
        },
    )
    partman_sql = _render_migration(
        migrations / "R__partman_register.sql",
        {
            "__PARTMAN_PARENT_QUALIFIED__": (f"{QUEUE_SCHEMA}.{TABLE_PREFIX}_message"),
            "__PARTMAN_INTERVAL__": "1 day",
            "__PARTMAN_PREMAKE__": "4",
        },
    )

    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_partman")
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{QUEUE_SCHEMA}"')
        cursor.execute(f'SET search_path TO "{QUEUE_SCHEMA}", public')
        cursor.execute(schema_sql)
        cursor.execute(partman_sql)
        cursor.execute(
            """
            SELECT n.nspname
            FROM pg_extension e
            INNER JOIN pg_namespace n ON n.oid = e.extnamespace
            WHERE e.extname = 'pg_partman'
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        assert row is not None
        partman_schema = quote_ident(str(row[0]), connection)
        parent_table = f"{QUEUE_SCHEMA}.{TABLE_PREFIX}_message".replace("'", "''")
        maintenance_sql = _render_migration(
            migrations / "R__maintenance_functions.sql",
            {
                "__PGQUEUE_PREFIX__": TABLE_PREFIX,
                "__PGQUEUE_SCHEMA__": quote_ident(QUEUE_SCHEMA, connection),
                "__BATCH_DELETE_LIMIT__": RETENTION_BATCH_DELETE_LIMIT,
                "__PGQUEUE_APPLY_RETENTION_PARTMAN_TAIL__": (
                    f"    PERFORM {partman_schema}.run_maintenance('{parent_table}');\n"
                ),
            },
        )
        cursor.execute(maintenance_sql)


@pytest.fixture(scope="module")
def pgqueue_config(
    docker_compose_runner: DockerComposeRunner, pytestconfig: pytest.Config
) -> Iterator[PgQueueConnectionConfig]:
    resources = pytestconfig.rootpath / "tests/integration/pgqueue"
    with docker_compose_runner(resources / "docker-compose.yml", "pgqueue") as services:
        wait_for_port(
            services,
            POSTGRES_CONTAINER,
            5432,
            timeout=120,
        )
        postgres_port = services.port_for(POSTGRES_SERVICE, 5432)
        config = PgQueueConnectionConfig(
            host_port=f"localhost:{postgres_port}",
            database="datahub",
            username="datahub",
            password="datahub",
            sslmode="disable",
            queue_schema=QUEUE_SCHEMA,
            table_prefix=TABLE_PREFIX,
        )
        connection = create_pgqueue_connection(config)
        try:
            _apply_pgqueue_sqlsetup(connection, pytestconfig.rootpath.parent)
        finally:
            connection.close()
        yield config


def _receive(
    repository: PgQueueRepository,
    connection: PGConnection,
    *,
    topic_id: int,
    lock_owner: str,
) -> List[PgQueueReceivedMessage]:
    return repository.receive_batch_for_group(
        connection,
        consumer_group="pgqueue-live-it",
        topic_id=topic_id,
        partition_ids=(0,),
        lock_owner=lock_owner,
        visibility_timeout=timedelta(seconds=30),
        max_messages=1,
    )


@pytest.mark.integration
def test_unacked_message_is_redelivered_then_advances_offset(
    pgqueue_config: PgQueueConnectionConfig,
) -> None:
    repository = PgQueueRepository(QUEUE_SCHEMA, TABLE_PREFIX)
    with closing(create_pgqueue_connection(pgqueue_config)) as first_connection:
        handle = repository.enqueue(
            first_connection,
            topic_name=TOPIC_NAME,
            routing_key="urn:li:dataset:(urn:li:dataPlatform:test,events,PROD)",
            partition_count=1,
            retention_max_age_seconds=604800,
            max_rows_per_topic=0,
            max_total_payload_bytes=0,
            priority=5,
            payload=b"metadata-change-log",
            content_type="application/avro",
            headers=(("trace-id", b"pgqueue-live-it"),),
        )
        repository.apply_topic_retention(first_connection)
        topic = repository.fetch_topic_row(first_connection, TOPIC_NAME)
        assert topic is not None
        topic_id, _, _ = topic

        first_delivery = _receive(
            repository,
            first_connection,
            topic_id=topic_id,
            lock_owner="pgqueue-live-it:first",
        )
        assert len(first_delivery) == 1
        assert first_delivery[0].handle == handle

    # Closing the first connection without an acknowledgement simulates worker loss.
    with closing(create_pgqueue_connection(pgqueue_config)) as recovery_connection:
        assert not _receive(
            repository,
            recovery_connection,
            topic_id=topic_id,
            lock_owner="pgqueue-live-it:recovery",
        )

        # Advance the persisted lease instead of sleeping through the visibility timeout.
        recovery_connection.autocommit = True
        with recovery_connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {QUEUE_SCHEMA}.{TABLE_PREFIX}_message_group_lease
                SET locked_until = NOW() - INTERVAL '1 second'
                WHERE consumer_group = %s
                """,
                ("pgqueue-live-it",),
            )
            assert cursor.rowcount == 1

        replayed = _receive(
            repository,
            recovery_connection,
            topic_id=topic_id,
            lock_owner="pgqueue-live-it:recovery",
        )
        assert len(replayed) == 1
        assert replayed[0].handle == handle
        assert replayed[0].payload == b"metadata-change-log"
        assert (
            repository.get_committed_offset(
                recovery_connection, "pgqueue-live-it", topic_id, 0
            )
            == 0
        )

        assert (
            repository.commit_for_group(
                recovery_connection, "pgqueue-live-it", [replayed[0].handle]
            )
            == 1
        )
        assert (
            repository.get_committed_offset(
                recovery_connection, "pgqueue-live-it", topic_id, 0
            )
            == 1
        )

    with closing(create_pgqueue_connection(pgqueue_config)) as final_connection:
        assert not _receive(
            repository,
            final_connection,
            topic_id=topic_id,
            lock_owner="pgqueue-live-it:final",
        )
        with final_connection.cursor() as cursor:
            # The authoritative SqlSetup must create actual pg_partman child tables.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM pg_inherits
                WHERE inhparent = to_regclass(%s)
                """,
                (f"{QUEUE_SCHEMA}.{TABLE_PREFIX}_message",),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] > 0
