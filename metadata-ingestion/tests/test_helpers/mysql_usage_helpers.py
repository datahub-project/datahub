"""Shared helpers for MySQL-family usage integration tests.

MySQL and MariaDB share the same usage-extraction code path (``MySQLSource``),
so their usage integration tests are structurally identical apart from the
platform name, port and credentials. Keeping the workload and assertions here
avoids verbatim copy-paste between the two test modules.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pymysql

from datahub.ingestion.run.pipeline import Pipeline

# Read/transform workload executed against the live server. It must run here
# (not in the init scripts) because MySQL/MariaDB run init scripts on a
# temporary server that is then replaced, wiping the in-memory
# performance_schema digests. The workload avoids variadic functions (e.g.
# SUBSTRING_INDEX): digest text truncates such argument lists to ``...``, which
# the SQL parser cannot read, so performance_schema-mode lineage would be lost.
# general_log keeps literal SQL.
USAGE_WORKLOAD = [
    "SELECT id, name FROM raw_customer_data WHERE id = 1",
    "SELECT id, name FROM raw_customer_data WHERE id = 2",
    "SELECT id, name FROM raw_customer_data WHERE id = 3",
    "SELECT COUNT(*) FROM raw_customer_data",
    "INSERT INTO processed_customers (customer_id, full_name) "
    "SELECT id, name FROM raw_customer_data",
]


def _run_workload_session(
    *,
    port: int,
    password: str,
    database: Optional[str],
    use_statement: Optional[str] = None,
) -> None:
    connect_kwargs: dict = {
        "host": "localhost",
        "port": port,
        "user": "root",
        "password": password,
    }
    if database is not None:
        connect_kwargs["database"] = database
    conn = pymysql.connect(**connect_kwargs)
    try:
        with conn.cursor() as cursor:
            if use_statement is not None:
                cursor.execute(f"USE {use_statement}")
            for _ in range(3):
                for statement in USAGE_WORKLOAD:
                    cursor.execute(statement)
                    cursor.fetchall()
        conn.commit()
    finally:
        conn.close()


def execute_usage_workload(
    *, port: int, password: str, database: str = "test_usage_db"
) -> None:
    # Primary session connects straight into the target database, so general_log
    # records the schema on the Connect event.
    _run_workload_session(port=port, password=password, database=database)
    # Secondary session connects with no default schema and switches in via USE.
    # This exercises general_log's Connect(empty)/USE session tracking against a
    # real mysql.general_log table (not just mocked rows) and keeps a second
    # concurrent thread_id in play.
    _run_workload_session(
        port=port, password=password, database=None, use_statement=database
    )


def run_usage_pipeline(
    *,
    platform: str,
    usage_source: str,
    port: int,
    password: str,
    output_path: Path,
    database: str = "test_usage_db",
) -> list:
    # The workload runs with the server's real clock, so pin an explicit wide
    # window around now instead of the default rolling one — a small gap between
    # workload and assertion can't push queries out of range.
    now = datetime.now(timezone.utc)
    pipeline = Pipeline.create(
        {
            "run_id": f"{platform}-usage-{usage_source}",
            "source": {
                "type": platform,
                "config": {
                    "host_port": f"localhost:{port}",
                    "database": database,
                    "username": "root",
                    "password": password,
                    "include_usage_statistics": True,
                    "usage_source": usage_source,
                    "usage": {
                        "start_time": (now - timedelta(days=1)).isoformat(),
                        "end_time": (now + timedelta(days=1)).isoformat(),
                    },
                    "profiling": {"enabled": False},
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return json.loads(output_path.read_text())


def aspects(mcps: list, aspect_name: str) -> list:
    return [m for m in mcps if m.get("aspectName") == aspect_name]


def assert_query_lineage_present(mcps: list) -> None:
    lineages = [
        m
        for m in aspects(mcps, "upstreamLineage")
        if "processed_customers" in m["entityUrn"]
    ]

    # Coarse (table-level) lineage.
    assert any(
        any(
            "raw_customer_data" in upstream["dataset"]
            for upstream in m["aspect"]["json"]["upstreams"]
        )
        for m in lineages
    ), "expected coarse lineage processed_customers <- raw_customer_data"

    # Fine-grained (column-level) lineage. The INSERT ... SELECT copies
    # raw_customer_data columns into processed_customers, so CLL must survive.
    # Asserted explicitly to catch silent degradation to coarse-only.
    fine = [
        fgl
        for m in lineages
        for fgl in (m["aspect"]["json"].get("fineGrainedLineages") or [])
    ]
    assert any(
        any("raw_customer_data" in up for up in (fgl.get("upstreams") or []))
        and any(
            "processed_customers" in down for down in (fgl.get("downstreams") or [])
        )
        for fgl in fine
    ), (
        "expected column-level lineage raw_customer_data.<col> -> processed_customers.<col>"
    )


def assert_top_sql_queries(mcps: list, *, usage_source: str) -> None:
    # topSqlQueries is the field that distinguishes the two modes:
    # performance_schema emits normalized digests (literals replaced with `?`),
    # general_log emits literal SQL (distinct predicate values preserved).
    top = [
        q
        for m in aspects(mcps, "datasetUsageStatistics")
        if "raw_customer_data" in m["entityUrn"]
        for q in (m["aspect"]["json"].get("topSqlQueries") or [])
    ]
    assert top, "expected topSqlQueries on raw_customer_data usage"

    if usage_source == "performance_schema":
        assert any("= ?" in q for q in top), (
            f"performance_schema should emit normalized digests, got {top}"
        )
        assert not any("= 1" in q for q in top), (
            f"performance_schema must not carry literal predicates, got {top}"
        )
    else:
        assert any("= 1" in q for q in top), (
            f"general_log should emit literal SQL, got {top}"
        )
        assert not any("= ?" in q for q in top), (
            f"general_log must not normalize literals, got {top}"
        )
