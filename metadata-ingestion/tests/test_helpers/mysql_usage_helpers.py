"""Shared helpers for MySQL-family usage integration tests.

MySQL and MariaDB share the same usage-extraction code path (``MySQLSource``),
so their usage integration tests are structurally identical apart from the
platform name, port and credentials. Keeping the workload and assertions here
avoids verbatim copy-paste between the two test modules.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def execute_usage_workload(
    *, port: int, password: str, database: str = "test_usage_db"
) -> None:
    conn = pymysql.connect(
        host="localhost",
        port=port,
        user="root",
        password=password,
        database=database,
    )
    try:
        with conn.cursor() as cursor:
            for _ in range(3):
                for statement in USAGE_WORKLOAD:
                    cursor.execute(statement)
                    cursor.fetchall()
        conn.commit()
    finally:
        conn.close()


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
    lineages = aspects(mcps, "upstreamLineage")
    found = any(
        "processed_customers" in m["entityUrn"]
        and any(
            "raw_customer_data" in upstream["dataset"]
            for upstream in m["aspect"]["json"]["upstreams"]
        )
        for m in lineages
    )
    assert found, "expected query lineage processed_customers <- raw_customer_data"
