"""Create test data in Flink for integration tests.

Three setup steps are needed:

1. **SQL client** (docker exec): Creates datagen/blackhole tables in the
   default in-memory catalog and submits a streaming job. Jobs appear on
   the JobManager REST API.

2. **psql** (docker exec): Creates test tables directly in Postgres.
   The JDBC catalog reflects these as Flink tables.

3. **SQL Gateway REST API**: Registers a JDBC catalog pointing to Postgres.
   The FileCatalogStore persists this registration so the connector's
   own SQL Gateway session can discover the catalog and its tables.
"""

import logging
import subprocess
import time

import requests

logger = logging.getLogger(__name__)

JOBMANAGER_URL = "http://localhost:8082"
SQL_GATEWAY_URL = "http://localhost:8084"
JOBMANAGER_CONTAINER = "test_flink_jobmanager"
POSTGRES_CONTAINER = "test_flink_postgres"
BROKER_CONTAINER = "test_flink_broker"

# Streaming job via SQL client (default_catalog, in-memory).
# Uses Kafka source/sink so the execution plan has real KafkaSource/KafkaSink
# nodes that the lineage extractor can parse into source-to-sink lineage.
JOB_SQL = """\
CREATE TABLE orders (
    order_id BIGINT,
    amount DECIMAL(10,2),
    customer_name STRING,
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'broker:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE enriched_orders (
    order_id BIGINT,
    amount DECIMAL(10,2),
    customer_name STRING,
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched-orders',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json'
);

SET 'pipeline.name' = 'test_enrich_orders';

INSERT INTO enriched_orders SELECT * FROM orders;
"""

# Tables created directly in Postgres (reflected by JDBC catalog)
POSTGRES_DDL = """\
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT,
    username VARCHAR(255),
    email VARCHAR(255),
    active BOOLEAN
);
CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT,
    title VARCHAR(255),
    price DOUBLE PRECISION,
    in_stock BOOLEAN
);
CREATE TABLE IF NOT EXISTS events (
    event_id BIGINT,
    event_type VARCHAR(100),
    payload VARCHAR(1000),
    event_time TIMESTAMP
);
"""

# JDBC catalog registration (persisted by FileCatalogStore on SQL Gateway)
CREATE_CATALOG_SQL = """CREATE CATALOG pg_catalog WITH (
    'type' = 'jdbc',
    'default-database' = 'flink_catalog',
    'username' = 'flink',
    'password' = 'flink',
    'base-url' = 'jdbc:postgresql://postgres:5432'
)"""


def _wait_for_running_job(timeout: float = 60.0) -> None:
    """Poll JobManager until at least one job is RUNNING."""
    waited = 0.0
    while waited < timeout:
        try:
            resp = requests.get(f"{JOBMANAGER_URL}/v1/jobs/overview", timeout=5)
            resp.raise_for_status()
            jobs = resp.json().get("jobs", [])
            running = [j for j in jobs if j.get("state") == "RUNNING"]
            if running:
                logger.info("Found %d running job(s)", len(running))
                return
        except Exception:
            pass
        time.sleep(1.0)
        waited += 1.0
    raise TimeoutError("No RUNNING job found within timeout")


def _create_kafka_topics() -> None:
    """Create Kafka topics used by Flink source/sink tables."""
    for topic in ["orders", "enriched-orders"]:
        result = subprocess.run(
            [
                "docker",
                "exec",
                BROKER_CONTAINER,
                "kafka-topics",
                "--bootstrap-server",
                "broker:9092",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
                "--if-not-exists",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to create topic {topic}: {result.stderr}")
    logger.info("Kafka topics created")


def _setup_streaming_job() -> None:
    """Submit Kafka->Kafka streaming job via SQL client."""
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            JOBMANAGER_CONTAINER,
            "/opt/flink/bin/sql-client.sh",
            "embedded",
        ],
        input=JOB_SQL,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        logger.error("SQL client stderr: %s", result.stderr)
        raise RuntimeError(
            f"Flink SQL client failed (exit {result.returncode}): {result.stderr}"
        )
    logger.info("Streaming job submitted")


def _setup_postgres_tables() -> None:
    """Create test tables directly in Postgres."""
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            POSTGRES_CONTAINER,
            "psql",
            "-U",
            "flink",
            "-d",
            "flink_catalog",
        ],
        input=POSTGRES_DDL,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Postgres setup failed: {result.stderr}")
    logger.info("Postgres tables created")


def _register_jdbc_catalog() -> None:
    """Register JDBC catalog via SQL Gateway (persisted by FileCatalogStore)."""
    resp = requests.post(
        f"{SQL_GATEWAY_URL}/v1/sessions",
        json={"properties": {}},
        timeout=10,
    )
    resp.raise_for_status()
    session = resp.json()["sessionHandle"]

    resp = requests.post(
        f"{SQL_GATEWAY_URL}/v1/sessions/{session}/statements",
        json={"statement": CREATE_CATALOG_SQL},
        timeout=30,
    )
    resp.raise_for_status()
    op = resp.json()["operationHandle"]

    for _ in range(30):
        status = requests.get(
            f"{SQL_GATEWAY_URL}/v1/sessions/{session}/operations/{op}/status",
            timeout=10,
        ).json()
        if status.get("status") in ("FINISHED", "ERROR", "CANCELED"):
            if status.get("status") == "ERROR":
                raise RuntimeError(f"CREATE CATALOG failed: {status}")
            break
        time.sleep(0.5)

    logger.info("JDBC catalog 'pg_catalog' registered via SQL Gateway")


def setup_test_data() -> None:
    """Create streaming job + JDBC catalog with Postgres tables."""
    logger.info("Setting up Flink test data...")
    _create_kafka_topics()
    _setup_streaming_job()
    _wait_for_running_job()
    _setup_postgres_tables()
    _register_jdbc_catalog()
    logger.info("Test data setup complete.")
