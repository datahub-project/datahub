"""Create test data in Flink for integration tests.

Test scenarios:
1. Multi-hop Kafka via HiveCatalog: orders → job_1 → enriched-orders → job_2 → final-output
2. JDBC/Postgres catalog (3-part: catalog=pg_catalog, database=flink_catalog, table=public.users)
3. Iceberg 2-level naming (REST catalog, MinIO-backed): ice_catalog.lake.events

Infrastructure:
- Kafka topics created via docker exec
- Postgres tables created via psql
- JDBC + Iceberg + Hive catalogs registered via SQL Gateway (persistent FileCatalogStore)
- Kafka tables created in HiveCatalog (visible across SQL Gateway sessions)
- All 4 streaming jobs submitted via SQL client (each creates its own catalogs)
"""

import logging
import subprocess
import time

import psycopg2
import requests

logger = logging.getLogger(__name__)

JOBMANAGER_URL = "http://localhost:8082"
SQL_GATEWAY_URL = "http://localhost:8084"
JOBMANAGER_CONTAINER = "test_flink_jobmanager"
POSTGRES_CONTAINER = "test_flink_postgres"
BROKER_CONTAINER = "test_flink_broker"

KAFKA_TOPICS = [
    "orders",
    "enriched-orders",
    "final-output",
    "user-events",
    "enriched-user-events",
]

# ── Catalogs registered via SQL Gateway (persistent FileCatalogStore) ──
# These are visible to the connector's own SQL Gateway session for platform resolution.

CREATE_JDBC_CATALOG_SQL = """\
DROP CATALOG IF EXISTS pg_catalog;
CREATE CATALOG pg_catalog WITH (
    'type' = 'jdbc',
    'default-database' = 'flink_catalog',
    'username' = 'flink',
    'password' = 'flink',
    'base-url' = 'jdbc:postgresql://postgres:5432'
)"""

CREATE_ICEBERG_CATALOG_SQL = """\
DROP CATALOG IF EXISTS ice_catalog;
CREATE CATALOG ice_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181'
)"""

# Iceberg table DDL has no WITH clause because the catalog type is already declared in
# CREATE_ICEBERG_CATALOG_SQL above. When Flink issues CREATE TABLE inside an Iceberg catalog,
# it delegates the DDL to the Iceberg REST Catalog server (a separate container in the test
# compose stack, backed by MinIO for data file storage). The REST catalog owns the table
# metadata — schema, partition spec, and eventually snapshot pointers — so no connector
# properties are needed in the DDL itself. This differs from HiveCatalog Kafka tables, where
# the WITH clause carries Flink-specific connector properties (topic, bootstrap servers, format)
# because Kafka is a runtime connector binding, not a managed table format.
CREATE_ICEBERG_DB_SQL = "CREATE DATABASE IF NOT EXISTS ice_catalog.lake"

CREATE_ICEBERG_TABLE_SQL = """CREATE TABLE IF NOT EXISTS ice_catalog.lake.events (
    event_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP(6)
)"""

CREATE_ICEBERG_RESULTS_TABLE_SQL = """CREATE TABLE IF NOT EXISTS ice_catalog.lake.results (
    event_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP(6)
)"""

CREATE_ICEBERG_KAFKA_EVENTS_TABLE_SQL = """CREATE TABLE IF NOT EXISTS ice_catalog.lake.kafka_events (
    user_id BIGINT,
    username STRING,
    email STRING,
    active BOOLEAN
)"""

CREATE_HIVE_CATALOG_SQL = """\
DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'default',
    'hive-conf-dir' = '/opt/hive-conf'
)"""

# Kafka tables in HiveCatalog persist across SQL Gateway sessions (production-correct setup).
HIVE_KAFKA_TABLES_SQL = """\
CREATE DATABASE IF NOT EXISTS hive_catalog.flink_db;

CREATE TABLE IF NOT EXISTS hive_catalog.flink_db.orders (
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

CREATE TABLE IF NOT EXISTS hive_catalog.flink_db.enriched_orders (
    order_id BIGINT,
    amount DECIMAL(10,2),
    customer_name STRING,
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched-orders',
    'properties.bootstrap.servers' = 'broker:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE IF NOT EXISTS hive_catalog.flink_db.user_events (
    user_id BIGINT,
    username STRING,
    email STRING,
    active BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = 'user-events',
    'properties.bootstrap.servers' = 'broker:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE IF NOT EXISTS hive_catalog.flink_db.enriched_user_events (
    user_id BIGINT,
    username STRING,
    email STRING,
    active BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched-user-events',
    'properties.bootstrap.servers' = 'broker:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

"""

# ── Postgres tables (reflected by JDBC catalog as public.users, public.products) ──

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
CREATE TABLE IF NOT EXISTS results (
    order_id BIGINT,
    amount DECIMAL(10,2),
    customer_name VARCHAR(255),
    order_time TIMESTAMP
);
"""

# ── Streaming jobs submitted via SQL client ──
# Each job creates its own HiveCatalog reference (SQL client session is independent
# from SQL Gateway). Tables are already persisted in Hive Metastore, so the job
# only needs to register the catalog and reference the existing tables.

# Job 1: orders → enriched-orders (multi-hop first leg, via HiveCatalog)
JOB1_SQL = """\
CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'default',
    'hive-conf-dir' = '/opt/hive-conf'
);

SET 'pipeline.name' = 'test_enrich_orders';

INSERT INTO hive_catalog.flink_db.enriched_orders
SELECT * FROM hive_catalog.flink_db.orders;
"""

# Job 2: Kafka → Postgres (dominant ETL materialization pattern).
# Reads orders from Kafka (via HiveCatalog) and writes to pg_catalog.flink_catalog.public.results.
# Plan shows:
#   TableSourceScan(table=[[hive_catalog, flink_db, orders]])             → Kafka (Tier 2 SHOW CREATE TABLE)
#   Sink(table=[[pg_catalog, flink_catalog, public.results]])             → Postgres (DESCRIBE CATALOG EXTENDED)
# Both endpoints are fully resolvable — first SQL Table API job with a non-Iceberg, non-empty output.
JOB2_SQL = """\
CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'default',
    'hive-conf-dir' = '/opt/hive-conf'
);

CREATE CATALOG pg_catalog WITH (
    'type' = 'jdbc',
    'default-database' = 'flink_catalog',
    'username' = 'flink',
    'password' = 'flink',
    'base-url' = 'jdbc:postgresql://postgres:5432'
);

SET 'pipeline.name' = 'test_kafka_to_pg';

INSERT INTO pg_catalog.flink_catalog.`public.results`
SELECT * FROM hive_catalog.flink_db.orders;
"""

# Job 3: JDBC 3-level naming test.
# Uses UNION ALL of JDBC scan (bounded) + Kafka (unbounded) to keep job RUNNING.
# FLIP-147 guarantees the job stays RUNNING — the Kafka branch is unbounded.
# Both sources use HiveCatalog or JDBC catalog so SHOW CREATE TABLE works from
# a new SQL Gateway session (temporary tables are session-scoped and invisible).
# Plan shows:
#   TableSourceScan(table=[[pg_catalog, flink_catalog, public.users]])   → Postgres
#   TableSourceScan(table=[[hive_catalog, flink_db, user_events]])       → Kafka
JOB3_SQL = """\
CREATE CATALOG pg_catalog WITH (
    'type' = 'jdbc',
    'default-database' = 'flink_catalog',
    'username' = 'flink',
    'password' = 'flink',
    'base-url' = 'jdbc:postgresql://postgres:5432'
);

CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'default',
    'hive-conf-dir' = '/opt/hive-conf'
);

SET 'pipeline.name' = 'test_pg_users_pipeline';

INSERT INTO hive_catalog.flink_db.enriched_user_events
SELECT * FROM pg_catalog.flink_catalog.`public.users`
UNION ALL
SELECT * FROM hive_catalog.flink_db.user_events;
"""

# Job 4: Iceberg 2-level naming test.
# Uses Iceberg streaming read (monitor-interval polls for new snapshots).
# Job stays RUNNING even with empty table (confirmed by Iceberg Issue #5803).
# Plan shows:
#   TableSourceScan(table=[[ice_catalog, lake, events]])     → Iceberg source
#   Sink(table=[[ice_catalog, lake, results]])               → Iceberg sink
# Both tables are in the REST-backed Iceberg catalog so SHOW CREATE TABLE from the
# connector's SQL Gateway session resolves connector=iceberg for both sides, giving
# fully resolved input AND output lineage. This is the correct way to demonstrate
# iceberg → flink → iceberg stitching without hitting the Kafka Sink V2 limitation.
JOB4_SQL = """\
CREATE CATALOG ice_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181'
);

CREATE DATABASE IF NOT EXISTS ice_catalog.lake;

CREATE TABLE IF NOT EXISTS ice_catalog.lake.events (
    event_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP(6)
);

CREATE TABLE IF NOT EXISTS ice_catalog.lake.results (
    event_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP(6)
);

SET 'pipeline.name' = 'test_iceberg_events_pipeline';

INSERT INTO ice_catalog.lake.results
SELECT * FROM ice_catalog.lake.events
  /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */;
"""


# Job 5: Kafka → Iceberg (canonical lakehouse ingestion pattern).
# Reads user_events (Kafka, via HiveCatalog) and writes to ice_catalog.lake.kafka_events.
# Plan shows:
#   TableSourceScan(table=[[hive_catalog, flink_db, user_events]])  → Kafka (Tier 2 SHOW CREATE TABLE)
#   Sink(table=[[ice_catalog, lake, kafka_events]])                 → Iceberg (catalog_platform_map)
# Both endpoints are fully resolvable — this is the production-representative
# kafka → flink → iceberg stitching scenario.
JOB5_SQL = """\
CREATE CATALOG ice_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181'
);

CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'default',
    'hive-conf-dir' = '/opt/hive-conf'
);

SET 'pipeline.name' = 'test_kafka_to_iceberg';

INSERT INTO ice_catalog.lake.kafka_events
SELECT user_id, username, email, active FROM hive_catalog.flink_db.user_events;
"""


def _wait_for_running_jobs(expected: int, timeout: float = 120.0) -> None:
    """Poll JobManager until expected number of jobs are RUNNING."""
    waited = 0.0
    while waited < timeout:
        try:
            resp = requests.get(f"{JOBMANAGER_URL}/v1/jobs/overview", timeout=5)
            resp.raise_for_status()
            jobs = resp.json().get("jobs", [])
            running = [j for j in jobs if j.get("state") == "RUNNING"]
            if len(running) >= expected:
                logger.info(
                    "Found %d running job(s): %s",
                    len(running),
                    [j["name"] for j in running],
                )
                return
        except Exception:
            pass
        time.sleep(2.0)
        waited += 2.0
    # On timeout, show what we have
    try:
        resp = requests.get(f"{JOBMANAGER_URL}/v1/jobs/overview", timeout=5)
        jobs = resp.json().get("jobs", [])
        logger.error(
            "Timeout: only %d/%d jobs running. States: %s",
            len([j for j in jobs if j.get("state") == "RUNNING"]),
            expected,
            [(j["name"], j["state"]) for j in jobs],
        )
    except Exception:
        pass
    raise TimeoutError(f"Expected {expected} RUNNING jobs within {timeout}s")


def _create_kafka_topics() -> None:
    """Create Kafka topics used by all Flink jobs."""
    for topic in KAFKA_TOPICS:
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
    logger.info("Kafka topics created: %s", KAFKA_TOPICS)


def _execute_sql_gateway(sql: str) -> None:
    """Execute one or more SQL statements via the SQL Gateway REST API."""
    resp = requests.post(
        f"{SQL_GATEWAY_URL}/v1/sessions",
        json={"properties": {}},
        timeout=10,
    )
    resp.raise_for_status()
    session = resp.json()["sessionHandle"]

    for statement in sql.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        resp = requests.post(
            f"{SQL_GATEWAY_URL}/v1/sessions/{session}/statements",
            json={"statement": statement},
            timeout=30,
        )
        resp.raise_for_status()
        op = resp.json()["operationHandle"]

        for _ in range(60):
            status = requests.get(
                f"{SQL_GATEWAY_URL}/v1/sessions/{session}/operations/{op}/status",
                timeout=10,
            ).json()
            if status.get("status") in ("FINISHED", "ERROR", "CANCELED"):
                if status.get("status") == "ERROR":
                    raise RuntimeError(f"SQL Gateway statement failed: {status}")
                break
            time.sleep(0.5)

    logger.info("SQL Gateway statements executed")


def _setup_postgres_tables() -> None:
    """Create test tables in Postgres using a direct psycopg2 connection.

    psql via docker exec silently swallows DDL errors (exit code 0 even on failure).
    psycopg2 raises real Python exceptions on any SQL error, so failures surface immediately.
    Using localhost:5433 (the host-mapped port) mirrors the same network path used by the
    DataHub Postgres source connector in the stitching tests.
    """
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="flink_catalog",
        user="flink",
        password="flink",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for statement in POSTGRES_DDL.split(";"):
                stmt = statement.strip()
                if stmt:
                    cur.execute(stmt)
    finally:
        conn.close()
    logger.info("Postgres tables created")


def _register_catalogs() -> None:
    """Register JDBC, Iceberg, and Hive catalogs via SQL Gateway (persistent).

    Catalogs are registered in the SQL Gateway for the connector's platform
    resolution (SHOW CREATE TABLE). Kafka tables are created in HiveCatalog
    so they persist across sessions (production-correct setup). Iceberg tables
    are also created via SQL Gateway (REST catalog backed by MinIO — no local
    volume mount required).
    """
    _execute_sql_gateway(CREATE_JDBC_CATALOG_SQL)
    logger.info("JDBC catalog 'pg_catalog' registered")
    _execute_sql_gateway(CREATE_ICEBERG_CATALOG_SQL)
    logger.info("Iceberg catalog 'ice_catalog' registered")
    _execute_sql_gateway(CREATE_ICEBERG_DB_SQL)
    logger.info("Iceberg database 'lake' created")
    _execute_sql_gateway(CREATE_ICEBERG_TABLE_SQL)
    logger.info("Iceberg table 'lake.events' created")
    _execute_sql_gateway(CREATE_ICEBERG_RESULTS_TABLE_SQL)
    logger.info("Iceberg table 'lake.results' created")
    _execute_sql_gateway(CREATE_ICEBERG_KAFKA_EVENTS_TABLE_SQL)
    logger.info("Iceberg table 'lake.kafka_events' created")
    _execute_sql_gateway(CREATE_HIVE_CATALOG_SQL)
    logger.info("Hive catalog 'hive_catalog' registered")
    _execute_sql_gateway(HIVE_KAFKA_TABLES_SQL)
    logger.info("Kafka tables created in hive_catalog")


def _submit_sql_job(sql: str, label: str) -> None:
    """Submit a Flink SQL job via the SQL client."""
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            JOBMANAGER_CONTAINER,
            "/opt/flink/bin/sql-client.sh",
            "embedded",
        ],
        input=sql,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        logger.error("SQL client stderr for %s: %s", label, result.stderr)
        raise RuntimeError(
            f"Flink SQL client failed for {label} (exit {result.returncode}): "
            f"{result.stderr[:500]}"
        )
    logger.info("Job '%s' submitted", label)


def _submit_pyflink_job(input_topic: str, output_topic: str, job_name: str) -> None:
    """Submit a DataStream Kafka job via PyFlink inside the JobManager container.

    Produces KafkaSource-{input_topic} / KafkaSink-{output_topic} plan patterns
    that the DataStreamKafkaExtractor parses for lineage.
    """
    result = subprocess.run(
        [
            "docker",
            "exec",
            JOBMANAGER_CONTAINER,
            "/opt/flink/bin/flink",
            "run",
            "--detached",
            "--python",
            "/opt/flink/pyflink_kafka_job.py",
            "--input-topic",
            input_topic,
            "--output-topic",
            output_topic,
            "--bootstrap-servers",
            "broker:9092",
            "--job-name",
            job_name,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        logger.error("PyFlink stderr for %s: %s", job_name, result.stderr)
        raise RuntimeError(
            f"PyFlink job failed for {job_name} (exit {result.returncode}): "
            f"{result.stderr[:500]}"
        )
    logger.info("PyFlink job '%s' submitted", job_name)


def setup_test_data() -> None:
    """Create all test data: catalogs, tables, and streaming jobs.

    Jobs cover five lineage patterns:
    - DataStream Kafka: KafkaSource-{topic} / KafkaSink-{topic} (via PyFlink)
    - SQL/Table API with HiveCatalog: TableSourceScan(table=[[hive_catalog, ...]])
    - SQL/Table API kafka → postgres: dominant ETL materialization (Kafka source, Postgres sink)
    - SQL/Table API with JDBC/Iceberg catalogs: 3-level and 2-level naming
    - SQL/Table API kafka → iceberg: canonical lakehouse ingestion (Kafka source, Iceberg sink)
    """
    logger.info("Setting up Flink test data...")
    _create_kafka_topics()
    _setup_postgres_tables()
    _register_catalogs()
    # DataStream Kafka multi-hop: topic1 → flink → topic2 → flink → topic3
    _submit_pyflink_job("orders", "enriched-orders", "test_ds_kafka_hop1")
    _submit_pyflink_job("enriched-orders", "final-output", "test_ds_kafka_hop2")
    # SQL/Table API with HiveCatalog (Kafka tables visible across sessions)
    _submit_sql_job(JOB1_SQL, "test_enrich_orders")
    # SQL/Table API kafka → postgres (first SQL job with fully-resolved non-Iceberg output)
    _submit_sql_job(JOB2_SQL, "test_kafka_to_pg")
    # SQL/Table API with JDBC (3-level naming) and Iceberg (2-level naming)
    _submit_sql_job(JOB3_SQL, "test_pg_users_pipeline")
    _submit_sql_job(JOB4_SQL, "test_iceberg_events_pipeline")
    # SQL/Table API kafka → iceberg (canonical lakehouse ingestion pattern)
    _submit_sql_job(JOB5_SQL, "test_kafka_to_iceberg")
    _wait_for_running_jobs(expected=7)
    logger.info("Test data setup complete.")
