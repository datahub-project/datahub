import json
import pathlib
from typing import Any, Dict

import pytest
import time_machine

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.flink.source import FlinkSource
from datahub.testing import mce_helpers
from tests.integration.flink.setup_flink_test_data import setup_test_data
from tests.test_helpers import test_connection_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration

FROZEN_TIME = "2026-03-05 10:00:00"

FLINK_REST_URL = "http://localhost:8082"
SQL_GATEWAY_URL = "http://localhost:8084"
TEST_RESOURCES_DIR = pathlib.Path(__file__).parent

# Dynamic fields that change every Docker run (job IDs, timestamps, durations).
# Golden file comparison ignores these so tests remain deterministic.
FLINK_IGNORE_PATHS = [
    # Job ID is a random UUID assigned by Flink each run
    r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['flink_job_id'\]",
    # External URL contains the job ID
    r"root\[\d+\]\['aspect'\]\['json'\]\['externalUrl'\]",
    # Start time and duration depend on when Docker starts the job
    r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['start_time'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['duration_ms'\]",
]


@pytest.fixture(scope="module")
def test_resources_dir() -> pathlib.Path:
    return pathlib.Path(__file__).parent


@pytest.fixture(scope="module")
def flink_runner(docker_compose_runner, test_resources_dir):  # type: ignore
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "flink"
    ) as docker_services:
        # wait_for_port probes container-internal ports; FLINK_REST_URL/SQL_GATEWAY_URL
        # use the host-mapped ports (8082/8084 respectively).
        wait_for_port(docker_services, "test_flink_jobmanager", 8081, timeout=120)
        wait_for_port(docker_services, "test_flink_sql_gateway", 8083, timeout=120)
        wait_for_port(docker_services, "test-flink-hive-metastore", 9083, timeout=120)
        wait_for_port(docker_services, "test_flink_iceberg_rest", 8181, timeout=120)
        setup_test_data()
        yield docker_services


@time_machine.travel(FROZEN_TIME)
def test_flink_ingest(
    flink_runner: Any, pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Full ingestion: jobs + platform-resolved lineage via SQL Gateway."""
    output_file = tmp_path / "flink_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "flink-golden-test",
            "source": {
                "type": "flink",
                "config": {
                    "connection": {
                        "rest_api_url": FLINK_REST_URL,
                        "sql_gateway_url": SQL_GATEWAY_URL,
                    },
                    "include_lineage": True,
                    "include_run_history": False,
                    "env": "TEST",
                    # Flink 1.19 doesn't support DESCRIBE CATALOG.
                    # Iceberg/Paimon catalogs need explicit platform in catalog_platform_map.
                    "catalog_platform_map": {
                        "ice_catalog": {"platform": "iceberg"},
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(output_file)},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=TEST_RESOURCES_DIR / "flink_mces_golden.json",
        ignore_paths=FLINK_IGNORE_PATHS,
    )


@time_machine.travel(FROZEN_TIME)
def test_flink_ingest_no_sql_gateway(
    flink_runner: Any, pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Ingestion without SQL Gateway: DataFlow + DataJob emitted, no SQL lineage."""
    output_file = tmp_path / "flink_no_gw_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "flink-no-gw-golden-test",
            "source": {
                "type": "flink",
                "config": {
                    "connection": {
                        "rest_api_url": FLINK_REST_URL,
                    },
                    "include_lineage": True,
                    "include_run_history": False,
                    "env": "TEST",
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(output_file)},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=TEST_RESOURCES_DIR / "flink_no_gw_mces_golden.json",
        ignore_paths=FLINK_IGNORE_PATHS,
    )


def test_connection_success(flink_runner: Any) -> None:
    report = test_connection_helpers.run_test_connection(
        FlinkSource,
        {
            "connection": {
                "rest_api_url": FLINK_REST_URL,
                "sql_gateway_url": SQL_GATEWAY_URL,
            },
            "env": "TEST",
        },
    )
    test_connection_helpers.assert_basic_connectivity_success(report)
    test_connection_helpers.assert_capability_report(
        capability_report=report.capability_report,
        success_capabilities=[
            SourceCapability.LINEAGE_COARSE,
        ],
    )


@time_machine.travel(FROZEN_TIME)
def test_flink_datastream_chained_lineage(
    flink_runner: Any, tmp_path: pathlib.Path
) -> None:
    """DataStream jobs with Kafka Sink V2 produce correct lineage despite the ': Writer' suffix.

    Flink 1.19 changed how Kafka sinks appear in plan descriptions:
      - Legacy (Flink < 1.15):   "Sink: KafkaSink-{topic}"
      - Flink 1.19 Sink V2:      "KafkaSink-{topic}: Writer"  (possibly chained with source)

    This test verifies that:
    1. The connector correctly parses the ': Writer' suffix format.
    2. If operators are chained into a single plan node (source + sink in one
       node with '<br/>' separator), both source and sink lineage are extracted.

    Jobs under test (submitted by setup_flink_test_data.py via PyFlink DataStream API):
      test_ds_kafka_hop1: orders -> enriched-orders
      test_ds_kafka_hop2: enriched-orders -> final-output
    """
    output_file = tmp_path / "flink_ds_chain_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "flink-ds-chain-test",
            "source": {
                "type": "flink",
                "config": {
                    "connection": {
                        "rest_api_url": FLINK_REST_URL,
                        "sql_gateway_url": SQL_GATEWAY_URL,
                    },
                    "include_lineage": True,
                    "include_run_history": False,
                    "env": "TEST",
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(output_file)},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mces = json.loads(output_file.read_text())

    def get_lineage(job_name: str) -> Dict[str, Any]:
        for mce in mces:
            if mce.get("aspectName") == "dataJobInputOutput" and job_name in mce.get(
                "entityUrn", ""
            ):
                return mce["aspect"]["json"]  # type: ignore[no-any-return]
        return {}

    hop1 = get_lineage("test_ds_kafka_hop1")
    assert hop1, "test_ds_kafka_hop1 lineage not found in output"
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,orders,TEST)"
        in hop1["inputDatasets"]
    )
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,enriched-orders,TEST)"
        in hop1["outputDatasets"]
    ), (
        "test_ds_kafka_hop1 sink 'enriched-orders' missing — likely KAFKA_DATASTREAM_SINK regex bug"
    )

    hop2 = get_lineage("test_ds_kafka_hop2")
    assert hop2, "test_ds_kafka_hop2 lineage not found in output"
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,enriched-orders,TEST)"
        in hop2["inputDatasets"]
    )
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,final-output,TEST)"
        in hop2["outputDatasets"]
    ), (
        "test_ds_kafka_hop2 sink 'final-output' missing — likely KAFKA_DATASTREAM_SINK regex bug"
    )


def test_iceberg_flink_lineage_stitching(
    flink_runner: Any, tmp_path: pathlib.Path
) -> None:
    """End-to-end lineage stitching: Iceberg and Flink connectors run independently
    and produce matching dataset URNs for the same Iceberg table.

    The DataHub Iceberg connector runs against the REST catalog server (backed by MinIO) — not Flink.
    The DataHub Flink connector runs against the Flink REST API and SQL Gateway.
    Both emit urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.events,TEST), so DataHub
    stitches Flink lineage edges with Iceberg schema/metadata into one dataset entity.
    """
    iceberg_output = tmp_path / "iceberg_mces.json"
    flink_output = tmp_path / "flink_stitch_mces.json"

    # Run the DataHub Iceberg connector independently against the REST catalog.
    # The REST catalog server (tabulario/iceberg-rest) backed by MinIO is the
    # same "Iceberg instance" that Flink uses — connectors discover the same
    # tables and emit identical dataset URNs, enabling lineage stitching.
    iceberg_pipeline = Pipeline.create(
        {
            "source": {
                "type": "iceberg",
                "config": {
                    "catalog": {
                        "default": {
                            "type": "rest",
                            "uri": "http://localhost:8181",
                            "s3.access-key-id": "admin",
                            "s3.secret-access-key": "password",
                            "s3.region": "us-east-1",
                            "warehouse": "s3a://warehouse/wh/",
                            "s3.endpoint": "http://localhost:9000",
                        }
                    },
                    "env": "TEST",
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(iceberg_output)},
            },
        }
    )
    iceberg_pipeline.run()
    iceberg_pipeline.raise_from_status()

    iceberg_mces = json.loads(iceberg_output.read_text())
    iceberg_dataset_urns = {
        mce["entityUrn"] for mce in iceberg_mces if mce.get("entityType") == "dataset"
    }

    # Run the DataHub Flink connector independently against Flink REST + SQL Gateway.
    flink_pipeline = Pipeline.create(
        {
            "source": {
                "type": "flink",
                "config": {
                    "connection": {
                        "rest_api_url": FLINK_REST_URL,
                        "sql_gateway_url": SQL_GATEWAY_URL,
                    },
                    "include_lineage": True,
                    "include_run_history": False,
                    "env": "TEST",
                    "catalog_platform_map": {
                        "ice_catalog": {"platform": "iceberg"},
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(flink_output)},
            },
        }
    )
    flink_pipeline.run()
    flink_pipeline.raise_from_status()

    flink_mces = json.loads(flink_output.read_text())
    flink_lineage_urns: set = set()
    for mce in flink_mces:
        if mce.get("aspectName") == "dataJobInputOutput":
            payload = mce["aspect"]["json"]
            flink_lineage_urns.update(payload.get("inputDatasets", []))
            flink_lineage_urns.update(payload.get("outputDatasets", []))

    # URNs that appear in both outputs can be stitched in DataHub.
    stitchable = iceberg_dataset_urns & flink_lineage_urns
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.events,TEST)" in stitchable
    ), (
        f"Lineage stitching broken — 'lake.events' URN not in both outputs.\n"
        f"  Iceberg connector dataset URNs: {sorted(iceberg_dataset_urns)}\n"
        f"  Flink lineage URNs:             {sorted(flink_lineage_urns)}"
    )


@time_machine.travel(FROZEN_TIME)
def test_flink_run_history_and_platform_instances(
    flink_runner: Any, tmp_path: pathlib.Path
) -> None:
    """Complex pipeline: run history, platform instances, job filtering, and multi-source fan-in.

    Jobs under test (filtered to exactly 2 via job_name_pattern):

      test_enrich_orders:
        hive_catalog.flink_db.orders (Kafka)
        → hive_catalog.flink_db.enriched_orders (Kafka)
        Single-source single-sink via HiveCatalog.

      test_pg_users_pipeline:
        pg_catalog.flink_catalog.public.users (Postgres, JDBC 3-level naming)
        UNION ALL user_events (Kafka, temporary table)
        → enriched_user_events (Kafka, temporary table)
        Multi-source fan-in: 2 platforms feeding 1 sink.

    Verifies:
    - job_name_pattern filters out the other 4 jobs → exactly 2 DataJobs emitted
    - platform_instance_map prepends instance prefix to all Kafka and Postgres URNs:
        kafka   → urn:...:dataPlatform:kafka,test-kafka.<topic>,TEST
        postgres → urn:...:dataPlatform:postgres,test-pg.<db>.<schema>.<table>,TEST
    - include_run_history=True emits DataProcessInstance start events for both RUNNING jobs
    - test_pg_users_pipeline lineage has sources from two different platforms (Postgres + Kafka)
    """
    output_file = tmp_path / "flink_complex_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "flink-complex-test",
            "source": {
                "type": "flink",
                "config": {
                    "connection": {
                        "rest_api_url": FLINK_REST_URL,
                        "sql_gateway_url": SQL_GATEWAY_URL,
                    },
                    "include_lineage": True,
                    "include_run_history": True,
                    "env": "TEST",
                    # Allow only the two jobs under test; filter out ds_kafka_hop1/2,
                    # test_final_output, and test_iceberg_events_pipeline.
                    "job_name_pattern": {
                        "allow": [
                            "test_enrich_orders",
                            "test_pg_users_pipeline",
                        ]
                    },
                    # Platform instance mapping: verifies URN construction with instances.
                    "platform_instance_map": {
                        "kafka": "test-kafka",
                        "postgres": "test-pg",
                    },
                    # Flink 1.19 requires explicit platform for Iceberg (DESCRIBE CATALOG unavailable).
                    "catalog_platform_map": {
                        "ice_catalog": {"platform": "iceberg"},
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(output_file)},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mces = json.loads(output_file.read_text())

    # ── 1. Job filtering: exactly 2 DataJobs ──────────────────────────────────
    datajob_urns = [
        mce["entityUrn"]
        for mce in mces
        if mce.get("entityType") == "dataJob" and mce.get("aspectName") == "dataJobInfo"
    ]
    assert len(datajob_urns) == 2, (
        f"Expected 2 DataJobs after job_name_pattern filter, got {len(datajob_urns)}: "
        f"{datajob_urns}"
    )
    assert any("test_enrich_orders" in u for u in datajob_urns), (
        f"test_enrich_orders DataJob missing: {datajob_urns}"
    )
    assert any("test_pg_users_pipeline" in u for u in datajob_urns), (
        f"test_pg_users_pipeline DataJob missing: {datajob_urns}"
    )

    # ── 2. Platform instances: all Kafka/Postgres URNs carry the instance prefix ─
    all_lineage_urns: set = set()
    lineage_by_job: Dict[str, Any] = {}
    for mce in mces:
        if mce.get("aspectName") == "dataJobInputOutput":
            payload = mce["aspect"]["json"]
            all_lineage_urns.update(payload.get("inputDatasets", []))
            all_lineage_urns.update(payload.get("outputDatasets", []))
            # Key by job URN for per-job assertions below.
            lineage_by_job[mce["entityUrn"]] = payload

    kafka_urns = [u for u in all_lineage_urns if "dataPlatform:kafka" in u]
    postgres_urns = [u for u in all_lineage_urns if "dataPlatform:postgres" in u]

    assert kafka_urns, "No Kafka dataset URNs found in lineage output"
    assert postgres_urns, (
        "No Postgres dataset URNs found in lineage output — "
        "test_pg_users_pipeline's JDBC source may not have been resolved"
    )
    for urn in kafka_urns:
        assert "test-kafka." in urn, (
            f"Kafka URN missing 'test-kafka.' instance prefix: {urn}"
        )
    for urn in postgres_urns:
        assert "test-pg." in urn, (
            f"Postgres URN missing 'test-pg.' instance prefix: {urn}"
        )

    # ── 3. Run history: at least 2 DPI start events (one per filtered RUNNING job) ─
    dpi_start_urns = [
        mce["entityUrn"]
        for mce in mces
        if mce.get("entityType") == "dataProcessInstance"
        and mce.get("aspectName") == "dataProcessInstanceRunEvent"
        and mce.get("aspect", {}).get("json", {}).get("status") == "STARTED"
    ]
    assert len(dpi_start_urns) >= 2, (
        f"Expected >=2 DPI STARTED events (one per filtered job), got {len(dpi_start_urns)}: "
        f"{dpi_start_urns}"
    )

    # ── 4. Multi-source fan-in: test_pg_users_pipeline has Postgres + Kafka sources ─
    pg_pipeline_key = next(
        (k for k in lineage_by_job if "test_pg_users_pipeline" in k), None
    )
    assert pg_pipeline_key, (
        f"test_pg_users_pipeline lineage not found. Available jobs: {list(lineage_by_job)}"
    )
    pg_inputs = lineage_by_job[pg_pipeline_key].get("inputDatasets", [])
    pg_input_platforms = {
        urn.split("dataPlatform:")[1].split(",")[0]
        for urn in pg_inputs
        if "dataPlatform:" in urn
    }
    assert "kafka" in pg_input_platforms, (
        f"test_pg_users_pipeline missing Kafka source. Input platforms: {pg_input_platforms}, "
        f"inputs: {pg_inputs}"
    )
    assert "postgres" in pg_input_platforms, (
        f"test_pg_users_pipeline missing Postgres source. Input platforms: {pg_input_platforms}, "
        f"inputs: {pg_inputs}"
    )


def test_connection_failure() -> None:
    report = test_connection_helpers.run_test_connection(
        FlinkSource,
        {"connection": {"rest_api_url": "http://localhost:9999"}, "env": "TEST"},
    )
    test_connection_helpers.assert_basic_connectivity_failure(
        report, "Cannot reach Flink REST API"
    )
