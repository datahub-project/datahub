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
        wait_for_port(docker_services, "test_flink_jobmanager", 8081, timeout=120)
        wait_for_port(docker_services, "test_flink_sql_gateway", 8083, timeout=120)
        wait_for_port(docker_services, "test-flink-hive-metastore", 9083, timeout=120)
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

    The DataHub Iceberg connector runs against the Hive Metastore (HMS) — not Flink.
    The DataHub Flink connector runs against the Flink SQL Gateway.
    Both emit urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.events,TEST), so DataHub
    stitches Flink lineage edges with Iceberg schema/metadata into one dataset entity.
    """
    iceberg_output = tmp_path / "iceberg_mces.json"
    flink_output = tmp_path / "flink_stitch_mces.json"

    # Run the DataHub Iceberg connector independently against HMS.
    # warehouse path is /tmp/iceberg-warehouse, bind-mounted from the containers.
    iceberg_pipeline = Pipeline.create(
        {
            "source": {
                "type": "iceberg",
                "config": {
                    "catalog": {
                        "default": {
                            "type": "hive",
                            "uri": "thrift://localhost:9083",
                            "warehouse": "/tmp/iceberg-warehouse",
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


def test_connection_failure() -> None:
    report = test_connection_helpers.run_test_connection(
        FlinkSource,
        {"connection": {"rest_api_url": "http://localhost:9999"}, "env": "TEST"},
    )
    test_connection_helpers.assert_basic_connectivity_failure(
        report, "Cannot reach Flink REST API"
    )
