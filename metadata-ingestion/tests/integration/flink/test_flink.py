import pathlib
from typing import Any

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
        setup_test_data()
        yield docker_services


@time_machine.travel(FROZEN_TIME)
def test_flink_ingest(
    flink_runner: Any, pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Full ingestion: jobs + catalog + lineage + DPI → golden file."""
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
                    "include_catalog_metadata": True,
                    "catalog_pattern": {"allow": ["^pg_catalog$"]},
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
        golden_path=TEST_RESOURCES_DIR / "flink_mces_golden.json",
        ignore_paths=FLINK_IGNORE_PATHS,
    )


@time_machine.travel(FROZEN_TIME)
def test_flink_ingest_catalog_only(
    flink_runner: Any, pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Catalog-only ingestion: containers + datasets + schemaMetadata → golden file."""
    output_file = tmp_path / "flink_catalog_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "flink-catalog-golden-test",
            "source": {
                "type": "flink",
                "config": {
                    "connection": {
                        "rest_api_url": FLINK_REST_URL,
                        "sql_gateway_url": SQL_GATEWAY_URL,
                    },
                    "include_lineage": False,
                    "include_run_history": False,
                    "include_catalog_metadata": True,
                    "catalog_pattern": {"allow": ["^pg_catalog$"]},
                    "job_name_pattern": {"deny": [".*"]},
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

    # Catalog data is fully deterministic — no dynamic ignore paths needed
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=TEST_RESOURCES_DIR / "flink_catalog_mces_golden.json",
        ignore_paths=[],
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
            SourceCapability.SCHEMA_METADATA,
            SourceCapability.CONTAINERS,
        ],
    )


def test_connection_failure() -> None:
    report = test_connection_helpers.run_test_connection(
        FlinkSource,
        {"connection": {"rest_api_url": "http://localhost:9999"}, "env": "TEST"},
    )
    test_connection_helpers.assert_basic_connectivity_failure(
        report, "Cannot reach Flink REST API"
    )
