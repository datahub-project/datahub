import json
import pathlib
from typing import Any, Dict, Optional

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


def _extract_schema_meta_urns(records: list) -> set:
    """Return dataset URNs that have a schemaMetadata aspect in either format.

    The DataHub file sink serialises MetaChangeProposals (MCPs) in the modern format:
      {"entityType": "dataset", "entityUrn": "...", "aspectName": "schemaMetadata", ...}

    Older SQL-based sources (including the Postgres connector) still emit
    MetaChangeEvents (MCEs) in the classic Pegasus/Avro format:
      {"proposedSnapshot": {"com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot":
          {"urn": "...", "aspects": [{"com.linkedin...SchemaMetadata": {...}}]}}}

    This helper handles both so the stitching tests correctly verify schema ownership
    regardless of which format the source connector uses.
    """
    urns: set = set()
    for record in records:
        # Modern MCP format
        if (
            record.get("entityType") == "dataset"
            and record.get("aspectName") == "schemaMetadata"
        ):
            urns.add(record["entityUrn"])
            continue
        # Legacy MCE format
        snapshot_wrapper = record.get("proposedSnapshot")
        if not snapshot_wrapper:
            continue
        for type_key, snapshot_data in snapshot_wrapper.items():
            if "DatasetSnapshot" not in type_key:
                continue
            urn = snapshot_data.get("urn", "")
            for aspect in snapshot_data.get("aspects", []):
                if any("SchemaMetadata" in k for k in aspect):
                    urns.add(urn)
                    break
    return urns


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
        wait_for_port(docker_services, "test_flink_postgres", 5432, timeout=60)
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


def _run_iceberg(output: pathlib.Path, platform_instance: Optional[str] = None) -> list:
    """Run the Iceberg connector against the local REST catalog; return parsed MCPs."""
    src_config: Dict[str, Any] = {
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
    }
    if platform_instance:
        src_config["platform_instance"] = platform_instance

    pipeline = Pipeline.create(
        {
            "source": {"type": "iceberg", "config": src_config},
            "sink": {"type": "file", "config": {"filename": str(output)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return json.loads(output.read_text())


def _run_flink_for_iceberg(
    output: pathlib.Path,
    platform_instance_map: Optional[Dict[str, str]] = None,
    catalog_platform_instance: Optional[str] = None,
    flink_platform_instance: Optional[str] = None,
) -> list:
    """Run the Flink connector scoped to the iceberg events pipeline; return parsed MCPs.

    Args:
        platform_instance_map: platform-level instance map (e.g. {"iceberg": "test-ice"}).
            Exercises DatasetLineageProviderConfigBase.platform_instance_map (fallback path).
        catalog_platform_instance: if set, injects platform_instance directly into
            catalog_platform_map for ice_catalog. Exercises the catalog-level override path
            in entities.py (takes priority over platform_instance_map).
        flink_platform_instance: global platform_instance for the Flink connector itself.
            Prefixes DataFlow, DataJob, and DataProcessInstance URNs (not dataset URNs).
    """
    catalog_detail: Dict[str, Any] = {"platform": "iceberg"}
    if catalog_platform_instance:
        catalog_detail["platform_instance"] = catalog_platform_instance

    src_config: Dict[str, Any] = {
        "connection": {
            "rest_api_url": FLINK_REST_URL,
            "sql_gateway_url": SQL_GATEWAY_URL,
        },
        "include_lineage": True,
        "include_run_history": False,
        "env": "TEST",
        "job_name_pattern": {"allow": ["test_iceberg_events_pipeline"]},
        "catalog_platform_map": {
            "ice_catalog": catalog_detail,
        },
    }
    if platform_instance_map:
        src_config["platform_instance_map"] = platform_instance_map
    if flink_platform_instance:
        src_config["platform_instance"] = flink_platform_instance

    pipeline = Pipeline.create(
        {
            "source": {"type": "flink", "config": src_config},
            "sink": {"type": "file", "config": {"filename": str(output)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return json.loads(output.read_text())


def _assert_iceberg_stitching(
    iceberg_mces: list,
    flink_mces: list,
    expected_iceberg_urns: set,
    label: str = "",
) -> None:
    """Assert the three stitching properties between Iceberg and Flink outputs."""
    prefix = f"[{label}] " if label else ""

    iceberg_dataset_urns = {
        mce["entityUrn"] for mce in iceberg_mces if mce.get("entityType") == "dataset"
    }
    iceberg_schema_urns = {
        mce["entityUrn"]
        for mce in iceberg_mces
        if mce.get("entityType") == "dataset"
        and mce.get("aspectName") == "schemaMetadata"
    }

    flink_lineage_urns: set = set()
    flink_schema_aspect_urns: set = set()
    for mce in flink_mces:
        if mce.get("aspectName") == "dataJobInputOutput":
            payload = mce["aspect"]["json"]
            flink_lineage_urns.update(payload.get("inputDatasets", []))
            flink_lineage_urns.update(payload.get("outputDatasets", []))
        if (
            mce.get("entityType") == "dataset"
            and mce.get("aspectName") == "schemaMetadata"
        ):
            flink_schema_aspect_urns.add(mce["entityUrn"])

    flink_iceberg_lineage_urns = {
        u for u in flink_lineage_urns if "dataPlatform:iceberg" in u
    }

    # 1. Stitching: all expected URNs appear in both outputs
    stitchable = iceberg_dataset_urns & flink_lineage_urns
    for urn in expected_iceberg_urns:
        assert urn in stitchable, (
            f"{prefix}Lineage stitching broken — '{urn}' not in both connector outputs.\n"
            f"  Iceberg connector dataset URNs: {sorted(iceberg_dataset_urns)}\n"
            f"  Flink lineage URNs:             {sorted(flink_lineage_urns)}"
        )

    # 2. Schema ownership: Flink must not emit schemaMetadata for Iceberg datasets
    spurious = flink_schema_aspect_urns & flink_iceberg_lineage_urns
    assert not spurious, (
        f"{prefix}Flink emitted schemaMetadata for Iceberg dataset(s) — schema "
        f"authority belongs exclusively to the Iceberg connector.\n"
        f"  Affected URNs: {sorted(spurious)}"
    )

    # 3. Matched datasets: every Iceberg URN Flink references exists in Iceberg output
    unmatched = flink_iceberg_lineage_urns - iceberg_dataset_urns
    assert not unmatched, (
        f"{prefix}Flink references Iceberg dataset(s) the Iceberg connector did not emit.\n"
        f"  Unmatched Flink lineage URNs: {sorted(unmatched)}\n"
        f"  Iceberg connector emitted:    {sorted(iceberg_dataset_urns)}"
    )

    # Iceberg connector must emit schemaMetadata for the tables it owns
    assert iceberg_schema_urns, (
        f"{prefix}Iceberg connector emitted no schemaMetadata aspects."
    )


def test_iceberg_flink_lineage_stitching(
    flink_runner: Any, tmp_path: pathlib.Path
) -> None:
    """End-to-end lineage stitching for the iceberg → flink → iceberg flow.

    test_iceberg_events_pipeline reads from ice_catalog.lake.events and writes to
    ice_catalog.lake.results — both Iceberg tables in the REST catalog (backed by MinIO).
    The DataHub Iceberg connector and the DataHub Flink connector both emit the same
    urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.*,TEST) URNs, so DataHub stitches
    Flink lineage edges with Iceberg schema/metadata into a single entity.

    Using an Iceberg sink (instead of a Kafka sink) is intentional: Kafka Sink V2 in
    SQL/Table API mode produces an unresolvable "tableName[N]: Writer" operator name,
    so iceberg → flink → kafka cannot be demonstrated in the SQL/Table API. Full Kafka
    sink stitching is covered by test_kafka_flink_lineage_stitching using DataStream jobs.

    Four scenarios cover all platform-mapping config paths:

    A. No instance — baseline stitching with catalog_platform_map[platform only].
    B. platform_instance_map={"iceberg": "test-ice"} — platform-level fallback path in
       entities.py (DatasetLineageProviderConfigBase).
    C. catalog_platform_map[ice_catalog].platform_instance="test-ice" — catalog-level
       override path in entities.py (takes priority over platform_instance_map). Produces
       identical URNs to B, proving both code paths yield the same result.
    D. platform_instance="flink-prod" — global Flink connector instance. Prefixes
       DataFlow and DataJob URNs (urn:li:dataFlow:(flink,flink-prod.job,TEST)); dataset
       URNs are unaffected, so Scenario A stitching assertions still hold.

    Each scenario asserts the same three properties:
    1. Stitching: lake.events (input) AND lake.results (output) URNs appear in both
       Iceberg and Flink connector outputs.
    2. Schema ownership: Flink does not emit schemaMetadata for Iceberg datasets.
    3. Matched datasets: every Iceberg-platform URN Flink references exists in the
       Iceberg connector output — no phantom datasets.
    """
    # ── Scenario A: no platform_instance ─────────────────────────────────────
    iceberg_mces = _run_iceberg(tmp_path / "iceberg_mces.json")
    flink_mces = _run_flink_for_iceberg(tmp_path / "flink_iceberg_mces.json")

    expected_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.events,TEST)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.results,TEST)",
    }
    _assert_iceberg_stitching(
        iceberg_mces, flink_mces, expected_urns, label="no-instance"
    )

    # ── Scenario B: platform_instance=test-ice ────────────────────────────────
    # Iceberg connector emits urn:li:dataset:(urn:li:dataPlatform:iceberg,test-ice.lake.events,TEST)
    # Flink connector maps ice_catalog → iceberg with platform_instance_map={"iceberg": "test-ice"}
    # so both sides agree on the same URN.
    iceberg_instance_mces = _run_iceberg(
        tmp_path / "iceberg_instance_mces.json", platform_instance="test-ice"
    )
    flink_instance_mces = _run_flink_for_iceberg(
        tmp_path / "flink_iceberg_instance_mces.json",
        platform_instance_map={"iceberg": "test-ice"},
    )

    expected_instance_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,test-ice.lake.events,TEST)",
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,test-ice.lake.results,TEST)",
    }
    _assert_iceberg_stitching(
        iceberg_instance_mces,
        flink_instance_mces,
        expected_instance_urns,
        label="platform_instance_map=test-ice",
    )

    # ── Scenario C: catalog_platform_map[ice_catalog].platform_instance ──────
    # Uses the catalog-level platform_instance override (entities.py lines 39-41),
    # which takes priority over platform_instance_map. Produces identical URNs to
    # Scenario B, proving both code paths reach the same result.
    flink_catalog_instance_mces = _run_flink_for_iceberg(
        tmp_path / "flink_iceberg_catalog_instance_mces.json",
        catalog_platform_instance="test-ice",
    )
    _assert_iceberg_stitching(
        iceberg_instance_mces,
        flink_catalog_instance_mces,
        expected_instance_urns,
        label="catalog_platform_map.platform_instance=test-ice",
    )

    # ── Scenario D: global platform_instance for Flink connector itself ───────
    # platform_instance="flink-prod" prefixes DataFlow and DataJob URNs
    # (e.g. urn:li:dataFlow:(flink,flink-prod.test_iceberg_events_pipeline,TEST)).
    # Dataset URNs are unaffected — stitching with the plain Iceberg output still holds.
    flink_global_instance_mces = _run_flink_for_iceberg(
        tmp_path / "flink_iceberg_global_instance_mces.json",
        flink_platform_instance="flink-prod",
    )
    datajob_urns = [
        mce["entityUrn"]
        for mce in flink_global_instance_mces
        if mce.get("entityType") == "dataJob" and mce.get("aspectName") == "dataJobInfo"
    ]
    assert datajob_urns, "No DataJob URNs emitted with platform_instance=flink-prod"
    assert all("flink-prod" in u for u in datajob_urns), (
        f"Global platform_instance 'flink-prod' not reflected in DataJob URNs: {datajob_urns}"
    )
    # Dataset URNs are unaffected by the global platform_instance — stitching still works.
    _assert_iceberg_stitching(
        iceberg_mces,
        flink_global_instance_mces,
        expected_urns,
        label="global platform_instance=flink-prod",
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
                    # test_kafka_to_pg, test_kafka_to_iceberg, and test_iceberg_events_pipeline.
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


def test_postgres_flink_lineage_stitching(
    flink_runner: Any, tmp_path: pathlib.Path
) -> None:
    """End-to-end lineage stitching: Postgres and Flink connectors run independently
    and produce matching dataset URNs for the same Postgres table.

    The DataHub Postgres connector runs against the postgres container (localhost:5433).
    The DataHub Flink connector runs against the Flink REST API and SQL Gateway and
    references the same table via the JDBC catalog (pg_catalog.flink_catalog.public.users).

    Mirrors the three properties verified in test_iceberg_flink_lineage_stitching:
    1. Stitching: both connectors emit the same dataset URN for public.users.
    2. Schema ownership: Flink does not emit schemaMetadata for Postgres datasets — schema
       authority belongs exclusively to the Postgres connector.
    3. Matched datasets only: every Postgres-platform dataset URN that Flink references also
       exists in the Postgres connector's output — no phantom datasets.
    Plus: a platform_instance variant that confirms stitching survives instance prefixes.

    Job under test: test_pg_users_pipeline
      Sources: pg_catalog.flink_catalog.public.users (Postgres, JDBC 3-level naming)
               hive_catalog.flink_db.user_events     (Kafka via HiveCatalog)
      Sink:    hive_catalog.flink_db.enriched_user_events (Kafka via HiveCatalog)
    """
    pg_output = tmp_path / "postgres_mces.json"
    flink_output = tmp_path / "flink_pg_stitch_mces.json"
    pg_instance_output = tmp_path / "postgres_instance_mces.json"
    flink_instance_output = tmp_path / "flink_pg_instance_stitch_mces.json"

    POSTGRES_USERS_URN = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,flink_catalog.public.users,TEST)"
    )
    POSTGRES_USERS_INSTANCE_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,test-pg.flink_catalog.public.users,TEST)"

    def _run_pg(output_file: pathlib.Path, platform_instance: str = "") -> list:
        config: Dict[str, Any] = {
            "host_port": "localhost:5433",
            "database": "flink_catalog",
            "username": "flink",
            "password": "flink",
            "env": "TEST",
            "include_tables": True,
        }
        if platform_instance:
            config["platform_instance"] = platform_instance
        p = Pipeline.create(
            {
                "source": {"type": "postgres", "config": config},
                "sink": {"type": "file", "config": {"filename": str(output_file)}},
            }
        )
        p.run()
        p.raise_from_status()
        return json.loads(output_file.read_text())  # type: ignore[return-value]

    def _run_flink(
        output_file: pathlib.Path, platform_instance_map: Optional[Dict] = None
    ) -> list:
        config: Dict[str, Any] = {
            "connection": {
                "rest_api_url": FLINK_REST_URL,
                "sql_gateway_url": SQL_GATEWAY_URL,
            },
            "include_lineage": True,
            "include_run_history": False,
            "env": "TEST",
            "catalog_platform_map": {
                "pg_catalog": {"platform": "postgres"},
                "ice_catalog": {"platform": "iceberg"},
            },
            "job_name_pattern": {"allow": ["test_pg_users_pipeline"]},
        }
        if platform_instance_map:
            config["platform_instance_map"] = platform_instance_map
        p = Pipeline.create(
            {
                "source": {"type": "flink", "config": config},
                "sink": {"type": "file", "config": {"filename": str(output_file)}},
            }
        )
        p.run()
        p.raise_from_status()
        return json.loads(output_file.read_text())  # type: ignore[return-value]

    def _assert_stitching(
        pg_mces: list,
        flink_mces: list,
        expected_users_urn: str,
        label: str,
    ) -> None:
        pg_dataset_urns = {
            mce["entityUrn"] for mce in pg_mces if mce.get("entityType") == "dataset"
        }
        pg_schema_urns = _extract_schema_meta_urns(pg_mces)

        flink_lineage_urns: set = set()
        flink_schema_aspect_urns: set = set()
        for mce in flink_mces:
            if mce.get("aspectName") == "dataJobInputOutput":
                payload = mce["aspect"]["json"]
                flink_lineage_urns.update(payload.get("inputDatasets", []))
                flink_lineage_urns.update(payload.get("outputDatasets", []))
            if (
                mce.get("entityType") == "dataset"
                and mce.get("aspectName") == "schemaMetadata"
            ):
                flink_schema_aspect_urns.add(mce["entityUrn"])

        flink_postgres_lineage_urns = {
            u for u in flink_lineage_urns if "dataPlatform:postgres" in u
        }

        # Postgres connector must emit schemaMetadata for the tables it owns.
        # The DataHub Postgres source emits schemaMetadata in the legacy MCE format
        # (proposedSnapshot/DatasetSnapshot), so _extract_schema_meta_urns handles both
        # the modern MCP and legacy MCE formats transparently.
        assert pg_schema_urns, (
            f"[{label}] Postgres connector emitted no schemaMetadata — "
            "expected schema for at least one table."
        )

        # ── 1. Stitching ──────────────────────────────────────────────────────
        stitchable = pg_dataset_urns & flink_lineage_urns
        assert expected_users_urn in stitchable, (
            f"[{label}] Lineage stitching broken — 'public.users' URN not in both outputs.\n"
            f"  Postgres connector dataset URNs: {sorted(pg_dataset_urns)}\n"
            f"  Flink lineage URNs:              {sorted(flink_lineage_urns)}"
        )

        # ── 2. Schema ownership ───────────────────────────────────────────────
        spurious = flink_schema_aspect_urns & flink_postgres_lineage_urns
        assert not spurious, (
            f"[{label}] Flink emitted schemaMetadata for Postgres dataset(s) — schema should "
            f"come only from the Postgres source connector.\n"
            f"  Affected URNs: {sorted(spurious)}"
        )

        # ── 3. Matched datasets only ──────────────────────────────────────────
        unmatched = flink_postgres_lineage_urns - pg_dataset_urns
        assert not unmatched, (
            f"[{label}] Flink references Postgres dataset(s) that the Postgres connector did not "
            f"emit — lineage stitching will be incomplete for these URNs.\n"
            f"  Unmatched Flink lineage URNs:    {sorted(unmatched)}\n"
            f"  Postgres connector emitted:      {sorted(pg_dataset_urns)}"
        )

    pg_mces = _run_pg(pg_output)
    flink_mces = _run_flink(flink_output)
    _assert_stitching(pg_mces, flink_mces, POSTGRES_USERS_URN, "no platform_instance")

    pg_instance_mces = _run_pg(pg_instance_output, platform_instance="test-pg")
    flink_instance_mces = _run_flink(
        flink_instance_output, platform_instance_map={"postgres": "test-pg"}
    )
    _assert_stitching(
        pg_instance_mces,
        flink_instance_mces,
        POSTGRES_USERS_INSTANCE_URN,
        "platform_instance=test-pg",
    )


def test_kafka_flink_lineage_stitching(
    flink_runner: Any, tmp_path: pathlib.Path
) -> None:
    """End-to-end lineage stitching: Kafka and Flink connectors run independently
    and produce matching dataset URNs for the same Kafka topics.

    The DataHub Kafka connector runs against the test broker (localhost:29092).
    The DataHub Flink connector runs against Flink REST and extracts Kafka lineage from
    DataStream jobs using the KafkaSource-{topic} / KafkaSink-{topic} plan patterns.

    Mirrors the three properties from test_iceberg_flink_lineage_stitching:
    1. Stitching: Kafka topics appear as dataset URNs in both connector outputs.
    2. Schema ownership: Flink does not emit schemaMetadata for Kafka datasets — schema
       authority belongs exclusively to the Kafka connector (Schema Registry-backed).
    3. Matched datasets only: every Kafka-platform dataset URN that Flink references also
       exists in the Kafka connector's output — no phantom topics.
    Plus: a platform_instance variant confirming stitching survives instance prefixes.

    Jobs under test: test_ds_kafka_hop1 (orders → enriched-orders, DataStream API)
                     test_ds_kafka_hop2 (enriched-orders → final-output, DataStream API)

    NOTE: test_enrich_orders (SQL/Table API, Kafka sink) is intentionally excluded because
    Kafka Sink V2 emits unresolvable sink patterns (tableName[N]: Writer), leaving
    outputDatasets empty. DataStream API jobs (test_ds_kafka_hop1/2) use the
    KafkaSource-{topic} / KafkaSink-{topic} plan patterns which resolve correctly for both
    source and sink. test_kafka_to_pg demonstrates SQL Table API with a fully-resolved
    non-Kafka output (Postgres sink via JDBC catalog).
    """
    kafka_output = tmp_path / "kafka_mces.json"
    flink_output = tmp_path / "flink_kafka_stitch_mces.json"
    kafka_instance_output = tmp_path / "kafka_instance_mces.json"
    flink_instance_output = tmp_path / "flink_kafka_instance_stitch_mces.json"

    def _run_kafka(output_file: pathlib.Path, platform_instance: str = "") -> list:
        config: Dict[str, Any] = {
            "connection": {"bootstrap": "localhost:29092"},
            "env": "TEST",
        }
        if platform_instance:
            config["platform_instance"] = platform_instance
        p = Pipeline.create(
            {
                "source": {"type": "kafka", "config": config},
                "sink": {"type": "file", "config": {"filename": str(output_file)}},
            }
        )
        p.run()
        p.raise_from_status()
        return json.loads(output_file.read_text())  # type: ignore[return-value]

    def _run_flink(
        output_file: pathlib.Path,
        platform_instance_map: Optional[Dict] = None,
        job_names: Optional[list] = None,
    ) -> list:
        config: Dict[str, Any] = {
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
            "job_name_pattern": {
                "allow": job_names or ["test_ds_kafka_hop1", "test_ds_kafka_hop2"]
            },
        }
        if platform_instance_map:
            config["platform_instance_map"] = platform_instance_map
        p = Pipeline.create(
            {
                "source": {"type": "flink", "config": config},
                "sink": {"type": "file", "config": {"filename": str(output_file)}},
            }
        )
        p.run()
        p.raise_from_status()
        return json.loads(output_file.read_text())  # type: ignore[return-value]

    def _assert_stitching(
        kafka_mces: list,
        flink_mces: list,
        expected_topic_urns: set,
        label: str,
    ) -> None:
        kafka_dataset_urns = {
            mce["entityUrn"] for mce in kafka_mces if mce.get("entityType") == "dataset"
        }

        flink_lineage_urns: set = set()
        flink_schema_aspect_urns: set = set()
        for mce in flink_mces:
            if mce.get("aspectName") == "dataJobInputOutput":
                payload = mce["aspect"]["json"]
                flink_lineage_urns.update(payload.get("inputDatasets", []))
                flink_lineage_urns.update(payload.get("outputDatasets", []))
            if (
                mce.get("entityType") == "dataset"
                and mce.get("aspectName") == "schemaMetadata"
            ):
                flink_schema_aspect_urns.add(mce["entityUrn"])

        flink_kafka_lineage_urns = {
            u for u in flink_lineage_urns if "dataPlatform:kafka" in u
        }

        # The Kafka connector must discover the test topics (datasetKey is always
        # emitted for discovered topics). schemaMetadata is not checked here because
        # the test environment has no Schema Registry — schema emission requires SR
        # integration and is tested separately in the Iceberg stitching test.
        assert kafka_dataset_urns, (
            f"[{label}] Kafka connector discovered no topics — "
            "expected at least one topic to be emitted."
        )

        # ── 1. Stitching ──────────────────────────────────────────────────────
        stitchable = kafka_dataset_urns & flink_lineage_urns
        missing = expected_topic_urns - stitchable
        assert not missing, (
            f"[{label}] Lineage stitching broken — expected topic URNs not in both outputs.\n"
            f"  Missing URNs:               {sorted(missing)}\n"
            f"  Kafka connector URNs:       {sorted(kafka_dataset_urns)}\n"
            f"  Flink lineage URNs:         {sorted(flink_lineage_urns)}"
        )

        # ── 2. Schema ownership ───────────────────────────────────────────────
        spurious = flink_schema_aspect_urns & flink_kafka_lineage_urns
        assert not spurious, (
            f"[{label}] Flink emitted schemaMetadata for Kafka dataset(s) — schema should "
            f"come only from the Kafka source connector.\n"
            f"  Affected URNs: {sorted(spurious)}"
        )

        # ── 3. Matched datasets only ──────────────────────────────────────────
        unmatched = flink_kafka_lineage_urns - kafka_dataset_urns
        assert not unmatched, (
            f"[{label}] Flink references Kafka topic(s) that the Kafka connector did not "
            f"emit — lineage stitching will be incomplete for these URNs.\n"
            f"  Unmatched Flink lineage URNs: {sorted(unmatched)}\n"
            f"  Kafka connector emitted:      {sorted(kafka_dataset_urns)}"
        )

    kafka_mces = _run_kafka(kafka_output)
    flink_mces = _run_flink(flink_output)
    _assert_stitching(
        kafka_mces,
        flink_mces,
        expected_topic_urns={
            "urn:li:dataset:(urn:li:dataPlatform:kafka,orders,TEST)",
            "urn:li:dataset:(urn:li:dataPlatform:kafka,enriched-orders,TEST)",
            "urn:li:dataset:(urn:li:dataPlatform:kafka,final-output,TEST)",
        },
        label="no platform_instance",
    )

    kafka_instance_mces = _run_kafka(
        kafka_instance_output, platform_instance="test-kafka"
    )
    flink_instance_mces = _run_flink(
        flink_instance_output, platform_instance_map={"kafka": "test-kafka"}
    )
    _assert_stitching(
        kafka_instance_mces,
        flink_instance_mces,
        expected_topic_urns={
            "urn:li:dataset:(urn:li:dataPlatform:kafka,test-kafka.orders,TEST)",
            "urn:li:dataset:(urn:li:dataPlatform:kafka,test-kafka.enriched-orders,TEST)",
            "urn:li:dataset:(urn:li:dataPlatform:kafka,test-kafka.final-output,TEST)",
        },
        label="platform_instance=test-kafka",
    )

    # ── Scenario B: SQL/Table API via HiveCatalog (Tier 2 SHOW CREATE TABLE) ──
    # test_enrich_orders reads orders via HiveCatalog: TableSourceScan([[hive_catalog,
    # flink_db, orders]]) → SHOW CREATE TABLE → connector=kafka, topic=orders → kafka:orders.
    # This exercises PlatformResolver._resolve_from_table_ddl, a completely different
    # code path from DataStream (regex on operator names). Only the INPUT is stitchable;
    # the Kafka Sink V2 output (enriched-orders) is unresolvable.
    flink_hive_output = tmp_path / "flink_kafka_hive_stitch_mces.json"
    flink_hive_mces = _run_flink(
        flink_hive_output,
        job_names=["test_enrich_orders"],
    )
    _assert_stitching(
        kafka_mces,
        flink_hive_mces,
        expected_topic_urns={
            "urn:li:dataset:(urn:li:dataPlatform:kafka,orders,TEST)",
        },
        label="sql-table-api-hive-source",
    )

    flink_hive_instance_output = tmp_path / "flink_kafka_hive_instance_stitch_mces.json"
    flink_hive_instance_mces = _run_flink(
        flink_hive_instance_output,
        platform_instance_map={"kafka": "test-kafka"},
        job_names=["test_enrich_orders"],
    )
    _assert_stitching(
        kafka_instance_mces,
        flink_hive_instance_mces,
        expected_topic_urns={
            "urn:li:dataset:(urn:li:dataPlatform:kafka,test-kafka.orders,TEST)",
        },
        label="sql-table-api-hive-source platform_instance=test-kafka",
    )


def _run_kafka_connector(
    output: pathlib.Path, platform_instance: Optional[str] = None
) -> list:
    """Run the Kafka connector against the local broker; return parsed MCPs."""
    config: Dict[str, Any] = {
        "connection": {"bootstrap": "localhost:29092"},
        "env": "TEST",
    }
    if platform_instance:
        config["platform_instance"] = platform_instance
    pipeline = Pipeline.create(
        {
            "source": {"type": "kafka", "config": config},
            "sink": {"type": "file", "config": {"filename": str(output)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return json.loads(output.read_text())


def _run_flink_for_kafka_iceberg(
    output: pathlib.Path,
    platform_instance_map: Optional[Dict[str, str]] = None,
    catalog_platform_instance: Optional[str] = None,
) -> list:
    """Run the Flink connector scoped to test_kafka_to_iceberg; return parsed MCPs."""
    catalog_detail: Dict[str, Any] = {"platform": "iceberg"}
    if catalog_platform_instance:
        catalog_detail["platform_instance"] = catalog_platform_instance

    config: Dict[str, Any] = {
        "connection": {
            "rest_api_url": FLINK_REST_URL,
            "sql_gateway_url": SQL_GATEWAY_URL,
        },
        "include_lineage": True,
        "include_run_history": False,
        "env": "TEST",
        "job_name_pattern": {"allow": ["test_kafka_to_iceberg"]},
        "catalog_platform_map": {"ice_catalog": catalog_detail},
    }
    if platform_instance_map:
        config["platform_instance_map"] = platform_instance_map

    pipeline = Pipeline.create(
        {
            "source": {"type": "flink", "config": config},
            "sink": {"type": "file", "config": {"filename": str(output)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return json.loads(output.read_text())


def test_kafka_flink_iceberg_stitching(
    flink_runner: Any, tmp_path: pathlib.Path
) -> None:
    """End-to-end stitching for the kafka → flink → iceberg flow.

    test_kafka_to_iceberg reads user_events (HiveCatalog Kafka table, topic=user-events)
    and writes to ice_catalog.lake.kafka_events (Iceberg REST catalog). This is the
    canonical lakehouse ingestion pattern: Kafka data flowing into an Iceberg table.

    Both endpoints are fully resolvable:
    - Input:  SqlTableExtractor Tier 2 SHOW CREATE TABLE → connector=kafka, topic=user-events
    - Output: SqlTableExtractor Tier 1 catalog_platform_map → iceberg:lake.kafka_events

    Two scenarios cover platform_instance mapping for both sides simultaneously:

    A. No instance — baseline stitching.
    B. platform_instance_map={"kafka": "test-kafka"} + catalog_platform_map.platform_instance
       ="test-ice" + Kafka connector platform_instance="test-kafka" + Iceberg connector
       platform_instance="test-ice". Both sides use instances simultaneously, covering the
       most realistic multi-tenant deployment scenario.
    """

    def _collect_flink_lineage(mces: list) -> tuple:
        """Return (all_lineage_urns, flink_schema_urns) from Flink MCPs."""
        lineage_urns: set = set()
        schema_urns: set = set()
        for mce in mces:
            if mce.get("aspectName") == "dataJobInputOutput":
                payload = mce["aspect"]["json"]
                lineage_urns.update(payload.get("inputDatasets", []))
                lineage_urns.update(payload.get("outputDatasets", []))
            if (
                mce.get("entityType") == "dataset"
                and mce.get("aspectName") == "schemaMetadata"
            ):
                schema_urns.add(mce["entityUrn"])
        return lineage_urns, schema_urns

    def _assert_kafka_iceberg_stitching(
        kafka_mces: list,
        iceberg_mces: list,
        flink_mces: list,
        expected_kafka_urn: str,
        expected_iceberg_urn: str,
        label: str = "",
    ) -> None:
        prefix = f"[{label}] " if label else ""
        kafka_dataset_urns = {
            mce["entityUrn"] for mce in kafka_mces if mce.get("entityType") == "dataset"
        }
        iceberg_dataset_urns = {
            mce["entityUrn"]
            for mce in iceberg_mces
            if mce.get("entityType") == "dataset"
        }
        flink_lineage_urns, flink_schema_urns = _collect_flink_lineage(flink_mces)

        # 1. Input stitching: Kafka topic URN in both Kafka connector and Flink inputs
        assert expected_kafka_urn in kafka_dataset_urns, (
            f"{prefix}Kafka connector did not emit {expected_kafka_urn}"
        )
        assert expected_kafka_urn in flink_lineage_urns, (
            f"{prefix}Flink did not reference {expected_kafka_urn} as an input — "
            f"HiveCatalog SHOW CREATE TABLE resolution may have failed.\n"
            f"  Flink lineage URNs: {sorted(flink_lineage_urns)}"
        )

        # 2. Output stitching: Iceberg table URN in both Iceberg connector and Flink outputs
        assert expected_iceberg_urn in iceberg_dataset_urns, (
            f"{prefix}Iceberg connector did not emit {expected_iceberg_urn}"
        )
        assert expected_iceberg_urn in flink_lineage_urns, (
            f"{prefix}Flink did not reference {expected_iceberg_urn} as an output — "
            f"catalog_platform_map Iceberg sink resolution may have failed.\n"
            f"  Flink lineage URNs: {sorted(flink_lineage_urns)}"
        )

        # 3. Schema ownership: Flink must not emit schemaMetadata for Kafka or Iceberg datasets
        flink_kafka_iceberg_urns = {
            u
            for u in flink_lineage_urns
            if "dataPlatform:kafka" in u or "dataPlatform:iceberg" in u
        }
        spurious = flink_schema_urns & flink_kafka_iceberg_urns
        assert not spurious, (
            f"{prefix}Flink emitted schemaMetadata for source/sink datasets — "
            f"schema authority belongs to the authoritative source connectors.\n"
            f"  Affected URNs: {sorted(spurious)}"
        )

        # 4. No phantom datasets: every Kafka/Iceberg URN Flink references exists in
        #    the respective connector output
        unmatched_kafka = {
            u for u in flink_lineage_urns if "dataPlatform:kafka" in u
        } - kafka_dataset_urns
        unmatched_iceberg = {
            u for u in flink_lineage_urns if "dataPlatform:iceberg" in u
        } - iceberg_dataset_urns
        assert not unmatched_kafka, (
            f"{prefix}Flink references Kafka dataset(s) the Kafka connector did not emit: {sorted(unmatched_kafka)}"
        )
        assert not unmatched_iceberg, (
            f"{prefix}Flink references Iceberg dataset(s) the Iceberg connector did not emit: {sorted(unmatched_iceberg)}"
        )

    # ── Scenario A: no platform_instance ─────────────────────────────────────
    kafka_mces = _run_kafka_connector(tmp_path / "ki_kafka_mces.json")
    iceberg_mces = _run_iceberg(tmp_path / "ki_iceberg_mces.json")
    flink_mces = _run_flink_for_kafka_iceberg(tmp_path / "ki_flink_mces.json")

    _assert_kafka_iceberg_stitching(
        kafka_mces,
        iceberg_mces,
        flink_mces,
        expected_kafka_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,user-events,TEST)",
        expected_iceberg_urn="urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.kafka_events,TEST)",
        label="no-instance",
    )

    # ── Scenario B: platform_instance on both sides ───────────────────────────
    # Kafka connector: platform_instance="test-kafka" → urn prefix test-kafka.
    # Iceberg connector: platform_instance="test-ice" → urn prefix test-ice.
    # Flink connector: platform_instance_map={"kafka": "test-kafka"} for Kafka inputs,
    #   catalog_platform_map[ice_catalog].platform_instance="test-ice" for Iceberg output.
    # Both sides use instances simultaneously — the most realistic multi-tenant scenario.
    kafka_instance_mces = _run_kafka_connector(
        tmp_path / "ki_kafka_instance_mces.json", platform_instance="test-kafka"
    )
    iceberg_instance_mces = _run_iceberg(
        tmp_path / "ki_iceberg_instance_mces.json", platform_instance="test-ice"
    )
    flink_instance_mces = _run_flink_for_kafka_iceberg(
        tmp_path / "ki_flink_instance_mces.json",
        platform_instance_map={"kafka": "test-kafka"},
        catalog_platform_instance="test-ice",
    )

    _assert_kafka_iceberg_stitching(
        kafka_instance_mces,
        iceberg_instance_mces,
        flink_instance_mces,
        expected_kafka_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,test-kafka.user-events,TEST)",
        expected_iceberg_urn="urn:li:dataset:(urn:li:dataPlatform:iceberg,test-ice.lake.kafka_events,TEST)",
        label="kafka=test-kafka ice=test-ice",
    )


def test_connection_failure() -> None:
    report = test_connection_helpers.run_test_connection(
        FlinkSource,
        {"connection": {"rest_api_url": "http://localhost:9999"}, "env": "TEST"},
    )
    test_connection_helpers.assert_basic_connectivity_failure(
        report, "Cannot reach Flink REST API"
    )
