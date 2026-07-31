"""Integration test for the Informatica (IDMC) connector.

The IDMC API is not easily exposed to a local test harness, so this test
builds a stubbed ``InformaticaClient`` with canned fixture data covering:

- projects and folders (Container hierarchy)
- taskflows (DataFlow entities)
- mappings (DataJob entities) with v2 metadata cross-references
- mapping tasks (DataJob entities on per-task DataFlows)
- connections and a lineage export that resolves to dataset URNs

The resulting MCE stream is compared against a checked-in golden file.
"""

import json
from pathlib import Path
from typing import Dict, Iterator, List, Optional
from unittest import mock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.informatica.models import (
    ExportJobState,
    ExportJobStatus,
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
    LineageTable,
    MappingLineageInfo,
    TaskflowDefinition,
    TaskflowStep,
)
from datahub.testing import mce_helpers

FROZEN_TIME = "2026-04-17 12:00:00"


# ------------------------------------------------------------------ fixtures

_PROJECTS: List[IdmcObject] = [
    IdmcObject(
        id="proj-sales",
        name="Sales",
        path="/Explore/Sales",
        object_type="Project",
        description="Sales analytics project",
        created_by="alice@acme.com",
        updated_by="alice@acme.com",
    ),
    IdmcObject(
        id="proj-mktg",
        name="Marketing",
        path="/Explore/Marketing",
        object_type="Project",
        description="Marketing analytics project",
        created_by="bob@acme.com",
        updated_by="bob@acme.com",
    ),
]

_FOLDERS: List[IdmcObject] = [
    IdmcObject(
        id="folder-sales-mappings",
        name="Mappings",
        path="/Explore/Sales/Mappings",
        object_type="Folder",
        updated_by="alice@acme.com",
    ),
    IdmcObject(
        id="folder-sales-taskflows",
        name="Taskflows",
        path="/Explore/Sales/Taskflows",
        object_type="Folder",
        updated_by="alice@acme.com",
    ),
    IdmcObject(
        id="folder-sales-mapping-tasks",
        name="MappingTasks",
        path="/Explore/Sales/MappingTasks",
        object_type="Folder",
        updated_by="alice@acme.com",
    ),
]

_TASKFLOWS: List[IdmcObject] = [
    IdmcObject(
        id="tf-daily-refresh",
        name="daily_refresh",
        path="/Explore/Sales/Taskflows/daily_refresh",
        object_type="TASKFLOW",
        description="Runs sales pipelines nightly",
        # Different creator/updater — exercises the DATAOWNER +
        # TECHNICAL_OWNER split in ``_owner_list``.
        created_by="alice@acme.com",
        updated_by="bob@acme.com",
    ),
]

_MAPPINGS: List[IdmcObject] = [
    IdmcObject(
        id="map-orders-copy",
        name="copy_orders_to_dw",
        path="/Explore/Sales/Mappings/copy_orders_to_dw",
        object_type="DTEMPLATE",
        description="Copy orders from OLTP to DWH",
        created_by="alice@acme.com",
        updated_by="alice@acme.com",
    ),
    IdmcObject(
        id="map-customers-enrich",
        name="enrich_customers",
        path="/Explore/Sales/Mappings/enrich_customers",
        object_type="DTEMPLATE",
        description="Enrich customer master data",
        created_by="alice@acme.com",
        updated_by="alice@acme.com",
    ),
]

_V2_MAPPINGS: List[IdmcMapping] = [
    IdmcMapping(
        v2_id="v2-map-1",
        name="copy_orders_to_dw",
        asset_frs_guid="map-orders-copy",
        valid=True,
    ),
    IdmcMapping(
        v2_id="v2-map-2",
        name="enrich_customers",
        asset_frs_guid="map-customers-enrich",
        valid=True,
    ),
]

_MAPPING_TASKS: List[IdmcMappingTask] = [
    IdmcMappingTask(
        v2_id="mttask-1",
        name="nightly_copy_orders",
        path="/Explore/Sales/MappingTasks/nightly_copy_orders",
        description="Scheduled runner for copy_orders_to_dw",
        mapping_id="v2-map-1",
        mapping_name="copy_orders_to_dw",
        connection_id="conn-snowflake-prod",
        created_by="alice@acme.com",
        updated_by="alice@acme.com",
    ),
    IdmcMappingTask(
        v2_id="mttask-2",
        name="nightly_enrich_customers",
        path="/Explore/Sales/MappingTasks/nightly_enrich_customers",
        description="Scheduled runner for enrich_customers",
        mapping_id="v2-map-2",
        mapping_name="enrich_customers",
        connection_id="conn-snowflake-prod",
        created_by="alice@acme.com",
        updated_by="alice@acme.com",
    ),
]

_CONNECTIONS: List[IdmcConnection] = [
    IdmcConnection(
        id="conn-snowflake-prod",
        name="prod-snowflake",
        conn_type="Snowflake_Cloud_Data_Warehouse",
        federated_id="fed-sf-prod",
        database="DWH",
        db_schema="ANALYTICS",
    ),
    IdmcConnection(
        id="conn-oracle-oltp",
        name="oltp-oracle",
        conn_type="Oracle",
        federated_id="fed-oracle-oltp",
        database="OLTP",
        db_schema="APPS",
    ),
]

_TASKFLOW_DEFINITIONS: Dict[str, TaskflowDefinition] = {
    # 2-step chain (copy → enrich) exercises step-DAG emission +
    # orchestrate DataJob + aggregated-lineage end-to-end.
    "tf-daily-refresh": TaskflowDefinition(
        taskflow_id="tf-daily-refresh",
        taskflow_name="daily_refresh",
        steps=[
            TaskflowStep(
                step_id="step-copy",
                step_name="Copy orders",
                step_type="data",
                task_type="MTT",
                task_ref_id="mttask-1",
                task_ref_name="nightly_copy_orders",
            ),
            TaskflowStep(
                step_id="step-enrich",
                step_name="Enrich customers",
                step_type="data",
                task_type="MTT",
                task_ref_id="mttask-2",
                task_ref_name="nightly_enrich_customers",
                predecessor_step_ids=["step-copy"],
            ),
        ],
    ),
}


_LINEAGE_INFOS: List[MappingLineageInfo] = [
    MappingLineageInfo(
        mapping_id="map-orders-copy",
        mapping_name="copy_orders_to_dw",
        source_tables=[
            LineageTable(
                table_name="ORDERS",
                schema_name="APPS",
                connection_federated_id="fed-oracle-oltp",
            )
        ],
        target_tables=[
            LineageTable(
                table_name="ORDERS",
                schema_name="ANALYTICS",
                connection_federated_id="fed-sf-prod",
            )
        ],
    ),
    MappingLineageInfo(
        mapping_id="map-customers-enrich",
        mapping_name="enrich_customers",
        source_tables=[
            LineageTable(
                table_name="CUSTOMERS",
                schema_name="APPS",
                connection_federated_id="fed-oracle-oltp",
            )
        ],
        target_tables=[
            LineageTable(
                table_name="CUSTOMERS_ENRICHED",
                schema_name="ANALYTICS",
                connection_federated_id="fed-sf-prod",
            )
        ],
    ),
]


def _fake_list_objects(
    object_type: str, tag: Optional[str] = None
) -> Iterator[IdmcObject]:
    table = {
        "Project": _PROJECTS,
        "Folder": _FOLDERS,
        "TASKFLOW": _TASKFLOWS,
        "DTEMPLATE": _MAPPINGS,
    }
    return iter(table.get(object_type, []))


class _StubClient:
    """Minimal InformaticaClient stand-in returning fixture data."""

    def __init__(self, config, report):
        self.config = config
        self.report = report

    def close(self) -> None:
        pass

    def login(self) -> str:
        return "stub-session"

    def list_objects(self, object_type, tag=None):
        return _fake_list_objects(object_type, tag)

    def list_mappings(self):
        return _V2_MAPPINGS

    def list_mapping_tasks(self):
        return _MAPPING_TASKS

    def list_connections(self):
        return _CONNECTIONS

    def get_taskflow_definition(self, taskflow_name="", taskflow_v3_guid=""):
        return _TASKFLOW_DEFINITIONS.get(taskflow_v3_guid)

    def prefetch_taskflow_definitions(self, taskflow_v3_guids):
        # ``get_taskflow_definition`` reads directly from the fixture
        # dict; no cache to warm.
        pass

    def submit_export_job(self, object_ids):
        return "stub-export-job"

    def wait_for_export(self, job_id):
        return ExportJobStatus(job_id=job_id, state=ExportJobState.SUCCESSFUL)

    def download_and_parse_export(self, job_id, submitted_ids):
        return iter(_LINEAGE_INFOS)


def _run_pipeline(
    stub_client_cls: type,
    output_path: Path,
    extra_config: Optional[dict] = None,
) -> Pipeline:
    """Shared pipeline runner for parametrized integration scenarios."""
    config = {
        "login_url": "https://dm-us.informaticacloud.com",
        "username": "test-user",
        "password": "test-password",
        "platform_instance": "idmc_test",
    }
    if extra_config:
        config.update(extra_config)
    with mock.patch(
        "datahub.ingestion.source.informatica.source.InformaticaClient",
        stub_client_cls,
    ):
        pipeline = Pipeline.create(
            {
                "pipeline_name": "test-informatica",
                "source": {"type": "informatica", "config": config},
                "sink": {"type": "file", "config": {"filename": str(output_path)}},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
    return pipeline


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_informatica_ingestion(pytestconfig, tmp_path):
    golden_dir = pytestconfig.rootpath / "tests/integration/informatica/golden"
    golden_path = golden_dir / "informatica_mces_golden.json"
    output_path = tmp_path / "informatica_mces.json"

    _run_pipeline(_StubClient, output_path)

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


class _FailedExportClient(_StubClient):
    """Client stub that reports an unsuccessful export job state."""

    def wait_for_export(self, job_id):
        return ExportJobStatus(
            job_id=job_id, state=ExportJobState.FAILED, message="export job failed"
        )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_preserves_case_when_convert_urns_to_lowercase_disabled(tmp_path):
    """With ``convert_urns_to_lowercase: false`` the emitted dataset URNs
    must use IDMC's original case.

    The default ``True`` (tested in ``test_informatica_ingestion``)
    matches Snowflake/Postgres/BigQuery defaults; this test covers the
    opt-out for orgs that have disabled lowercasing on those sources.
    """
    output_path = tmp_path / "informatica_mces_case_preserved.json"
    _run_pipeline(
        _StubClient,
        output_path,
        extra_config={"convert_urns_to_lowercase": False},
    )
    with open(output_path) as f:
        records = json.load(f)
    dataset_urns = {r["entityUrn"] for r in records if r.get("entityType") == "dataset"}
    # Upstream URNs don't pick up the source's own ``platform_instance`` —
    # that comes from ``connection_to_platform_instance``, unset here.
    expected_uppercase_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:oracle,OLTP.APPS.ORDERS,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,DWH.ANALYTICS.ORDERS,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:oracle,OLTP.APPS.CUSTOMERS,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,DWH.ANALYTICS.CUSTOMERS_ENRICHED,PROD)",
    }
    assert expected_uppercase_urns.issubset(dataset_urns), (
        "convert_urns_to_lowercase=False must preserve IDMC's original "
        f"case in dataset URNs. Emitted: {sorted(dataset_urns)}"
    )
    # Regression guard against the flag being silently ignored.
    lowercased_variants = {
        "urn:li:dataset:(urn:li:dataPlatform:oracle,oltp.apps.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.analytics.orders,PROD)",
    }
    assert not (lowercased_variants & dataset_urns), (
        "convert_urns_to_lowercase=False produced a lowercased URN: "
        f"{lowercased_variants & dataset_urns}"
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_failed_export_emits_entities_without_lineage(tmp_path):
    """When IDMC's export job fails, entity metadata still ingests but no
    lineage MCPs are emitted — and the source report records a warning so
    users aren't left guessing why lineage is missing.
    """
    output_path = tmp_path / "informatica_no_lineage.json"
    pipeline = _run_pipeline(_FailedExportClient, output_path)

    # Entities still emitted (containers + dataflows + datajobs).
    with open(output_path) as f:
        records = json.load(f)
    entity_types = {r.get("entityType") for r in records}
    assert "container" in entity_types
    assert "dataFlow" in entity_types
    assert "dataJob" in entity_types
    # Mapping Tasks still wire the MT→Mapping ``inputDatajobs`` edge (that
    # relationship is independent of export success), but no dataset-level
    # lineage is present — inputDatasets/outputDatasets must both be empty.
    lineage_aspects = [
        r for r in records if r.get("aspectName") == "dataJobInputOutput"
    ]
    for record in lineage_aspects:
        aspect_json = record["aspect"]["json"]
        assert aspect_json.get("inputDatasets") == []
        assert aspect_json.get("outputDatasets") == []
    # Report carries a warning so the failure is discoverable.
    source_report = pipeline.source.get_report()
    assert any(
        w.title and "export" in w.title.lower() for w in source_report.warnings
    ), f"expected an export-related warning, got: {source_report.warnings}"
