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
from typing import Iterator, List, Optional
from unittest import mock

import pytest
from freezegun import freeze_time

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
        created_by="alice@acme.com",
        updated_by="alice@acme.com",
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

    def login(self) -> str:
        return "stub-session"

    def list_objects(self, object_type, tag=None):
        return _fake_list_objects(object_type, tag)

    def list_mappings(self):
        return _V2_MAPPINGS

    def list_mapplets_v2(self):
        # Fixture instance returns mapplets via the v3 MAPPLET path already
        # (there are none here), so the v2 fallback stays a no-op.
        return []

    def list_mapping_tasks(self):
        return _MAPPING_TASKS

    def list_connections(self):
        return _CONNECTIONS

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


@freeze_time(FROZEN_TIME)
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


@freeze_time(FROZEN_TIME)
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
    # No dataJobInputOutput aspect was emitted because the export failed.
    lineage_aspects = [
        r for r in records if r.get("aspectName") == "dataJobInputOutput"
    ]
    assert lineage_aspects == []
    # Report carries a warning so the failure is discoverable.
    source_report = pipeline.source.get_report()
    assert any(
        w.title and "export" in w.title.lower() for w in source_report.warnings
    ), f"expected an export-related warning, got: {source_report.warnings}"


class _V2MappletFallbackClient(_StubClient):
    """Client stub simulating an IDMC pod where v3 MAPPLET returns empty and
    mapplets are only reachable via the v2 /api/v2/mapplet fallback.
    """

    def list_mapplets_v2(self):
        return [
            IdmcObject(
                id="mpl-guid-1",
                name="reusable_logic",
                path="/Explore/Sales/Mapplets/reusable_logic",
                object_type="MAPPLET",
                description="Shared lookup mapplet",
                updated_by="alice@acme.com",
            )
        ]


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_v2_mapplet_fallback_emits_mapplet_when_v3_empty(tmp_path):
    """v3 MAPPLET query returns nothing; the v2 fallback surfaces the mapplet
    and the pipeline emits it as a DataFlow with subtype=Mapplet.
    """
    output_path = tmp_path / "informatica_v2_fallback.json"
    _run_pipeline(_V2MappletFallbackClient, output_path)

    with open(output_path) as f:
        records = json.load(f)
    mapplet_flow_names = [
        r.get("aspect", {}).get("json", {}).get("name")
        for r in records
        if r.get("aspectName") == "dataFlowInfo"
    ]
    assert "reusable_logic" in mapplet_flow_names, (
        f"mapplet from v2 fallback missing; got names: {mapplet_flow_names}"
    )
