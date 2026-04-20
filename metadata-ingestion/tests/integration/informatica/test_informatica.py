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
        schema="ANALYTICS",
    ),
    IdmcConnection(
        id="conn-oracle-oltp",
        name="oltp-oracle",
        conn_type="Oracle",
        federated_id="fed-oracle-oltp",
        database="OLTP",
        schema="APPS",
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


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_informatica_ingestion(pytestconfig, tmp_path):
    golden_dir = pytestconfig.rootpath / "tests/integration/informatica/golden"
    golden_path = golden_dir / "informatica_mces_golden.json"
    output_path = tmp_path / "informatica_mces.json"

    with mock.patch(
        "datahub.ingestion.source.informatica.source.InformaticaClient",
        _StubClient,
    ):
        pipeline = Pipeline.create(
            {
                "pipeline_name": "test-informatica",
                "source": {
                    "type": "informatica",
                    "config": {
                        "login_url": "https://dm-us.informaticacloud.com",
                        "username": "test-user",
                        "password": "test-password",
                        "platform_instance": "idmc_test",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )
