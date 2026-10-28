"""FabricOneLakeSource end to end over the fake OneLake tenant: which
containers and tables one ingestion emits, and how a failing Fabric call
degrades to a warning or a failure instead of aborting the run."""

from typing import Any, Dict, List, Tuple, Union, cast
from unittest.mock import MagicMock, patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource
from datahub.metadata.schema_classes import StatusClass
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from tests.unit.fabric_onelake.onelake_fakes import LH_ID, WH_ID, WS, FakeOneLakeClient

Emitted = Union[MetadataWorkUnit, Entity]

# No SQL Analytics Endpoint: tables come from the REST listing alone.
_REST_ONLY: Dict[str, Any] = {
    "sql_endpoint": None,
    "extract_views": False,
    "extract_schema": {"enabled": False},
    "usage": {"include_usage_statistics": False},
}


def _source(client: FakeOneLakeClient, **config: Any) -> FabricOneLakeSource:
    source = FabricOneLakeSource.create(
        {**_REST_ONLY, **config}, PipelineContext(run_id="onelake-wu")
    )
    source.client = cast(OneLakeClient, client)
    return source


def _ingest(
    client: FakeOneLakeClient, **config: Any
) -> Tuple[FabricOneLakeSource, List[Emitted]]:
    source = _source(client, **config)
    return source, list(source.get_workunits_internal())


def _containers(emitted: List[Emitted]) -> List[Container]:
    return [e for e in emitted if isinstance(e, Container)]


def _datasets(emitted: List[Emitted]) -> List[Dataset]:
    return [e for e in emitted if isinstance(e, Dataset)]


def _warning_titles(source: FabricOneLakeSource) -> List[str]:
    return [w.title or "" for w in source.report.warnings]


def test_workspace_items_and_tables_are_emitted_under_their_containers() -> None:
    client = FakeOneLakeClient()
    source, emitted = _ingest(
        client,
        workspace_pattern={"deny": ["^hr-ws$"]},
        lakehouse_pattern={"deny": ["^shared_name$"]},
        warehouse_pattern={"deny": ["^shared_name$"]},
    )

    containers = {c.display_name for c in _containers(emitted)}
    # Workspace, lakehouse, warehouse and one schema container per schema;
    # the schemas-disabled lakehouse table files under dbo.
    assert containers == {WS.name, "lh_main", "wh_main", "dbo", "finance", "staging"}
    datasets = {d.display_name: str(d.urn) for d in _datasets(emitted)}
    assert set(datasets) == {"orders", "refunds", "tmp_load", "facts"}
    assert f"{WS.id}.{LH_ID}.dbo.orders" in datasets["orders"]
    assert f"{WS.id}.{WH_ID}.dbo.facts" in datasets["facts"]

    assert list(source.report.filtered_workspaces) == ["hr-ws"]
    assert list(source.report.filtered_lakehouses) == ["shared_name"]
    assert list(source.report.filtered_warehouses) == ["shared_name"]
    assert source.report.tables_scanned == 4
    assert not source.report.warnings
    assert "lakehouse_tables" in client.calls and "warehouse_tables" in client.calls


def test_without_schema_containers_tables_sit_directly_under_their_item() -> None:
    _, emitted = _ingest(
        FakeOneLakeClient(),
        extract_schemas=False,
        extract_warehouses=False,
        workspace_pattern={"deny": ["^hr-ws$"]},
        lakehouse_pattern={"deny": ["^shared_name$"]},
    )

    assert {c.display_name for c in _containers(emitted)} == {WS.name, "lh_main"}
    lakehouse = next(c for c in _containers(emitted) if c.display_name == "lh_main")
    for dataset in _datasets(emitted):
        assert dataset.parent_container == lakehouse.urn


def test_workspace_listing_failure_is_a_failure() -> None:
    client = FakeOneLakeClient()
    client.workspaces_error = RuntimeError("401")
    source, emitted = _ingest(client)

    assert _containers(emitted) == [] and _datasets(emitted) == []
    assert [f.title for f in source.report.failures] == ["Failed to List Workspaces"]


def test_lakehouse_listing_failure_still_processes_warehouses() -> None:
    client = FakeOneLakeClient()
    client.lakehouses_error = RuntimeError("403")
    source, emitted = _ingest(client, workspace_pattern={"deny": ["^hr-ws$"]})

    assert {d.display_name for d in _datasets(emitted)} == {"facts"}
    assert _warning_titles(source) == ["Failed to List Lakehouses"]


def test_warehouse_listing_failure_is_a_warning() -> None:
    client = FakeOneLakeClient()
    with patch.object(client, "list_warehouses", side_effect=RuntimeError("403")):
        source, emitted = _ingest(client, workspace_pattern={"deny": ["^hr-ws$"]})

    assert "facts" not in {d.display_name for d in _datasets(emitted)}
    assert source.report.lakehouses_scanned == 2
    assert _warning_titles(source) == ["Failed to List Warehouses"]


def test_table_listing_failure_is_a_warning_for_that_item_only() -> None:
    client = FakeOneLakeClient()
    with patch.object(client, "list_lakehouse_tables", side_effect=RuntimeError("500")):
        source, emitted = _ingest(
            client,
            workspace_pattern={"deny": ["^hr-ws$"]},
            lakehouse_pattern={"deny": ["^shared_name$"]},
            warehouse_pattern={"deny": ["^shared_name$"]},
        )

    assert {d.display_name for d in _datasets(emitted)} == {"facts"}
    assert _warning_titles(source) == ["Failed to Process Tables"]


def test_aggregator_output_is_emitted_after_the_catalog() -> None:
    client = FakeOneLakeClient()
    source = _source(client, extract_lakehouses=False, extract_warehouses=False)
    aggregator = MagicMock()
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,x,PROD)",
        aspect=StatusClass(removed=False),
    )
    aggregator.gen_metadata.return_value = [mcp]
    source.aggregator = aggregator

    emitted = list(source.get_workunits_internal())

    assert isinstance(emitted[-1], MetadataWorkUnit)
    assert emitted[-1].metadata is mcp
    assert not source.report.failures


def test_aggregator_failure_is_a_failure_not_a_crash() -> None:
    client = FakeOneLakeClient()
    source = _source(client, extract_lakehouses=False, extract_warehouses=False)
    aggregator = MagicMock()
    aggregator.gen_metadata.side_effect = RuntimeError("parse state corrupt")
    source.aggregator = aggregator

    list(source.get_workunits_internal())

    assert [f.title for f in source.report.failures] == [
        "Failed to Generate Lineage / Usage"
    ]
