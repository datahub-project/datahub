"""FabricOneLakeSource end to end over the fake OneLake tenant: which
containers and tables one ingestion emits, and how a failing Fabric call
degrades to a warning or a failure instead of aborting the run."""

from typing import Any, List, Tuple, Union
from unittest.mock import create_autospec, patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource
from datahub.metadata.schema_classes import StatusClass
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from tests.unit.conftest import log_titles
from tests.unit.fabric_onelake.conftest import (
    LH_ID,
    REST_ONLY,
    WH_ID,
    WS,
    FakeOneLakeClient,
    OneLakeSourceFactory,
)

Emitted = Union[MetadataWorkUnit, Entity]

# One workspace, one lakehouse, one warehouse: the duplicates the fake serves
# exist for the probe's name-resolution tests.
_ONE_OF_EACH = {
    "workspace_pattern": {"deny": ["^hr-ws$"]},
    "lakehouse_pattern": {"deny": ["^shared_name$"]},
    "warehouse_pattern": {"deny": ["^shared_name$"]},
}


def _ingest(
    make_onelake_source: OneLakeSourceFactory,
    client: FakeOneLakeClient,
    **recipe: Any,
) -> Tuple[FabricOneLakeSource, List[Emitted]]:
    source = make_onelake_source(client, **{**REST_ONLY, **recipe})
    return source, list(source.get_workunits_internal())


def _containers(emitted: List[Emitted]) -> List[Container]:
    return [e for e in emitted if isinstance(e, Container)]


def _datasets(emitted: List[Emitted]) -> List[Dataset]:
    return [e for e in emitted if isinstance(e, Dataset)]


def test_workspace_items_and_tables_are_emitted_under_their_containers(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    client = FakeOneLakeClient()
    source, emitted = _ingest(make_onelake_source, client, **_ONE_OF_EACH)

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


def test_without_schema_containers_tables_sit_directly_under_their_item(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    _, emitted = _ingest(
        make_onelake_source,
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


def test_workspace_listing_failure_is_a_failure(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    """Without the workspace listing nothing can be ingested, so this is the
    one Fabric read whose failure fails the run rather than warning."""
    client = FakeOneLakeClient()
    client.workspaces_error = RuntimeError("401")
    source, emitted = _ingest(make_onelake_source, client)

    assert _containers(emitted) == [] and _datasets(emitted) == []
    assert "Failed to List Workspaces" in log_titles(source.report.failures)


def test_lakehouse_listing_failure_still_processes_warehouses(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    client = FakeOneLakeClient()
    client.lakehouses_error = RuntimeError("403")
    source, emitted = _ingest(
        make_onelake_source, client, workspace_pattern={"deny": ["^hr-ws$"]}
    )

    assert {d.display_name for d in _datasets(emitted)} == {"facts"}
    assert "Failed to List Lakehouses" in log_titles(source.report.warnings)
    assert not source.report.failures


def test_warehouse_listing_failure_is_a_warning(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    client = FakeOneLakeClient()
    with patch.object(client, "list_warehouses", side_effect=RuntimeError("403")):
        source, emitted = _ingest(
            make_onelake_source, client, workspace_pattern={"deny": ["^hr-ws$"]}
        )

    assert "facts" not in {d.display_name for d in _datasets(emitted)}
    assert source.report.lakehouses_scanned == 2
    assert "Failed to List Warehouses" in log_titles(source.report.warnings)
    assert not source.report.failures


def test_table_listing_failure_is_a_warning_for_that_item_only(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    client = FakeOneLakeClient()
    with patch.object(client, "list_lakehouse_tables", side_effect=RuntimeError("500")):
        source, emitted = _ingest(make_onelake_source, client, **_ONE_OF_EACH)

    assert {d.display_name for d in _datasets(emitted)} == {"facts"}
    assert "Failed to Process Tables" in log_titles(source.report.warnings)
    assert not source.report.failures


def test_aggregator_output_is_emitted_after_the_catalog(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    """Lineage and usage are generated once every view definition and query
    has been registered, so they have to come last."""
    source = make_onelake_source(
        FakeOneLakeClient(),
        **REST_ONLY,
        extract_lakehouses=False,
        extract_warehouses=False,
    )
    aggregator = create_autospec(SqlParsingAggregator, instance=True)
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


def test_aggregator_failure_is_a_failure_not_a_crash(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    source = make_onelake_source(
        FakeOneLakeClient(),
        **REST_ONLY,
        extract_lakehouses=False,
        extract_warehouses=False,
    )
    aggregator = create_autospec(SqlParsingAggregator, instance=True)
    aggregator.gen_metadata.side_effect = RuntimeError("parse state corrupt")
    source.aggregator = aggregator

    list(source.get_workunits_internal())

    assert "Failed to Generate Lineage / Usage" in log_titles(source.report.failures)
