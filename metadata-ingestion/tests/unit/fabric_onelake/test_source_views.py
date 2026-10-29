"""FabricOneLakeSource view processing and the per-item SQL endpoint plumbing,
driven through a stubbed schema client so no ODBC driver is needed."""

from typing import Dict, List, Optional, Set, Tuple
from unittest.mock import NonCallableMagicMock, create_autospec

from datahub.ingestion.source.fabric.onelake.models import FabricColumn, FabricView
from datahub.ingestion.source.fabric.onelake.schema_client import (
    SqlAnalyticsEndpointClient,
)
from datahub.ingestion.source.fabric.onelake.source import (
    FabricOneLakeSource,
    LakehouseKey,
)
from datahub.ingestion.source.fabric.onelake.usage import FabricUsageExtractor
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from tests.unit.conftest import log_titles
from tests.unit.fabric_common.conftest import fabric_response
from tests.unit.fabric_onelake.conftest import LH_ID, WS, OneLakeSourceFactory

_KEY = LakehouseKey(workspace_id=WS.id, lakehouse_id=LH_ID)


def _source(
    make_onelake_source: OneLakeSourceFactory, **recipe: object
) -> Tuple[FabricOneLakeSource, NonCallableMagicMock]:
    """A source over a specced client, with a specced aggregator returned so
    the test can read what views registered."""
    source = make_onelake_source(**recipe)
    aggregator = create_autospec(SqlParsingAggregator, instance=True)
    source.aggregator = aggregator
    return source, aggregator


def _schema_client() -> NonCallableMagicMock:
    return create_autospec(SqlAnalyticsEndpointClient, instance=True)


def _view(schema: str, name: str, definition: Optional[str] = None) -> FabricView:
    return FabricView(
        name=name,
        schema_name=schema,
        item_id=LH_ID,
        workspace_id=WS.id,
        view_definition=definition,
    )


def _process_views(
    source: FabricOneLakeSource,
    schema_client: NonCallableMagicMock,
    schema_map: Optional[Dict[Tuple[str, str], List[FabricColumn]]] = None,
    emitted_schemas: Optional[Set[str]] = None,
) -> Tuple[List[Container], List[Dataset]]:
    emitted = list(
        source._process_item_views(
            WS,
            LH_ID,
            "Lakehouse",
            _KEY,
            schema_client=schema_client,
            schema_map=schema_map or {},
            emitted_schemas=emitted_schemas if emitted_schemas is not None else set(),
        )
    )
    containers = [e for e in emitted if isinstance(e, Container)]
    datasets = [e for e in emitted if isinstance(e, Dataset)]
    return containers, datasets


def _views_client(views: List[FabricView]) -> NonCallableMagicMock:
    schema_client = _schema_client()
    schema_client.get_all_views.return_value = views
    return schema_client


def test_views_get_columns_and_only_defined_views_register_lineage(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    """A view with no readable definition is still emitted (with its columns
    when known) but must not reach the aggregator, which would parse None."""
    source, aggregator = _source(make_onelake_source)
    emitted_schemas: Set[str] = set()
    containers, datasets = _process_views(
        source,
        _views_client(
            [
                _view("dbo", "v_defined", "SELECT id FROM dbo.orders"),
                _view("dbo", "v_opaque"),
            ]
        ),
        schema_map={("dbo", "v_defined"): [FabricColumn("id", "int", False)]},
        emitted_schemas=emitted_schemas,
    )

    assert [c.display_name for c in containers] == ["dbo"]
    assert emitted_schemas == {"dbo"}
    by_name = {d.display_name: d for d in datasets}
    assert set(by_name) == {"v_defined", "v_opaque"}
    assert [f.field_path for f in by_name["v_defined"].schema or []] == ["id"]

    aggregator.add_view_definition.assert_called_once()
    call = aggregator.add_view_definition.call_args.kwargs
    assert call["view_urn"] == str(by_name["v_defined"].urn)
    assert call["default_db"] == f"{WS.id}.{LH_ID}"
    assert list(source.report.views_missing_definition) == ["dbo.v_opaque"]


def test_a_schema_container_already_emitted_for_tables_is_not_repeated(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    source, _ = _source(make_onelake_source)
    containers, datasets = _process_views(
        source, _views_client([_view("dbo", "v")]), emitted_schemas={"dbo"}
    )
    assert containers == []
    assert len(datasets) == 1


def test_views_are_filtered_by_schema_and_view_patterns(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    """schema_pattern sees the bare schema name; view_pattern sees
    `schema.view`."""
    source, _ = _source(
        make_onelake_source,
        schema_pattern={"deny": ["^finance$"]},
        view_pattern={"deny": ["^dbo\\.tmp_.*"]},
    )
    _, datasets = _process_views(
        source,
        _views_client(
            [_view("finance", "v_fin"), _view("dbo", "tmp_v"), _view("dbo", "v_keep")]
        ),
    )
    assert [d.display_name for d in datasets] == ["v_keep"]
    assert list(source.report.filtered_schemas) == ["finance"]
    assert list(source.report.filtered_views) == ["dbo.tmp_v"]
    assert source.report.views_scanned == 1


def test_schemaless_views_are_filed_under_dbo_without_schema_containers(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    source, _ = _source(make_onelake_source, extract_schemas=False)
    containers, datasets = _process_views(source, _views_client([_view("", "v")]))
    assert containers == []
    assert len(datasets) == 1
    assert ".dbo.v" in str(datasets[0].urn)


def test_view_discovery_failure_is_reported_not_raised(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    source, _ = _source(make_onelake_source)
    schema_client = _schema_client()
    schema_client.get_all_views.side_effect = RuntimeError("endpoint down")

    assert _process_views(source, schema_client) == ([], [])
    assert "View Discovery Failed" in log_titles(source.report.warnings)


def test_column_fetch_failure_degrades_to_tables_without_columns(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    source, _ = _source(make_onelake_source)
    schema_client = _schema_client()
    schema_client.get_all_table_columns.side_effect = RuntimeError("denied")

    assert source._fetch_schema_map(schema_client, WS, LH_ID, "Lakehouse") == {}
    assert "Column Metadata Extraction Failed" in log_titles(source.report.warnings)


def test_schema_map_is_not_fetched_when_schema_extraction_is_disabled(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    source, _ = _source(make_onelake_source, extract_schema={"enabled": False})
    schema_client = _schema_client()

    assert source._fetch_schema_map(schema_client, WS, LH_ID, "Lakehouse") == {}
    schema_client.get_all_table_columns.assert_not_called()


def test_no_sql_endpoint_config_means_no_schema_client_and_no_warning(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    """With every SQL-endpoint feature off, the item GET that looks up the
    endpoint must not be made at all (it costs one call per item)."""
    source, _ = _source(
        make_onelake_source,
        sql_endpoint=None,
        extract_views=False,
        extract_schema={"enabled": False},
        usage={"include_usage_statistics": False},
    )
    client = source.client
    assert isinstance(client, NonCallableMagicMock)

    assert source._create_schema_client(WS, LH_ID, "Lakehouse", "lh_main") is None
    client.get.assert_not_called()
    assert not source.report.warnings


def test_an_item_without_a_provisioned_endpoint_is_a_warning(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    source, _ = _source(make_onelake_source, sql_endpoint={"enabled": True})
    client = source.client
    assert isinstance(client, NonCallableMagicMock)
    client.get.return_value = fabric_response(200, {"properties": {}})

    assert source._create_schema_client(WS, LH_ID, "Lakehouse", "lh_main") is None
    assert "SQL Analytics Endpoint Initialization Failed" in log_titles(
        source.report.warnings
    )


def test_usage_is_skipped_for_items_without_an_endpoint(
    make_onelake_source: OneLakeSourceFactory,
) -> None:
    source, _ = _source(make_onelake_source, usage={"include_usage_statistics": True})
    usage_extractor = create_autospec(FabricUsageExtractor, instance=True)
    source.usage_extractor = usage_extractor

    source._extract_item_usage(WS.id, LH_ID, "lh_main", schema_client=None)

    usage_extractor.extract.assert_not_called()
    assert source.report.num_usage_queries_skipped["no_sql_endpoint_for_item"] == 1
