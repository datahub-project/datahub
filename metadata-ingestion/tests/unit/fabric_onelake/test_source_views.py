"""FabricOneLakeSource view processing and the per-item SQL endpoint plumbing,
driven through a stubbed schema client so no ODBC driver is needed."""

from typing import Dict, List, Optional, Set, Tuple, cast
from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fabric.onelake.models import FabricColumn, FabricView
from datahub.ingestion.source.fabric.onelake.source import (
    FabricOneLakeSource,
    LakehouseKey,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from tests.unit.fabric_onelake.onelake_fakes import LH_ID, WS

_KEY = LakehouseKey(workspace_id=WS.id, lakehouse_id=LH_ID)


def _source(**config: object) -> FabricOneLakeSource:
    source = FabricOneLakeSource.create(dict(config), PipelineContext(run_id="views"))
    source.client = MagicMock()
    source.aggregator = MagicMock()
    return source


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
    views: List[FabricView],
    schema_map: Optional[Dict[Tuple[str, str], List[FabricColumn]]] = None,
    emitted_schemas: Optional[Set[str]] = None,
) -> Tuple[List[Container], List[Dataset]]:
    schema_client = MagicMock()
    schema_client.get_all_views.return_value = views
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


def test_views_get_columns_and_only_defined_views_register_lineage() -> None:
    source = _source()
    emitted_schemas: Set[str] = set()
    containers, datasets = _process_views(
        source,
        [
            _view("dbo", "v_defined", "SELECT id FROM dbo.orders"),
            _view("dbo", "v_opaque"),
        ],
        schema_map={("dbo", "v_defined"): [FabricColumn("id", "int", False)]},
        emitted_schemas=emitted_schemas,
    )

    assert [c.display_name for c in containers] == ["dbo"]
    assert emitted_schemas == {"dbo"}
    by_name = {d.display_name: d for d in datasets}
    assert set(by_name) == {"v_defined", "v_opaque"}
    assert [f.field_path for f in by_name["v_defined"].schema or []] == ["id"]

    cast(MagicMock, source.aggregator).add_view_definition.assert_called_once()
    call = cast(MagicMock, source.aggregator).add_view_definition.call_args.kwargs
    assert call["view_urn"] == str(by_name["v_defined"].urn)
    assert call["default_db"] == f"{WS.id}.{LH_ID}"
    assert list(source.report.views_missing_definition) == ["dbo.v_opaque"]


def test_a_schema_container_already_emitted_for_tables_is_not_repeated() -> None:
    containers, datasets = _process_views(
        _source(), [_view("dbo", "v")], emitted_schemas={"dbo"}
    )
    assert containers == []
    assert len(datasets) == 1


def test_views_are_filtered_by_schema_and_view_patterns() -> None:
    source = _source(
        schema_pattern={"deny": ["^finance$"]},
        view_pattern={"deny": ["^dbo\\.tmp_.*"]},
    )
    _, datasets = _process_views(
        source,
        [_view("finance", "v_fin"), _view("dbo", "tmp_v"), _view("dbo", "v_keep")],
    )
    assert [d.display_name for d in datasets] == ["v_keep"]
    assert list(source.report.filtered_schemas) == ["finance"]
    assert list(source.report.filtered_views) == ["dbo.tmp_v"]
    assert source.report.views_scanned == 1


def test_schemaless_views_are_filed_under_dbo_without_schema_containers() -> None:
    source = _source(extract_schemas=False)
    containers, datasets = _process_views(source, [_view("", "v")])
    assert containers == []
    assert len(datasets) == 1
    assert ".dbo.v" in str(datasets[0].urn)


def test_view_discovery_failure_is_reported_not_raised() -> None:
    source = _source()
    schema_client = MagicMock()
    schema_client.get_all_views.side_effect = RuntimeError("endpoint down")
    emitted = list(
        source._process_item_views(
            WS,
            LH_ID,
            "Lakehouse",
            _KEY,
            schema_client=schema_client,
            schema_map={},
            emitted_schemas=set(),
        )
    )
    assert emitted == []
    assert len(source.report.warnings) == 1


def test_column_fetch_failure_degrades_to_tables_without_columns() -> None:
    source = _source()
    schema_client = MagicMock()
    schema_client.get_all_table_columns.side_effect = RuntimeError("denied")
    assert source._fetch_schema_map(schema_client, WS, LH_ID, "Lakehouse") == {}
    assert len(source.report.warnings) == 1


def test_schema_map_is_not_fetched_when_schema_extraction_is_disabled() -> None:
    source = _source(extract_schema={"enabled": False})
    schema_client = MagicMock()
    assert source._fetch_schema_map(schema_client, WS, LH_ID, "Lakehouse") == {}
    schema_client.get_all_table_columns.assert_not_called()


def test_no_sql_endpoint_config_means_no_schema_client_and_no_warning() -> None:
    source = _source(
        sql_endpoint=None,
        extract_views=False,
        extract_schema={"enabled": False},
        usage={"include_usage_statistics": False},
    )
    assert source._create_schema_client(WS, LH_ID, "Lakehouse", "lh_main") is None
    cast(MagicMock, source.client).get.assert_not_called()
    assert not source.report.warnings


def test_an_item_without_a_provisioned_endpoint_is_a_warning() -> None:
    source = _source(sql_endpoint={"enabled": True})
    cast(MagicMock, source.client).get.return_value.json.return_value = {
        "properties": {}
    }
    assert source._create_schema_client(WS, LH_ID, "Lakehouse", "lh_main") is None
    assert len(source.report.warnings) == 1


def test_usage_is_skipped_for_items_without_an_endpoint() -> None:
    source = _source(usage={"include_usage_statistics": True})
    source.usage_extractor = MagicMock()
    source._extract_item_usage(WS.id, LH_ID, "lh_main", schema_client=None)
    source.usage_extractor.extract.assert_not_called()
    assert source.report.num_usage_queries_skipped["no_sql_endpoint_for_item"] == 1
