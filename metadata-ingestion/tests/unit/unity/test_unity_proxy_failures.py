"""UnityCatalogApiProxy degrades per table / per query when the Databricks
APIs misbehave, instead of aborting ingestion. The WorkspaceClient and the
databricks-sql ``connect`` are the mocked boundaries."""

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, TableInfo, TableType

from datahub.ingestion.source.unity.config import LineageDataSource
from datahub.ingestion.source.unity.proxy import (
    TableInfoWithGeneration,
    UnityCatalogApiProxy,
)
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    CustomCatalogType,
    Schema,
    Table,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport


def _ws(warehouse_id: Optional[str] = "wh") -> MagicMock:
    ws = MagicMock(spec=WorkspaceClient)
    ws.config.host = "https://ws.cloud.databricks.com"
    ws.config.warehouse_id = warehouse_id
    return ws


def _proxy(
    ws: MagicMock, lineage_data_source: LineageDataSource = LineageDataSource.API
) -> UnityCatalogApiProxy:
    return UnityCatalogApiProxy(
        ws, report=UnityCatalogReport(), lineage_data_source=lineage_data_source
    )


def _schema(catalog_type: Optional[CustomCatalogType] = None) -> Schema:
    catalog = Catalog(
        id="main",
        name="main",
        comment=None,
        metastore=None,
        owner=None,
        type=catalog_type,
    )
    return Schema(
        id="main.sales", name="sales", catalog=catalog, comment=None, owner=None
    )


def _table(catalog_type: Optional[CustomCatalogType] = None) -> Table:
    return Table(
        id="main.sales.orders",
        name="orders",
        comment=None,
        schema=_schema(catalog_type),
        columns=[],
        storage_location=None,
        data_source_format=None,
        table_type=TableType.MANAGED,
        owner=None,
        generation=None,
        created_at=None,
        created_by=None,
        updated_at=None,
        updated_by=None,
        table_id=None,
        view_definition=None,
        properties={},
    )


def test_constraint_lookup_failure_is_a_warning_with_no_constraints() -> None:
    ws = _ws()
    ws.tables.get.side_effect = RuntimeError("PERMISSION_DENIED")
    proxy = _proxy(ws)

    assert proxy.get_table_constraints(_table()) == []
    assert len(proxy.report.warnings) == 1


def test_hive_metastore_tables_skip_constraint_and_lineage_apis() -> None:
    ws = _ws()
    proxy = _proxy(ws)
    table = _table(CustomCatalogType.HIVE_METASTORE_CATALOG)

    assert proxy.get_table_constraints(table) == []
    proxy.table_lineage(table, include_entity_lineage=False)

    ws.tables.get.assert_not_called()
    ws.api_client.do.assert_not_called()
    assert table.upstreams == {}


def test_lineage_api_errors_leave_the_table_without_lineage() -> None:
    ws = _ws()
    ws.api_client.do.side_effect = RuntimeError("503 Service Unavailable")
    proxy = _proxy(ws)
    table = _table()

    proxy.table_lineage(table, include_entity_lineage=True)
    proxy.get_column_lineage(table, ["id"], max_workers=1)

    assert table.upstreams == {}
    assert table.external_upstreams == set()


def test_system_table_column_lineage_is_folded_into_table_upstreams() -> None:
    rows: List[Dict[str, Any]] = [
        {
            "target_table_schema": "sales",
            "target_table_name": "orders",
            "target_column_name": "id",
            "source_table_catalog": "main",
            "source_table_schema": "raw",
            "source_table_name": "events",
            "source_column_name": "event_id",
            "last_updated": None,
        }
    ]
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    proxy = _proxy(_ws(), LineageDataSource.SYSTEM_TABLES)
    table = _table()

    with patch("datahub.ingestion.source.unity.proxy.connect", return_value=connection):
        proxy.get_column_lineage(table, ["id", "amount"])

    (upstream_ref,) = table.upstreams
    assert upstream_ref.qualified_table_name == "main.raw.events"
    assert table.upstreams[upstream_ref] == {"id": ["event_id"]}


def test_proxy_related_sql_failure_reports_which_proxy_variables_are_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://user:secret@proxy.example.com:3128")
    proxy = _proxy(_ws())

    with patch(
        "datahub.ingestion.source.unity.proxy.connect",
        side_effect=RuntimeError("407 Proxy Authentication Required"),
    ):
        assert proxy._execute_sql_query("SELECT 1") == []

    (warning,) = proxy.report.warnings
    assert "secret" not in str(warning)


def test_sql_queries_are_skipped_without_a_warehouse() -> None:
    proxy = _proxy(_ws(warehouse_id=None))

    with patch("datahub.ingestion.source.unity.proxy.connect") as connect:
        assert proxy.get_table_tags("main") == {}

    connect.assert_not_called()
    assert len(proxy.report.warnings) == 1


def test_unparseable_table_is_reported_and_the_rest_still_listed() -> None:
    ws = _ws()
    ws.tables.list.return_value = [
        # A plain TableInfo lacks the generation attribute the proxy relies on.
        TableInfo(name="legacy", columns=[ColumnInfo(name="id")]),
        TableInfoWithGeneration(name="orders", columns=[ColumnInfo(name="id")]),
        TableInfoWithGeneration(name=None),
    ]
    proxy = _proxy(ws)

    tables = list(proxy.tables(_schema()))

    assert [t.name for t in tables] == ["orders"]
    assert proxy.report.num_tables_missing_name == 1
    assert len(proxy.report.warnings) == 1


def test_query_history_entries_that_cannot_be_parsed_are_skipped() -> None:
    ws = _ws()
    good = {
        "query_id": "q1",
        "query_text": "SELECT 1",
        "query_start_time_ms": 1_700_000_000_000,
        "query_end_time_ms": 1_700_000_001_000,
    }
    bad = {**good, "query_id": "q2", "query_start_time_ms": "not-a-timestamp"}
    ws.api_client.do.return_value = {"res": [bad, good]}
    proxy = _proxy(ws)

    from datetime import datetime, timezone

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    queries = list(proxy.query_history(start, start))

    assert [q.query_id for q in queries] == ["q1"]
    assert len(proxy.report.warnings) == 1
