"""Column lineage uses each table's stored column names. Databricks column names
are case-insensitive, and lineage reports them in the case the query used."""

from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

from databricks.sdk.service.catalog import ColumnInfo, TableInfo

from datahub.ingestion.source.unity.config import LineageDataSource
from datahub.ingestion.source.unity.proxy import (
    TableInfoWithGeneration,
    UnityCatalogApiProxy,
)
from tests.unit.unity.conftest import unity_schema, unity_table, warning_messages

ProxyBuilder = Callable[..., UnityCatalogApiProxy]

_UPSTREAM = "main.raw.customers"

_API_RESPONSE = {
    "upstream_cols": [
        {
            "catalog_name": "main",
            "schema_name": "raw",
            "table_name": "customers",
            "name": "Customer_ID",
        }
    ]
}


def _stored_columns(workspace_client: MagicMock, *names: str) -> None:
    workspace_client.tables.get.return_value = TableInfo(
        columns=[ColumnInfo(name=n) for n in names]
    )


def _row(target: str, source_table: str, source: str) -> Dict[str, Any]:
    """A system.access.column_lineage row into main.sales.orders."""
    return {
        "target_table_schema": "sales",
        "target_table_name": "orders",
        "target_column_name": target,
        "source_table_catalog": "main",
        "source_table_schema": "raw",
        "source_table_name": source_table,
        "source_column_name": source,
        "last_updated": None,
    }


def _lineage(
    proxy: UnityCatalogApiProxy,
    column: str,
    rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, List[str]]:
    """{upstream table: upstream columns} recorded for `column` of
    main.sales.orders. `rows` are system-table rows; the HTTP API path reads
    the mocked workspace client instead."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    table = unity_table()
    with patch("datahub.ingestion.source.unity.proxy.connect", return_value=connection):
        proxy.get_column_lineage(table, [column], max_workers=1)
    return {
        ref.qualified_table_name: cols[column] for ref, cols in table.upstreams.items()
    }


def _list_upstream(proxy: UnityCatalogApiProxy, *columns: str) -> None:
    raw = unity_schema()
    raw.name = "raw"
    proxy._create_table(
        raw,
        TableInfoWithGeneration(
            name="customers", columns=[ColumnInfo(name=c) for c in columns]
        ),
    )


def test_system_tables_lineage_uses_stored_column_case(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    _stored_columns(workspace_client, "customer_id", "name")
    proxy = api_proxy(
        workspace_client, lineage_data_source=LineageDataSource.SYSTEM_TABLES
    )

    # The downstream column is matched although lineage reports it as CUSTOMER_ID.
    rows = [_row("CUSTOMER_ID", "customers", "Customer_ID")]
    assert _lineage(proxy, "Customer_Id", rows) == {_UPSTREAM: ["customer_id"]}


def test_http_api_lineage_uses_stored_column_case(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    workspace_client.api_client.do.return_value = _API_RESPONSE
    _stored_columns(workspace_client, "customer_id")
    proxy = api_proxy(workspace_client, lineage_data_source=LineageDataSource.API)

    assert _lineage(proxy, "customer_id") == {_UPSTREAM: ["customer_id"]}


def test_unreadable_upstream_keeps_reported_column_name(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    workspace_client.api_client.do.return_value = _API_RESPONSE
    workspace_client.tables.get.side_effect = RuntimeError("PERMISSION_DENIED")
    proxy = api_proxy(workspace_client, lineage_data_source=LineageDataSource.API)

    assert _lineage(proxy, "customer_id") == {_UPSTREAM: ["Customer_ID"]}
    assert warning_messages(proxy.report) == {
        "Unable to read upstream table columns for column lineage; "
        "its column names are used as the lineage reports them"
    }


def test_upstream_without_columns_is_counted(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    workspace_client.api_client.do.return_value = _API_RESPONSE
    _stored_columns(workspace_client)
    proxy = api_proxy(workspace_client, lineage_data_source=LineageDataSource.API)

    assert _lineage(proxy, "customer_id") == {_UPSTREAM: ["Customer_ID"]}
    assert proxy.report.num_column_lineage_upstream_without_columns == 1


def test_lineage_reported_in_several_casings_is_merged_once(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    """Queries written in different case report one column under several names;
    all their upstreams are kept, and each stored column only once."""
    _stored_columns(workspace_client, "customer_id")
    proxy = api_proxy(
        workspace_client, lineage_data_source=LineageDataSource.SYSTEM_TABLES
    )
    rows = [
        _row("Customer_Id", "customers", "Customer_ID"),
        _row("CUSTOMER_ID", "accounts", "CUSTOMER_ID"),
        _row("Customer_Id", "customers", "customer_id"),
    ]

    assert _lineage(proxy, "Customer_Id", rows) == {
        "main.raw.customers": ["customer_id"],
        "main.raw.accounts": ["customer_id"],
    }


def test_listed_upstream_needs_no_api_call(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    proxy = api_proxy(
        workspace_client, lineage_data_source=LineageDataSource.SYSTEM_TABLES
    )
    _list_upstream(proxy, "Customer_Id", "name")
    rows = [
        _row("Customer_Id", "customers", "CUSTOMER_ID"),
        _row("Customer_Id", "customers", "NAME"),
    ]

    # A mixed-case stored name is kept; an all-lowercase one is its casefold.
    assert _lineage(proxy, "Customer_Id", rows) == {_UPSTREAM: ["Customer_Id", "name"]}
    workspace_client.tables.get.assert_not_called()


def test_upstream_listed_after_a_failed_read_uses_stored_case(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    """A downstream can be processed before its upstream is listed. A failed
    read then must not hide the listing from later downstreams."""
    workspace_client.api_client.do.return_value = _API_RESPONSE
    workspace_client.tables.get.side_effect = RuntimeError("PERMISSION_DENIED")
    proxy = api_proxy(workspace_client, lineage_data_source=LineageDataSource.API)

    assert _lineage(proxy, "customer_id") == {_UPSTREAM: ["Customer_ID"]}
    _list_upstream(proxy, "customer_id")
    assert _lineage(proxy, "customer_id") == {_UPSTREAM: ["customer_id"]}


def test_unlisted_upstream_is_read_once(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    _stored_columns(workspace_client, "customer_id")
    proxy = api_proxy(workspace_client, lineage_data_source=LineageDataSource.API)
    workspace_client.api_client.do.return_value = _API_RESPONSE

    for _ in range(2):
        proxy.get_column_lineage(unity_table(), ["customer_id"], max_workers=1)

    workspace_client.tables.get.assert_called_once_with(
        full_name=_UPSTREAM, include_browse=True
    )
