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
from tests.unit.unity.conftest import warning_messages
from tests.unit.unity.test_unity_proxy_failures import _schema, _table

ProxyBuilder = Callable[..., UnityCatalogApiProxy]

_UPSTREAM = "main.raw.customers"


def _stored_columns(workspace_client: MagicMock, *names: str) -> None:
    workspace_client.tables.get.return_value = TableInfo(
        columns=[ColumnInfo(name=n) for n in names]
    )


def _upstream_columns(
    proxy: UnityCatalogApiProxy,
    column: str,
    rows: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    """Upstream columns recorded for `column`; `rows` are system-table rows."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    table = _table()
    with patch("datahub.ingestion.source.unity.proxy.connect", return_value=connection):
        proxy.get_column_lineage(table, [column], max_workers=1)
    (ref,) = table.upstreams
    assert ref.qualified_table_name == _UPSTREAM
    return table.upstreams[ref][column]


def test_system_tables_lineage_uses_stored_column_case(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    rows = [
        {
            "target_table_schema": "sales",
            "target_table_name": "orders",
            "target_column_name": "CUSTOMER_ID",
            "source_table_catalog": "main",
            "source_table_schema": "raw",
            "source_table_name": "customers",
            "source_column_name": "Customer_ID",
            "last_updated": None,
        }
    ]
    _stored_columns(workspace_client, "customer_id", "name")
    proxy = api_proxy(
        workspace_client, lineage_data_source=LineageDataSource.SYSTEM_TABLES
    )

    # The downstream column is matched although lineage reports it as CUSTOMER_ID.
    assert _upstream_columns(proxy, "Customer_Id", rows) == ["customer_id"]


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


def test_http_api_lineage_uses_stored_column_case(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    workspace_client.api_client.do.return_value = _API_RESPONSE
    _stored_columns(workspace_client, "customer_id")
    proxy = api_proxy(workspace_client, lineage_data_source=LineageDataSource.API)

    assert _upstream_columns(proxy, "customer_id") == ["customer_id"]


def test_unreadable_upstream_keeps_reported_column_name(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    workspace_client.api_client.do.return_value = _API_RESPONSE
    workspace_client.tables.get.side_effect = RuntimeError("PERMISSION_DENIED")
    proxy = api_proxy(workspace_client, lineage_data_source=LineageDataSource.API)

    assert _upstream_columns(proxy, "customer_id") == ["Customer_ID"]
    assert warning_messages(proxy.report) == {
        "Unable to read upstream table columns for column lineage; "
        "its column names are used as the lineage reports them"
    }


def _row(target: str, source_table: str, source: str) -> Dict[str, Any]:
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


def _system_tables_lineage(
    proxy: UnityCatalogApiProxy, rows: List[Dict[str, Any]], column: str
) -> Dict[str, List[str]]:
    """{upstream table: upstream columns} recorded for `column`."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    table = _table()
    with patch("datahub.ingestion.source.unity.proxy.connect", return_value=connection):
        proxy.get_column_lineage(table, [column])
    return {
        ref.qualified_table_name: cols[column] for ref, cols in table.upstreams.items()
    }


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

    assert _system_tables_lineage(proxy, rows, "Customer_Id") == {
        "main.raw.customers": ["customer_id"],
        "main.raw.accounts": ["customer_id"],
    }


def test_listed_upstream_needs_no_api_call(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    proxy = api_proxy(
        workspace_client, lineage_data_source=LineageDataSource.SYSTEM_TABLES
    )
    raw = _schema()
    raw.name = "raw"
    proxy._create_table(
        raw,
        TableInfoWithGeneration(
            name="customers", columns=[ColumnInfo(name="customer_id")]
        ),
    )

    lineage = _system_tables_lineage(
        proxy, [_row("Customer_Id", "customers", "Customer_ID")], "Customer_Id"
    )

    assert lineage == {"main.raw.customers": ["customer_id"]}
    workspace_client.tables.get.assert_not_called()


def test_unlisted_upstream_is_read_once(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    _stored_columns(workspace_client, "customer_id")
    proxy = api_proxy(workspace_client, lineage_data_source=LineageDataSource.API)
    workspace_client.api_client.do.return_value = _API_RESPONSE

    for _ in range(2):
        proxy.get_column_lineage(_table(), ["customer_id"], max_workers=1)

    workspace_client.tables.get.assert_called_once_with(
        full_name=_UPSTREAM, include_browse=True
    )
