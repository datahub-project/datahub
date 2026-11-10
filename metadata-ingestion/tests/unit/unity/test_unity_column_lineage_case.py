"""Column lineage uses each table's stored column names. Databricks column names
are case-insensitive, and lineage reports them in the case the query used."""

from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

from databricks.sdk.service.catalog import ColumnInfo, TableInfo

from datahub.ingestion.source.unity.config import LineageDataSource
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from tests.unit.unity.test_unity_proxy_failures import _table

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
