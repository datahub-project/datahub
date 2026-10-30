"""UnityCatalogApiProxy degrades per table / per query when the Databricks
APIs misbehave, instead of aborting ingestion. The WorkspaceClient and the
databricks-sql ``connect`` are the mocked boundaries."""

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import ColumnInfo, TableInfo, TableType

from datahub.ingestion.source.unity.config import LineageDataSource
from datahub.ingestion.source.unity.proxy import (
    TableInfoWithGeneration,
    UnityCatalogApiProxy,
)
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    CustomCatalogType,
    ExternalTableReference,
    Schema,
    Table,
)
from tests.unit.unity.conftest import warning_messages

ProxyBuilder = Callable[..., UnityCatalogApiProxy]


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


def test_constraint_lookup_failure_is_a_warning_with_no_constraints(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    workspace_client.tables.get.side_effect = RuntimeError("PERMISSION_DENIED")
    proxy = api_proxy(workspace_client)

    assert proxy.get_table_constraints(_table()) == []
    assert warning_messages(proxy.report) == {"Unable to fetch table constraints"}


def test_hive_metastore_tables_skip_constraint_and_lineage_apis(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    """Unity's constraint and lineage APIs reject hive_metastore tables, so the
    proxy must not even call them for that catalog."""
    proxy = api_proxy(workspace_client)
    table = _table(CustomCatalogType.HIVE_METASTORE_CATALOG)

    assert proxy.get_table_constraints(table) == []
    proxy.table_lineage(table, include_entity_lineage=False)

    workspace_client.tables.get.assert_not_called()
    workspace_client.api_client.do.assert_not_called()
    assert table.upstreams == {}


def test_lineage_api_errors_leave_the_table_without_lineage(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    workspace_client.api_client.do.side_effect = RuntimeError("503 Service Unavailable")
    proxy = api_proxy(workspace_client)
    table = _table()

    proxy.table_lineage(table, include_entity_lineage=True)
    proxy.get_column_lineage(table, ["id"], max_workers=1)

    assert table.upstreams == {}
    assert table.external_upstreams == set()


def test_system_table_column_lineage_is_folded_into_table_upstreams(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    """system.access.column_lineage rows are keyed per column; the proxy must
    group them under one TableReference per upstream table."""
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
    proxy = api_proxy(
        workspace_client, lineage_data_source=LineageDataSource.SYSTEM_TABLES
    )
    table = _table()

    with patch("datahub.ingestion.source.unity.proxy.connect", return_value=connection):
        proxy.get_column_lineage(table, ["id", "amount"])

    (upstream_ref,) = table.upstreams
    assert upstream_ref.qualified_table_name == "main.raw.events"
    assert table.upstreams[upstream_ref] == {"id": ["event_id"]}


def test_proxy_related_sql_failure_reports_which_proxy_variables_are_set(
    workspace_client: MagicMock,
    api_proxy: ProxyBuilder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 407 is almost always a proxy-auth problem; the warning names the proxy
    variables in effect but must never carry the credentials in them."""
    monkeypatch.setenv("HTTPS_PROXY", "http://user:secret@proxy.example.com:3128")
    proxy = api_proxy(workspace_client)

    with patch(
        "datahub.ingestion.source.unity.proxy.connect",
        side_effect=RuntimeError("407 Proxy Authentication Required"),
    ):
        assert proxy._execute_sql_query("SELECT 1") == []

    (warning,) = proxy.report.warnings
    assert warning.message is not None
    assert "secret" not in str(warning)


def test_sql_queries_are_skipped_without_a_warehouse(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    workspace_client.config.warehouse_id = None
    proxy = api_proxy(workspace_client)

    with patch("datahub.ingestion.source.unity.proxy.connect") as connect:
        assert proxy.get_table_tags("main") == {}

    connect.assert_not_called()
    assert {w.title for w in proxy.report.warnings} == {"Cannot execute SQL query"}


def test_unparseable_table_is_reported_and_the_rest_still_listed(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    """tables() patches the SDK's TableInfo so that generation is carried, but a
    plain TableInfo (the patch not applying) must still parse, with generation
    unknown; only a genuinely malformed entry is dropped with a warning."""
    workspace_client.tables.list.return_value = [
        TableInfo(name="legacy", columns=[ColumnInfo(name="id")]),
        TableInfoWithGeneration(name="orders", columns=[ColumnInfo(name="id")]),
        TableInfoWithGeneration(name=None),
        TableInfoWithGeneration(name="broken", created_at="not-a-timestamp"),  # type: ignore[arg-type]
    ]
    proxy = api_proxy(workspace_client)

    tables = {t.name: t for t in proxy.tables(_schema())}

    assert set(tables) == {"legacy", "orders"}
    assert tables["legacy"].generation is None
    assert proxy.report.num_tables_missing_name == 1
    assert warning_messages(proxy.report) == {"Error parsing table"}


def test_query_history_entries_that_cannot_be_parsed_are_skipped(
    workspace_client: MagicMock, api_proxy: ProxyBuilder
) -> None:
    good = {
        "query_id": "q1",
        "query_text": "SELECT 1",
        "query_start_time_ms": 1_700_000_000_000,
        "query_end_time_ms": 1_700_000_001_000,
    }
    bad = {**good, "query_id": "q2", "query_start_time_ms": "not-a-timestamp"}
    workspace_client.api_client.do.return_value = {"res": [bad, good]}
    proxy = api_proxy(workspace_client)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    queries = list(proxy.query_history(start, start))

    assert [q.query_id for q in queries] == ["q1"]
    assert {w.title for w in proxy.report.warnings} == {"Failed to parse query"}


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "ExternalTableReference.create_from_lineage uses "
        "`d.get('has_permission') or True`, so a lineage entry the token cannot "
        "read is recorded as permitted and is never counted under "
        "num_external_upstreams_lacking_permissions"
    ),
)
def test_external_upstream_without_permission_is_recorded_as_such() -> None:
    ref = ExternalTableReference.create_from_lineage(
        {"path": "s3://bucket/restricted", "has_permission": False}
    )

    assert ref is not None
    assert not ref.has_permission
