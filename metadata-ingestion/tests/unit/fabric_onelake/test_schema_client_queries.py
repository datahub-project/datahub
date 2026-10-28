"""SqlAnalyticsEndpointClient query behaviour against a fake SQLAlchemy engine.

These tests never create a real engine (pyodbc is not loadable everywhere), so
engine acquisition is stubbed and the assertions are on the SQL and bind
parameters the client sends and on how it shapes the rows it gets back."""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.fabric.onelake.config import SqlEndpointConfig
from datahub.ingestion.source.fabric.onelake.schema_client import (
    SqlAnalyticsEndpointClient,
    create_schema_extraction_client,
)
from datahub.ingestion.source.fabric.onelake.schema_report import (
    SqlAnalyticsEndpointReport,
)


class _Row:
    def __init__(self, **values: object) -> None:
        self._mapping = values
        self.__dict__.update(values)


class _Result:
    def __init__(self, rows: List[_Row]) -> None:
        self._rows = list(rows)
        self.fetchmany_sizes: List[int] = []

    def __iter__(self):
        return iter(self._rows)

    def fetchmany(self, size: int) -> List[_Row]:
        self.fetchmany_sizes.append(size)
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch


class _Connection:
    def __init__(self, result: _Result) -> None:
        self.result = result
        self.executed: List[Tuple[str, Dict[str, object]]] = []

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: object, params: Dict[str, object]) -> _Result:
        self.executed.append((str(query), params))
        return self.result


class _Engine:
    def __init__(self, rows: List[_Row]) -> None:
        self.connection = _Connection(_Result(rows))

    def connect(self) -> _Connection:
        return self.connection


def _client(rows: List[_Row]) -> Tuple[SqlAnalyticsEndpointClient, _Engine]:
    client = SqlAnalyticsEndpointClient(
        auth_helper=MagicMock(),
        config=SqlEndpointConfig(enabled=True),
        endpoint_url="abc.datawarehouse.fabric.microsoft.com",
        item_display_name="lh_main",
    )
    engine = _Engine(rows)
    return client, engine


def test_get_all_views_excludes_system_schemas_and_keeps_null_definitions() -> None:
    client, engine = _client(
        [
            _Row(TABLE_SCHEMA="dbo", TABLE_NAME="v1", VIEW_DEFINITION="SELECT 1"),
            _Row(TABLE_SCHEMA="sales", TABLE_NAME="v2", VIEW_DEFINITION=None),
        ]
    )
    with patch.object(client, "_get_engine", return_value=engine):
        views = client.get_all_views("ws", "lh")

    assert [(v.schema_name, v.name, v.view_definition, v.item_id) for v in views] == [
        ("dbo", "v1", "SELECT 1", "lh"),
        ("sales", "v2", None, "lh"),
    ]
    _, params = engine.connection.executed[0]
    excluded = params["system_schemas"]
    assert isinstance(excluded, list)
    assert {"INFORMATION_SCHEMA", "sys", "queryinsights"} <= set(excluded)


@pytest.mark.parametrize("skip_failed", [True, False])
def test_stream_usage_history_binds_naive_utc_and_filters_failures(
    skip_failed: bool,
) -> None:
    rows = [
        _Row(
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            statement_type="Query",
            login_name="user@example.com",
            row_count=3,
            status="Succeeded",
            command=f"SELECT {i}",
        )
        for i in range(3)
    ]
    client, engine = _client(rows)
    tz_plus_two = timezone(timedelta(hours=2))
    start = datetime(2024, 1, 2, 10, 0, tzinfo=tz_plus_two)
    end = start + timedelta(days=1)

    with patch.object(client, "_get_engine", return_value=engine):
        streamed = list(
            client.stream_usage_history(
                "ws", "lh", start, end, skip_failed, fetch_chunk_size=2
            )
        )

    assert [row.command for row in streamed] == ["SELECT 0", "SELECT 1", "SELECT 2"]
    sql, params = engine.connection.executed[0]
    assert ("status = 'Succeeded'" in sql) == skip_failed
    bound_start = params["start_time"]
    assert isinstance(bound_start, datetime)
    assert bound_start.tzinfo is None
    assert bound_start == datetime(2024, 1, 2, 8, 0)
    # Rows are pulled in fetch_chunk_size batches, not materialized at once.
    assert engine.connection.result.fetchmany_sizes[0] == 2


def _fabric_client(item_body: Optional[Dict[str, object]]) -> MagicMock:
    base_client = MagicMock()
    base_client.get.return_value.json.return_value = item_body or {}
    return base_client


def test_factory_fails_and_counts_when_the_item_has_no_endpoint() -> None:
    report = SqlAnalyticsEndpointReport()
    with pytest.raises(ValueError):
        create_schema_extraction_client(
            method="sql_analytics_endpoint",
            auth_helper=MagicMock(),
            config=SqlEndpointConfig(enabled=True),
            report=report,
            workspace_id="ws",
            item_id="lh",
            item_type="Lakehouse",
            base_client=_fabric_client({"properties": {}}),
            item_display_name="lh_main",
        )
    assert report.failures == 1


def test_factory_configures_the_client_from_the_provisioned_endpoint() -> None:
    body: Dict[str, object] = {
        "properties": {
            "sqlEndpointProperties": {
                "provisioningStatus": "Success",
                "connectionString": " abc.datawarehouse.fabric.microsoft.com ",
            }
        }
    }
    client = create_schema_extraction_client(
        method="sql_analytics_endpoint",
        auth_helper=MagicMock(),
        config=SqlEndpointConfig(enabled=True),
        report=None,
        workspace_id="ws",
        item_id="wh",
        item_type="Warehouse",
        base_client=_fabric_client(body),
        item_display_name=" wh_main ",
    )
    assert isinstance(client, SqlAnalyticsEndpointClient)
    assert client.endpoint_url == "abc.datawarehouse.fabric.microsoft.com"
    assert "Database=wh_main;" in client._get_connection_string(
        "ws", "wh", client.endpoint_url
    )
