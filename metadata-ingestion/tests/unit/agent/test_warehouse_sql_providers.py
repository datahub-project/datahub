"""Shape tests for the Snowflake and BigQuery catalog-query surfaces.

Fakes, not live warehouses: these mirror the row shapes each client returns
(Snowflake's DictCursor, BigQuery's RowIterator + schema), so they pin the
mapping and the limit handling but do not prove either client's real behaviour.
"""

from typing import Dict, List, Optional

from datahub.ingestion.source.bigquery_v2.bigquery_probe import BigQueryMetadataProbe
from datahub.ingestion.source.snowflake.snowflake_probe import SnowflakeMetadataProbe


class FakeSnowflakeConnection:
    """SnowflakeConnection.query uses a DictCursor, so rows arrive as dicts."""

    def __init__(self, rows: List[Dict[str, object]]) -> None:
        self._rows = rows
        self.closed = False

    def query(self, sql: str) -> object:
        return iter(self._rows)

    def close(self) -> None:
        self.closed = True


class _Field:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeBigQueryClient:
    def __init__(self, columns: List[str], rows: List[Dict[str, object]]) -> None:
        self._columns = columns
        self._rows = rows
        self.max_results_seen: Optional[int] = None
        self.closed = False

    def query(self, sql: str) -> object:
        client = self

        class _Iterator:
            def __init__(self, capped: List[Dict[str, object]]) -> None:
                self.schema = [_Field(c) for c in client._columns]
                self._capped = capped

            def __iter__(self) -> object:
                return iter(self._capped)

        class _Job:
            def result(self, max_results: int) -> object:
                client.max_results_seen = max_results
                return _Iterator(client._rows[:max_results])

        return _Job()

    def close(self) -> None:
        self.closed = True


def test_snowflake_maps_dict_rows_to_columns_and_values() -> None:
    conn = FakeSnowflakeConnection(
        [
            {"TABLE_NAME": "ORDERS", "ROW_COUNT": 3},
            {"TABLE_NAME": "USERS", "ROW_COUNT": 1},
        ]
    )
    with SnowflakeMetadataProbe(conn) as probe:
        assert probe.sql_dialect == "snowflake"
        rows = probe.execute_sql("SELECT 1", 10)
    assert rows.columns == ["TABLE_NAME", "ROW_COUNT"]
    assert rows.rows == [["ORDERS", 3], ["USERS", 1]]
    assert conn.closed is True


def test_snowflake_stops_at_the_limit() -> None:
    conn = FakeSnowflakeConnection([{"N": i} for i in range(10)])
    rows = SnowflakeMetadataProbe(conn).execute_sql("SELECT 1", 3)
    assert rows.rows == [[0], [1], [2]]


def test_bigquery_caps_the_page_it_asks_the_server_for() -> None:
    client = FakeBigQueryClient(
        ["table_name"], [{"table_name": f"t{i}"} for i in range(9)]
    )
    with BigQueryMetadataProbe(client) as probe:
        assert probe.sql_dialect == "bigquery"
        rows = probe.execute_sql("SELECT 1", 4)
    # Capped server-side, so a broad catalog query is not streamed then discarded.
    assert client.max_results_seen == 4
    assert rows.columns == ["table_name"]
    assert len(rows.rows) == 4
    assert client.closed is True


def test_neither_exposes_execute_sql_as_a_probe_method() -> None:
    for cls in (SnowflakeMetadataProbe, BigQueryMetadataProbe):
        assert getattr(cls.execute_sql, "__probe_command__", None) is None
