import datetime
import decimal
from typing import List

import pytest
from sqlalchemy import create_engine

from datahub.ingestion.agent.sql_gate import SqlScopeError
from datahub.ingestion.agent.sql_query import (
    SqlRows,
    execute_scoped_sql,
)
from datahub.ingestion.source.sql.sqlalchemy_probe import (
    SqlAlchemyMetadataProbe,
    sqlglot_dialect_for,
)

CATALOG_QUERY = "SELECT table_name FROM information_schema.tables"


class FakeProvider:
    """Stands in for a connector's provider so the ordering between the scope
    check and execution can be asserted without a database."""

    sql_dialect = "postgres"

    def __init__(self, rows: List[List[object]], columns: List[str]) -> None:
        self._rows = rows
        self._columns = columns
        self.executed: List[str] = []

    def execute_sql(self, query: str, limit: int) -> SqlRows:
        self.executed.append(query)
        return SqlRows(columns=self._columns, rows=self._rows[:limit])


class NotSqlCapable:
    pass


def test_a_refused_query_never_reaches_the_connector():
    # The point of the gate: refusal has to happen before execution, not be
    # judged afterwards from the results.
    provider = FakeProvider(rows=[["x"]], columns=["c"])
    with pytest.raises(SqlScopeError):
        execute_scoped_sql(provider, "postgres", "SELECT * FROM public.orders", 10)
    assert provider.executed == []


def test_a_permitted_query_returns_columns_and_rows():
    provider = FakeProvider(rows=[["orders"], ["users"]], columns=["table_name"])
    result = execute_scoped_sql(provider, "postgres", CATALOG_QUERY, 10)
    assert result.columns == ["table_name"]
    assert result.rows == [["orders"], ["users"]]
    assert result.truncated is False
    assert provider.executed == [CATALOG_QUERY]


def test_results_past_the_limit_are_trimmed_and_flagged():
    provider = FakeProvider(rows=[[str(i)] for i in range(10)], columns=["c"])
    result = execute_scoped_sql(provider, "postgres", CATALOG_QUERY, 3)
    assert result.rows == [["0"], ["1"], ["2"]]
    assert result.truncated is True


def test_exactly_the_limit_is_not_flagged_as_truncated():
    provider = FakeProvider(rows=[["a"], ["b"]], columns=["c"])
    result = execute_scoped_sql(provider, "postgres", CATALOG_QUERY, 2)
    assert result.rows == [["a"], ["b"]]
    assert result.truncated is False


def test_values_the_json_encoder_cannot_handle_are_stringified():
    # Catalog reads routinely return dates, decimals and bytes; the result has
    # to survive json.dumps without a custom encoder at every call site.
    provider = FakeProvider(
        rows=[[datetime.date(2020, 1, 2), decimal.Decimal("1.5"), b"raw", None]],
        columns=["d", "n", "b", "nil"],
    )
    result = execute_scoped_sql(provider, "postgres", CATALOG_QUERY, 10)
    assert result.rows == [["2020-01-02", "1.5", "raw", None]]


def test_a_provider_without_a_sql_surface_is_a_clear_error():
    with pytest.raises(ValueError, match="does not support"):
        execute_scoped_sql(NotSqlCapable(), "kafka", CATALOG_QUERY, 10)


def test_execute_sql_is_not_exposed_as_a_probe_method():
    # If this were annotated with @probe_method, `probe run execute_sql --query ...`
    # would reach the engine without passing the scope check.
    assert (
        getattr(SqlAlchemyMetadataProbe.execute_sql, "__probe_command__", None) is None
    )


def test_sqlalchemy_dialect_names_are_mapped_to_sqlglot_names():
    # SQLAlchemy calls it "postgresql"; sqlglot only accepts "postgres", and a
    # mismatch would fail every Postgres probe query closed.
    assert sqlglot_dialect_for("postgresql") == "postgres"


def test_an_unmapped_dialect_passes_through_for_the_gate_to_refuse():
    assert sqlglot_dialect_for("some_new_dialect") == "some_new_dialect"


def test_the_property_reports_the_engines_dialect():
    probe = SqlAlchemyMetadataProbe(create_engine("sqlite://"))
    assert probe.sql_dialect == "sqlite"
