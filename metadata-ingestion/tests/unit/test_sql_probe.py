"""The SQL family's raw-cursor passthrough and the structural verdicts the
pattern fields alone cannot state."""

import sqlite3
from typing import Iterator, Optional

import pytest

from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent.verdicts import VerdictContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.sql.sql_probe import execute_on_cursor
from datahub.ingestion.source.sql.sql_probe_verdicts import sql_structural_verdict


@pytest.fixture
def connection() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, "x"), (2, "y"), (3, "z")])
    yield conn
    conn.close()


def test_execute_on_cursor_returns_named_columns_and_closes_the_cursor(
    connection: sqlite3.Connection,
) -> None:
    cursor = connection.cursor()

    rows = execute_on_cursor(cursor, "SELECT a, b FROM t ORDER BY a", 2)

    assert rows.columns == ["a", "b"]
    assert rows.rows == [[1, "x"], [2, "y"]]
    with pytest.raises(sqlite3.ProgrammingError):
        cursor.execute("SELECT 1")


def test_execute_on_cursor_closes_even_when_the_query_fails(
    connection: sqlite3.Connection,
) -> None:
    cursor = connection.cursor()
    with pytest.raises(sqlite3.OperationalError):
        execute_on_cursor(cursor, "SELECT * FROM missing", 2)
    with pytest.raises(sqlite3.ProgrammingError):
        cursor.execute("SELECT 1")


def test_execute_on_cursor_with_no_result_set_returns_nothing(
    connection: sqlite3.Connection,
) -> None:
    rows = execute_on_cursor(connection.cursor(), "CREATE TABLE u (x INTEGER)", 5)
    assert (rows.columns, rows.rows) == ([], [])


class _QualifiedConfig(ConfigModel):
    match_fully_qualified_names: bool = True


def _schema_context(pattern_field: Optional[str]) -> VerdictContext:
    return VerdictContext(
        kind=DatasetContainerSubTypes.SCHEMA,
        name="sales",
        target="sales",
        parent_path=("db",),
        pattern_field=pattern_field,
        structural=None,
        attributes={},
        warn=lambda _: None,
    )


def test_a_qualified_schema_without_a_filtering_field_gets_no_verdict() -> None:
    config = _QualifiedConfig()
    # No field filters schemas on this config, and a field that does not exist
    # is not one either: the pattern decides, which here means nothing does.
    assert sql_structural_verdict(config, _schema_context(None)) is None
    assert sql_structural_verdict(config, _schema_context("no_such_pattern")) is None
