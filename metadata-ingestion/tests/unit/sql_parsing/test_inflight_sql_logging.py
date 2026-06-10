import pathlib

import pytest
import sqlglot

from datahub.sql_parsing.sqlglot_utils import parse_statement

_SNOWFLAKE = sqlglot.Dialect.get_or_raise("snowflake")


def test_inflight_sql_is_recorded_for_crash_diagnosis(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The native SIGSEGV that crashes ingestion happens *inside* sqlglot's
    # optimizer/parser, where no try/except and no buffered log line survives.
    # parse_statement must crash-safely record the SQL it is about to parse to a
    # file (flushed immediately) so that, after a hard crash, that file names the
    # offending statement.
    inflight = tmp_path / "inflight.sql"
    monkeypatch.setenv("DATAHUB_SQL_PARSE_INFLIGHT_LOG_FILE", str(inflight))

    sql = "SELECT a, b FROM some_db.some_schema.some_table WHERE c > 1"
    parse_statement(sql, dialect=_SNOWFLAKE)

    assert inflight.exists()
    assert "some_db.some_schema.some_table" in inflight.read_text()


def test_inflight_sql_records_only_latest_statement(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The file is truncated each call, so after a crash it holds the statement
    # that was in flight - not an unbounded history.
    inflight = tmp_path / "inflight.sql"
    monkeypatch.setenv("DATAHUB_SQL_PARSE_INFLIGHT_LOG_FILE", str(inflight))

    parse_statement("SELECT 1 FROM first_table", dialect=_SNOWFLAKE)
    parse_statement("SELECT 2 FROM second_table", dialect=_SNOWFLAKE)

    contents = inflight.read_text()
    assert "second_table" in contents
    assert "first_table" not in contents


def test_inflight_sql_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No-op (no error) when the env var is unset.
    monkeypatch.delenv("DATAHUB_SQL_PARSE_INFLIGHT_LOG_FILE", raising=False)
    parse_statement("SELECT 1", dialect=_SNOWFLAKE)
