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


def test_inflight_sql_defaults_to_temp_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # On this debugging branch the breadcrumb is on by default at a temp path,
    # so it needs no per-run configuration.
    import tempfile

    from datahub.configuration.env_vars import get_sql_parse_inflight_log_file

    monkeypatch.delenv("DATAHUB_SQL_PARSE_INFLIGHT_LOG_FILE", raising=False)
    path = get_sql_parse_inflight_log_file()
    assert path and path.startswith(tempfile.gettempdir())


def test_optimize_inputs_dumped_for_replay(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # To turn a state-dependent native optimizer crash into a replayable
    # reproducer, capture the exact inputs to the column-lineage optimize() call
    # (post-qualify SQL + schema mapping + dialect + rules) crash-safely.
    import json

    from datahub.sql_parsing.schema_resolver import SchemaResolver
    from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

    dump = tmp_path / "optimize.json"
    monkeypatch.setenv("DATAHUB_SQL_PARSE_OPTIMIZE_DUMP_FILE", str(dump))

    sr = SchemaResolver(platform="snowflake")
    sqlglot_lineage(
        "SELECT a, b FROM db.s.t",
        schema_resolver=sr,
        default_db="db",
        default_schema="s",
    )

    assert dump.exists()
    payload = json.loads(dump.read_text())
    assert payload["sql"]
    assert payload["dialect"] == "snowflake"
    assert "schema_mapping" in payload
    assert isinstance(payload["rules"], list) and payload["rules"]
