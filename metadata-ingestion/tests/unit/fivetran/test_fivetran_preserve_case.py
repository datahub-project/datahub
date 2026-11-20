"""Unit tests for preserve_case behaviour on the Snowflake-engine branch.

The Snowflake destination uppercases unquoted database/schema identifiers
for backward compatibility with recipes from before the quoted-identifier
migration. Case-preserving Snowflake schemas (created with quoted
lowercase names, or surfaced via catalog-linked databases) retain
lowercase identifiers — so the uppercasing path emits queries against
schemas that don't exist. `preserve_case=True` skips the uppercasing.
For Managed Data Lake setups specifically, `log_source: rest_api`
sidesteps this issue entirely.
"""

from typing import Callable, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.fivetran.fivetran_log_db_reader import (
    FivetranLogDbReader,
)
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery


def _make_cfg(database: str, log_schema: str, preserve_case: bool) -> MagicMock:
    """Build a stub config exposing the attrs `_setup_snowflake_engine` reads."""
    cfg = MagicMock()
    cfg.get_sql_alchemy_url.return_value = "snowflake://stub"
    cfg.get_options.return_value = {}
    cfg.database = database
    cfg.log_schema = log_schema
    cfg.preserve_case = preserve_case
    return cfg


def _run_snowflake_setup(
    monkeypatch: pytest.MonkeyPatch, cfg: MagicMock
) -> Tuple[MagicMock, FivetranLogQuery, str]:
    """Run the static `_setup_snowflake_engine` with mocked engine creation.

    SA 2.0 removed `Engine.execute()`; the USE DATABASE statement now runs
    inside a `@event.listens_for(engine, "connect")` listener via a raw
    DBAPI cursor. `event.listens_for` rejects a MagicMock target, so we
    stub it to capture the registered listener, then invoke it against a
    fake DBAPI connection to recover the executed USE DATABASE SQL.

    Returns the (engine, query, executed-USE-DATABASE statement) tuple so
    tests can assert on the resolved identifier casing.
    """
    fake_engine = MagicMock()
    monkeypatch.setattr(
        "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine",
        lambda *_a, **_kw: fake_engine,
    )

    registered = {}

    def _fake_listens_for(
        _target: object, _identifier: str
    ) -> Callable[[Callable], Callable]:
        def _decorator(fn: Callable) -> Callable:
            registered["fn"] = fn
            return fn

        return _decorator

    monkeypatch.setattr(
        "datahub.ingestion.source.fivetran.fivetran_log_db_reader.event.listens_for",
        _fake_listens_for,
    )

    query = FivetranLogQuery()
    FivetranLogDbReader._setup_snowflake_engine(cfg, query)

    # Invoke the captured connect-listener to recover the USE DATABASE SQL it
    # runs on each pooled connection.
    fake_cursor = MagicMock()
    fake_dbapi_conn = MagicMock()
    fake_dbapi_conn.cursor.return_value = fake_cursor
    registered["fn"](fake_dbapi_conn, MagicMock())
    use_db_call = fake_cursor.execute.call_args[0][0]
    return fake_engine, query, use_db_call


def _build_snowflake_setup(
    monkeypatch: pytest.MonkeyPatch, preserve_case: bool
) -> Tuple[MagicMock, FivetranLogQuery, str]:
    cfg = _make_cfg(
        database="mdl_log_db",
        log_schema="fivetran_metadata_test",
        preserve_case=preserve_case,
    )
    return _run_snowflake_setup(monkeypatch, cfg)


class TestPreserveCaseSnowflakeBranch:
    def test_preserve_case_true_passes_lowercase_identifiers_verbatim(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The crucial assertion: when preserve_case is set, the lowercase CLD
        # schema name reaches set_schema/use_database without being uppercased.
        # Without this fix, Snowflake emits queries against an uppercased
        # schema that doesn't exist in the catalog-linked database.
        _, query, use_db_sql = _build_snowflake_setup(monkeypatch, preserve_case=True)

        assert use_db_sql == 'use database "mdl_log_db"'
        assert query.schema_clause == '"fivetran_metadata_test".'

    def test_preserve_case_false_uppercases_unquoted_identifiers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The legacy backward-compat path: unquoted identifiers (as Snowflake
        # would resolve them at parse time) get uppercased before being
        # quoted. This is what existing Snowflake-warehouse recipes rely on.
        _, query, use_db_sql = _build_snowflake_setup(monkeypatch, preserve_case=False)

        assert use_db_sql == 'use database "MDL_LOG_DB"'
        assert query.schema_clause == '"FIVETRAN_METADATA_TEST".'

    def test_preserve_case_false_keeps_pre_quoted_identifier_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Pre-quoted identifiers (e.g., names with special chars) must not
        # be uppercased even on the legacy path — the user has signalled
        # they want exact control by quoting. Pin the exact emitted SQL
        # since the quote-doubling behaviour is the most fragile part.
        cfg = _make_cfg(
            database='"My Mixed-Case DB"',
            log_schema='"My-Schema"',
            preserve_case=False,
        )
        _, query, use_db_sql = _run_snowflake_setup(monkeypatch, cfg)
        # Pre-quoted input takes the else branch (no .upper()); the wrapping
        # quotes are doubled per Snowflake escaping rules so the identifier
        # round-trips correctly when re-quoted by `use_database`/`set_schema`.
        assert use_db_sql == 'use database """My Mixed-Case DB"""'
        assert query.schema_clause == '"""My-Schema""".'
