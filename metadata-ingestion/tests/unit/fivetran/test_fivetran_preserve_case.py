"""Unit tests for preserve_case behaviour on the Snowflake-engine branch.

The Snowflake destination uppercases unquoted database/schema identifiers
for backward compatibility with recipes from before the quoted-identifier
migration. Catalog-linked databases (CLDs), used by the Fivetran Managed
Data Lake Service when surfacing logs through Snowflake, retain
case-preserving lowercase identifiers — so the uppercasing path emits
queries against schemas that don't exist. `preserve_case=True` skips the
uppercasing.
"""

from typing import Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.fivetran.fivetran_log_api import FivetranLogAPI
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery


def _build_snowflake_setup(
    monkeypatch: pytest.MonkeyPatch, preserve_case: bool
) -> Tuple[MagicMock, FivetranLogQuery, str]:
    """Run the static `_setup_snowflake_engine` with mocked engine creation.

    Returns the (engine, query) pair plus the executed-USE-DATABASE statement
    so tests can assert on the resolved identifier casing.
    """
    fake_engine = MagicMock()

    def fake_create_engine(*_args, **_kwargs):
        return fake_engine

    # Patch in the same module that holds the import.
    monkeypatch.setattr(
        "datahub.ingestion.source.fivetran.fivetran_log_api.create_engine",
        fake_create_engine,
    )

    snowflake_config = MagicMock()
    snowflake_config.get_sql_alchemy_url.return_value = "snowflake://stub"
    snowflake_config.get_options.return_value = {}

    query = FivetranLogQuery()
    FivetranLogAPI._setup_snowflake_engine(
        snowflake_config=snowflake_config,
        database="lh_source_fivetran_usw2",
        log_schema="fivetran_metadata_test",
        preserve_case=preserve_case,
        fivetran_log_query=query,
    )
    # Pull the USE DATABASE statement that hit the engine.
    use_db_call = fake_engine.execute.call_args[0][0]
    return fake_engine, query, use_db_call


class TestPreserveCaseSnowflakeBranch:
    def test_preserve_case_true_passes_lowercase_identifiers_verbatim(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The crucial assertion: when preserve_case is set, the lowercase CLD
        # schema name reaches set_schema/use_database without being uppercased.
        # Without this fix, Snowflake emits queries against an uppercased
        # schema that doesn't exist in the catalog-linked database.
        _, query, use_db_sql = _build_snowflake_setup(monkeypatch, preserve_case=True)

        assert use_db_sql == 'use database "lh_source_fivetran_usw2"'
        assert query.schema_clause == '"fivetran_metadata_test".'

    def test_preserve_case_false_uppercases_unquoted_identifiers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The legacy backward-compat path: unquoted identifiers (as Snowflake
        # would resolve them at parse time) get uppercased before being
        # quoted. This is what existing Snowflake-warehouse recipes rely on.
        _, query, use_db_sql = _build_snowflake_setup(monkeypatch, preserve_case=False)

        assert use_db_sql == 'use database "LH_SOURCE_FIVETRAN_USW2"'
        assert query.schema_clause == '"FIVETRAN_METADATA_TEST".'

    def test_preserve_case_false_keeps_pre_quoted_identifier_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Pre-quoted identifiers (e.g., names with special chars) must not
        # be uppercased even on the legacy path — the user has signalled
        # they want exact control by quoting.
        fake_engine = MagicMock()
        monkeypatch.setattr(
            "datahub.ingestion.source.fivetran.fivetran_log_api.create_engine",
            lambda *_a, **_kw: fake_engine,
        )

        snowflake_config = MagicMock()
        snowflake_config.get_sql_alchemy_url.return_value = "snowflake://stub"
        snowflake_config.get_options.return_value = {}

        query = FivetranLogQuery()
        FivetranLogAPI._setup_snowflake_engine(
            snowflake_config=snowflake_config,
            database='"My Mixed-Case DB"',
            log_schema='"My-Schema"',
            preserve_case=False,
            fivetran_log_query=query,
        )

        use_db_sql = fake_engine.execute.call_args[0][0]
        # Already-quoted input takes the else branch (no .upper()); the inner
        # quotes get doubled per Snowflake escaping rules.
        assert '"My Mixed-Case DB"' in use_db_sql
        assert '"My-Schema"' in query.schema_clause
