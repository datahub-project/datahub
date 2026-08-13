from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters.snowflake import (
    SnowflakeAdapter,
)


def _engine(dialect: Any) -> Any:
    engine = MagicMock()
    engine.dialect = dialect
    return engine


@pytest.fixture
def snowflake_engine() -> Any:
    from snowflake.sqlalchemy import dialect as snowflake_dialect

    return _engine(snowflake_dialect())


@pytest.fixture
def adapter(snowflake_engine: Any) -> SnowflakeAdapter:
    return SnowflakeAdapter(ProfilingConfig(), SQLSourceReport(), snowflake_engine)


def _reflected(names: List[Any]) -> List[Dict[str, Any]]:
    return [{"name": name, "type": sa.String()} for name in names]


# What snowflake-sqlalchemy's normalize_name() actually returns: an all-lowercase
# stored identifier keeps its case behind a quote flag, while an all-uppercase one
# is folded to a plain lowercase string. The flag is what tells them apart.
QUOTED_LOWER = sa.sql.quoted_name("col", quote=True)
FOLDED_UPPER = "col"


def _folded_table(engine: Any, folded_names: List[str]) -> sa.Table:
    """A table as SQLAlchemy reflects it — case-only duplicates already collapsed."""
    return sa.Table(
        "Case_Collision_Table",
        sa.MetaData(),
        *[sa.Column(name, sa.String()) for name in folded_names],
        schema="Schema_MixedCase",
    )


class TestRestoreCaseFoldedColumns:
    def test_rebuilds_when_a_column_was_folded_away(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        # Snowflake normalizes "col" and COL to the same name, so sa.Table keeps one.
        table = _folded_table(snowflake_engine, ["col", "id"])
        assert len(table.columns) == 2

        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(
            [QUOTED_LOWER, FOLDED_UPPER, "id"]
        )
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._restore_case_folded_columns(table, snowflake_engine)

        # Names are the as-stored identifiers, so they are distinct strings and
        # each addresses exactly one real column.
        assert [str(c.name) for c in rebuilt.columns] == ["col", "COL", "ID"]
        assert len({str(c.name) for c in rebuilt.columns}) == 3

    def test_generated_sql_targets_each_column_separately(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        table = _folded_table(snowflake_engine, ["col"])
        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected([QUOTED_LOWER, FOLDED_UPPER])
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._restore_case_folded_columns(table, snowflake_engine)

        rendered = [
            str(sa.select([sa.func.min(c)]).compile(dialect=snowflake_engine.dialect))
            for c in rebuilt.columns
        ]
        assert any('"col"' in sql for sql in rendered)
        assert any('"COL"' in sql for sql in rendered)

    def test_untouched_when_no_column_was_folded(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        table = _folded_table(snowflake_engine, ["customer_id", "amount"])
        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(["customer_id", "amount"])
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._restore_case_folded_columns(table, snowflake_engine)

        assert rebuilt is table

    def test_untouched_for_dialects_that_do_not_normalize(
        self, adapter: SnowflakeAdapter
    ) -> None:
        from sqlalchemy.dialects import postgresql

        engine = _engine(postgresql.dialect())  # type: ignore[misc]
        table = _folded_table(engine, ["col", "id"])

        # Postgres cannot fold two columns together, so no re-inspection happens.
        with patch.object(sa, "inspect", side_effect=AssertionError("must not run")):
            assert adapter._restore_case_folded_columns(table, engine) is table

    def test_falls_back_when_reinspection_fails(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        table = _folded_table(snowflake_engine, ["col", "id"])
        inspector = MagicMock()
        inspector.get_columns.side_effect = sa.exc.SQLAlchemyError("boom")
        with patch.object(sa, "inspect", return_value=inspector):
            assert (
                adapter._restore_case_folded_columns(table, snowflake_engine) is table
            )
