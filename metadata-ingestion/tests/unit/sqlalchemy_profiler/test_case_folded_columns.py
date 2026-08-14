import gc
import weakref
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters.snowflake import (
    SnowflakeAdapter,
)
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
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

        # The colliding pair takes as-stored identifiers, so they are distinct
        # strings and each addresses exactly one real column. "id" did not
        # collide, so it keeps the name reflection gave it -- renaming it would
        # detach its profile on dialects that do not re-map field paths.
        assert [str(c.name) for c in rebuilt.columns] == ["col", "COL", "id"]
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

    def test_reuses_one_inspector_per_bind(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        # Dialects cache a whole schema's columns on the Inspector, so reflecting
        # each table through a fresh one would cost a round trip per table.
        table = _folded_table(snowflake_engine, ["col", "id"])
        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(["col", "id"])
        with patch.object(sa, "inspect", return_value=inspector) as inspect_mock:
            for _ in range(3):
                adapter._restore_case_folded_columns(table, snowflake_engine)
        assert inspect_mock.call_count == 1

    def test_sampled_temp_table_is_repaired(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        # The sampling path reflects a CTAS temp table, which carries case-only
        # duplicate columns through from the source, so it needs the same repair
        # as a directly reflected table.
        context = ProfilingContext(
            pretty_name="db.sch.t", schema="sch", table="t", row_count=1_000_000
        )
        conn = MagicMock()
        conn.dialect = snowflake_engine.dialect
        repaired = _folded_table(snowflake_engine, ["col", "COL", "id"])

        with (
            patch.object(sa, "Table", return_value=repaired),
            patch.object(
                adapter, "_restore_case_folded_columns", return_value=repaired
            ) as repair,
        ):
            adapter._create_sampled_temp_table(context, conn, row_count=1_000_000)

        assert repair.called

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


class TestInspectorCaching:
    """Profiling opens one Connection per table, and an Inspector strongly
    references its bind. Caching per-bind would therefore retain a dead Connection
    for every table profiled, for the lifetime of the adapter.
    """

    def test_base_engine_inspector_is_reused(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        with patch.object(sa, "inspect", side_effect=lambda bind: MagicMock()) as insp:
            first = adapter._case_fold_inspector(snowflake_engine)
            second = adapter._case_fold_inspector(snowflake_engine)

        assert first is second
        assert insp.call_count == 1

    def test_per_table_connection_can_be_garbage_collected(self) -> None:
        # A real engine and a real Inspector: the retention being tested is
        # SQLAlchemy's own (Inspector.bind), which mocks would not reproduce.
        engine = sa.create_engine("sqlite://")
        adapter = SnowflakeAdapter(ProfilingConfig(), SQLSourceReport(), engine)

        connection = engine.connect()
        adapter._case_fold_inspector(connection)
        reference = weakref.ref(connection)

        connection.close()
        del connection
        gc.collect()

        assert reference() is None, (
            "the adapter retained a profiled table's connection; over a run this "
            "pins one dead connection per table"
        )


class TestNonSnowflakeNormalizingDialects:
    """Oracle normalizes identifiers too, and reaches this code.

    sql_common hands every SQLAlchemy source the SQLAlchemyProfiler, and a
    platform without its own adapter falls through to the generic one, so the
    repair runs far beyond Snowflake. Only Snowflake re-maps profile field paths
    onto the emitted schema afterwards; everywhere else a renamed column simply
    stops matching its schema field and loses its profile. So the repair has to
    leave non-colliding columns exactly as reflection produced them.
    """

    @staticmethod
    def _oracle_adapter() -> Any:
        from sqlalchemy.dialects import oracle

        from datahub.ingestion.source.sqlalchemy_profiler.adapters.generic import (
            GenericAdapter,
        )

        engine = _engine(oracle.dialect())
        assert engine.dialect.requires_name_normalize, (
            "test assumes Oracle normalizes; it is why this path is reachable"
        )
        return GenericAdapter(ProfilingConfig(), SQLSourceReport(), engine), engine

    def test_non_colliding_columns_keep_their_reflected_names(self) -> None:
        adapter, engine = self._oracle_adapter()
        table = _folded_table(engine, ["col", "id", "amount"])

        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(
            [QUOTED_LOWER, FOLDED_UPPER, "id", "amount"]
        )
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._restore_case_folded_columns(table, engine)

        names = [str(c.name) for c in rebuilt.columns]
        assert "id" in names and "amount" in names, (
            f"reflected names must survive untouched, got {names}"
        )
        # The collision is still repaired: both spellings present, distinctly.
        assert sorted(n for n in names if n.lower() == "col") == ["COL", "col"]

    def test_table_without_a_collision_is_untouched(self) -> None:
        adapter, engine = self._oracle_adapter()
        table = _folded_table(engine, ["id", "amount"])

        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(["id", "amount"])
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._restore_case_folded_columns(table, engine)

        assert rebuilt is table
