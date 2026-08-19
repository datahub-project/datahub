import gc
import threading
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
from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)
from datahub.metadata.schema_classes import DatasetFieldProfileClass
from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombiner


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
            rebuilt = adapter._use_stored_column_names(table, snowflake_engine)

        # Every column takes its as-stored identifier, so the colliding pair are
        # distinct strings and each addresses exactly one real column.
        assert [str(c.name) for c in rebuilt.columns] == ["col", "COL", "ID"]
        assert len({str(c.name) for c in rebuilt.columns}) == 3

    def test_generated_sql_targets_each_column_separately(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        table = _folded_table(snowflake_engine, ["col"])
        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected([QUOTED_LOWER, FOLDED_UPPER])
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._use_stored_column_names(table, snowflake_engine)

        rendered = [
            str(sa.select([sa.func.min(c)]).compile(dialect=snowflake_engine.dialect))
            for c in rebuilt.columns
        ]
        assert any('"col"' in sql for sql in rendered)
        assert any('"COL"' in sql for sql in rendered)

    def test_uncollided_columns_take_their_stored_names(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        # No collision here, but naming is unconditional: SQL always addresses the
        # stored identifier, and field_path_for translates back at emission.
        table = _folded_table(snowflake_engine, ["customer_id", "amount"])
        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(["customer_id", "amount"])
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._use_stored_column_names(table, snowflake_engine)

        assert [str(c.name) for c in rebuilt.columns] == ["CUSTOMER_ID", "AMOUNT"]

    def test_untouched_for_dialects_that_do_not_normalize(
        self, adapter: SnowflakeAdapter
    ) -> None:
        from sqlalchemy.dialects import postgresql

        engine = _engine(postgresql.dialect())  # type: ignore[misc]
        table = _folded_table(engine, ["col", "id"])

        # Postgres cannot fold two columns together, so no re-inspection happens.
        with patch.object(sa, "inspect", side_effect=AssertionError("must not run")):
            assert adapter._use_stored_column_names(table, engine) is table

    def test_a_thread_keeps_one_adapter_across_its_tables(self) -> None:
        """The cache is only worth having if the adapter outlives one table.

        `_generate_single_profile` used to build an adapter per table, so
        `_inspector` was always None on entry and the schema-wide get_columns ran
        once per table -- exactly what the cache exists to avoid. Reusing the same
        adapter three times, as the test below does, never showed it.
        """
        profiler = SQLAlchemyProfiler(
            conn=_engine(MagicMock()),
            report=SQLSourceReport(),
            config=ProfilingConfig(),
            platform="snowflake",
            env="PROD",
        )
        assert profiler._thread_adapter("snowflake") is profiler._thread_adapter(
            "snowflake"
        )

        from_other_thread = []
        thread = threading.Thread(
            target=lambda: from_other_thread.append(
                profiler._thread_adapter("snowflake")
            )
        )
        thread.start()
        thread.join()
        # Separate thread, separate adapter: an Inspector's info cache is a plain
        # dict and tables are profiled concurrently.
        assert from_other_thread[0] is not profiler._thread_adapter("snowflake")

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
                adapter._use_stored_column_names(table, snowflake_engine)
        assert inspect_mock.call_count == 1

    def test_sampled_temp_table_is_repaired(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        """The sampling path reflects a CTAS temp table, which carries case-only
        duplicates through from the source, so it needs the same repair.

        Patching _use_stored_column_names out and asserting it was called proves
        the wiring and nothing else -- with the method under test replaced by a
        mock, the repair could be broken and this would still pass. Patch what it
        reads instead, and check the columns that come back.
        """
        context = ProfilingContext(
            pretty_name="db.sch.t", schema="sch", table="t", row_count=1_000_000
        )
        conn = MagicMock()
        conn.dialect = snowflake_engine.dialect

        # As reflection hands it over: the case-only pair already folded to one.
        folded = _folded_table(snowflake_engine, ["col", "id"])
        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(
            [QUOTED_LOWER, FOLDED_UPPER, "id"]
        )

        # Stand in for the reflection only. Replacing sa.Table wholesale also
        # replaces the rebuild inside the repair, which then hands the folded
        # table straight back and the test passes while proving nothing.
        real_table = sa.Table

        def only_the_reflection(*args: Any, **kwargs: Any) -> Any:
            if "autoload_with" in kwargs:
                return folded
            return real_table(*args, **kwargs)

        with (
            patch.object(sa, "Table", side_effect=only_the_reflection),
            patch.object(sa, "inspect", return_value=inspector),
        ):
            adapter._create_sampled_temp_table(context, conn, row_count=1_000_000)

        assert context.sql_table is not None
        assert [str(c.name) for c in context.sql_table.columns] == ["col", "COL", "ID"]

    def test_falls_back_when_reinspection_fails(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        table = _folded_table(snowflake_engine, ["col", "id"])
        inspector = MagicMock()
        inspector.get_columns.side_effect = sa.exc.SQLAlchemyError("boom")
        with patch.object(sa, "inspect", return_value=inspector):
            assert adapter._use_stored_column_names(table, snowflake_engine) is table


class TestSnowflakeDefersFieldPaths:
    def test_snowflake_hands_back_the_stored_name(
        self, adapter: SnowflakeAdapter, snowflake_engine: Any
    ) -> None:
        # The contract the two-stage design rests on. Snowflake's schema comes
        # from INFORMATION_SCHEMA and its field path depends on config this layer
        # cannot see, so the adapter must not translate -- snowflake_profiler
        # applies the same rule it used to build schemaMetadata.
        assert adapter.field_path_for("MixedCol", snowflake_engine) == "MixedCol"

    def test_the_generic_adapter_does_translate(self) -> None:
        from sqlalchemy.dialects import oracle

        from datahub.ingestion.source.sqlalchemy_profiler.adapters.generic import (
            GenericAdapter,
        )

        engine = _engine(oracle.dialect())
        generic = GenericAdapter(ProfilingConfig(), SQLSourceReport(), engine)
        assert generic.field_path_for("MIXEDCOL", engine) == "mixedcol"


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
    stored-name rebuild runs far beyond Snowflake — unconditionally, for any
    dialect that normalizes.

    What keeps profiles attached is not leaving names alone but translating them
    back: ``field_path_for`` maps a stored name to reflection's normalized one,
    which is what a SQLAlchemy source emitted in schemaMetadata. That is the
    default for every normalizing dialect, not a Snowflake special case —
    Snowflake is the one that opts out, because its schema comes from
    INFORMATION_SCHEMA instead.

    So these pin the round trip rather than the naming: SQL addresses the stored
    column, the emitted path matches the schema.
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

    def test_stored_name_round_trips_to_the_reflected_one(self) -> None:
        """The guarantee that matters: SQL uses the stored name, the emitted path
        is the one the source's schemaMetadata carries."""
        adapter, engine = self._oracle_adapter()
        table = _folded_table(engine, ["id", "amount"])

        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(["id", "amount"])
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._use_stored_column_names(table, engine)

        # SQL side: as stored, so it addresses exactly one physical column.
        assert [str(c.name) for c in rebuilt.columns] == ["ID", "AMOUNT"]
        # Emission side: back to what reflection -- and so the schema -- uses.
        assert [
            adapter.field_path_for(str(c.name), engine) for c in rebuilt.columns
        ] == ["id", "amount"]

    def test_collision_repaired_and_both_paths_translate(self) -> None:
        adapter, engine = self._oracle_adapter()
        table = _folded_table(engine, ["col", "id"])

        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(
            [QUOTED_LOWER, FOLDED_UPPER, "id"]
        )
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._use_stored_column_names(table, engine)

        names = [str(c.name) for c in rebuilt.columns]
        # Both spellings survive as distinct SQL identifiers.
        assert sorted(n for n in names if n.lower() == "col") == ["COL", "col"]
        # Both fold to the one field path the schema declares, which the profiler
        # then de-duplicates -- rather than emitting a path nothing backs.
        assert {
            adapter.field_path_for(n, engine) for n in names if n.lower() == "col"
        } == {"col"}

    def test_rebuilt_columns_survive_the_real_emission_boundary(self) -> None:
        """Hand the boundary the column objects, not str() of their names.

        The two tests above translate `str(c.name)`, which quietly does the one
        thing the production path was missing. `_use_stored_column_names` names
        columns with quoted_name, a str subclass whose .lower()/.upper() return
        self while quoted -- enough to defeat Oracle's normalize_name, which
        decides by comparing name.upper() to name. Nothing else covers this:
        tests/integration/oracle runs a real database but sets
        profiling.enabled: false in every recipe.
        """
        adapter, engine = self._oracle_adapter()
        table = _folded_table(engine, ["id", "amount"])

        inspector = MagicMock()
        inspector.get_columns.return_value = _reflected(["id", "amount"])
        with patch.object(sa, "inspect", return_value=inspector):
            rebuilt = adapter._use_stored_column_names(table, engine)

        profiler = SQLAlchemyProfiler(
            conn=engine,
            report=SQLSourceReport(),
            config=ProfilingConfig(),
            platform="oracle",
            env="PROD",
        )
        emitted = profiler._to_emitted_field_paths(
            [DatasetFieldProfileClass(fieldPath=c.name) for c in rebuilt.columns],
            adapter,
            engine,
            "db.tbl",
        )

        assert [p.fieldPath for p in emitted] == ["id", "amount"]

    def test_untouched_for_a_dialect_that_does_not_normalize(self) -> None:
        from sqlalchemy.dialects import postgresql

        from datahub.ingestion.source.sqlalchemy_profiler.adapters.generic import (
            GenericAdapter,
        )

        engine = _engine(postgresql.dialect())  # type: ignore[misc]
        adapter = GenericAdapter(ProfilingConfig(), SQLSourceReport(), engine)
        table = _folded_table(engine, ["id", "amount"])

        with patch.object(sa, "inspect", side_effect=AssertionError("must not run")):
            assert adapter._use_stored_column_names(table, engine) is table
        assert adapter.field_path_for("Id", engine) == "Id"

    def test_a_quoted_name_comes_back_as_a_plain_string(self) -> None:
        # The two branches of field_path_for disagreed: the normalizing one wraps
        # in str(), this early return handed the argument straight back. A
        # quoted_name is a str subclass whose .lower() returns self while quoted,
        # so any caller that folds the result would silently get no folding --
        # the same failure this PR fixes at the profiler boundary. field_path_for
        # is public and overridable, so it has to hold the contract on its own.
        from sqlalchemy.dialects import postgresql

        from datahub.ingestion.source.sqlalchemy_profiler.adapters.generic import (
            GenericAdapter,
        )

        engine = _engine(postgresql.dialect())  # type: ignore[misc]
        adapter = GenericAdapter(ProfilingConfig(), SQLSourceReport(), engine)

        result = adapter.field_path_for(
            sa.sql.quoted_name("MixedCol", quote=True), engine
        )

        assert type(result) is str
        assert result.lower() == "mixedcol"


class TestTranslationIsActuallyWired:
    """Every piece of this was tested alone and none of it was tested connected.

    Each of the three call sites could be deleted with the whole suite still
    green, because the helpers are exercised directly and the assembly never is.
    The existing end-to-end tests run on sqlite, which does not set
    requires_name_normalize, so every translation is an identity there and a
    missing one looks exactly like a working one.

    Teaching sqlite to normalize is what makes the seam observable: profiling
    then addresses `MixedCol` while the source emits `mixedcol`, and the profile
    only lands on its schema field if the wiring is present.
    """

    @staticmethod
    def _normalizing_sqlite(engine: Any) -> None:
        dialect = engine.dialect
        dialect.requires_name_normalize = True
        # Same shape as Oracle's: fold an all-uppercase stored name, keep anything
        # else, and reverse it on the way back to SQL.
        dialect.normalize_name = lambda name: name.lower()
        dialect.denormalize_name = lambda name: name

    def test_a_profile_lands_on_the_path_the_source_emitted(self) -> None:
        engine = sa.create_engine("sqlite:///:memory:")
        metadata = sa.MetaData()
        table = sa.Table(
            "mixed",
            metadata,
            sa.Column("MixedCol", sa.Integer),
            sa.Column("id", sa.Integer),
        )
        metadata.create_all(engine)
        with engine.connect() as conn, conn.begin():
            conn.execute(sa.insert(table), [{"MixedCol": 1, "id": 1}])

        self._normalizing_sqlite(engine)
        profiler = SQLAlchemyProfiler(
            conn=engine,
            report=SQLSourceReport(),
            config=ProfilingConfig(enabled=True, include_field_null_count=True),
            platform="sqlite",
            env="TEST",
        )

        profile = profiler._generate_single_profile(
            query_combiner=SQLAlchemyQueryCombiner(
                enabled=False,
                catch_exceptions=True,
                is_single_row_query_method=lambda query: False,
                serial_execution_fallback_enabled=True,
            ),
            pretty_name="mixed",
            table="mixed",
        )

        assert profile is not None and profile.fieldProfiles is not None
        paths = sorted(p.fieldPath for p in profile.fieldProfiles)
        # Not MixedCol: the source's schemaMetadata carries the normalized name,
        # so a profile under the stored spelling would attach to nothing.
        assert paths == ["id", "mixedcol"], paths
        assert all(type(p.fieldPath) is str for p in profile.fieldProfiles)
