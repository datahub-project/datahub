"""Unit tests for SQLAlchemyQueryCombiner — pins current behavior of the combiner in
isolation, as a regression guard for the PR 4 flattening change.

These tests exercise the combiner directly (not via QueryCombinerRunner) against a real
in-memory SQLite engine. They cover the behaviors PR 4 will rewrite: queue batching,
result extraction by col.name, the index == len(row) invariant, the combined-failure
exception path, and the serial fallback. They follow the repo testing philosophy
(behavior over implementation; no reflection into privates; no exact-error-message
assertions).
"""

import dataclasses
from typing import Any, Optional

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine
from sqlalchemy.engine import Connection

from datahub.utilities.sqlalchemy_query_combiner import (
    MAX_QUERIES_TO_COMBINE_AT_ONCE,
    SQLAlchemyQueryCombiner,
    get_query_columns,
)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def test_table(engine):
    metadata = sa.MetaData()
    table = sa.Table(
        "test_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("value", Float),
    )
    metadata.create_all(engine)
    with engine.connect() as conn, conn.begin():
        conn.execute(
            sa.insert(table),
            [
                {"id": 1, "name": "Alice", "value": 10.5},
                {"id": 2, "name": "Bob", "value": 20.5},
                {"id": 3, "name": "Charlie", "value": 30.5},
            ],
        )
    return table


def _make_combiner(**overrides: Any) -> SQLAlchemyQueryCombiner:
    defaults: dict[str, Any] = {
        "enabled": True,
        "catch_exceptions": True,
        "is_single_row_query_method": lambda q: True,
        "serial_execution_fallback_enabled": True,
    }
    defaults.update(overrides)
    return SQLAlchemyQueryCombiner(**defaults)


@dataclasses.dataclass
class _Capture:
    """Mirrors QueryCombinerRunner._ResultContainer: catches the result or the
    re-raised exception so a failing query's greenlet doesn't tear down flush()."""

    result: Any = None
    exc: Optional[BaseException] = None
    done: bool = False


def _schedule(qc: SQLAlchemyQueryCombiner, conn: Connection, query: Any) -> _Capture:
    """Schedule `query` via qc.run(); the closure runs in a greenlet and captures
    the _ResultProxyFake returned by the patched Connection.execute (or the
    re-raised exception). Returns the capture; populated after qc.flush()."""
    cap = _Capture()

    def execute() -> None:
        try:
            cap.result = conn.execute(query)
        except Exception as e:
            cap.exc = e
        finally:
            cap.done = True

    qc.run(execute)
    return cap


class TestQueuePartitioningAndBatching:
    def test_combines_single_row_queries_into_one_statement(self, engine, test_table):
        queries = [
            sa.select(sa.func.count().label("rowcount")).select_from(test_table),
            sa.select(sa.func.min(test_table.c.value).label("min_value")),
            sa.select(sa.func.max(test_table.c.value).label("max_value")),
        ]
        combiner = _make_combiner()

        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert all(c.done for c in caps)
        assert caps[0].result.scalar() == 3
        assert caps[1].result.scalar() == 10.5
        assert caps[2].result.scalar() == 30.5
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.queries_combined == 3
        assert combiner.report.uncombined_queries_issued == 0

    def test_max_queries_batched_at_40_boundary(self, engine, test_table):
        # MAX_QUERIES_TO_COMBINE_AT_ONCE caps each _execute_queue pass at 40, so
        # 41 single-row queries must produce two combined statements (40 + 1).
        n = MAX_QUERIES_TO_COMBINE_AT_ONCE + 1
        queries = [
            sa.select(sa.func.count().label(f"rowcount_{i}")).select_from(test_table)
            for i in range(n)
        ]
        combiner = _make_combiner()

        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert all(c.done for c in caps)
        assert all(c.result is not None and c.exc is None for c in caps)
        assert combiner.report.queries_combined == n
        assert combiner.report.combined_queries_issued == 2


class TestResultExtraction:
    def test_result_row_mapped_by_col_name(self, engine, test_table):
        # The extraction loop keys each query's row dict by col.name; callers access
        # results by label, not by position. Pin that contract.
        query = sa.select(
            sa.func.count().label("rowcount"),
            sa.func.sum(test_table.c.value).label("value_sum"),
        ).select_from(test_table)
        combiner = _make_combiner()

        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            qc.flush()

        row = cap.result.one()
        assert row["rowcount"] == 3
        assert row["value_sum"] == pytest.approx(61.5)
        # int-indexing resolves to the column at that position, by insertion order
        assert row[0] == 3
        assert row[1] == pytest.approx(61.5)

    def test_index_len_row_invariant_across_mixed_column_queries(
        self, engine, test_table
    ):
        # The combined row width must equal the sum of each query's column count;
        # the extraction loop's `index` cursor must consume the whole row. Mixing
        # a 1-column and a 2-column query exercises the invariant non-trivially.
        q1 = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        q2 = sa.select(
            sa.func.min(test_table.c.value).label("min_value"),
            sa.func.max(test_table.c.value).label("max_value"),
        ).select_from(test_table)
        combiner = _make_combiner()

        with engine.connect() as conn, combiner.activate() as qc:
            cap1 = _schedule(qc, conn, q1)
            cap2 = _schedule(qc, conn, q2)
            qc.flush()

        # If the invariant were violated the combiner's internal assert would fire
        # during flush(); reaching here with correct values means extraction lined up.
        assert cap1.result.scalar() == 3
        assert cap2.result.one()["min_value"] == 10.5
        assert cap2.result.one()["max_value"] == 30.5
        assert combiner.report.combined_queries_issued == 1


class TestExceptionAndFallback:
    def test_serial_fallback_runs_each_query_when_combined_fails(
        self, engine, test_table
    ):
        # A query referencing a non-existent table makes the combined CTE statement
        # fail; with fallback enabled, each query runs serially instead. The good
        # queries still get results; the bad query's exception is captured per-future.
        good = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        bad = sa.select(sa.func.count().label("bad")).select_from(
            sa.table("does_not_exist")
        )
        combiner = _make_combiner()

        with engine.connect() as conn, combiner.activate() as qc:
            cap_good = _schedule(qc, conn, good)
            cap_bad = _schedule(qc, conn, bad)
            qc.flush()

        assert cap_good.result is not None and cap_good.exc is None
        assert cap_good.result.scalar() == 3
        assert cap_bad.result is None
        assert cap_bad.exc is not None
        assert combiner.report.query_exceptions >= 1

    def test_no_fallback_raises_when_disabled(self, engine, test_table):
        bad = sa.select(sa.func.count().label("bad")).select_from(
            sa.table("does_not_exist")
        )
        combiner = _make_combiner(serial_execution_fallback_enabled=False)

        with engine.connect() as conn, combiner.activate() as qc:
            _schedule(qc, conn, bad)
            with pytest.raises(sa.exc.SQLAlchemyError):
                qc.flush()

    def test_enabled_false_runs_queries_directly(self, engine, test_table):
        # With the combiner disabled, run() invokes the closure directly on the
        # main greenlet; _handle_execute short-circuits and Connection.execute
        # runs the query normally. flush() is a no-op; nothing is combined.
        query = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        combiner = _make_combiner(enabled=False)

        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            # With enabled=False the closure runs synchronously inside run().
            assert cap.done
            qc.flush()

        assert cap.result is not None
        assert cap.result.scalar() == 3
        assert combiner.report.combined_queries_issued == 0
        assert combiner.report.queries_combined == 0
        assert combiner.report.uncombined_queries_issued == 1


class TestResultProxyContract:
    def test_scalar_one_one_or_none_and_keyed_access(self, engine, test_table):
        # The combiner returns _ResultProxyFake objects; pin the access patterns the
        # profiler relies on: scalar(), one(), one_or_none(), int- and name-keyed row
        # access. A combined query always returns exactly one row (asserted in the
        # combiner), so MultipleResultsFound/NoResultFound are not exercised here.
        query = sa.select(
            sa.func.count().label("rowcount"),
            sa.func.sum(test_table.c.value).label("value_sum"),
        ).select_from(test_table)
        combiner = _make_combiner()

        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            qc.flush()

        result = cap.result
        assert result.scalar() == 3
        row = result.one()
        assert row["rowcount"] == 3
        assert row["value_sum"] == pytest.approx(61.5)
        assert result.one_or_none() is row
        # fetchone is aliased to one
        assert result.fetchone() is row


class TestGetQueryColumns:
    def test_returns_inner_columns_when_available(self, test_table):
        query = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        cols = get_query_columns(query)
        assert len(cols) == 1

    def test_falls_back_to_columns_on_attribute_error(self):
        # Some query shapes expose .columns but not .inner_columns; the helper must
        # fall back rather than raise. PR 4 may touch this, so pin the contract.

        class _FakeQuery:
            @property
            def inner_columns(self):
                raise AttributeError("no inner_columns")

            @property
            def columns(self):
                return [sa.column("a"), sa.column("b")]

        cols = get_query_columns(_FakeQuery())
        assert [c.name for c in cols] == ["a", "b"]
