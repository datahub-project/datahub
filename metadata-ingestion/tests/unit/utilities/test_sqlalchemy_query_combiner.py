"""Unit tests for SQLAlchemyQueryCombiner — pins current behavior in isolation.

Regression guard for the PR 4 flattening change. Exercises the combiner
directly (not via QueryCombinerRunner) against a real in-memory SQLite
engine. Follows the repo testing philosophy: behavior over implementation,
no exact-error-message assertions.

Two edge-case branches (a zero-row proxy, and a one-row/zero-column proxy)
are unreachable through the combiner, which asserts exactly one row, so
those tests construct _ResultProxyFake / _RowProxyFake directly. That is
deliberate direct construction of internal types to reach branches the
public surface cannot, not reflection into implementation details.
"""

import dataclasses
from typing import Any, Dict, Optional

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.orm.exc import NoResultFound

from datahub.utilities.sqlalchemy_query_combiner import (
    MAX_QUERIES_TO_COMBINE_AT_ONCE,
    SQLAlchemyQueryCombiner,
    _ResultProxyFake,
    _RowProxyFake,
    get_query_columns,
)


@pytest.fixture
def engine():
    # In-memory SQLite uses SingletonThreadPool, so rows inserted in the
    # test_table fixture (which opens its own connection) survive into the
    # separate engine.connect() opened in each test. If this fixture ever
    # switches to a pool that doesn't share one connection per identifier
    # (e.g. NullPool), the test_table rows would vanish mid-test. Don't
    # change the pool without re-checking that implicit dependency.
    eng = create_engine("sqlite:///:memory:")
    try:
        yield eng
    finally:
        eng.dispose()


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
    defaults: Dict[str, Any] = {
        "enabled": True,
        "catch_exceptions": True,
        "is_single_row_query_method": lambda q: True,
        "serial_execution_fallback_enabled": True,
    }
    defaults.update(overrides)
    return SQLAlchemyQueryCombiner(**defaults)


@dataclasses.dataclass
class _Capture:
    # Mirrors QueryCombinerRunner._ResultContainer: catches the result or the
    # re-raised exception so a failing query's greenlet doesn't tear down
    # flush(). exc is Optional[Exception] (not BaseException) because
    # _schedule only catches Exception.
    result: Any = None
    exc: Optional[Exception] = None
    done: bool = False


def _schedule(
    qc: SQLAlchemyQueryCombiner,
    conn: Connection,
    query: Any,
    multiparams: Any = (),
) -> _Capture:
    cap = _Capture()

    def execute() -> None:
        try:
            cap.result = conn.execute(query, *multiparams)
        except Exception as e:
            cap.exc = e
        finally:
            cap.done = True

    qc.run(execute)
    return cap


class TestBatchingAndPartitioning:
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
        # query_exceptions == 0 is the load-bearing assertion: a silent
        # fall-through to the serial path (e.g. an invariant violation in
        # _execute_queue) increments query_exceptions and leaves
        # uncombined_queries_issued at 0, so the uncombined assertion alone
        # would not catch it. See test_index_len_row_invariant_* for the
        # mechanism.
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 3

    def test_batches_at_max_queries_boundary(self, engine, test_table):
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
        assert combiner.report.uncombined_queries_issued == 0
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == n

    def test_non_single_row_query_goes_uncombined(self, engine, test_table):
        # is_single_row_query_method partitions the queue: a query the
        # predicate rejects is NOT batched — it goes out via uncombined and
        # runs normally. This is the partitioning the class name claims.
        single = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        rejected = sa.select(
            sa.func.max(test_table.c.value).label("max_value")
        ).select_from(test_table)
        combiner = _make_combiner(
            is_single_row_query_method=lambda q: q is not rejected
        )
        with engine.connect() as conn, combiner.activate() as qc:
            cap_single = _schedule(qc, conn, single)
            cap_rejected = _schedule(qc, conn, rejected)
            qc.flush()

        assert cap_single.result.scalar() == 3
        assert cap_rejected.result.scalar() == 30.5
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.queries_combined == 1
        assert combiner.report.uncombined_queries_issued == 1
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 2

    def test_enabled_false_runs_queries_directly(self, engine, test_table):
        # With the combiner disabled, run() invokes the closure directly on
        # the main greenlet; _handle_execute short-circuits and
        # Connection.execute runs the query normally. flush() is a no-op;
        # nothing is combined.
        query = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        combiner = _make_combiner(enabled=False)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            assert cap.done  # runs synchronously inside run()
            qc.flush()

        assert cap.result is not None
        assert cap.result.scalar() == 3
        assert combiner.report.combined_queries_issued == 0
        assert combiner.report.queries_combined == 0
        assert combiner.report.uncombined_queries_issued == 1
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 1


class TestResultExtraction:
    def test_result_row_mapped_by_col_name(self, engine, test_table):
        # The extraction loop keys each query's row dict by col.name; callers
        # access results by label, not by position. Pin that contract.
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
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.uncombined_queries_issued == 0
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 1

    def test_index_len_row_invariant_across_mixed_column_queries(
        self, engine, test_table
    ):
        # The combined row width must equal the sum of each query's column
        # count; the extraction loop's `index` cursor must consume the
        # whole row. Mixing a 1-column and a 2-column query exercises the
        # invariant non-trivially.
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

        assert cap1.result.scalar() == 3
        # Bind one() once: idempotent on _ResultProxyFake but NOT on a real
        # CursorResult (which is what the serial fallback stores), so
        # calling it twice would behave differently across paths.
        row2 = cap2.result.one()
        assert row2["min_value"] == 10.5
        assert row2["max_value"] == 30.5
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.uncombined_queries_issued == 0
        # query_exceptions == 0 is what actually detects an invariant
        # violation here. The combiner sets query_future.done = True inside
        # the per-query extraction loop, then asserts index == len(row)
        # AFTER the loop. If the assert fires, flush() catches it,
        # increments query_exceptions, and calls _execute_queue_fallback —
        # which skips every (already-done) future, so corrupted-but-populated
        # results stand and uncombined_queries_issued stays 0. Without
        # this assertion the mutant that appends a trailing column to the
        # combined SELECT passes this test.
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 2

    def test_anonymous_columns_scalar_works_keyed_access_unavailable(
        self, engine, test_table
    ):
        # An unlabeled aggregate: get_query_columns reports 'count' (from
        # inner_columns), but the extraction loop keys off
        # query.subquery().columns, whose name is an anon label embedding
        # an object id (non-deterministic across runs). scalar() (positional)
        # works; keyed access by 'count' does not. PR 4 must not regress
        # scalar().
        query = sa.select(sa.func.count()).select_from(test_table)
        combiner = _make_combiner()
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            qc.flush()

        assert cap.result.scalar() == 3
        row = cap.result.one()
        # Pin the shape without asserting the anon key (it embeds an object id):
        assert len(dict(row)) == 1
        # Keyed access by the inner_columns name is unavailable:
        with pytest.raises(KeyError):
            row["count"]
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.uncombined_queries_issued == 0
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 1

    def test_duplicate_labels_fallback_then_ambiguous_at_consumption(
        self, engine, test_table
    ):
        # Two columns labeled 'v' in one query. The combined CTE fails to
        # compile (ambiguous), so the combiner falls back to serial execution
        # — which SUCCEEDS at the DB level and stores a real CursorResult.
        # The InvalidRequestError (ambiguous column name) surfaces later, at
        # CONSUMPTION (row['v']), not at flush(). This is exactly where the
        # combined and serial paths diverge, and where PR 4's flattening must
        # not silently regress: today data[col.name] = row[index] silently
        # overwrites on a label collision while `index` still advances, so
        # the extracted dict has one entry but the combined row has two
        # columns.
        query = sa.select(
            sa.func.min(test_table.c.value).label("v"),
            sa.func.max(test_table.c.value).label("v"),
        ).select_from(test_table)
        combiner = _make_combiner()
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            qc.flush()  # NOT in pytest.raises: flush() succeeds (fallback)
            assert combiner.report.combined_queries_issued == 0
            assert combiner.report.uncombined_queries_issued == 1
            assert combiner.report.query_exceptions == 1
            assert combiner.report.total_queries == 1
            # The ambiguity surfaces at consumption, inside the connection
            # scope (accessing the real CursorResult after the `with` block
            # closes the connection raises ResourceClosedError instead).
            row = cap.result.one()
            with pytest.raises(sa.exc.InvalidRequestError):
                row["v"]


class TestExceptionAndFallback:
    def test_serial_fallback_runs_each_query_when_combined_fails(
        self, engine, test_table
    ):
        # A query referencing a non-existent table makes the combined CTE
        # statement fail; with fallback enabled, each query runs serially
        # instead. The good queries still get results; the bad query's
        # exception is captured per-future.
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
        # Deterministically 1: the combined statement fails once and the
        # fallback runs once. `>= 1` would hide a regression where the
        # fallback loops.
        assert combiner.report.query_exceptions == 1
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.uncombined_queries_issued == 2
        assert combiner.report.total_queries == 2

    def test_no_fallback_raises_when_disabled(self, engine, test_table):
        bad = sa.select(sa.func.count().label("bad")).select_from(
            sa.table("does_not_exist")
        )
        combiner = _make_combiner(serial_execution_fallback_enabled=False)
        with engine.connect() as conn, combiner.activate() as qc:
            _schedule(qc, conn, bad)
            with pytest.raises(sa.exc.SQLAlchemyError):
                qc.flush()
            # The scheduled greenlet is left parked in _handle_execute's
            # `while not query_future.done: main_greenlet.switch()` loop,
            # and the queue still holds the not-done future. There is no
            # clean drain without contortions (resuming the greenlet would
            # re-enter the queue and re-fail the combined execute). This is
            # per-instance state, so it cannot leak across tests, but
            # greenlet GC raises GreenletExit at an arbitrary later point —
            # a known source of intermittent CI noise for the no-fallback
            # path. Expected here.

    def test_catch_exceptions_false_propagates_handle_execute_error(
        self, engine, test_table
    ):
        # catch_exceptions=False makes the activate() fake executor
        # re-raise any exception out of _handle_execute (rather than
        # falling back to the underlying execute). is_single_row_query_method
        # is called inside _handle_execute, so a predicate that raises
        # exercises this path: the exception propagates to the closure.
        query = sa.select(sa.func.count().label("rowcount")).select_from(test_table)

        def boom(_q: Any) -> bool:
            raise RuntimeError("is_single_row_query_method blew up")

        combiner = _make_combiner(
            catch_exceptions=False, is_single_row_query_method=boom
        )
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            qc.flush()

        assert cap.exc is not None
        assert isinstance(cap.exc, RuntimeError)
        assert combiner.report.total_queries == 1
        assert combiner.report.combined_queries_issued == 0
        assert combiner.report.queries_combined == 0


class TestResultProxyContract:
    def test_scalar_one_one_or_none_and_keyed_access(self, engine, test_table):
        # The combiner returns _ResultProxyFake objects; pin the access
        # patterns the profiler relies on: scalar(), one(), one_or_none(),
        # int- and name-keyed row access. A combined query always returns
        # exactly one row (asserted in the combiner), so
        # MultipleResultsFound/NoResultFound are not exercised here.
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
        # fetchone is aliased to one (see test_fetchone_alias_to_one_* for the
        # alias contract on a zero-row proxy).
        assert result.fetchone() is row
        assert combiner.report.query_exceptions == 0
        assert combiner.report.uncombined_queries_issued == 0
        assert combiner.report.total_queries == 1

    def test_fetchone_alias_to_one_on_zero_row_proxy(self):
        # fetchone is aliased to one. On a zero-row proxy, one() raises
        # NoResultFound while first() returns None — so fetchone() must
        # raise, not return None. This kills the `fetchone = first` mutant.
        empty = _ResultProxyFake([])
        assert empty.first() is None
        with pytest.raises(NoResultFound):
            empty.one()
        with pytest.raises(NoResultFound):
            empty.fetchone()

    def test_fetchall_returns_all_rows(self):
        # .fetchall() is used at 4 production result-consumption sites, and
        # .first() returns the first row (not None) on a populated proxy —
        # pin both. The first() assertion kills the `first() -> None` mutant
        # that the zero-row test alone cannot.
        rows = [
            _RowProxyFake({"a": 1, "b": 10}),
            _RowProxyFake({"a": 2, "b": 20}),
        ]
        result = _ResultProxyFake(rows)
        assert result.first() is rows[0]
        fetched = result.fetchall()
        assert len(fetched) == 2
        assert fetched[0]["a"] == 1
        assert fetched[1]["b"] == 20

    def test_int_index_out_of_range_raises_indexerror(self):
        # Pin the CONTRACT that out-of-range int access raises IndexError
        # (rather than KeyError or a silent wrong value). Note this does
        # NOT pin the explicit guard in _RowProxyFake.__getitem__: that
        # guard is message-only dead code, since the underlying keys[k]
        # raises IndexError on its own for every out-of-range index. The
        # "remove the guard" mutant is an equivalent mutant and cannot be
        # killed by behavior alone. The contract will matter if PR 4
        # reshapes row access.
        row = _RowProxyFake({"a": 1})
        with pytest.raises(IndexError):
            row[5]

    def test_scalar_on_empty_row_returns_none(self):
        # scalar()'s empty-row branch: a one-row, zero-column result
        # returns None rather than indexing into an empty row.
        result = _ResultProxyFake([_RowProxyFake({})])
        assert result.scalar() is None

    def test_multiparams_bypasses_combiner(self, engine, test_table):
        # Passing multiparams/params bypasses the combiner (returns
        # (False, None) in _handle_execute) and runs the query normally via
        # the underlying execute. Pin that the query goes out uncombined.
        query = (
            sa.select(sa.func.count().label("rowcount"))
            .select_from(test_table)
            .where(test_table.c.id == sa.bindparam("x"))
        )
        combiner = _make_combiner()
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query, multiparams=({"x": 1},))
            qc.flush()

        assert cap.result is not None and cap.exc is None
        assert cap.result.scalar() == 1
        assert combiner.report.combined_queries_issued == 0
        assert combiner.report.queries_combined == 0
        assert combiner.report.uncombined_queries_issued == 1
        assert combiner.report.total_queries == 1


class TestGetQueryColumns:
    def test_prefers_inner_columns_over_columns(self):
        # inner_columns keeps semantic names ('count', 'count') for duplicate
        # unlabeled aggregates; .columns yields anon labels embedding an
        # object id (non-deterministic). Asserting the names (not the count
        # — both return 2) distinguishes the two branches and kills the
        # `.columns`-first mutant. Use sa.table/sa.column (pure metadata),
        # no engine needed.
        t = sa.table("t", sa.column("value"))
        query = sa.select(sa.func.count(), sa.func.count()).select_from(t)
        assert [c.name for c in get_query_columns(query)] == ["count", "count"]

    def test_falls_back_to_columns_on_attribute_error(self):
        # Some query shapes expose .columns but not .inner_columns; the
        # helper must fall back rather than raise. PR 4 may touch this, so
        # pin the contract.

        class _FakeQuery:
            @property
            def inner_columns(self):
                raise AttributeError("no inner_columns")

            @property
            def columns(self):
                return [sa.column("a"), sa.column("b")]

        cols = get_query_columns(_FakeQuery())
        assert [c.name for c in cols] == ["a", "b"]


class TestFlattenPath:
    """PR 4: the flatten path partitions by FROM signature and emits one flat
    SELECT per group (COUNT(DISTINCT) capped at MAX_DISTINCT_PER_STATEMENT),
    instead of one CTE per query. Flag off by default; these tests opt in via
    _make_combiner(flatten_enabled=True).
    """

    def test_flat_path_combines_same_from_queries_into_one_statement(
        self, engine, test_table
    ):
        # Three cheap aggregates on the same table, no clauses. The flatten
        # path emits ONE flat SELECT (not one CTE per query).
        queries = [
            sa.select(sa.func.count().label("rowcount")).select_from(test_table),
            sa.select(sa.func.min(test_table.c.value).label("minv")).select_from(
                test_table
            ),
            sa.select(sa.func.max(test_table.c.value).label("maxv")).select_from(
                test_table
            ),
        ]
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert all(c.done and c.exc is None for c in caps)
        assert caps[0].result.scalar() == 3
        assert caps[1].result.scalar() == 10.5
        assert caps[2].result.scalar() == 30.5
        assert combiner.report.flat_queries_issued == 1
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.queries_combined == 3
        assert combiner.report.uncombined_queries_issued == 0
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 3

    def test_flat_path_unique_labels_for_anonymous_columns(self, engine, test_table):
        # Two unlabeled COUNT() queries. The flat SELECT must assign unique
        # generated labels and map results back by POSITION, so both futures
        # get the right value despite the colliding inner-column name. This
        # kills the "key by col.name" mutant (which would overwrite on the
        # collision and return one wrong value).
        queries = [
            sa.select(sa.func.count()).select_from(test_table),
            sa.select(sa.func.count(sa.column("id"))).select_from(test_table),
        ]
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert caps[0].result.scalar() == 3
        assert caps[1].result.scalar() == 3
        assert combiner.report.flat_queries_issued == 1
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 2

    def test_flat_path_index_invariant_mixed_width(self, engine, test_table):
        # A 1-column and a 2-column query in one flat group: the position
        # cursor must consume the whole row (index == len(row)).
        q1 = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        q2 = sa.select(
            sa.func.min(test_table.c.value).label("minv"),
            sa.func.max(test_table.c.value).label("maxv"),
        ).select_from(test_table)
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            cap1 = _schedule(qc, conn, q1)
            cap2 = _schedule(qc, conn, q2)
            qc.flush()

        assert cap1.result.scalar() == 3
        row2 = cap2.result.one()
        assert row2["minv"] == 10.5
        assert row2["maxv"] == 30.5
        assert combiner.report.flat_queries_issued == 1
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 2

    def test_count_distinct_cap_splits_into_multiple_statements(
        self, engine, test_table
    ):
        # 7 COUNT(DISTINCT) aggregates with MAX_DISTINCT_PER_STATEMENT = 5 must
        # split into ceil(7/5) = 2 flat statements, each carrying at most 5
        # distinct trees. This is the spec §3.7 memory cap.
        queries = [
            sa.select(
                sa.func.count(sa.func.distinct(test_table.c.id)).label(f"uc{i}")
            ).select_from(test_table)
            for i in range(7)
        ]
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert all(c.done and c.exc is None for c in caps)
        assert all(c.result.scalar() == 3 for c in caps)
        assert combiner.report.flat_queries_issued == 2
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 7

    def test_cheap_and_distinct_heavy_split_into_separate_statements(
        self, engine, test_table
    ):
        # 3 cheap + 7 distinct-heavy on the same table: 1 cheap flat SELECT +
        # 2 distinct flat SELECTs (ceil(7/5)) = 3 flat statements total. The
        # cheap and distinct aggregates never coexist in one statement.
        cheap = [
            sa.select(sa.func.count().label("rowcount")).select_from(test_table),
            sa.select(sa.func.min(test_table.c.value).label("minv")).select_from(
                test_table
            ),
            sa.select(sa.func.max(test_table.c.value).label("maxv")).select_from(
                test_table
            ),
        ]
        distinct = [
            sa.select(
                sa.func.count(sa.func.distinct(test_table.c.id)).label(f"uc{i}")
            ).select_from(test_table)
            for i in range(7)
        ]
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in cheap + distinct]
            qc.flush()

        assert all(c.done and c.exc is None for c in caps)
        assert combiner.report.flat_queries_issued == 3
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 10

    def test_unmatched_shape_falls_through_to_cte_path(self, engine, test_table):
        # A query with a WHERE clause is not flattenable (conservative
        # signature) and falls through to the legacy CTE path. The flat
        # counter does not increment; the CTE combine handles both futures.
        flat_query = sa.select(sa.func.count().label("rowcount")).select_from(
            test_table
        )
        where_query = (
            sa.select(sa.func.count().label("filtered"))
            .select_from(test_table)
            .where(test_table.c.id > 1)
        )
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            cap_flat = _schedule(qc, conn, flat_query)
            cap_where = _schedule(qc, conn, where_query)
            qc.flush()

        assert cap_flat.result.scalar() == 3
        assert cap_where.result.scalar() == 2
        # No flat statement issued (the WHERE query is unmatched; the cheap
        # query groups alone but a 1-member group still emits a flat SELECT).
        # The CTE path is used for the unmatched subset. Both counters reflect
        # the mix: at least one flat + one CTE combine.
        assert combiner.report.flat_queries_issued >= 1
        assert combiner.report.combined_queries_issued >= 2
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 2

    def test_flatten_disabled_uses_cte_path(self, engine, test_table):
        # Flag off: today's CTE path. flat_queries_issued never increments.
        queries = [
            sa.select(sa.func.count().label("rowcount")).select_from(test_table),
            sa.select(sa.func.min(test_table.c.value).label("minv")).select_from(
                test_table
            ),
        ]
        combiner = _make_combiner(flatten_enabled=False)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert caps[0].result.scalar() == 3
        assert caps[1].result.scalar() == 10.5
        assert combiner.report.flat_queries_issued == 0
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.queries_combined == 2
        assert combiner.report.total_queries == 2
