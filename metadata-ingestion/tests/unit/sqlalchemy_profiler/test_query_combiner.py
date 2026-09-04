"""Unit tests for SQLAlchemyQueryCombiner — pins current behavior in isolation.

Exercises the combiner directly (not via QueryCombinerRunner) against a real
in-memory SQLite engine. Follows the repo testing philosophy: behavior over
implementation, no exact-error-message assertions.
"""

import dataclasses
import logging
from typing import Any, Dict, FrozenSet, Optional

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sqlalchemy_profiler import (
    query_combiner as query_combiner_module,
)
from datahub.ingestion.source.sqlalchemy_profiler.query_combiner import (
    FLATTENABLE_AGGREGATES_EXECUTION_OPTION,
    MAX_QUERIES_TO_COMBINE_AT_ONCE,
    MisTaggedQueryError,
    SQLAlchemyQueryCombiner,
    _FlattenVerdict,
    _QueryFuture,
    _ResultProxyFake,
    _RowProxyFake,
    get_query_columns,
    is_single_row_query,
    single_row_query,
)


@pytest.fixture
def engine():
    # In-memory SQLite shares one connection per identifier; explicit so the
    # fixture doesn't silently break under a different pool.
    eng = create_engine("sqlite:///:memory:", poolclass=sa.pool.SingletonThreadPool)
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


@pytest.fixture
def cardinality_table(engine):
    """A table whose column c{k} holds exactly k distinct values.

    Distinct-chunking tests need results that differ per column: if every
    COUNT(DISTINCT) returns the same number, a chunk-to-future mapping bug
    hands each future the wrong column and every assertion still passes.
    """
    metadata = sa.MetaData()
    cols = [Column(f"c{k}", Integer) for k in range(1, 8)]
    table = sa.Table("cardinality_table", metadata, *cols)
    metadata.create_all(engine)
    with engine.connect() as conn, conn.begin():
        conn.execute(
            sa.insert(table),
            [{f"c{k}": min(row, k) for k in range(1, 8)} for row in range(1, 8)],
        )
    return table


def _make_combiner(**overrides: Any) -> SQLAlchemyQueryCombiner:
    defaults: Dict[str, Any] = {
        "enabled": True,
        "catch_exceptions": True,
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


# What PlatformAdapter declares. Tests assert against this rather than
# importing it, so a change to the adapter's set surfaces here as a decision
# rather than silently retuning every expectation.
BASE_SET = frozenset({"count", "min", "max", "avg", "stddev_samp"})
MSSQL_SET = (BASE_SET - {"stddev_samp"}) | {"stdev"}
CLICKHOUSE_SET = (BASE_SET - {"stddev_samp"}) | {"stddevsamp"}


def flattenable_query(query: Any, names: FrozenSet[str] = BASE_SET) -> Any:
    """Tag a query the way ProfilingConnection.execute_single_row would."""
    return single_row_query(query).execution_options(
        **{FLATTENABLE_AGGREGATES_EXECUTION_OPTION: names}
    )


def _schedule(
    qc: SQLAlchemyQueryCombiner,
    conn: Connection,
    query: Any,
    multiparams: Any = (),
    combinable: bool = True,
    flattenable_names: Optional[FrozenSet[str]] = BASE_SET,
) -> _Capture:
    """Schedule a query on the combiner.

    Tags the query as single-row by default, since most tests here exercise
    batching. Pass combinable=False to schedule an untagged query.

    flattenable_names has three meanings, and they are not interchangeable:
    a set attaches that allowlist, frozenset() attaches an empty one, and None
    attaches no option at all. The missing-tag and orthogonality tests need the
    last two to be distinguishable.
    """
    cap = _Capture()
    if combinable:
        query = single_row_query(query)
    if flattenable_names is not None:
        query = query.execution_options(
            **{FLATTENABLE_AGGREGATES_EXECUTION_OPTION: flattenable_names}
        )

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
        # would not catch it.
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
        assert all(c.result.scalar() == 3 for c in caps)
        assert combiner.report.queries_combined == n
        assert combiner.report.combined_queries_issued == 2
        assert combiner.report.uncombined_queries_issued == 0
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == n

    def test_untagged_query_goes_uncombined(self, engine, test_table):
        # The single-row tag partitions the queue: an untagged query is NOT
        # batched — it goes out via uncombined and runs normally. This is the
        # partitioning the class name claims.
        single = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        rejected = sa.select(
            sa.func.max(test_table.c.value).label("max_value")
        ).select_from(test_table)
        combiner = _make_combiner()
        with engine.connect() as conn, combiner.activate() as qc:
            cap_single = _schedule(qc, conn, single)
            cap_rejected = _schedule(qc, conn, rejected, combinable=False)
            qc.flush()

        assert cap_single.result.scalar() == 3
        assert cap_rejected.result.scalar() == 30.5
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.queries_combined == 1
        assert combiner.report.uncombined_queries_issued == 1
        assert combiner.report.uncombined_queries_in_greenlet == 1
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
        assert cap.result.fetchone() is row
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
        # results stand and uncombined_queries_issued stays 0.
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 2

    def test_anonymous_columns_scalar_works_keyed_access_unavailable(
        self, engine, test_table
    ):
        # An unlabeled aggregate: get_query_columns reports 'count' (from
        # inner_columns), but the extraction loop keys off
        # query.subquery().columns, whose name is an anon label embedding
        # an object id (non-deterministic across runs). scalar() (positional)
        # works; keyed access by 'count' does not.
        query = sa.select(sa.func.count()).select_from(test_table)
        combiner = _make_combiner()
        with engine.connect() as conn, combiner.activate() as qc:
            cap_scalar = _schedule(qc, conn, query)
            cap_one = _schedule(qc, conn, query)
            qc.flush()

        assert cap_scalar.result.scalar() == 3
        row = cap_one.result.one()
        # Pin the shape without asserting the anon key (it embeds an object id):
        assert len(dict(row)) == 1
        # Keyed access by the inner_columns name is unavailable:
        with pytest.raises(KeyError):
            row["count"]
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.uncombined_queries_issued == 0
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 2

    def test_duplicate_labels_fallback_then_ambiguous_at_consumption(
        self, engine, test_table
    ):
        # Two columns labeled 'v' in one query make the combined CTE fail to
        # compile (SQLAlchemy raises while populating the CTE's column
        # collection, before combined_queries_issued is incremented), so the
        # combiner falls back to serial execution. Serial execution succeeds
        # at the DB level and stores a real CursorResult; the ambiguity then
        # surfaces at consumption (row['v']), not at flush().
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

    @pytest.mark.parametrize("flatten_enabled", [False, True])
    def test_duplicate_labels_across_queries_do_not_collide(
        self, engine, test_table, flatten_enabled
    ):
        # Identical labels across two queries must not collide. Run on both
        # paths: the flat path rebuilds result dicts from its own plan, so it
        # can collide where the CTE path cannot.
        q_min = sa.select(sa.func.min(test_table.c.value).label("v")).select_from(
            test_table
        )
        q_max = sa.select(sa.func.max(test_table.c.value).label("v")).select_from(
            test_table
        )
        combiner = _make_combiner(flatten_enabled=flatten_enabled)
        with engine.connect() as conn, combiner.activate() as qc:
            cap_min = _schedule(qc, conn, q_min)
            cap_max = _schedule(qc, conn, q_max)
            qc.flush()

        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.uncombined_queries_issued == 0
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 2
        assert cap_min.result.one()["v"] == 10.5
        assert cap_max.result.one()["v"] == 30.5


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

    def test_serial_fallback_skips_already_done_futures(self, engine, test_table):
        # The whole-queue caller can be handed an already-done queue, and
        # without the skip-done guard those futures re-execute serially:
        # correct results, N wasted round trips.
        combiner = _make_combiner()
        with engine.connect() as conn:
            done_query = sa.select(sa.func.count().label("done")).select_from(
                test_table
            )
            undone_query = sa.select(sa.func.count().label("rowcount")).select_from(
                test_table
            )

            # Two futures already done, carrying a sentinel result that
            # re-execution would overwrite.
            done_fut_a = _QueryFuture(conn, done_query, (), {})
            done_fut_a.done = True
            done_fut_a.res = _ResultProxyFake(
                [_RowProxyFake({"sentinel": "a-untouched"})]
            )
            done_fut_b = _QueryFuture(conn, done_query, (), {})
            done_fut_b.done = True
            done_fut_b.res = _ResultProxyFake(
                [_RowProxyFake({"sentinel": "b-untouched"})]
            )

            # Two futures not yet done — these should be executed.
            undone_fut_a = _QueryFuture(conn, undone_query, (), {})
            undone_fut_b = _QueryFuture(conn, undone_query, (), {})

            combiner._execute_futures_serially(
                [done_fut_a, undone_fut_a, done_fut_b, undone_fut_b]
            )

        # Only the two un-done futures counted.
        assert combiner.report.uncombined_queries_issued == 2
        # Done futures were skipped: sentinel results untouched, still done.
        assert done_fut_a.res.fetchone()["sentinel"] == "a-untouched"
        assert done_fut_b.res.fetchone()["sentinel"] == "b-untouched"
        assert done_fut_a.done and done_fut_b.done
        # Un-done futures were executed: real result, now done.
        assert undone_fut_a.done and undone_fut_b.done
        assert undone_fut_a.res is not None and undone_fut_b.res is not None
        assert undone_fut_a.res.scalar() == 3
        assert undone_fut_b.res.scalar() == 3

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
        self, engine, test_table, monkeypatch
    ):
        # catch_exceptions=False makes the activate() fake executor
        # re-raise any exception out of _handle_execute (rather than
        # falling back to the underlying execute). is_single_row_query() is
        # called inside _handle_execute, so making it raise exercises this
        # path: the exception propagates to the closure.
        query = sa.select(sa.func.count().label("rowcount")).select_from(test_table)

        def boom(_q: Any) -> bool:
            raise RuntimeError("is_single_row_query blew up")

        monkeypatch.setattr(query_combiner_module, "is_single_row_query", boom)
        combiner = _make_combiner(catch_exceptions=False)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            qc.flush()

        assert cap.exc is not None
        assert isinstance(cap.exc, RuntimeError)
        assert combiner.report.total_queries == 1
        assert combiner.report.combined_queries_issued == 0
        assert combiner.report.queries_combined == 0


class TestGetQueryColumns:
    def test_prefers_inner_columns_and_falls_back_to_columns_for_cte(self):
        # A Select exposes inner_columns, which keeps semantic names for
        # duplicate unlabeled aggregates; .columns yields anon labels
        # embedding an object id. A CTE has no inner_columns, so
        # get_query_columns falls through to .columns on every combined query.
        t = sa.table("t", sa.column("value"))
        select_query = sa.select(sa.func.count(), sa.func.count()).select_from(t)
        assert [c.name for c in get_query_columns(select_query)] == ["count", "count"]

        cte = select_query.cte("c")
        assert not hasattr(cte, "inner_columns")
        cte_cols = list(get_query_columns(cte))
        assert len(cte_cols) == 2
        # .columns yields anon labels (embed an object id), not 'count':
        assert all(c.name != "count" for c in cte_cols)


class TestSingleRowTagging:
    """The tag is the contract: only tagged statements are combined."""

    def test_tagging_is_generative(self, test_table):
        # execution_options() returns a copy. A caller who writes
        # `query.execution_options(...)` without reassigning gets a silent
        # no-op, so pin that single_row_query leaves the original alone.
        query = sa.select(sa.func.count()).select_from(test_table)
        tagged = single_row_query(query)

        assert tagged is not query
        assert is_single_row_query(tagged)
        assert not is_single_row_query(query)

    def test_untagged_statement_is_not_combinable(self, test_table):
        assert not is_single_row_query(
            sa.select(sa.func.count()).select_from(test_table)
        )

    def test_non_executable_input_is_rejected_not_raised(self):
        # _handle_execute passes whatever reached Connection.execute. Anything
        # without get_execution_options must be rejected quietly rather than
        # blowing up the query path.
        assert not is_single_row_query(object())
        assert not is_single_row_query(None)
        assert not is_single_row_query("SELECT 1")

    def test_text_clause_can_be_tagged(self):
        # TextClause is an Executable, so the accessor works there too.
        assert is_single_row_query(single_row_query(sa.text("SELECT 1")))

    def test_counter_stays_zero_when_everything_is_tagged(self, engine, test_table):
        query = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        combiner = _make_combiner()
        with engine.connect() as conn, combiner.activate() as qc:
            _schedule(qc, conn, query)
            qc.flush()

        assert combiner.report.uncombined_queries_in_greenlet == 0

    @pytest.mark.parametrize(
        "clause,build",
        [
            ("LIMIT", lambda t: sa.select([t.c.value]).limit(2)),
            ("OFFSET", lambda t: sa.select([t.c.value]).offset(1)),
            (
                "GROUP BY",
                lambda t: (
                    sa.select([sa.func.count()]).select_from(t).group_by(t.c.name)
                ),
            ),
            ("DISTINCT", lambda t: sa.select([t.c.value]).select_from(t).distinct()),
        ],
    )
    def test_every_vetoed_clause_is_detected(self, engine, test_table, clause, build):
        """One case per clause the veto claims to catch.

        Each is read off a private SQLAlchemy attribute, so a version bump could
        silently rename one and turn its branch into dead code -- with no
        symptom until a mis-tag slipped through and poisoned a batch.
        """
        combiner = _make_combiner(catch_exceptions=False)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, build(test_table))
            qc.flush()

        assert isinstance(cap.exc, MisTaggedQueryError), (
            f"{clause} was not vetoed; the attribute it reads may have been renamed"
        )

    def test_mistagged_query_raises_for_the_developer(self, engine, test_table):
        # A tag the SQL contradicts is a call-site bug, so it fails loudly
        # rather than being counted. catch_exceptions=False is what dev and CI
        # runs use, so the error reaches the caller.
        two_rows = sa.select(test_table.c.value.label("value")).limit(2)

        combiner = _make_combiner(catch_exceptions=False)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, two_rows)
            qc.flush()

        assert isinstance(cap.exc, MisTaggedQueryError)
        # Says what to do, not just what happened.
        assert "execute_rows()" in str(cap.exc)

    def test_mistagged_query_degrades_safely_in_production(self, engine, test_table):
        # catch_exceptions=True is the production default. A mis-tag must not
        # fail an ingestion run: the query executes on its own, results are
        # correct, and the mistake stays visible via query_exceptions.
        good = sa.select(sa.func.count().label("rowcount")).select_from(test_table)
        two_rows = sa.select(test_table.c.value.label("value")).limit(2)

        combiner = _make_combiner(catch_exceptions=True)
        with engine.connect() as conn, combiner.activate() as qc:
            cap_good = _schedule(qc, conn, good)
            cap_two = _schedule(qc, conn, two_rows)
            qc.flush()

        assert cap_good.result.scalar() == 3
        assert len(cap_two.result.fetchall()) == 2
        assert combiner.report.query_exceptions == 1


class TestUnbatchableQueries:
    """Not every uncombined query is a mistake, so most of them stay quiet."""

    def test_raw_string_runs_but_is_never_combined(self, engine, test_table):
        # A raw string is the only non-Executable that reaches the combiner --
        # SQLAlchemy rejects None, ints and Tables itself with
        # ObjectNotExecutableError. It executes fine, it just cannot be batched.
        combiner = _make_combiner(catch_exceptions=False)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(
                qc, conn, "SELECT 1", combinable=False, flattenable_names=None
            )
            qc.flush()

        assert cap.exc is None
        assert cap.result.scalar() == 1
        assert combiner.report.combined_queries_issued == 0
        assert combiner.report.uncombined_queries_in_greenlet == 1

    def test_ordinary_untagged_statement_is_silent(self, engine, test_table, caplog):
        # execute_rows() on a genuine multi-row query is a correct choice, not a
        # mistake, so it must not log -- only a false row-shape claim raises.
        query = sa.select(test_table.c.value).select_from(test_table)
        combiner = _make_combiner()
        with (
            caplog.at_level(logging.WARNING),
            engine.connect() as conn,
            combiner.activate() as qc,
        ):
            _schedule(qc, conn, query, combinable=False)
            qc.flush()

        assert [r for r in caplog.records if r.levelno == logging.WARNING] == []
        assert combiner.report.uncombined_queries_in_greenlet == 1


class TestFlattenPath:
    """The flatten path partitions by FROM signature and emits one flat
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
        assert combiner.report.scans_avoided == 2  # 3 queued - 1 scan
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 3

    def test_flat_path_unique_labels_for_anonymous_columns(self, engine, test_table):
        # The flat SELECT must use generated labels and map back by position,
        # so colliding inner-column names still resolve correctly.
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

    def test_unmatched_shape_falls_through_to_cte_path(self, engine, test_table):
        # A WHERE clause is not flattenable and falls through to the CTE path.
        # Two flattenable queries so the group is not a demoted singleton.
        flat_query = sa.select(sa.func.count().label("rowcount")).select_from(
            test_table
        )
        flat_query2 = sa.select(
            sa.func.min(test_table.c.value).label("minv")
        ).select_from(test_table)
        where_query = (
            sa.select(sa.func.count().label("filtered"))
            .select_from(test_table)
            .where(test_table.c.id > 1)
        )
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            cap_flat = _schedule(qc, conn, flat_query)
            cap_flat2 = _schedule(qc, conn, flat_query2)
            cap_where = _schedule(qc, conn, where_query)
            qc.flush()

        assert cap_flat.result.scalar() == 3
        assert cap_flat2.result.scalar() == 10.5
        assert cap_where.result.scalar() == 2
        # The two cheap queries flatten into 1 flat SELECT; the WHERE query is
        # unmatched and runs through the CTE path (1 CTE combine).
        assert combiner.report.flat_queries_issued == 1
        assert combiner.report.combined_queries_issued == 2
        assert combiner.report.scans_avoided == 1
        assert combiner.report.flatten_rejected == 1
        assert combiner.report.query_exceptions == 0
        assert combiner.report.total_queries == 3

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

    @pytest.mark.parametrize("flatten_enabled", [False, True])
    def test_duplicate_explicit_labels_route_away_from_flat_path(
        self, engine, test_table, flatten_enabled
    ):
        # Duplicate explicit .label() names make subquery().columns raise.
        # The gate must reject the query rather than let it raise inside
        # _execute_flat_select and demote a whole batch.
        q = sa.select(
            sa.func.min(test_table.c.value).label("v"),
            sa.func.max(test_table.c.value).label("v"),
        ).select_from(test_table)
        combiner = _make_combiner(flatten_enabled=flatten_enabled)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, q)
            qc.flush()

        assert cap.done and cap.exc is None
        row = cap.result.one()
        assert row[0] == 10.5
        assert row[1] == 30.5
        # The flat path must not have issued a statement for this query.
        assert combiner.report.flat_queries_issued == 0

    @pytest.mark.parametrize(
        "distinct_fn",
        [
            lambda c: sa.func.count(sa.func.distinct(c)),
            lambda c: sa.func.count(sa.distinct(c)),
            lambda c: sa.func.count(c.distinct()),
        ],
        ids=["func.distinct", "sa.distinct", "col.distinct()"],
    )
    def test_count_distinct_cap_covers_all_three_spellings(
        self, engine, cardinality_table, distinct_fn
    ):
        # All three COUNT(DISTINCT) spellings must trip the cap; missing one
        # trades a scan problem for a server-memory problem.
        queries = [
            sa.select(
                distinct_fn(cardinality_table.c[f"c{k}"]).label(f"uc{k}")
            ).select_from(cardinality_table)
            for k in range(1, 8)
        ]
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert all(c.done and c.exc is None for c in caps)
        assert [c.result.scalar() for c in caps] == [1, 2, 3, 4, 5, 6, 7]
        assert combiner.report.flat_queries_issued == 2
        assert combiner.report.query_exceptions == 0

    def test_is_flattenable_rejects_every_clause_family(self, engine, test_table):
        # The gate must reject every clause that renders extra SQL. HAVING is
        # the correctness case: the flat path would drop it and fabricate a
        # row. The rest would silently change semantics.
        t = test_table
        non_flattenable = {
            "where": sa.select(sa.func.count().label("c"))
            .select_from(t)
            .where(t.c.id > 1),
            "group_by": sa.select(sa.func.count().label("c"))
            .select_from(t)
            .group_by(t.c.id),
            "order_by": sa.select(sa.func.count().label("c"))
            .select_from(t)
            .order_by(t.c.id),
            "limit": sa.select(sa.func.count().label("c")).select_from(t).limit(1),
            "offset": sa.select(sa.func.count().label("c")).select_from(t).offset(1),
            "distinct": sa.select(sa.func.count().label("c")).select_from(t).distinct(),
            "having": sa.select(sa.func.count().label("c"))
            .select_from(t)
            .having(sa.func.count() > 100),
            "for_update": sa.select(sa.func.count().label("c"))
            .select_from(t)
            .with_for_update(),
            "prefix_distinct": sa.select(sa.func.count().label("c"))
            .select_from(t)
            .prefix_with("DISTINCT"),
            "suffix_for_share": sa.select(sa.func.count().label("c"))
            .select_from(t)
            .suffix_with("FOR SHARE"),
            "fetch": sa.select(sa.func.count().label("c")).select_from(t).fetch(1),
            "with_hint": sa.select(sa.func.count().label("c"))
            .select_from(t)
            .with_hint(t, "USE INDEX (PRIMARY)"),
        }
        # Tagged: an untagged query is rejected on the empty allowlist before
        # the clause gate runs, which would make every assertion below pass
        # even with the gate deleted.
        for label, q in non_flattenable.items():
            assert not SQLAlchemyQueryCombiner._is_flattenable(flattenable_query(q)), (
                f"_is_flattenable should reject {label!r}"
            )

        # And the plain shapes that ARE flattenable.
        flattenable = {
            "plain single": sa.select(sa.func.count().label("c")).select_from(t),
            "plain multi": sa.select(
                sa.func.count(t.c.id), sa.func.count(t.c.value)
            ).select_from(t),
            "anon count": sa.select(sa.func.count()).select_from(t),
            "count distinct": sa.select(
                sa.func.count(sa.func.distinct(t.c.id)).label("uc")
            ).select_from(t),
        }
        for label, q in flattenable.items():
            assert SQLAlchemyQueryCombiner._is_flattenable(flattenable_query(q)), (
                f"_is_flattenable should accept {label!r}"
            )

    @pytest.mark.parametrize("flatten_enabled", [False, True])
    def test_having_query_returns_zero_rows_under_both_flags(
        self, engine, test_table, flatten_enabled
    ):
        # HAVING count(*) > 100 over 3 rows must return zero rows under both
        # flags. A fabricated row here would be a silent correctness break.
        q = (
            sa.select(sa.func.count().label("c"))
            .select_from(test_table)
            .having(sa.func.count() > 100)
        )
        combiner = _make_combiner(flatten_enabled=flatten_enabled)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, q)
            qc.flush()

        assert cap.done and cap.exc is None
        assert cap.result.fetchall() == []

    def test_failing_unit_does_not_demote_out_of_window_futures(
        self, engine, test_table
    ):
        # Futures beyond MAX_QUERIES_TO_COMBINE_AT_ONCE were never attempted
        # and must still flatten next pass; a global fallback would demote
        # them and zero scans_avoided.
        bad = sa.select(
            sa.func.count(sa.column("no_such_col")).label("bad")
        ).select_from(test_table)
        good_count = MAX_QUERIES_TO_COMBINE_AT_ONCE + 10
        good = [
            sa.select(sa.func.count().label(f"c{i}")).select_from(test_table)
            for i in range(good_count)
        ]
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in [bad] + good]
            qc.flush()

        assert caps[0].exc is not None  # the bad query fails
        assert all(c.done and c.exc is None for c in caps[1:])  # all good resolve
        assert all(c.result.scalar() == 3 for c in caps[1:])
        # uncombined is the failed group's size, not the whole queue.
        assert (
            combiner.report.uncombined_queries_issued == MAX_QUERIES_TO_COMBINE_AT_ONCE
        )
        # The out-of-window good futures collapsed into one scan.
        assert (
            combiner.report.scans_avoided == good_count - MAX_QUERIES_TO_COMBINE_AT_ONCE
        )

    def test_same_name_different_object_tables_not_grouped(self, engine):
        # Two Table objects named "t" must not group, or the flat SELECT
        # becomes `FROM t, t`. SQLite shares one physical table across
        # MetaData, so this asserts grouping, not per-table data.
        md1 = sa.MetaData()
        t1 = sa.Table("t", md1, Column("id", Integer))
        md2 = sa.MetaData()
        t2 = sa.Table("t", md2, Column("id", Integer))
        md1.create_all(engine)
        md2.create_all(engine)
        with engine.connect() as conn, conn.begin():
            conn.execute(sa.insert(t1).values(id=1))
            conn.execute(sa.insert(t2).values(id=2))

        # Two queries per table, so each group has a partner and survives the
        # singleton demotion — otherwise both would land in the CTE path and
        # the grouping being tested here would be invisible.
        q1 = sa.select(sa.func.count().label("c1")).select_from(t1)
        q1b = sa.select(sa.func.min(t1.c.id).label("m1")).select_from(t1)
        q2 = sa.select(sa.func.count().label("c2")).select_from(t2)
        q2b = sa.select(sa.func.min(t2.c.id).label("m2")).select_from(t2)
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            cap1 = _schedule(qc, conn, q1)
            cap1b = _schedule(qc, conn, q1b)
            cap2 = _schedule(qc, conn, q2)
            cap2b = _schedule(qc, conn, q2b)
            qc.flush()

        assert all(c.done and c.exc is None for c in (cap1, cap1b, cap2, cap2b))
        assert cap1.result.scalar() == 2
        assert cap2.result.scalar() == 2
        # Two separate groups (keyed on from-object identity) -> two flat
        # statements, not one cross-joined statement.
        assert combiner.report.flat_queries_issued == 2
        assert combiner.report.flatten_singletons == 0
        assert combiner.report.query_exceptions == 0

    def test_no_fallback_raises_when_disabled_under_flatten(self, engine, test_table):
        # With fallback disabled the failure must propagate rather than leave
        # futures un-done (a livelock). Two members so this is a real flat
        # group, not a demoted singleton.
        bad = sa.select(
            sa.func.count(sa.column("no_such_col")).label("bad")
        ).select_from(test_table)
        bad2 = sa.select(
            sa.func.max(sa.column("no_such_col")).label("bad2")
        ).select_from(test_table)
        combiner = _make_combiner(
            flatten_enabled=True, serial_execution_fallback_enabled=False
        )
        with engine.connect() as conn, combiner.activate() as qc:
            _schedule(qc, conn, bad)
            _schedule(qc, conn, bad2)
            with pytest.raises(sa.exc.SQLAlchemyError):
                qc.flush()
        assert combiner.report.flatten_singletons == 0

    def test_max_distinct_per_statement_knob_splits_distinct_heavy(
        self, engine, cardinality_table
    ):
        # K=3 with 7 one-distinct queries packs into 3 statements; ignoring
        # the knob and using the module default of 5 would give 2.
        queries = [
            sa.select(
                sa.func.count(sa.func.distinct(cardinality_table.c[f"c{k}"])).label(
                    f"uc{k}"
                )
            ).select_from(cardinality_table)
            for k in range(1, 8)
        ]
        combiner = _make_combiner(flatten_enabled=True, max_distinct_per_statement=3)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert all(c.done and c.exc is None for c in caps)
        assert [c.result.scalar() for c in caps] == [1, 2, 3, 4, 5, 6, 7]
        assert combiner.report.flat_queries_issued == 3
        assert combiner.report.scans_avoided == 4  # (3-1) + (3-1) + (1-1)
        assert combiner.report.query_exceptions == 0

    def test_text_clause_column_rejected_by_parity(self, engine, test_table):
        # A bare sa.text() column gives 1 emit-side column and 0 name-side,
        # which would trip the assert inside _execute_flat_select. The parity
        # check rejects it up front.
        text_query = sa.select(sa.text("name")).select_from(test_table)
        assert not SQLAlchemyQueryCombiner._is_flattenable(text_query)

    @pytest.mark.parametrize(
        "agg,names",
        [
            # SQLAlchemy lowercases only names it has a GenericFunction for,
            # so avg and the platform stddev spellings keep the caller's
            # casing and must be matched case-insensitively.
            (lambda c: sa.func.count(), BASE_SET),
            (lambda c: sa.func.min(c), BASE_SET),
            (lambda c: sa.func.max(c), BASE_SET),
            (lambda c: sa.func.avg(c), BASE_SET),
            (lambda c: sa.func.AVG(c), BASE_SET),
            (lambda c: sa.func.stddev_samp(c), BASE_SET),
            (lambda c: sa.func.stdev(c), MSSQL_SET),
            (lambda c: sa.func.stddevSamp(c), CLICKHOUSE_SET),
        ],
    )
    def test_declared_aggregates_are_flattenable(self, test_table, agg, names):
        query = sa.select(agg(test_table.c.value).label("m")).select_from(test_table)
        assert SQLAlchemyQueryCombiner._is_flattenable(flattenable_query(query, names))

    @pytest.mark.parametrize(
        "agg,names",
        [
            # A platform must not flatten a spelling it never emits, nor the
            # base spelling it replaced.
            (lambda c: sa.func.stddev_samp(c), MSSQL_SET),
            (lambda c: sa.func.stddev_samp(c), CLICKHOUSE_SET),
            (lambda c: sa.func.stdev(c), BASE_SET),
            (lambda c: sa.func.stddevSamp(c), BASE_SET),
            (lambda c: sa.func.sum(c), BASE_SET),
        ],
    )
    def test_undeclared_aggregates_are_not_flattenable(self, test_table, agg, names):
        query = sa.select(agg(test_table.c.value).label("m")).select_from(test_table)
        assert not SQLAlchemyQueryCombiner._is_flattenable(
            flattenable_query(query, names)
        )

    def test_missing_tag_rejects_rather_than_crashing(self, test_table):
        # A query built outside ProfilingConnection carries no allowlist. That
        # is a designed rejection, not a gate failure.
        query = sa.select(sa.func.count().label("c")).select_from(test_table)
        assert (
            SQLAlchemyQueryCombiner._flatten_verdict(single_row_query(query))
            is _FlattenVerdict.REJECTED
        )
        assert (
            SQLAlchemyQueryCombiner._flatten_verdict(
                flattenable_query(query, frozenset())
            )
            is _FlattenVerdict.REJECTED
        )

    def test_untagged_query_still_cte_batches(self, engine, test_table):
        # Orthogonality: the flatten tag must not disturb the existing
        # combiner. A single-row query with an empty allowlist batches exactly
        # as it did before the flatten path existed.
        queries = [
            sa.select(sa.func.count().label("c")).select_from(test_table),
            sa.select(sa.func.min(test_table.c.value).label("m")).select_from(
                test_table
            ),
        ]
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [
                _schedule(qc, conn, q, flattenable_names=frozenset()) for q in queries
            ]
            qc.flush()

        assert caps[0].result.scalar() == 3
        assert caps[1].result.scalar() == 10.5
        assert combiner.report.flat_queries_issued == 0
        assert combiner.report.scans_avoided == 0
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.query_exceptions == 0

    def test_gate_crash_is_counted_apart_from_allowlist_rejection(
        self, engine, test_table
    ):
        # Only an unanticipated failure counts as a gate error. Duplicate
        # labels and a WHERE clause are both by-design rejections, so neither
        # may inflate the defect counter.
        duplicate_labels = sa.select(
            sa.func.min(test_table.c.value).label("x"),
            sa.func.max(test_table.c.value).label("x"),
        ).select_from(test_table)
        where_clause = (
            sa.select(sa.func.count().label("c"))
            .select_from(test_table)
            .where(test_table.c.id > 1)
        )
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            cap_dup = _schedule(qc, conn, duplicate_labels)
            cap_where = _schedule(qc, conn, where_clause)
            qc.flush()

        assert cap_dup.done and cap_where.done
        assert combiner.report.flatten_rejected == 2
        assert combiner.report.flatten_gate_errors == 0

    def test_unexpected_gate_failure_is_counted_as_a_gate_error(
        self, engine, test_table, monkeypatch
    ):
        # A gate that throws on everything must not read as a workload with
        # nothing to flatten.
        monkeypatch.setattr(
            query_combiner_module.SQLAlchemyQueryCombiner,
            "_flatten_verdict",
            staticmethod(lambda q: query_combiner_module._FlattenVerdict.GATE_ERROR),
        )
        query = sa.select(sa.func.count().label("c")).select_from(test_table)
        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, query)
            qc.flush()

        assert cap.done and cap.exc is None
        assert combiner.report.flatten_gate_errors == 1
        assert combiner.report.flatten_rejected == 0

    def test_singleton_groups_are_demoted_to_one_cte_combine(self, engine):
        # A window spanning N tables gives one group each. Flattening them
        # would cost N round trips at identical scan count, where the CTE path
        # needs one.
        md = sa.MetaData()
        tables = [sa.Table(f"st{i}", md, Column("id", Integer)) for i in range(5)]
        md.create_all(engine)

        combiner = _make_combiner(flatten_enabled=True)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [
                _schedule(
                    qc, conn, sa.select(sa.func.count().label(f"c{i}")).select_from(t)
                )
                for i, t in enumerate(tables)
            ]
            qc.flush()

        assert all(c.done and c.exc is None for c in caps)
        assert all(c.result.scalar() == 0 for c in caps)
        assert combiner.report.flatten_singletons == 5
        assert combiner.report.flat_queries_issued == 0
        # The whole window still costs one round trip, as it did flag-off.
        assert combiner.report.combined_queries_issued == 1
        assert combiner.report.query_exceptions == 0

    def test_distinct_budget_counts_columns_not_queries(self, engine, test_table):
        # Two queries with two COUNT(DISTINCT) each are four trees, so a cap
        # of 2 must split them. A per-future cap would see "2 <= 2" and emit
        # one statement holding all four.
        queries = [
            sa.select(
                sa.func.count(sa.func.distinct(test_table.c.id)).label(f"a{i}"),
                sa.func.count(sa.func.distinct(test_table.c.name)).label(f"b{i}"),
            ).select_from(test_table)
            for i in range(2)
        ]
        combiner = _make_combiner(flatten_enabled=True, max_distinct_per_statement=2)
        with engine.connect() as conn, combiner.activate() as qc:
            caps = [_schedule(qc, conn, q) for q in queries]
            qc.flush()

        assert all(c.done and c.exc is None for c in caps)
        assert combiner.report.flat_queries_issued == 2
        assert combiner.report.query_exceptions == 0

    def test_query_exceeding_the_budget_still_executes_alone(self, engine, test_table):
        # A query over budget cannot be split, so it must still run rather
        # than be dropped. Given a partner so the group reaches the packer.
        big = sa.select(
            sa.func.count(sa.func.distinct(test_table.c.id)).label("a"),
            sa.func.count(sa.func.distinct(test_table.c.name)).label("b"),
            sa.func.count(sa.func.distinct(test_table.c.value)).label("c"),
        ).select_from(test_table)
        partner = sa.select(
            sa.func.count(sa.func.distinct(test_table.c.id)).label("d")
        ).select_from(test_table)
        combiner = _make_combiner(flatten_enabled=True, max_distinct_per_statement=1)
        with engine.connect() as conn, combiner.activate() as qc:
            cap = _schedule(qc, conn, big)
            cap_partner = _schedule(qc, conn, partner)
            qc.flush()

        assert cap.done and cap.exc is None
        assert cap_partner.done and cap_partner.exc is None
        # 3 columns in one query > budget of 1, so it forms its own statement
        # rather than being dropped; the partner takes a second.
        assert combiner.report.flat_queries_issued == 2
        assert combiner.report.flatten_singletons == 0
        row = cap.result.one()
        assert (row["a"], row["b"], row["c"]) == (3, 3, 3)
        assert cap_partner.result.scalar() == 3
        assert combiner.report.query_exceptions == 0
