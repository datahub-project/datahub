"""Unit tests for SQLAlchemyQueryCombiner — pins current behavior in isolation.

Exercises the combiner directly (not via QueryCombinerRunner) against a real
in-memory SQLite engine. Follows the repo testing philosophy: behavior over
implementation, no exact-error-message assertions.
"""

import dataclasses
import logging
from typing import Any, Dict, Optional

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, String, create_engine
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sqlalchemy_profiler import (
    query_combiner as query_combiner_module,
)
from datahub.ingestion.source.sqlalchemy_profiler.query_combiner import (
    MAX_QUERIES_TO_COMBINE_AT_ONCE,
    MisTaggedQueryError,
    SQLAlchemyQueryCombiner,
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


def _schedule(
    qc: SQLAlchemyQueryCombiner,
    conn: Connection,
    query: Any,
    multiparams: Any = (),
    combinable: bool = True,
) -> _Capture:
    """Schedule a query on the combiner.

    Tags the query as single-row by default, since most tests here exercise
    batching. Pass combinable=False to schedule an untagged query.
    """
    cap = _Capture()
    if combinable:
        query = single_row_query(query)

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

    def test_duplicate_labels_across_queries_do_not_collide(self, engine, test_table):
        # Two separate queries, each with a single column labelled 'v', combine
        # into one statement. Each query gets its own result dict, so identical
        # labels across queries must not collide. This is the case that matters
        # for the flattening change.
        q_min = sa.select(sa.func.min(test_table.c.value).label("v")).select_from(
            test_table
        )
        q_max = sa.select(sa.func.max(test_table.c.value).label("v")).select_from(
            test_table
        )
        combiner = _make_combiner()
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
            cap = _schedule(qc, conn, "SELECT 1", combinable=False)
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
