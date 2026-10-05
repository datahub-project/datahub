import collections
import contextlib
import copy
import dataclasses
import itertools
import logging
import random
import string
import threading
import unittest.mock
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    cast,
)

import greenlet
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.sql
import sqlalchemy.sql.elements
import sqlalchemy.sql.functions
import sqlalchemy.sql.operators
import sqlalchemy.sql.visitors
from sqlalchemy.engine import Connection
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from datahub.ingestion.api.report import Report
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)

MAX_QUERIES_TO_COMBINE_AT_ONCE = 40

_StatementT = TypeVar("_StatementT")

SINGLE_ROW_EXECUTION_OPTION = "datahub_single_row"
"""Statement execution option marking a query as returning exactly one row.

Set it with single_row_query(); read it with is_single_row_query().

WHAT TO TAG: only a statement that returns exactly one row for every possible
database state. In practice that means a bare aggregate (COUNT / MIN / MAX /
AVG / STDDEV / MEDIAN) over a table, with no GROUP BY, no LIMIT / OFFSET and no
row-filtering WHERE.

WHY IT MATTERS: SQLAlchemyQueryCombiner folds tagged statements into a single
round-trip by wrapping each one in a CTE and cross-joining up to
MAX_QUERIES_TO_COMBINE_AT_ONCE of them. That transform is only valid when every
CTE yields exactly one row. A tagged statement that returns zero rows (a
filtered catalog lookup that misses) or two rows (an OFFSET/LIMIT window)
collapses or multiplies the join, trips the row-count assertion in
_execute_queue(), and forces the whole pending batch -- every unrelated query
combined with it -- to be re-issued serially.

Tagging is therefore a correctness claim, not a hint. When in doubt, do not tag:
an untagged statement is simply executed on its own.
"""


FLATTENABLE_EXECUTION_OPTION = "datahub_flattenable"
"""Marks a statement the flatten path may merge with others over the same table.

Set only by ProfilingConnection.execute_aggregate, which builds the query itself, so
the tag cannot be attached to something carrying a clause. Absent means no flattening.
"""


GATE_EXECUTION_OPTION = "datahub_gate"
"""Marks a statement whose failure makes the rest of its batch pointless.

The row count is one: if a table cannot be counted it cannot be read at all, so
retrying its columns one at a time just produces one failure per column. When a
combined statement fails, _execute_futures_serially runs gates first and, if a
gate still fails alone, gives the same error to the rest without issuing them.
"""


def flattenable_query(query: _StatementT) -> _StatementT:
    """Tag a statement as safe to merge into a flat SELECT."""
    return query.execution_options(  # type: ignore[attr-defined,no-any-return]
        **{FLATTENABLE_EXECUTION_OPTION: True}
    )


def gate_query(query: _StatementT) -> _StatementT:
    """Tag a statement the rest of its batch is pointless without.

    See GATE_EXECUTION_OPTION.
    """
    return query.execution_options(  # type: ignore[attr-defined,no-any-return]
        **{GATE_EXECUTION_OPTION: True}
    )


def single_row_query(query: _StatementT) -> _StatementT:
    """Tag a statement as returning exactly one row.

    See SINGLE_ROW_EXECUTION_OPTION for when this is valid.

    execution_options() is generative, so this returns a tagged copy and leaves
    the original untouched. Callers must use the returned value.
    """
    return query.execution_options(  # type: ignore[attr-defined,no-any-return]
        **{SINGLE_ROW_EXECUTION_OPTION: True}
    )


class FlatResultMappingError(AssertionError):
    """The flat statement's columns did not line up with the queued futures.

    Raised rather than asserted because these checks guard result mapping, and
    `python -O` strips asserts -- which would mark misaligned values as done.
    Subclasses AssertionError so existing handlers treat it as they did the
    asserts: the group handler catches it and re-routes through the CTE path.
    """


class MisTaggedQueryError(AssertionError):
    """A query was tagged single-row but the SQL says otherwise.

    A programming error at the call site, not a runtime condition: it depends
    only on how the query was built, so it does not vary with data. Raised
    rather than counted for that reason -- there is nothing to monitor, only
    something to fix.

    How it actually surfaces, which is not by crashing a profiling run:

    - With catch_exceptions on (the production default), _sa_execute_fake
      catches it before it can reach the caller, executes the query on its own
      and bumps report.query_exceptions. Profiling results stay correct; only
      batching is lost. The integration test asserting query_exceptions == 0
      is what turns this into a CI failure.
    - With catch_exceptions off, it propagates to FutureResult.result(). Note
      that the profiler wraps every result() call in `except Exception` and
      converts it to a report warning, so even then a run does not abort -- the
      mistake shows up as a warning naming the fix. Unit tests calling result()
      directly are the only place it is observed as a raised exception.
    """


def is_single_row_query(query: Any) -> bool:
    """Whether a statement carries the single-row tag and may be combined.

    Total by design: this is called on whatever reached Connection.execute, so
    it answers False for a non-statement rather than raising.

    SQLAlchemy 2.0 rejects every non-Executable (a raw string, None, a Table)
    with ObjectNotExecutableError, so the isinstance check is only a guard. An
    untagged text() clause executes fine, simply never batches, and shows up in
    uncombined_queries_in_greenlet like any other unbatched query.
    """
    if not isinstance(query, sqlalchemy.sql.Executable):
        return False
    return bool(query.get_execution_options().get(SINGLE_ROW_EXECUTION_OPTION, False))


# Max COUNT(DISTINCT) columns per flat statement: each builds a distinct-value
# tree on the server, so letting all of them coexist trades a scan problem for
# a memory problem. Not yet measured; overridable via a hidden config knob.
DEFAULT_MAX_DISTINCT_PER_STATEMENT = 5


def _chunk_by_distinct_budget(
    members: List[Tuple[str, "_QueryFuture", int]], budget: int
) -> Iterator[List[Tuple[str, "_QueryFuture"]]]:
    # A single future over budget still gets its own statement rather than
    # being dropped. budget < 1 raises so a misconfigured cap re-routes through
    # the CTE path instead of silently yielding nothing.
    if budget < 1:
        raise ValueError(f"distinct budget must be >= 1, got {budget}")
    chunk: List[Tuple[str, "_QueryFuture"]] = []
    used = 0
    for k, fut, n in members:
        if chunk and used + n > budget:
            yield chunk
            chunk = []
            used = 0
        chunk.append((k, fut))
        used += n
    if chunk:
        yield chunk


# We need to make sure that only one query combiner attempts to patch
# the SQLAlchemy execute method at a time so that they don't interfere.
# Generally speaking, there will only be one query combiner in existence
# at a time anyways, so this lock shouldn't really be doing much.
_sa_execute_method_patching_lock = threading.Lock()
_sa_execute_underlying_method = sqlalchemy.engine.Connection.execute


class _RowProxyFake(collections.OrderedDict):
    def __getitem__(self, k):  # type: ignore
        if isinstance(k, int):
            keys = list(self.keys())
            if k >= len(keys):
                raise IndexError(
                    f"Row has {len(keys)} columns, cannot access index {k}"
                )
            k = keys[k]
        return super().__getitem__(k)


def _skip_like(cause: Exception) -> Exception:
    """A per-future copy of `cause`, so skipping cannot grow one traceback.

    Each skipped future's result() re-raises what it is given, and re-raising a
    single shared object appends a frame every time -- hundreds on a wide
    unreadable table, logged in full at debug level. A shallow copy keeps the
    type and message that callers match on.

    Never raises: this runs inside the recovery path, and failing to prettify a
    traceback must not abandon the futures it was resolving. Reconstructing the
    type from .args does throw for some drivers, hence the copy.
    """
    try:
        skipped = copy.copy(cause)
        skipped.__traceback__ = None
        skipped.__cause__ = cause
        return skipped
    except Exception:
        return cause


class _EmptyTableSkip(Exception):
    """A gate reported an exact row count of 0, so its batch was not issued.

    Not a failure: the profiler emits no field profiles for an empty table, so
    the column queries it had queued would have been thrown away. Kept distinct
    from a real gate failure, which means the table could not be read at all.
    """


def _buffer_one_row(res: Any) -> "_ResultProxyFake":
    """Read a single-row result into memory so it can be read again."""
    rows = res.fetchall()
    return _ResultProxyFake([_RowProxyFake(dict(row._mapping)) for row in rows])


class _ResultProxyFake:
    # This imitates the subset of sqlalchemy.engine.CursorResult that the
    # profiler reads from combined-query results.
    # Adapted from https://github.com/rajivsarvepalli/mock-alchemy/blob/2eba95588e7693aab973a6d60441d2bc3c4ea35d/src/mock_alchemy/mocking.py#L213

    def __init__(self, result: List[_RowProxyFake]) -> None:
        self._result = result

    def fetchall(self) -> List[_RowProxyFake]:
        return self._result

    def __iter__(self) -> Iterator[_RowProxyFake]:
        return iter(self._result)

    def first(self) -> Optional[_RowProxyFake]:
        return next(iter(self._result), None)

    def one(self) -> Any:
        if len(self._result) == 1:
            return self._result[0]
        elif self._result:
            raise MultipleResultsFound("Multiple rows returned for one()")
        else:
            raise NoResultFound("No rows returned for one()")

    def one_or_none(self) -> Optional[Any]:
        if len(self._result) == 1:
            return self._result[0]
        elif self._result:
            raise MultipleResultsFound("Multiple rows returned for one_or_none()")
        else:
            return None

    def scalar(self) -> Any:
        if len(self._result) == 1:
            row = self._result[0]
            if len(row) == 0:
                # Row exists but has no columns (empty result)
                return None
            return row[0]
        elif self._result:
            raise MultipleResultsFound(
                "Multiple rows were found when exactly one was required"
            )
        return None

    def update(self) -> None:
        # No-op.
        pass

    def close(self) -> None:
        # No-op.
        pass

    all = fetchall
    fetchone = one


@dataclasses.dataclass
class _QueryFuture:
    conn: Connection
    query: sqlalchemy.sql.Select
    multiparams: Any
    params: Any

    done: bool = False
    res: Optional[_ResultProxyFake] = None
    exc: Optional[Exception] = None
    # See GATE_EXECUTION_OPTION.
    is_gate: bool = False


def get_query_columns(query: Any) -> List[Any]:
    # On SQLAlchemy 2.0 a Select exposes `selected_columns`; a CTE/subquery
    # exposes its columns via `.columns`.
    cols = getattr(query, "selected_columns", None)
    if cols is not None:
        return list(cols)
    return list(query.columns)


@dataclasses.dataclass
class SQLAlchemyQueryCombinerReport(Report):
    total_queries: int = 0
    uncombined_queries_issued: int = 0

    combined_queries_issued: int = 0
    queries_combined: int = 0

    # Queries issued inside a greenlet scheduled via run() that were not tagged
    # single-row, so they cost a round-trip of their own.
    #
    # Non-zero is normal, not a defect: a scheduled method may legitimately
    # issue a multi-row query -- get_column_median's OFFSET/LIMIT fallback on
    # platforms without a native MEDIAN, or get_estimated_row_count's filtered
    # catalog lookup. This measures how much batching a given platform and
    # config actually achieve, which cannot be known statically. A mis-tag, by
    # contrast, raises MisTaggedQueryError rather than landing here.
    uncombined_queries_in_greenlet: int = 0

    # Flat statements attempted; incremented before execution, so a failed one
    # still counts. scans_avoided is the success signal.
    flat_queries_issued: int = 0

    # Table scans avoided: sum(len(members) - 1) per flat statement. Read with
    # combined_queries_issued -- flattening trades round trips for scans, so
    # that counter can rise while scans fall.
    scans_avoided: int = 0

    # Why queued queries did not flatten. rejected = not built by
    # execute_aggregate, so untaggable. singletons = alone in its FROM group.
    flatten_rejected: int = 0
    flatten_singletons: int = 0

    # Recovery ladder, each rung strictly worse than the one above.
    # failures - cte_recoveries is the number of groups that ended up serial,
    # where the flat path costs round trips instead of saving scans.
    flat_group_failures: int = 0
    flat_group_cte_recoveries: int = 0
    flat_group_serial_fallbacks: int = 0

    # Queries never issued because a gate in their batch failed alone, so the
    # table could not be read at all. See GATE_EXECUTION_OPTION.
    queries_skipped_after_gate: int = 0

    # Queries never issued because the table's exact row count came back 0, so
    # their results would have been discarded. Not a failure; counted apart
    # from queries_skipped_after_gate, which means "could not be read".
    queries_skipped_empty_table: int = 0

    query_exceptions: int = 0

    # Rollbacks before a retry that raised. Non-zero means the retried queries
    # likely ran in an aborted transaction, so their failures are collateral.
    rollback_failures: int = 0


@dataclasses.dataclass
class SQLAlchemyQueryCombiner:
    """
    This class adds support for dynamically combining multiple SQL queries into
    a single query. Specifically, it can combine queries which each return a
    single row. It uses greenlets to manage the execution lifecycle of the queries.

    Only statements tagged with single_row_query() are combined; anything else
    is executed on its own. See SINGLE_ROW_EXECUTION_OPTION for what qualifies
    and why a wrong tag is expensive.
    """

    enabled: bool
    catch_exceptions: bool
    serial_execution_fallback_enabled: bool
    # Partition the queue by FROM signature and emit one flat SELECT per
    # group instead of one CTE per query. Off for everyone by default.
    flatten_enabled: bool = False
    # See DEFAULT_MAX_DISTINCT_PER_STATEMENT.
    max_distinct_per_statement: int = DEFAULT_MAX_DISTINCT_PER_STATEMENT

    # The Python GIL ensures that modifications to the report's counters
    # are safe.
    report: SQLAlchemyQueryCombinerReport = dataclasses.field(
        default_factory=SQLAlchemyQueryCombinerReport
    )

    # There will be one main greenlet per thread. As such, queries will be
    # queued according to the main greenlet's thread ID. We also keep track
    # of the greenlets we spawn for bookkeeping purposes.
    _queries_by_thread_lock: threading.Lock = dataclasses.field(
        default_factory=lambda: threading.Lock()
    )
    _greenlets_by_thread_lock: threading.Lock = dataclasses.field(
        default_factory=lambda: threading.Lock()
    )
    _queries_by_thread: Dict[greenlet.greenlet, Dict[str, _QueryFuture]] = (
        dataclasses.field(default_factory=lambda: collections.defaultdict(dict))
    )
    _greenlets_by_thread: Dict[greenlet.greenlet, Set[greenlet.greenlet]] = (
        dataclasses.field(default_factory=lambda: collections.defaultdict(set))
    )
    # The gate failure, if any, for the flush currently running on each main
    # greenlet. Scoped to the flush and cleared at its start: a main greenlet
    # profiles one table after another, and an unreadable table must not
    # suppress the next one. See GATE_EXECUTION_OPTION.
    _gate_failure_by_thread: Dict[greenlet.greenlet, Exception] = dataclasses.field(
        default_factory=dict
    )

    @staticmethod
    def _generate_sql_safe_identifier() -> str:
        # The value of k=16 should be more than enough to ensure uniqueness.
        # Adapted from https://stackoverflow.com/a/30779367/5004662.
        return "".join(random.choices(string.ascii_lowercase, k=16))

    @staticmethod
    def _generate_query_id() -> str:
        # Short 5-character ID for correlating query execution logs.
        return "".join(random.choices(string.ascii_lowercase + string.digits, k=5))

    def _get_main_greenlet(self) -> greenlet.greenlet:
        let = greenlet.getcurrent()
        while let.parent is not None:
            let = let.parent
        return let

    def _get_queue(self, main_greenlet: greenlet.greenlet) -> Dict[str, _QueryFuture]:
        assert main_greenlet.parent is None

        with self._queries_by_thread_lock:
            return self._queries_by_thread.setdefault(main_greenlet, {})

    def _get_greenlet_pool(
        self, main_greenlet: greenlet.greenlet
    ) -> Set[greenlet.greenlet]:
        assert main_greenlet.parent is None

        with self._greenlets_by_thread_lock:
            return self._greenlets_by_thread[main_greenlet]

    def _handle_execute(
        self, conn: Connection, query: Any, multiparams: Any, params: Any
    ) -> Tuple[bool, Optional[_QueryFuture]]:
        # Returns True with result if the query was handled, False if it
        # should be executed normally using the fallback method.

        if not self.enabled:
            return False, None

        # Must handle synchronously if the query was issued from the main greenlet.
        main_greenlet = self._get_main_greenlet()
        if greenlet.getcurrent() == main_greenlet:
            return False, None

        # It's unclear what the expected behavior of the query combiner should
        # be if the query has one of these set. As such, we'll just serialize these
        # queries for now. This clause was not hit during my testing and probably
        # doesn't do anything, but it's better to ensure correct behavior.
        if multiparams or params:
            return False, None

        # Only statements explicitly tagged as returning exactly one row can be
        # folded into the CTE cross-join. Reaching here means the caller
        # scheduled this via run() but did not tag it, so batching is lost.
        if not is_single_row_query(query):
            # Not a mistake in itself: a scheduled method may legitimately need
            # a multi-row query (see the counter's definition). It just cannot
            # join the batch.
            self.report.uncombined_queries_in_greenlet += 1
            return False, None

        # Trust, but verify. The tag is a claim about row shape; if the SQL
        # contradicts it outright, the call site is wrong. Guarded with getattr
        # so a future SQLAlchemy bump degrades to no-veto rather than raising on
        # a renamed internal.
        #
        # Deliberately partial. These four clauses are the ones that *provably*
        # break the exactly-one-row guarantee, so vetoing them cannot produce a
        # false positive -- which matters now that a veto raises. A WHERE clause
        # is the notable omission: it is what made get_estimated_row_count
        # return zero rows, but it cannot be vetoed, because
        # `SELECT count(*) ... WHERE x` returns exactly one row and so does a
        # lookup on a unique key. Telling those apart needs to know whether the
        # column list is aggregate, which is undecidable here: five adapters
        # build their median with sa.literal_column, an opaque string. The
        # zero-row shape is caught by tests and by review of the call site, not
        # by this veto.
        if isinstance(query, sqlalchemy.sql.Select) and (
            getattr(query, "_limit_clause", None) is not None
            or getattr(query, "_offset_clause", None) is not None
            or getattr(query, "_group_by_clauses", None)
            or getattr(query, "_distinct", False)
        ):
            raise MisTaggedQueryError(
                "This query is tagged as returning exactly one row, but it has a "
                "LIMIT, OFFSET, GROUP BY or DISTINCT clause, so it cannot. Fix the "
                "call site to use execute_rows() instead of execute_single_row(). "
                f"Query: {query}"
            )

        # Figure out how many columns this query returns.
        # This also implicitly ensures that the typing is generally correct.
        try:
            assert len(get_query_columns(query)) > 0
        except AttributeError as e:
            logger.debug(
                f"Query of type: '{type(query)}' does not contain attributes required by 'get_query_columns()'. AttributeError: {e}"
            )
            return False, None

        # Add query to the queue.
        queue = self._get_queue(main_greenlet)
        query_id = SQLAlchemyQueryCombiner._generate_sql_safe_identifier()
        query_future = _QueryFuture(
            conn,
            query,
            multiparams,
            params,
            is_gate=bool(
                query.get_execution_options().get(GATE_EXECUTION_OPTION, False)
            ),
        )
        queue[query_id] = query_future
        self.report.queries_combined += 1

        # Yield control back to the main greenlet until the query is done.
        # We assume that the main greenlet will be the one that actually executes the query.
        while not query_future.done:
            main_greenlet.switch()

        del queue[query_id]
        return True, query_future

    @contextlib.contextmanager
    def activate(self) -> Iterator["SQLAlchemyQueryCombiner"]:
        def _sa_execute_fake(
            conn: Connection, query: Any, *args: Any, **kwargs: Any
        ) -> Any:
            try:
                self.report.total_queries += 1
                handled, result = self._handle_execute(conn, query, args, kwargs)
            except Exception as e:
                if not self.catch_exceptions:
                    raise e
                logger.warning(
                    f"Failed to execute query normally, using fallback: {str(query)}"
                )
                logger.debug("Failed to execute query normally", exc_info=e)
                self.report.query_exceptions += 1
                return _sa_execute_underlying_method(conn, query, *args, **kwargs)
            else:
                if handled:
                    logger.debug(f"Query was handled: {str(query)}")
                    assert result is not None
                    if result.exc is not None:
                        raise result.exc
                    return result.res
                else:
                    logger.debug(f"Executing query normally: {str(query)}")
                    self.report.uncombined_queries_issued += 1
                    return _sa_execute_underlying_method(conn, query, *args, **kwargs)

        with (
            _sa_execute_method_patching_lock,
            unittest.mock.patch(
                "sqlalchemy.engine.Connection.execute", _sa_execute_fake
            ),
        ):
            yield self

    def run(self, method: Callable[[], None]) -> None:
        """
        Run a method inside of a greenlet. The method is guaranteed to have finished
        after a call to flush() returns.
        """

        if self.enabled:
            let = greenlet.greenlet(method)

            pool = self._get_greenlet_pool(self._get_main_greenlet())
            pool.add(let)

            let.switch()
        else:
            # If not enabled, run immediately.
            method()

    def _execute_queue(self, main_greenlet: greenlet.greenlet) -> None:
        full_queue = self._get_queue(main_greenlet)

        pending_queue = {k: v for k, v in full_queue.items() if not v.done}

        # A gate that already failed on its own means the table cannot be read
        # at all, so the queries still queued for it would each fail in turn.
        # The gate sits in the first chunk, and its future is gone from the
        # queue by now, so the failure is remembered for the flush instead.
        gate_exc = self._gate_failure_by_thread.get(main_greenlet)
        if gate_exc is not None:
            for fut in pending_queue.values():
                fut.exc = _skip_like(gate_exc)
                fut.done = True
                self._count_skip(gate_exc)
            return

        # Gates first, so the window that is attempted always contains them --
        # otherwise this holds only because the profiler schedules them first.
        pending_queue = dict(
            itertools.islice(
                sorted(pending_queue.items(), key=lambda kv: not kv[1].is_gate),
                MAX_QUERIES_TO_COMBINE_AT_ONCE,
            )
        )

        if pending_queue:
            if self.flatten_enabled:
                self._execute_queue_flattened(pending_queue)
            else:
                try:
                    self._execute_cte_combine(pending_queue)
                except Exception as e:
                    # Recover only this chunk, as the flatten path does. Letting
                    # it reach flush() would fall back the whole queue, so one
                    # bad column would serialize every other query for the table.
                    if not self.serial_execution_fallback_enabled:
                        raise
                    self.report.query_exceptions += 1
                    logger.warning(
                        f"Failed to execute combined query of "
                        f"{len(pending_queue)} queries ({type(e).__name__}); "
                        f"will run them one at a time."
                    )
                    logger.debug("Failed to execute combined query", exc_info=e)
                    self._execute_futures_serially(
                        [fut for fut in pending_queue.values() if not fut.done]
                    )

    def _execute_cte_combine(self, pending_queue: Dict[str, _QueryFuture]) -> None:
        # Two or more queries are combined by putting each into its own CTE and
        # cross-joining them, then extracting each one's columns back out of the
        # single result row. A lone query is issued as written -- there is
        # nothing to cross-join, and the wrapper would only make the server
        # materialize a one-row result. This is also the fallback path for
        # queries that flattening cannot handle.
        queue_item = next(iter(pending_queue.values()))

        # Columns to read each query's results back from, by queue key. Taken
        # from a CTE or subquery rather than the original query because on SA
        # 2.0 the original may hold unlabeled BindParameters with no .name;
        # wrapping always yields stable string names, in the same order.
        if len(pending_queue) == 1:
            # Nothing to cross-join, and a one-member CTE only makes the server
            # materialize the query. Issue it as written.
            key = next(iter(pending_queue))
            combined_query = queue_item.query
            cols_by_key = {key: list(get_query_columns(queue_item.query.subquery()))}
        else:
            ctes = {
                k: query_future.query.cte(k)
                for k, query_future in pending_queue.items()
            }
            cols_by_key = {k: list(get_query_columns(cte)) for k, cte in ctes.items()}

            combined_cols = list(
                itertools.chain.from_iterable(cols_by_key[k] for k in ctes)
            )
            # SA 2.0 removed the list form of select() and Select.append_from().
            combined_query = sqlalchemy.select(*combined_cols)
            for cte in ctes.values():
                combined_query = combined_query.select_from(cte)

        query_id = SQLAlchemyQueryCombiner._generate_query_id()
        self.report.combined_queries_issued += 1
        logger.info(
            f"[{query_id}] Executing combined query ({len(pending_queue)} queries combined)"
        )
        logger.debug(f"[{query_id}] SQL: {str(combined_query)}")
        with PerfTimer() as timer:
            sa_res = _sa_execute_underlying_method(queue_item.conn, combined_query)

        logger.info(
            f"[{query_id}] Combined query executed in {timer.elapsed_seconds():.3f}s"
        )

        # Fetch the results and ensure that exactly one row is returned.
        results = sa_res.fetchall()
        assert len(results) == 1
        row = results[0]

        # Extract the results into a result for each query.
        index = 0
        for k, query_future in pending_queue.items():
            data = {}
            for col in cols_by_key[k]:
                data[col.name] = row[index]
                index += 1

            query_future.res = _ResultProxyFake([_RowProxyFake(data)])

        # Assert before marking done: a wrong-but-done future is skipped by
        # the recovery paths' `if not fut.done` filters.
        assert index == len(row)
        for _, query_future in pending_queue.items():
            query_future.done = True
        self._note_empty_gate(pending_queue.values())

    # -- flatten path -------------------------------------------------------

    @staticmethod
    def _is_flattenable(query: Any) -> bool:
        # Only ProfilingConnection.execute_aggregate sets this, and it builds
        # the statement itself, so a tagged query cannot carry a clause.
        return bool(
            query.get_execution_options().get(FLATTENABLE_EXECUTION_OPTION, False)
        )

    @staticmethod
    def _flatten_signature(fut: "_QueryFuture") -> Tuple[Tuple[Any, ...], Any, bool]:
        # FROM objects and the connection, by identity (not id(), which is only
        # unique among live objects): two same-named tables must not merge, or
        # the flat SELECT becomes `FROM t, t`, and the group runs on
        # members[0].conn. Distinct-heavy queries are keyed apart because they
        # end up in separate statements anyway, and hiding that from the
        # singleton demotion costs a round trip.
        return (
            tuple(fut.query.get_final_froms()),
            fut.conn,
            bool(SQLAlchemyQueryCombiner._count_distinct_columns(fut.query)),
        )

    @staticmethod
    def _count_distinct_columns(query: Any) -> int:
        # COUNT(DISTINCT) has three SQLAlchemy spellings -- func.distinct(c) is
        # a FunctionElement, sa.distinct(c) and c.distinct() are
        # UnaryExpressions. Missing one bypasses the cap silently.
        total = 0
        for col in get_query_columns(query):
            for elem in sqlalchemy.sql.visitors.iterate(col):
                if (
                    isinstance(elem, sqlalchemy.sql.functions.FunctionElement)
                    and elem.name.lower() == "distinct"
                ) or (
                    isinstance(elem, sqlalchemy.sql.elements.UnaryExpression)
                    and elem.operator is sqlalchemy.sql.operators.distinct_op
                ):
                    total += 1
                    break
        return total

    def _execute_queue_flattened(self, pending_queue: Dict[str, _QueryFuture]) -> None:
        # Partition the capped pending queue into flatten groups (by FROM
        # signature) plus an unmatched subset.
        groups: Dict[Any, List[Tuple[str, _QueryFuture]]] = collections.defaultdict(
            list
        )
        unmatched: Dict[str, _QueryFuture] = {}
        for k, fut in pending_queue.items():
            if self._is_flattenable(fut.query):
                groups[self._flatten_signature(fut)].append((k, fut))
            else:
                self.report.flatten_rejected += 1
                unmatched[k] = fut

        # A one-member group saves no scans and costs a round trip per group
        # where the CTE path needs one for all (measured: 40 tables -> 1
        # statement flag-off, 40 flag-on).
        for sig in [sig for sig, members in groups.items() if len(members) == 1]:
            k, fut = groups.pop(sig)[0]
            self.report.flatten_singletons += 1
            unmatched[k] = fut

        # Each unit recovers independently: flat -> CTE re-route -> serial.
        # Scoped, not the global _execute_queue_fallback, which would demote
        # futures never attempted (measured: scans_avoided 4 -> 0).
        for members in groups.values():
            if self._gate_failure_by_thread.get(self._get_main_greenlet()) is not None:
                # An earlier group's gate failed alone, so this table cannot be
                # read. The unmatched block below checks the same record and is
                # skipped too; _execute_queue then resolves what is left.
                break
            # Precomputed: a diagnostic string must not be able to raise inside
            # the except and skip the recovery it is announcing.
            froms = members[0][1].query.get_final_froms()
            try:
                self._execute_flat_group(members)
            except Exception as e:
                # Counted before the raise, so a disabled fallback still shows.
                self.report.flat_group_failures += 1
                if not self.serial_execution_fallback_enabled:
                    raise
                self.report.query_exceptions += 1
                logger.warning(
                    f"Failed to execute flat group of {len(members)} queries "
                    f"over {froms} ({type(e).__name__}); will attempt CTE "
                    f"re-route."
                )
                logger.debug("Failed to execute flat group", exc_info=e)
                group_queue = {k: fut for k, fut in members if not fut.done}
                if group_queue:
                    # Without a rollback the failed flat query leaves Postgres/
                    # Redshift in an aborted transaction (25P02), so the CTE
                    # re-route would always fail too.
                    self._rollback_quietly(members[0][1].conn)
                    try:
                        self._execute_cte_combine(group_queue)
                        self.report.flat_group_cte_recoveries += 1
                    except Exception as e2:
                        self.report.flat_group_serial_fallbacks += 1
                        # Warning, not debug: the first failure already warned,
                        # so a silent second one reads as a successful recovery.
                        logger.warning(
                            f"Flat-group CTE re-route also failed for "
                            f"{len(group_queue)} queries ({type(e2).__name__}); "
                            f"running them serially."
                        )
                        logger.debug(
                            "Flat-group CTE re-route also failed",
                            exc_info=e2,
                        )
                        self._execute_futures_serially(
                            [fut for _, fut in members if not fut.done]
                        )

        if (
            unmatched
            and self._gate_failure_by_thread.get(self._get_main_greenlet()) is None
        ):
            try:
                self._execute_cte_combine(unmatched)
            except Exception as e:
                if not self.serial_execution_fallback_enabled:
                    raise
                self.report.query_exceptions += 1
                logger.warning(
                    f"Failed to execute unmatched CTE combine "
                    f"({type(e).__name__}); will fallback its futures."
                )
                logger.debug("Failed to execute unmatched CTE combine", exc_info=e)
                self._execute_futures_serially(
                    [fut for fut in unmatched.values() if not fut.done]
                )

    def _execute_flat_group(self, members: List[Tuple[str, _QueryFuture]]) -> None:
        # The group is homogeneous by signature, so it is either all cheap --
        # one flat SELECT -- or all distinct-heavy, in which case it is split
        # so no statement exceeds max_distinct_per_statement distinct trees.
        sized = [
            (k, fut, self._count_distinct_columns(fut.query)) for k, fut in members
        ]
        if not sized[0][2]:
            self._execute_flat_select(members)
            return
        for chunk in _chunk_by_distinct_budget(sized, self.max_distinct_per_statement):
            self._execute_flat_select(chunk)

    def _execute_flat_select(self, members: List[Tuple[str, _QueryFuture]]) -> None:
        # Map back BY POSITION -- labels collide across anonymous aggregates.
        # Keys come from subquery().columns, as on the CTE path, so flipping
        # the flag cannot change result keys.
        labeled_cols: List[Any] = []
        # plan: (future, [original col.name from subquery().columns, ...])
        plan: List[Tuple[_QueryFuture, List[str]]] = []
        for _, fut in members:
            emit_cols = get_query_columns(fut.query)
            # Names from subquery().columns, which anon-labels duplicates;
            # emission still from get_query_columns, so order is unchanged.
            name_cols = fut.query.subquery().columns
            if len(emit_cols) != len(name_cols):
                raise FlatResultMappingError(
                    "emit/name column count mismatch; this group re-routes to "
                    "the CTE path"
                )
            names: List[str] = []
            for col in emit_cols:
                uid = self._generate_sql_safe_identifier()
                labeled_cols.append(col.label(uid))
            for col in name_cols:
                names.append(col.name)
            plan.append((fut, names))

        # All members share the same FROM by signature; use one representative
        # so we append exactly one table and avoid a cross-join.
        rep_froms = members[0][1].query.get_final_froms()
        combined_query = sqlalchemy.select(*labeled_cols)
        for f in rep_froms:
            combined_query = combined_query.select_from(f)

        query_id = SQLAlchemyQueryCombiner._generate_query_id()
        self.report.combined_queries_issued += 1
        self.report.flat_queries_issued += 1
        logger.info(
            f"[{query_id}] Executing flat query ({len(members)} queries flattened)"
        )
        logger.debug(f"[{query_id}] SQL: {str(combined_query)}")
        with PerfTimer() as timer:
            sa_res = _sa_execute_underlying_method(members[0][1].conn, combined_query)

        logger.info(
            f"[{query_id}] Flat query executed in {timer.elapsed_seconds():.3f}s"
        )

        results = sa_res.fetchall()
        if len(results) != 1:
            raise FlatResultMappingError(
                f"flat query returned {len(results)} rows, expected exactly 1"
            )
        row = results[0]

        index = 0
        for fut, names in plan:
            data = {}
            for name in names:
                data[name] = row[index]
                index += 1
            fut.res = _ResultProxyFake([_RowProxyFake(data)])

        # Check before marking done: a wrong-but-done future is skipped by the
        # recovery paths' `if not fut.done` filters, so it would keep the wrong
        # value rather than being re-run.
        if index != len(row):
            raise FlatResultMappingError(
                f"consumed {index} of {len(row)} columns; results would be misaligned"
            )
        for fut, _ in plan:
            fut.done = True
        self._note_empty_gate(fut for fut, _ in plan)

        # N queued aggregates collapsed into one scan over the same table.
        self.report.scans_avoided += len(members) - 1

    def _note_empty_gate(self, futures: Iterable["_QueryFuture"]) -> None:
        """Record an exact row count of 0 so later windows are not issued.

        The skip in _execute_futures_serially only covers a batch that failed.
        A table that is simply empty succeeds, and without this every remaining
        window is issued and its results thrown away -- free on a row store,
        but one statement each on an engine billed per query. The result is a
        buffered _ResultProxyFake, so reading it here does not consume it.
        """
        for fut in futures:
            if fut.is_gate and fut.res is not None and fut.res.scalar() == 0:
                self._gate_failure_by_thread[self._get_main_greenlet()] = (
                    _EmptyTableSkip()
                )

    def _count_skip(self, exc: Exception) -> None:
        if isinstance(exc, _EmptyTableSkip):
            self.report.queries_skipped_empty_table += 1
        else:
            self.report.queries_skipped_after_gate += 1

    def _rollback_quietly(self, conn: Connection) -> None:
        # SA 2.0 has no autocommit, so after a failed statement e.g. Postgres/
        # Redshift return 25P02 ("current transaction is aborted") for every
        # later statement until a rollback. Everything the combiner runs is a
        # read-only profiling SELECT whose results are already materialized, so
        # rolling back loses nothing.
        try:
            conn.rollback()
        except Exception as rollback_err:
            self.report.rollback_failures += 1
            # Warn once per combiner: this runs before every fallback query, so
            # a dead connection would otherwise emit one warning per query. The
            # counter carries the total.
            if self.report.rollback_failures == 1:
                logger.warning(
                    f"Rollback before retrying queries failed "
                    f"({type(rollback_err).__name__}: {rollback_err}); retried "
                    f"queries may fail on an aborted transaction. Further "
                    f"rollback failures are counted in rollback_failures."
                )
            else:
                logger.debug(f"Rollback before retrying queries failed: {rollback_err}")

    def _execute_futures_serially(self, futures: List["_QueryFuture"]) -> None:
        # Scoped to specific futures, so a failed flat group resolves only its
        # own. The skip-done guard is load-bearing for the whole-queue caller,
        # which can be handed an already-done queue -- do not delete it as
        # redundant just because the flatten path pre-filters.
        # Gates first, so a table that cannot be read at all costs one failure
        # per gate rather than one per column. Only this call's own gate is
        # tracked: a gate that failed earlier in the flush stops every caller
        # before it gets here -- _execute_queue checks the record on entry, the
        # flatten group loop breaks on it, and the unmatched block skips on it.
        # See GATE_EXECUTION_OPTION.
        gate_exc: Optional[Exception] = None
        for query_future in sorted(futures, key=lambda f: not f.is_gate):
            if query_future.done:
                continue

            if gate_exc is not None:
                # A fresh instance per future: sharing one object lets its
                # traceback grow by a frame per skip, and the profiler logs it.
                query_future.exc = _skip_like(gate_exc)
                query_future.done = True
                self._count_skip(gate_exc)
                continue

            query_id = SQLAlchemyQueryCombiner._generate_query_id()
            self.report.uncombined_queries_issued += 1

            logger.info(f"[{query_id}] Executing fallback query")
            logger.debug(f"[{query_id}] SQL: {str(query_future.query)}")

            # The failed combined query (or a preceding fallback query) may have
            # left the transaction aborted.
            self._rollback_quietly(query_future.conn)

            with PerfTimer() as timer:
                try:
                    res = _sa_execute_underlying_method(
                        query_future.conn,
                        query_future.query,
                        *query_future.multiparams,
                        **query_future.params,
                    )

                    if query_future.is_gate:
                        # Buffer rather than read through: the profiler reads
                        # this same result afterwards, and a consumed
                        # CursorResult either yields None -- which get_row_count
                        # turns into 0, silently dropping every field profile --
                        # or raises ResourceClosedError.
                        buffered = _buffer_one_row(res)
                        query_future.res = buffered
                        if buffered.scalar() == 0:
                            # An exact count of 0 means the profiler discards the
                            # column results anyway, so do not issue them.
                            gate_exc = _EmptyTableSkip()
                            self._gate_failure_by_thread[self._get_main_greenlet()] = (
                                gate_exc
                            )
                    else:
                        # CursorResult's interface is shimmed by _ResultProxyFake.
                        query_future.res = cast(_ResultProxyFake, res)

                    logger.info(
                        f"[{query_id}] Fallback query executed in {timer.elapsed_seconds():.3f}s"
                    )
                except Exception as e:
                    query_future.exc = e
                    if query_future.is_gate:
                        # Nothing else queued for this table can succeed either.
                        gate_exc = e
                        self._gate_failure_by_thread[self._get_main_greenlet()] = e
                    logger.warning(
                        f"[{query_id}] Fallback query failed in {timer.elapsed_seconds():.3f}s "
                        f"({type(e).__name__})"
                    )
                finally:
                    query_future.done = True

    def _execute_queue_fallback(self, main_greenlet: greenlet.greenlet) -> None:
        # flush() calls this when the entire _execute_queue raises; it falls
        # back the whole queue. Per-unit recovery in the flatten path uses the
        # scoped _execute_futures_serially directly.
        full_queue = self._get_queue(main_greenlet)
        self._execute_futures_serially(list(full_queue.values()))

    def flush(self) -> None:
        """Executes until the queue and pool are empty."""

        if not self.enabled:
            return

        main_greenlet = self._get_main_greenlet()
        pool = self._get_greenlet_pool(main_greenlet)
        self._gate_failure_by_thread.pop(main_greenlet, None)

        while pool:
            try:
                self._execute_queue(main_greenlet)
            except Exception as e:
                if not self.serial_execution_fallback_enabled:
                    raise e
                logger.warning(
                    "Failed to execute queue using combiner, will fallback to execute one by one."
                )
                logger.debug("Failed to execute queue using combiner", exc_info=e)
                self.report.query_exceptions += 1
                self._execute_queue_fallback(main_greenlet)

            for let in list(pool):
                if let.dead:
                    pool.remove(let)
                else:
                    let.switch()

        # Not just on entry: the record holds an exception, and keeping it
        # alive until this thread's next flush keeps its frames alive too.
        self._gate_failure_by_thread.pop(main_greenlet, None)
        assert len(self._get_queue(main_greenlet)) == 0
