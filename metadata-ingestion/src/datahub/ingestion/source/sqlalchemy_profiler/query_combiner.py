import collections
import contextlib
import dataclasses
import enum
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
from packaging import version
from sqlalchemy.engine import Connection
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from datahub.ingestion.api.report import Report
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)

# The type annotations for SA 1.3.x don't have the __version__ attribute,
# so we need to ignore the error here.
SQLALCHEMY_VERSION = sqlalchemy.__version__  # type: ignore[attr-defined]
IS_SQLALCHEMY_1_4 = version.parse(SQLALCHEMY_VERSION) >= version.parse("1.4.0")


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
_execute_queue(), and forces the whole pending batch -- up to 40 unrelated
queries -- to be re-issued serially.

Tagging is therefore a correctness claim, not a hint. When in doubt, do not tag:
an untagged statement is simply executed on its own.
"""


FLATTENABLE_AGGREGATES_EXECUTION_OPTION = "datahub_flattenable_aggregates"
"""Aggregate names the emitting adapter allows the flatten path to merge.

A frozenset of lowercased function names, set by ProfilingConnection from
PlatformAdapter.FLATTENABLE_AGGREGATES. Per-adapter because spellings vary by dialect.
It is an allowlist, not a verdict: the clause gate in _flatten_verdict can still refuse
a query whose names all qualify. Absent or empty means nothing flattens.
"""


def single_row_query(query: _StatementT) -> _StatementT:
    """Tag a statement as returning exactly one row.

    See SINGLE_ROW_EXECUTION_OPTION for when this is valid.

    execution_options() is generative, so this returns a tagged copy and leaves
    the original untouched. Callers must use the returned value.
    """
    return query.execution_options(  # type: ignore[attr-defined,no-any-return]
        **{SINGLE_ROW_EXECUTION_OPTION: True}
    )


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

    In practice a raw SQL string is the only non-Executable that gets this far.
    SQLAlchemy itself rejects every other kind (None, a Table, an int) with
    ObjectNotExecutableError, so there is nothing extra to report about them. A
    raw string executes fine, simply never batches, and shows up in
    uncombined_queries_in_greenlet like any other unbatched query.
    """
    if not isinstance(query, sqlalchemy.sql.Executable):
        return False
    return bool(query.get_execution_options().get(SINGLE_ROW_EXECUTION_OPTION, False))


# Max COUNT(DISTINCT) columns per flat statement: each builds a distinct-value
# tree on the server, so letting all of them coexist trades a scan problem for
# a memory problem. Not yet measured; overridable via a hidden config knob.
DEFAULT_MAX_DISTINCT_PER_STATEMENT = 5


class _FlattenVerdict(enum.Enum):
    # REJECTED and GATE_ERROR both fall back to the CTE path, but only
    # GATE_ERROR means the gate itself broke. Kept apart so a gate that throws
    # on everything is not mistaken for a workload with nothing to flatten.
    FLATTENABLE = "flattenable"
    REJECTED = "rejected"
    GATE_ERROR = "gate_error"


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


class _ResultProxyFake:
    # This imitates the interface provided by sqlalchemy.engine.result.ResultProxy (sqlalchemy 1.3.x)
    # or sqlalchemy.engine.Result (1.4.x).
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


def get_query_columns(query: Any) -> List[Any]:
    try:
        # inner_columns will be more accurate if the column names are unnamed,
        # since .columns will remove the "duplicates".
        return list(query.inner_columns)
    except AttributeError:
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

    # Why queued queries did not flatten. Only gate_errors is a defect
    # signal; rejected and singletons are the gate working as designed.
    flatten_rejected: int = 0
    flatten_gate_errors: int = 0
    flatten_singletons: int = 0

    query_exceptions: int = 0


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
        query_future = _QueryFuture(conn, query, multiparams, params)
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

        pending_queue = dict(
            itertools.islice(pending_queue.items(), MAX_QUERIES_TO_COMBINE_AT_ONCE)
        )

        if pending_queue:
            if self.flatten_enabled:
                self._execute_queue_flattened(pending_queue)
            else:
                self._execute_cte_combine(pending_queue)

    def _execute_cte_combine(self, pending_queue: Dict[str, _QueryFuture]) -> None:
        # One CTE per query, cross-joined. Unchanged from before the flatten
        # path; also the fallback for queries flattening cannot handle.
        queue_item = next(iter(pending_queue.values()))

        # Actually combine these queries together. We do this by (1) putting
        # each query into its own CTE, (2) selecting all the columns we need
        # and (3) extracting the results once the query finishes.

        ctes = {
            k: query_future.query.cte(k) for k, query_future in pending_queue.items()
        }

        combined_cols = itertools.chain(
            *[
                [
                    col  # .label(self._generate_sql_safe_identifier())
                    for col in get_query_columns(cte)
                ]
                for _, cte in ctes.items()
            ]
        )
        combined_query = sqlalchemy.select(combined_cols)
        for cte in ctes.values():
            combined_query.append_from(cte)

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
        for _, query_future in pending_queue.items():
            query = query_future.query
            if IS_SQLALCHEMY_1_4:
                # On 1.4, it prints a warning if we don't call subquery.
                query = query.subquery()  # type: ignore
            cols = query.columns

            data = {}
            for col in cols:
                data[col.name] = row[index]
                index += 1

            res = _ResultProxyFake([_RowProxyFake(data)])

            query_future.res = res

        # Assert before marking done: a wrong-but-done future is skipped by
        # the recovery paths' `if not fut.done` filters.
        assert index == len(row)
        for _, query_future in pending_queue.items():
            query_future.done = True

    # -- flatten path -------------------------------------------------------

    @staticmethod
    def _is_flattenable(query: Any) -> bool:
        return (
            SQLAlchemyQueryCombiner._flatten_verdict(query)
            is _FlattenVerdict.FLATTENABLE
        )

    @staticmethod
    def _flatten_verdict(query: Any) -> "_FlattenVerdict":
        # Fail closed: rebuild a bare `SELECT <cols> FROM <froms>` and require
        # an identical render, so an unknown clause is rejected by default.
        # HAVING is the dangerous one -- the flat path would drop it and
        # fabricate a row.
        #
        # Limitation: both sides render under the default dialect, so a
        # dialect-scoped construct passes and is then dropped. None today.
        try:
            rebuilt = sqlalchemy.select(get_query_columns(query))
            for f in query.get_final_froms():
                rebuilt.append_from(f)
            if str(query) != str(rebuilt):
                return _FlattenVerdict.REJECTED
            # Adapter's allowlist; absent means empty, so nothing flattens.
            # Matched on name, not type -- upper(v) is also a FunctionElement
            # but returns N rows. Top-level only: count(distinct(c)) is named
            # `count`, and walking deeper would reject every COUNT(DISTINCT).
            allowed = query.get_execution_options().get(
                FLATTENABLE_AGGREGATES_EXECUTION_OPTION, frozenset()
            )
            for col in get_query_columns(query):
                elem = (
                    col.element
                    if isinstance(col, sqlalchemy.sql.elements.Label)
                    else col
                )
                if not (
                    isinstance(elem, sqlalchemy.sql.functions.FunctionElement)
                    and elem.name.lower() in allowed
                ):
                    return _FlattenVerdict.REJECTED
            # Duplicate explicit .label() names make .columns raise. Caught
            # here rather than by the outer handler, because that one counts
            # gate errors and this rejection is by design.
            try:
                name_cols = len(query.subquery().columns)
            except sqlalchemy.exc.InvalidRequestError:
                return _FlattenVerdict.REJECTED
            # Emit-side and name-side column counts must match: a bare
            # sa.text() gives 1 and 0, tripping the assert in
            # _execute_flat_select mid-plan.
            if len(get_query_columns(query)) != name_cols:
                return _FlattenVerdict.REJECTED
            return _FlattenVerdict.FLATTENABLE
        except Exception as e:
            # The gate broke, which is not the same as refusing the query.
            # Counted apart so a gate throwing on everything is visible.
            logger.debug(
                "flatten gate raised; treating query as unflattenable", exc_info=e
            )
            return _FlattenVerdict.GATE_ERROR

    @staticmethod
    def _flatten_signature(fut: "_QueryFuture") -> Tuple[Tuple[Any, ...], Any]:
        # FROM objects plus the connection, by identity (not id(), which is
        # only unique among live objects). Two same-named tables must not
        # merge, or the flat SELECT becomes `FROM t, t`. The connection is in
        # the key because the group runs on members[0].conn.
        return (tuple(fut.query.get_final_froms()), fut.conn)

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
            verdict = self._flatten_verdict(fut.query)
            if verdict is _FlattenVerdict.FLATTENABLE:
                groups[self._flatten_signature(fut)].append((k, fut))
            else:
                if verdict is _FlattenVerdict.GATE_ERROR:
                    self.report.flatten_gate_errors += 1
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
            try:
                self._execute_flat_group(members)
            except Exception as e:
                if not self.serial_execution_fallback_enabled:
                    raise
                self.report.query_exceptions += 1
                logger.warning(
                    f"Failed to execute flat group of {len(members)} queries "
                    f"over {members[0][1].query.get_final_froms()} "
                    f"({type(e).__name__}); will attempt CTE re-route."
                )
                logger.debug("Failed to execute flat group", exc_info=e)
                group_queue = {k: fut for k, fut in members if not fut.done}
                if group_queue:
                    try:
                        self._execute_cte_combine(group_queue)
                    except Exception as e2:
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

        if unmatched:
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
        # One flat SELECT for the cheap aggregates, plus enough more that no
        # statement exceeds max_distinct_per_statement distinct trees.
        cheap: List[Tuple[str, _QueryFuture]] = []
        distinct_heavy: List[Tuple[str, _QueryFuture, int]] = []
        for k, fut in members:
            n_distinct = self._count_distinct_columns(fut.query)
            if n_distinct:
                distinct_heavy.append((k, fut, n_distinct))
            else:
                cheap.append((k, fut))

        if cheap:
            self._execute_flat_select(cheap)
        for chunk in _chunk_by_distinct_budget(
            distinct_heavy, self.max_distinct_per_statement
        ):
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
            assert len(emit_cols) == len(name_cols), (
                "emit/name column count mismatch — _is_flattenable should have "
                "excluded this query"
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
        combined_query = sqlalchemy.select(labeled_cols)
        for f in rep_froms:
            combined_query.append_from(f)

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
        assert len(results) == 1
        row = results[0]

        index = 0
        for fut, names in plan:
            data = {}
            for name in names:
                data[name] = row[index]
                index += 1
            fut.res = _ResultProxyFake([_RowProxyFake(data)])

        # Verify that we consumed all the columns before marking futures
        # done — a failing assert would otherwise leave wrong-but-done
        # futures that the `if not fut.done` re-route filter then skips.
        assert index == len(row)
        for fut, _ in plan:
            fut.done = True

        # N queued aggregates collapsed into one scan over the same table.
        self.report.scans_avoided += len(members) - 1

    def _execute_futures_serially(self, futures: List["_QueryFuture"]) -> None:
        # Scoped to specific futures, so a failed flat group resolves only its
        # own. The skip-done guard is load-bearing for the whole-queue caller,
        # which can be handed an already-done queue -- do not delete it as
        # redundant just because the flatten path pre-filters.
        for query_future in futures:
            if query_future.done:
                continue

            query_id = SQLAlchemyQueryCombiner._generate_query_id()
            self.report.uncombined_queries_issued += 1

            logger.info(f"[{query_id}] Executing fallback query")
            logger.debug(f"[{query_id}] SQL: {str(query_future.query)}")

            with PerfTimer() as timer:
                try:
                    res = _sa_execute_underlying_method(
                        query_future.conn,
                        query_future.query,
                        *query_future.multiparams,
                        **query_future.params,
                    )

                    # The actual execute method returns a CursorResult on SQLAlchemy 1.4.x
                    # and a ResultProxy on SQLAlchemy 1.3.x. Both interfaces are shimmed
                    # by _ResultProxyFake.
                    query_future.res = cast(_ResultProxyFake, res)

                    logger.info(
                        f"[{query_id}] Fallback query executed in {timer.elapsed_seconds():.3f}s"
                    )
                except Exception as e:
                    query_future.exc = e
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

        assert len(self._get_queue(main_greenlet)) == 0
