import collections
import contextlib
import dataclasses
import itertools
import logging
import random
import string
import threading
import unittest.mock
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, cast

import greenlet
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.sql
import sqlalchemy.sql.functions
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

# Cap on the number of COUNT(DISTINCT) aggregates that may coexist in a single
# flattened statement. Each COUNT(DISTINCT) materializes a distinct-value tree
# on the server; letting all of them coexist trades a scan problem for a memory
# problem (see spec §3.7). Cheap aggregates (COUNT/MIN/MAX/AVG/STDDEV) coexist
# freely; distinct-heavy aggregates are split into chunks of at most this many.
# Starting value, pending PR 5 measurement.
MAX_DISTINCT_PER_STATEMENT = 5


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

    # Flat statements issued under the flatten path. Distinct from
    # combined_queries_issued, which counts both the legacy CTE path and the
    # flat path so existing assertions stay green when the flag is off.
    flat_queries_issued: int = 0

    query_exceptions: int = 0


@dataclasses.dataclass
class SQLAlchemyQueryCombiner:
    """
    This class adds support for dynamically combining multiple SQL queries into
    a single query. Specifically, it can combine queries which each return a
    single row. It uses greenlets to manage the execution lifecycle of the queries.
    """

    enabled: bool
    catch_exceptions: bool
    is_single_row_query_method: Callable[[Any], bool]
    serial_execution_fallback_enabled: bool
    # When True, _execute_queue partitions the pending queue by FROM signature
    # and emits one flat SELECT per group (with COUNT(DISTINCT) capped at
    # MAX_DISTINCT_PER_STATEMENT per statement) instead of one CTE per query.
    # Default False — off for everyone. See spec §5 PR 4.
    flatten_enabled: bool = False

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

        # Attempt to match against the known single-row query methods.
        if not self.is_single_row_query_method(query):
            return False, None

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
        # The legacy combine path: one CTE per queued query, all appended into
        # one SELECT. Each CTE is an independent aggregate over the same
        # table, so the DB scans the table once per CTE. Preserved unchanged
        # for the flag-off path and as the fallback for queries the flatten
        # path cannot handle (those with WHERE/GROUP BY/ORDER BY/LIMIT).
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
            query_future.done = True

        # Verify that we consumed all the columns.
        assert index == len(row)

    # -- flatten path -------------------------------------------------------

    @staticmethod
    def _is_flattenable(query: Any) -> bool:
        # Conservative signature: only queries with no WHERE / GROUP BY /
        # ORDER BY / LIMIT / DISTINCT are flattenable. These are exactly the
        # profiling aggregate shapes (sa.select([agg]).select_from(table)).
        # Anything with a clause falls through to the CTE path unchanged,
        # avoiding the risk of comparing WHERE clauses for equality (saved
        # for a general follow-up).
        #
        # Note: on legacy Select, _group_by_clause / _order_by_clause are
        # ClauseList objects that always exist (len 0 when empty) rather than
        # None, so we check emptiness via len(). whereclause is a public
        # property that returns None when there is no WHERE clause.
        try:
            if query.whereclause is not None:
                return False
            if len(getattr(query, "_group_by_clause", ())) > 0:
                return False
            if len(getattr(query, "_order_by_clause", ())) > 0:
                return False
            if getattr(query, "_limit", None) is not None:
                return False
            if getattr(query, "_distinct", False):
                return False
            return True
        except (AttributeError, TypeError):
            return False

    @staticmethod
    def _flatten_signature(query: Any) -> Tuple[str, ...]:
        # Group key: the rendered FROM clause(s). Two queries on the same
        # table render identically and group together; _execute_flat_select
        # then appends one representative's froms, so no cross-join is
        # introduced even if two different Table objects share a name.
        return tuple(str(f) for f in query.froms)

    @staticmethod
    def _has_count_distinct(query: Any) -> bool:
        # True if any column expression in the query contains a
        # count(distinct(...)) sub-expression. We detect the `distinct`
        # function element anywhere in the expression tree — that is the
        # memory-heavy aggregate the MAX_DISTINCT_PER_STATEMENT cap governs
        # (spec §3.7). SELECT-level DISTINCT is excluded by _is_flattenable.
        for col in get_query_columns(query):
            for elem in sqlalchemy.sql.visitors.iterate(col, {}):
                if (
                    isinstance(elem, sqlalchemy.sql.functions.FunctionElement)
                    and elem.name == "distinct"
                ):
                    return True
        return False

    def _execute_queue_flattened(self, pending_queue: Dict[str, _QueryFuture]) -> None:
        # Partition the capped pending queue into flatten groups (by FROM
        # signature) plus an unmatched subset. Unmatched futures and any
        # group that fails go through the legacy CTE path.
        groups: Dict[Tuple[str, ...], List[Tuple[str, _QueryFuture]]] = (
            collections.defaultdict(list)
        )
        unmatched: Dict[str, _QueryFuture] = {}
        for k, fut in pending_queue.items():
            if self._is_flattenable(fut.query):
                groups[self._flatten_signature(fut.query)].append((k, fut))
            else:
                unmatched[k] = fut

        if unmatched:
            self._execute_cte_combine(unmatched)

        for members in groups.values():
            self._execute_flat_group(members)

    def _execute_flat_group(self, members: List[Tuple[str, _QueryFuture]]) -> None:
        # Split a flatten group into cheap and distinct-heavy aggregates and
        # emit one flat SELECT for the cheap ones plus ceil(n / K) flat SELECTs
        # for the distinct-heavy ones, each carrying at most K distinct trees.
        cheap: List[Tuple[str, _QueryFuture]] = []
        distinct_heavy: List[Tuple[str, _QueryFuture]] = []
        for k, fut in members:
            if self._has_count_distinct(fut.query):
                distinct_heavy.append((k, fut))
            else:
                cheap.append((k, fut))

        if cheap:
            self._execute_flat_select(cheap)
        for i in range(0, len(distinct_heavy), MAX_DISTINCT_PER_STATEMENT):
            self._execute_flat_select(
                distinct_heavy[i : i + MAX_DISTINCT_PER_STATEMENT]
            )

    def _execute_flat_select(self, members: List[Tuple[str, _QueryFuture]]) -> None:
        # Build one flat SELECT with unique generated labels, execute it, and
        # map results back to each future BY POSITION (not by name — labels can
        # collide for anonymous or duplicate-named aggregates). Each future's
        # result dict is still keyed by the original col.name for caller
        # compatibility (callers access by label like "rowcount").
        labeled_cols: List[Any] = []
        # plan: (future, [original col.name, ...]) in emission order
        plan: List[Tuple[_QueryFuture, List[str]]] = []
        for _, fut in members:
            cols = get_query_columns(fut.query)
            names: List[str] = []
            for col in cols:
                uid = self._generate_sql_safe_identifier()
                labeled_cols.append(col.label(uid))
                names.append(col.name)
            plan.append((fut, names))

        # All members share the same FROM by signature; use one representative
        # so we append exactly one table and avoid a cross-join.
        rep_froms = members[0][1].query.froms
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
            fut.done = True

        # Verify that we consumed all the columns.
        assert index == len(row)

    def _execute_queue_fallback(self, main_greenlet: greenlet.greenlet) -> None:
        full_queue = self._get_queue(main_greenlet)

        for _, query_future in full_queue.items():
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
