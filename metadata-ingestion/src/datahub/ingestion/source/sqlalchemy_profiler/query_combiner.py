import collections
import contextlib
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
import sqlalchemy.sql
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
    only on how the query was built, so it surfaces on the first run in dev or
    CI rather than varying with data. Raised rather than counted for that
    reason -- there is nothing to monitor, only something to fix.

    In production catch_exceptions is on, so this degrades to executing the
    query on its own (correct, just unbatched) with a warning and a bump to
    report.query_exceptions.
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
            queue_item = next(iter(pending_queue.values()))

            # Actually combine these queries together. We do this by (1) putting
            # each query into its own CTE, (2) selecting all the columns we need
            # and (3) extracting the results once the query finishes.

            ctes = {
                k: query_future.query.cte(k)
                for k, query_future in pending_queue.items()
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
