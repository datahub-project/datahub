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

# Cap on the number of COUNT(DISTINCT) aggregates that may coexist in a single
# flattened statement. Each COUNT(DISTINCT) materializes a distinct-value tree
# on the server; letting all of them coexist trades a scan problem for a memory
# problem (see spec §3.7). Cheap aggregates (COUNT/MIN/MAX/AVG/STDDEV) coexist
# freely; distinct-heavy aggregates are split into chunks of at most this many.
# Starting value, pending PR 5 measurement. Overridable via a hidden config
# knob (see SQLAlchemyQueryCombiner.max_distinct_per_statement).
DEFAULT_MAX_DISTINCT_PER_STATEMENT = 5


def _chunked(seq: List[Any], n: int) -> Iterator[List[Any]]:
    # Yield successive n-sized chunks. Makes the ceil(len(seq) / n) claim in
    # _execute_flat_group self-evident.
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


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

    # Flat statements *attempted* under the flatten path. Counts attempts, not
    # successes: the counter increments before execution (consistent with
    # combined_queries_issued), so a flat statement that fails still counts.
    # Read scans_avoided for the success signal — it increments only after
    # extraction succeeds. Distinct from combined_queries_issued, which counts
    # both the legacy CTE path and the flat path so existing assertions stay
    # green when the flag is off.
    flat_queries_issued: int = 0

    # Estimated table scans avoided by flattening. Each flat statement
    # collapses N queued aggregates over one table into one scan, so this
    # counter reads as `sum(len(members) - 1) per flat statement`. Read it
    # alongside combined_queries_issued: flattening trades round trips for
    # scans, so combined_queries_issued rises (2 -> 9 in the mixed batch) while
    # scans fall (~80 -> 9). Without this counter the rise in
    # combined_queries_issued reads as a regression on a round-trip dashboard.
    scans_avoided: int = 0

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
    # max_distinct_per_statement per statement) instead of one CTE per query.
    # Default False — off for everyone. See spec §5 PR 4.
    flatten_enabled: bool = False
    # Cap on the number of COUNT(DISTINCT) aggregates that may coexist in one
    # flat statement, to bound server-side memory. Module-level
    # DEFAULT_MAX_DISTINCT_PER_STATEMENT is the starting value (5); a hidden
    # config knob threads an override through so PR 5 measurement does not
    # cost a release cycle.
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
        # Fail-closed allowlist. Rather than enumerating clauses to reject
        # (a denylist silently misses OFFSET, HAVING, FOR UPDATE,
        # prefix_with("DISTINCT"), suffix_with, with_hint, and fetch() — all
        # of which render extra SQL), rebuild the minimal
        # `SELECT <cols> FROM <froms>` and require it to render identically.
        # Anything carrying additional state renders differently and falls
        # through to the CTE path, so a SQLAlchemy clause we have never
        # heard of is safe by default. HAVING is the reason this must fail
        # closed: the flat path would drop the clause and fabricate a row
        # that must not exist (verified).
        #
        # The subquery().columns access guards item 1: a query with duplicate
        # explicit .label() names makes .columns raise InvalidRequestError.
        # The CTE path hits the same raise and recovers via serial fallback;
        # route there rather than demoting a whole flat batch inside
        # _execute_flat_select.
        #
        # Cost: two str() compiles plus one subquery build per queued query
        # per flush pass. Cheap against a table scan, not free. Both sides
        # use the default dialect, so the str() comparison is sound.
        try:
            rebuilt = sqlalchemy.select(get_query_columns(query))
            for f in query.froms:
                rebuilt.append_from(f)
            if str(query) != str(rebuilt):
                return False
            # Accessing .columns on the subquery raises InvalidRequestError
            # when the query has duplicate explicit .label() names; that
            # routes the query to the CTE path (which recovers via serial
            # fallback) instead of demoting a whole flat batch.
            _ = query.subquery().columns
            return True
        except Exception:
            return False

    @staticmethod
    def _flatten_signature(fut: "_QueryFuture") -> Tuple[Any, ...]:
        # Group key: the FROM clause objects (by identity) plus the connection
        # the future executes on. This function owns the whole key so the next
        # reader cannot fix one half and forget the other.
        #
        # From-clause elements (Table, Subquery, Join) are hashable with
        # identity semantics — hash(t) is stable, and two distinct Table
        # objects that share a name compare unequal and hash differently
        # (verified). So tuple(query.froms) groups by object identity directly,
        # without id()'s only-unique-among-live-objects caveat (a latent hazard
        # for a future reader who caches or defers). Same-name-different-object
        # tables therefore land in separate groups instead of one self-cross-
        # join (FROM t, t) the server would reject.
        #
        # The connection is folded in because _execute_flat_select runs the
        # whole group on members[0].conn; mixing connections in one group
        # would run some futures on the wrong connection.
        return (*fut.query.froms, id(fut.conn))

    @staticmethod
    def _has_count_distinct(query: Any) -> bool:
        # True if any column expression in the query contains a
        # count(distinct(...)) sub-expression. COUNT(DISTINCT) has three
        # spellings in SQLAlchemy, all of which must trip the cap:
        #   sa.func.count(sa.func.distinct(c))  -> FunctionElement name "distinct"
        #   sa.func.count(sa.distinct(c))       -> UnaryExpression, operator distinct_op
        #   sa.func.count(c.distinct())         -> UnaryExpression, operator distinct_op
        # Matching only the first silently bypasses max_distinct_per_statement
        # for the other two — the server-memory failure mode the cap exists to
        # prevent. SELECT-level DISTINCT is excluded by _is_flattenable.
        for col in get_query_columns(query):
            for elem in sqlalchemy.sql.visitors.iterate(col):
                if (
                    isinstance(elem, sqlalchemy.sql.functions.FunctionElement)
                    and elem.name == "distinct"
                ):
                    return True
                if (
                    isinstance(elem, sqlalchemy.sql.elements.UnaryExpression)
                    and elem.operator is sqlalchemy.sql.operators.distinct_op
                ):
                    return True
        return False

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
                unmatched[k] = fut

        # Each sub-unit is independently recoverable. A failing unit does not
        # cancel the others (item 4): a single bad query must not zero out the
        # scan-reduction benefit of the whole batch. Flat groups run first so
        # the common case is not hostage to an exceptional unmatched query.
        # On failure, increment query_exceptions and try a gentler landing
        # (re-route the unit's untouched futures through the CTE path); if
        # that also fails, run THAT UNIT's futures serially via the scoped
        # _execute_futures_serially — NOT the global _execute_queue_fallback,
        # which operates on the whole queue and would demote out-of-window
        # futures that were never attempted and would flatten on the next
        # pass (measured: one failing group demoted all 50 queued futures,
        # scans_avoided 4 -> 0). Every failed unit's futures are resolved
        # here, so no trailing guard is needed — flush()'s greenlet loop
        # cannot spin on futures parked in _handle_execute's done-loop.
        for members in groups.values():
            try:
                self._execute_flat_group(members)
            except Exception as e:
                self.report.query_exceptions += 1
                logger.warning(
                    "Failed to execute flat group; will attempt CTE re-route."
                )
                logger.debug("Failed to execute flat group", exc_info=e)
                group_queue = {k: fut for k, fut in members if not fut.done}
                if group_queue:
                    try:
                        self._execute_cte_combine(group_queue)
                    except Exception as e2:
                        logger.debug(
                            "Flat-group CTE re-route also failed; "
                            "running this group's futures serially",
                            exc_info=e2,
                        )
                        self._execute_futures_serially(
                            [fut for _, fut in members if not fut.done]
                        )

        if unmatched:
            try:
                self._execute_cte_combine(unmatched)
            except Exception as e:
                self.report.query_exceptions += 1
                logger.warning(
                    "Failed to execute unmatched CTE combine; "
                    "will fallback its futures."
                )
                logger.debug("Failed to execute unmatched CTE combine", exc_info=e)
                self._execute_futures_serially(
                    [fut for fut in unmatched.values() if not fut.done]
                )

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
        for chunk in _chunked(distinct_heavy, self.max_distinct_per_statement):
            self._execute_flat_select(chunk)

    def _execute_flat_select(self, members: List[Tuple[str, _QueryFuture]]) -> None:
        # Build one flat SELECT with unique generated labels, execute it, and
        # map results back to each future BY POSITION (not by name — labels can
        # collide for anonymous or duplicate-named aggregates). Each future's
        # result dict is keyed by the names from query.subquery().columns,
        # matching the CTE path's keying exactly so a flag flip does not
        # change result keys (item 6). _is_flattenable has already guaranteed
        # subquery().columns does not raise for these queries.
        labeled_cols: List[Any] = []
        # plan: (future, [original col.name from subquery().columns, ...])
        plan: List[Tuple[_QueryFuture, List[str]]] = []
        for _, fut in members:
            emit_cols = get_query_columns(fut.query)
            # Names come from subquery().columns (the same source the CTE path
            # uses), which anon-labels duplicates so keys stay distinct within
            # a future. Emission columns still come from get_query_columns so
            # emission order and count are unchanged.
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

        # N queued aggregates collapsed into one scan over the same table.
        self.report.scans_avoided += len(members) - 1

    def _execute_futures_serially(self, futures: List["_QueryFuture"]) -> None:
        # Serial fallback scoped to a specific list of futures. Extracted from
        # _execute_queue_fallback so the flatten path can recover a failed unit
        # WITHOUT demoting out-of-window futures: _execute_queue_fallback
        # operates on the whole queue (it is called by flush() when the entire
        # _execute_queue raises), but a failed unit in _execute_queue_flattened
        # must only resolve its own futures — the rest of the batch should
        # still flatten on the next pass. Skip-done contract preserved.
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
