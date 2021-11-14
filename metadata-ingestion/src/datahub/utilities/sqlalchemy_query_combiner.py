import collections
import contextlib
import dataclasses
import itertools
import logging
import random
import string
import threading
import traceback
import unittest.mock
from typing import Any, Callable, ClassVar, Dict, Iterator, List, Optional, Set, Tuple

import greenlet
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.sql
from sqlalchemy.engine import Connection
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from typing_extensions import ParamSpec

logger: logging.Logger = logging.getLogger(__name__)

P = ParamSpec("P")


def _generate_sql_safe_identifier() -> str:
    # See https://stackoverflow.com/a/30779367/5004662.
    return "".join(random.choices(string.ascii_lowercase, k=16))


class _RowProxyFake(collections.OrderedDict):
    def __getitem__(self, k):  # type: ignore
        if isinstance(k, int):
            k = list(self.keys())[k]
        return super().__getitem__(k)


RowType = _RowProxyFake


class _ResultProxyFake:
    # This imitates the interface provided by sqlalchemy.engine.result.ResultProxy.
    # Adapted from https://github.com/rajivsarvepalli/mock-alchemy/blob/2eba95588e7693aab973a6d60441d2bc3c4ea35d/src/mock_alchemy/mocking.py#L213

    def __init__(self, result: List[RowType]) -> None:
        self._result = result

    def fetchall(self) -> List[RowType]:
        return self._result

    def __iter__(self) -> Iterator[RowType]:
        return iter(self._result)

    def count(self) -> int:
        return len(self._result)

    def first(self) -> Optional[RowType]:
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
            try:
                return row[0]
            except TypeError:
                return row
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


@dataclasses.dataclass
class SQLAlchemyQueryCombiner:
    # TODO this must be a singleton, should not be set up per-thread
    # at minimum the activate method should check for other activations and
    # bail out if already active.

    # Without the staticmethod decorator, Python thinks that this is an instance
    # method and attempts to bind self to it.
    _underlying_sa_execute_method = staticmethod(sqlalchemy.engine.Connection.execute)

    # TODO refactor this into argument
    _allowed_single_row_query_methods: ClassVar = [
        (
            "great_expectations/dataset/sqlalchemy_dataset.py",
            {
                "get_row_count",
                "get_column_min",
                "get_column_max",
                "get_column_mean",
                # "get_column_median",  # This actually returns two rows, not a single row.
                "get_column_stdev",
                "get_column_stdev",
                "get_column_nonnull_count",
                "get_column_unique_count",
                # TODO document this and also figure out when it would actually work
                # "inner_wrapper",
            },
        ),
    ]

    enabled: bool
    catch_exceptions: bool

    # There will be one main greenlet per thread. As such, queries will be
    # queued according to the main greenlet's thread ID. We also keep track
    # of the greenlets we spawn for bookkeeping purposes.
    _queries_by_thread: Dict[
        greenlet.greenlet, Dict[str, _QueryFuture]
    ] = dataclasses.field(default_factory=dict)
    _greenlets_by_thread: Dict[
        greenlet.greenlet, Set[greenlet.greenlet]
    ] = dataclasses.field(default_factory=lambda: collections.defaultdict(set))
    _thread_unsafe_operation_lock = dataclasses.field(
        default_factory=lambda: threading.Lock()
    )

    def _is_single_row_query_method(
        self, stack: traceback.StackSummary, query: Any
    ) -> bool:
        # We'll do this the inefficient way since the arrays are pretty small.
        for frame in stack:
            for file_suffix, allowed_methods in self._allowed_single_row_query_methods:
                if not frame.filename.endswith(file_suffix):
                    continue
                if frame.name in allowed_methods:
                    return True
        return False

    def _get_main_greenlet(self) -> greenlet.greenlet:
        let = greenlet.getcurrent()
        while let.parent is not None:
            let = let.parent
        return let

    def _get_queue(self, main_greenlet: greenlet.greenlet) -> Dict[str, _QueryFuture]:
        assert main_greenlet.parent is None

        with self._thread_unsafe_operation_lock:
            return self._queries_by_thread.setdefault(main_greenlet, {})

    def _get_greenlet_pool(
        self, main_greenlet: greenlet.greenlet
    ) -> Set[greenlet.greenlet]:
        assert main_greenlet.parent is None

        with self._thread_unsafe_operation_lock:
            return self._greenlets_by_thread[main_greenlet]

    def _handle_execute(
        self, conn: Connection, query: Any, multiparams: Any, params: Any
    ) -> Tuple[bool, Any]:
        # Returns True with result if the query was handled, False if it
        # should be executed normally using the fallback method.

        # TODO remove this
        # if str(query).startswith("SELECT count(*) AS element_count, sum(CASE WHEN"):
        #     breakpoint()

        if not self.enabled:
            return False, None

        # Must handle synchronously if the query was issued from the main greenlet.
        main_greenlet = self._get_main_greenlet()
        if greenlet.getcurrent() == main_greenlet:
            return False, None

        # Don't attempt to handle if these are set.
        if multiparams or params:
            return False, None

        # Attempt to match against the known single-row query methods.
        stack = traceback.extract_stack()
        if not self._is_single_row_query_method(stack, query):
            return False, None

        # Figure out how many columns this query returns.
        # This also implicitly ensures that the typing is generally correct.
        if not hasattr(query, "columns"):
            return False, None
        assert len(query.columns) > 0

        # Add query to the queue.
        queue = self._get_queue(main_greenlet)
        query_id = _generate_sql_safe_identifier()
        query_future = _QueryFuture(conn, query, multiparams, params)
        queue[query_id] = query_future

        # Yield control back to the main greenlet until the query is done.
        # We assume that the main greenlet will be the one that actually executes the query.
        while not query_future.done:
            main_greenlet.switch()

        del queue[query_id]
        return True, query_future.res

    @contextlib.contextmanager
    def activate(self) -> Iterator["SQLAlchemyQueryCombiner"]:
        def _sa_execute_fake(
            conn: Connection, query: Any, *args: Any, **kwargs: Any
        ) -> Any:
            try:
                handled, result = self._handle_execute(conn, query, args, kwargs)
            except Exception as e:
                if not self.catch_exceptions:
                    raise e
                logger.exception(
                    f"Failed to execute query normally, using fallback: {str(query)}"
                )
                return self._underlying_sa_execute_method(conn, query, *args, **kwargs)
            else:
                if handled:
                    logger.info(f"Query was handled: {str(query)}")
                    return result
                else:
                    logger.info(f"Executing query normally: {str(query)}")
                    return self._underlying_sa_execute_method(
                        conn, query, *args, **kwargs
                    )

        with unittest.mock.patch(
            "sqlalchemy.engine.Connection.execute", _sa_execute_fake
        ):
            yield self

    def run(self, method: Callable[[], None]) -> None:
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
        if pending_queue:
            queue_item = next(iter(pending_queue.values()))

            # Actually combine these queries together. We do this by (1) putting
            # each query into its own CTE, (2) selecting all the columns we need
            # and (3) extracting the results once the query finishes.

            ctes = {
                k: query_future.query.cte(k)
                for k, query_future in pending_queue.items()
            }

            # TODO: determine if we need to use col.label() here.
            combined_cols = itertools.chain(
                *[[col for col in cte.columns] for _, cte in ctes.items()]
            )
            combined_query = sqlalchemy.select(combined_cols)
            for cte in ctes.values():
                combined_query.append_from(cte)

            logger.info(f"Executing combined query: {str(combined_query)}")
            sa_res = self._underlying_sa_execute_method(queue_item.conn, combined_query)

            row = sa_res.fetchone()
            # TODO verify that only one row is returned

            index = 0
            for _, query_future in pending_queue.items():
                cols = query_future.query.columns

                data = {}
                for col in cols:
                    data[col.name] = row[index]
                    index += 1

                res = _ResultProxyFake([_RowProxyFake(data)])

                query_future.res = res
                query_future.done = True

            # Verify that we consumed all the columns.
            assert index == len(row)

        # for query_future in full_queue.values():
        #     if query_future.done:
        #         continue

        #     sa_res = self._underlying_sa_execute_method(
        #         query_future.conn,
        #         query_future.query,
        #         *query_future.multiparams,
        #         **query_future.params,
        #     )

        #     data = [_RowProxyFake(row) for row in sa_res.fetchall()]
        #     res = _ResultProxyFake(data)

        #     query_future.res = res
        #     query_future.done = True

    def flush(self) -> None:
        # Executes until the queue and pool are empty.

        if not self.enabled:
            return

        main_greenlet = self._get_main_greenlet()
        pool = self._get_greenlet_pool(main_greenlet)

        while pool:
            self._execute_queue(main_greenlet)

            for let in list(pool):
                if let.dead:
                    pool.remove(let)
                else:
                    let.switch()

        assert len(self._get_queue(main_greenlet)) == 0
