import contextlib
import dataclasses
import logging
import traceback
import unittest.mock
from typing import Any, Callable, ClassVar, Dict, Iterator, List, Tuple

import greenlet
import sqlalchemy.engine
from sqlalchemy.engine import Connection
from typing_extensions import ParamSpec

logger: logging.Logger = logging.getLogger(__name__)

P = ParamSpec("P")  # type: ignore


@dataclasses.dataclass
class _QueryFuture:
    conn: Connection
    query: Any
    multiparams: Any
    params: Any

    done: bool = False
    res: Any = None


@dataclasses.dataclass
class SQLAlchemyQueryCombiner:
    # TODO this must be a singleton, should not be set up per-thread

    # Without the staticmethod decorator, Python thinks that this is an instance
    # method and attempts to bind self to it.
    _underlying_sa_execute_method: ClassVar = staticmethod(
        sqlalchemy.engine.Connection.execute
    )

    # TODO refactor this into argument
    _allowed_single_row_query_methods: ClassVar = [
        (
            "great_expectations/dataset/sqlalchemy_dataset.py",
            {
                "get_row_count",
                "get_column_min",
                "get_column_max",
                "get_column_mean",
                "get_column_median",
                "get_column_stdev",
                "get_column_stdev",
                "get_column_nonnull_count",
                "get_column_unique_count",
            },
        ),
    ]

    catch_exceptions: bool

    # There will be one main greenlet per thread. As such, queries will be
    # queued according to the main greenlet's thread ID.
    _queries_by_thread: Dict[greenlet.greenlet, List[_QueryFuture]] = {}

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

    def _get_queue(self, main_greenlet: greenlet.greenlet) -> List[_QueryFuture]:
        assert main_greenlet.parent is None

        # Because of the GIL, this operation is thread-safe. Hence, we can
        # just add the main greenlet here without any special consideration.
        # https://stackoverflow.com/a/6953515/5004662
        # https://docs.python.org/3/glossary.html#term-global-interpreter-lock

        return self._queries_by_thread.setdefault(main_greenlet, [])

    def _handle_execute(
        self, conn: Connection, query: Any, multiparams: Any, params: Any
    ) -> Tuple[bool, Any]:
        # Returns True with result if the query was handled, False if it
        # should be executed normally using the fallback method.

        # Must handle synchronously if the query was issued from the main greenlet.
        main_greenlet = self._get_main_greenlet()
        if greenlet.getcurrent() == main_greenlet:
            breakpoint()
            return False, None

        # Don't attempt to handle if these are set.
        if multiparams or params:
            return False, None

        # Attempt to match against the known single-row query methods.
        stack = traceback.extract_stack()
        if not self._is_single_row_query_method(stack, query):
            return False, None

        # Figure out how many columns this query returns.
        # TODO add escape hatch
        if not hasattr(query, "columns"):
            return False, None
        columns = list(query.columns)
        assert len(columns) > 0

        # Add query to the queue.
        queue = self._get_queue(main_greenlet)
        query_future = _QueryFuture(conn, query, multiparams, params)
        queue.append(query_future)

        # Yield control back to the main greenlet until the query is done.
        # We assume that the main greenlet will be the one that actually executes the query.
        while not query_future.done:
            main_greenlet.switch()

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

    # mypy does not yet support ParamSpec. See https://github.com/python/mypy/issues/8645.
    def run(self, method: Callable[P, None], *args: P.args, **kwargs: P.kwargs) -> None:  # type: ignore
        pass
