import concurrent.futures
import multiprocessing
import pathlib
import threading
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, Optional, Set, Tuple, Union

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.sql_parsing.schema_resolver import (
    SchemaResolver,
    SchemaResolverInterface,
)
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult, sqlglot_lineage
from datahub.sql_parsing.sqlglot_utils import try_format_query


class ParallelParserUnavailable(Exception):
    """Raised when the parallel parser process pool cannot be created.

    The caller is expected to catch this and fall back to serial parsing.
    """


# Allowed types for ParseTask/ParseOutcome correlation keys.  The key crosses
# the process boundary (pickle), so only simple, unconditionally-picklable
# types are permitted.
TaskKey = Optional[Union[str, int, Tuple[Union[str, int], ...]]]


def _validate_task_key(key: Any) -> None:
    """Raise TypeError if *key* is not a valid task key (None, str, int, or a
    tuple whose elements are all str or int)."""
    if key is None:
        return
    if isinstance(key, (str, int)):
        return
    if isinstance(key, tuple):
        if not all(isinstance(el, (str, int)) for el in key):
            raise TypeError(
                f"ParseTask/ParseOutcome.key tuple elements must all be str or int "
                f"(got {[type(el).__name__ for el in key if not isinstance(el, (str, int))]})"
            )
        return
    raise TypeError(
        f"ParseTask/ParseOutcome.key must be None, str, int, or tuple of str/int "
        f"(got {type(key).__name__}); it must be picklable to cross the worker "
        f"process boundary."
    )


@dataclass(frozen=True)
class ParseTask:
    """A single query to parse. ``key`` is an opaque correlation id the caller
    uses to match the resulting :class:`ParseOutcome` back to its source, since
    results are returned out of order."""

    key: TaskKey
    query: str
    default_db: Optional[str]
    default_schema: Optional[str]
    override_dialect: Optional[str] = None
    generate_column_lineage: bool = True

    def __post_init__(self) -> None:
        # ``key`` is shipped across the process boundary, so a non-picklable key
        # would only surface later as an opaque broken pool. Restrict it to the
        # simple picklable types the correlation-id use actually needs, and fail
        # loudly at construction instead.
        _validate_task_key(self.key)


@dataclass(frozen=True)
class ParseOutcome:
    """The result of parsing a single :class:`ParseTask`.

    There are three possible states:

    * ``result`` is non-None, ``error`` is None — the worker ran to completion
      and produced a result (even if ``result.debug_info`` records a normal
      parse failure, the worker itself succeeded).
    * ``result`` is None, ``error`` is non-None — the worker itself raised or
      its process died; the caller decides how to handle it.
    * ``result`` is None, ``error`` is None — the worker ran but produced
      nothing (e.g. an empty outcome). This is treated as a failure:
      :attr:`failed` returns True and :attr:`ok` returns False.

    ``result`` and ``error`` are never both set. ``formatted_query`` is only
    meaningful on a successful outcome (``error`` is None); it is always None
    when ``error`` is set.

    Callers should test :attr:`failed` (or :attr:`ok`) rather than
    re-checking the individual fields by hand.
    """

    key: TaskKey
    result: Optional[SqlParsingResult]
    error: Optional[str]
    # The query formatted in-worker (byte-identical to try_format_query on the
    # main thread). Populated only when the worker was told to format; left None
    # when formatting is disabled or the format step raised, in which case the
    # main thread formats as it does on the serial path.
    formatted_query: Optional[str] = None

    def __post_init__(self) -> None:
        if self.result is not None and self.error is not None:
            raise ValueError("ParseOutcome cannot have both a result and an error set.")
        if self.error is not None and self.formatted_query is not None:
            raise ValueError("formatted_query must be None when error is set.")
        _validate_task_key(self.key)

    @property
    def failed(self) -> bool:
        """True if the worker did not produce a usable result.

        This covers two distinct sub-cases:
        * ``error`` is set — the worker raised or its process died.
        * ``result`` is None and ``error`` is also None — the worker ran but
          produced nothing.

        Normal parse failures captured inside ``result.debug_info`` are NOT
        failures at this level — the worker ran to completion, so ``failed``
        is False."""
        return self.error is not None or self.result is None

    @property
    def ok(self) -> bool:
        return not self.failed


# ---------------------------------------------------------------------------
# Worker side. These are module-level so the spawn start method can pickle them.
# ---------------------------------------------------------------------------

# Populated once per worker process by _worker_init, then reused for every task
# that worker handles. Process-local, so no cross-process sharing concerns.
_WORKER_RESOLVER: Optional[SchemaResolverInterface] = None
# The platform and format toggle are also process-local, set once per worker so
# _worker_parse can format the query in-worker (moving that serial-bottleneck
# work off the main thread).
_WORKER_PLATFORM: Optional[str] = None
_WORKER_FORMAT_QUERIES: bool = False


def _worker_init(
    snapshot_path: pathlib.Path,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    graph_config: Optional[DatahubClientConfig],
    format_queries: bool,
) -> None:
    global _WORKER_RESOLVER, _WORKER_PLATFORM, _WORKER_FORMAT_QUERIES
    _WORKER_PLATFORM = platform
    _WORKER_FORMAT_QUERIES = format_queries
    if graph_config is not None:
        # Rebuild a live graph client inside the worker from the picklable config
        # so cache-miss lazy hydration works in-worker exactly like the serial
        # path. The live DataHubGraph object itself is not safely picklable, so it
        # is constructed here rather than passed across the process boundary.
        #
        # Use for_worker: a two-tier resolver reads the bulk of schemas from the
        # shared read-only snapshot but keeps graph-hydrated results (and None-miss
        # dedup) in a small writable overlay. load_readonly's cache is read-only,
        # so its writes are no-ops and graph-hydrated lineage would be lost.
        # Import DataHubGraph here (not at module level) for two reasons: it is a
        # heavyweight client that should not be loaded in every worker process when
        # graph_config is None, and the live client object is not safely picklable
        # so it must be constructed inside the worker rather than passed across the
        # process boundary.
        from datahub.ingestion.graph.client import DataHubGraph

        resolver = SchemaResolver.for_worker(
            snapshot_path,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=DataHubGraph(graph_config),
        )
    else:
        # No graph → no hydration, no writes: the lowest-memory read-only path.
        resolver = SchemaResolver.load_readonly(
            snapshot_path,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
        )
    _WORKER_RESOLVER = resolver


def _worker_parse(task: ParseTask) -> ParseOutcome:
    assert _WORKER_RESOLVER is not None, "worker resolver not initialized"
    try:
        result = sqlglot_lineage(
            task.query,
            schema_resolver=_WORKER_RESOLVER,
            default_db=task.default_db,
            default_schema=task.default_schema,
            override_dialect=task.override_dialect,
            generate_column_lineage=task.generate_column_lineage,
        )
        # Format in-worker so the (otherwise serial) formatting work is spread
        # across the pool. try_format_query is pure and deterministic in
        # (query, platform), so this is byte-identical to formatting on the main
        # thread. A formatting failure must not fail the parse — fall back to
        # None and let the main thread format as it does on the serial path.
        formatted_query: Optional[str] = None
        if _WORKER_FORMAT_QUERIES:
            assert _WORKER_PLATFORM is not None
            try:
                formatted_query = try_format_query(task.query, _WORKER_PLATFORM)
            except Exception:
                # Defensive catch: try_format_query has raises=False today and
                # won't propagate, but this guards against a future contract
                # change. On failure the main thread reformats — no data loss.
                formatted_query = None
        # Normal parse failures are already captured inside result.debug_info,
        # so they are returned as a result, not an error.
        return ParseOutcome(
            key=task.key,
            result=result,
            error=None,
            formatted_query=formatted_query,
        )
    except Exception as e:
        # A worker-side exception becomes an error outcome rather than killing
        # the pool.
        return ParseOutcome(key=task.key, result=None, error=repr(e))


# ---------------------------------------------------------------------------
# Parent side.
# ---------------------------------------------------------------------------


@dataclass
class ParallelSqlParser(Closeable):
    """Parses SQL across worker processes to sidestep the GIL for pure-Python
    sqlglot work.

    The pool is created eagerly at construction so that a
    :class:`ParallelParserUnavailable` failure is raised deterministically here
    (where the caller already handles it) rather than on a racing first parse
    from a worker thread. Each worker opens the schema snapshot read-only (and,
    if ``graph_config`` is provided, rebuilds a live graph client for cache-miss
    hydration). Results are yielded unordered; the caller correlates them via
    :attr:`ParseTask.key`.
    """

    num_workers: int
    snapshot_path: pathlib.Path
    platform: str
    platform_instance: Optional[str]
    env: str = DEFAULT_ENV
    graph_config: Optional[DatahubClientConfig] = None
    max_pending: Optional[int] = None
    # Constructor-only: threaded to workers via initargs at pool creation.
    # Mutating format_queries after construction has no effect on the pool.
    format_queries: bool = False

    _executor: Optional[ProcessPoolExecutor] = field(
        default=None, init=False, repr=False
    )
    _closed: bool = field(default=False, init=False, repr=False)
    pool_broke: threading.Event = field(
        default_factory=threading.Event, init=False, repr=False
    )

    def __post_init__(self) -> None:
        try:
            # Spawn avoids inheriting the parent's threading.Lock / SQLite fds;
            # fork-with-threads can deadlock. (macOS already defaults to spawn.)
            mp_context = multiprocessing.get_context("spawn")
            self._executor = ProcessPoolExecutor(
                max_workers=self.num_workers,
                mp_context=mp_context,
                initializer=_worker_init,
                initargs=(
                    self.snapshot_path,
                    self.platform,
                    self.platform_instance,
                    self.env,
                    self.graph_config,
                    self.format_queries,
                ),
            )
        except Exception as e:
            raise ParallelParserUnavailable(
                f"Failed to create parallel SQL parser process pool: {e!r}"
            ) from e

    def _ensure_executor(self) -> ProcessPoolExecutor:
        # The pool is created eagerly in __post_init__; a None executor here means
        # the parser was already closed, which is a caller bug on the parse path.
        # This is NOT ParallelParserUnavailable (reserved for genuine
        # pool-creation failure): the aggregator's serial-fallback catches that
        # exception, and swallowing a use-after-close there would hide the bug.
        if self._executor is None:
            raise RuntimeError("ParallelSqlParser has been closed")
        return self._executor

    def map_unordered(self, tasks: Iterable[ParseTask]) -> Iterator[ParseOutcome]:
        """Parse each task in a worker process, yielding outcomes as they complete
        (unordered). Pending futures are bounded (default ``2*num_workers``) so we
        don't accumulate results in memory faster than the caller consumes them.

        The bounded drain mirrors
        :class:`datahub.utilities.backpressure_aware_executor.BackpressureAwareExecutor`.
        It is inlined here (rather than reusing that helper) because we must map
        each future back to its task ``key`` to synthesize a
        ``ParseOutcome(error=...)`` if a worker process dies at the executor layer
        — information the helper's future-only return type does not expose.
        """
        executor = self._ensure_executor()

        max_pending = self.max_pending
        if max_pending is None:
            max_pending = 2 * self.num_workers
        assert max_pending >= self.num_workers

        future_to_key: Dict[Future, Any] = {}
        pending: Set[Future] = set()

        def drain(future: Future) -> ParseOutcome:
            key = future_to_key.pop(future)
            try:
                return future.result()
            except concurrent.futures.process.BrokenProcessPool as e:
                self.pool_broke.set()
                return ParseOutcome(key=key, result=None, error=repr(e))
            except Exception as e:
                # Executor-layer failure (e.g. the worker process died before it
                # could return a ParseOutcome). Surface it as an error outcome
                # instead of propagating.
                return ParseOutcome(key=key, result=None, error=repr(e))

        for task in tasks:
            if len(pending) >= max_pending:
                done, _ = concurrent.futures.wait(
                    pending, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done:
                    pending.remove(future)
                    yield drain(future)

            submitted = executor.submit(_worker_parse, task)
            future_to_key[submitted] = task.key
            pending.add(submitted)

        for future in concurrent.futures.as_completed(pending):
            yield drain(future)

    def parse_one(self, task: ParseTask) -> ParseOutcome:
        """Parse a single task in a worker process and block until its outcome.

        Unlike :meth:`map_unordered`, this submits exactly one task and waits for
        it, so the caller can drive its own per-task ordering (e.g. a
        PartitionExecutor) rather than relying on the unordered stream. A dead
        worker process is surfaced as a :class:`ParseOutcome` with ``error`` set,
        mirroring the drain behavior of :meth:`map_unordered`.
        """
        executor = self._ensure_executor()
        future = executor.submit(_worker_parse, task)
        try:
            return future.result()
        except concurrent.futures.process.BrokenProcessPool as e:
            self.pool_broke.set()
            return ParseOutcome(key=task.key, result=None, error=repr(e))
        except Exception as e:
            return ParseOutcome(key=task.key, result=None, error=repr(e))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
