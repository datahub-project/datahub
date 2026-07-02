import concurrent.futures
import multiprocessing
import pathlib
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, Optional, Set

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.sql_parsing.schema_resolver import (
    SchemaResolver,
    SchemaResolverInterface,
)
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult, sqlglot_lineage


class ParallelParserUnavailable(Exception):
    """Raised when the parallel parser process pool cannot be created.

    The caller is expected to catch this and fall back to serial parsing.
    """


@dataclass
class ParseTask:
    """A single query to parse. ``key`` is an opaque correlation id the caller
    uses to match the resulting :class:`ParseOutcome` back to its source, since
    results are returned out of order."""

    key: Any
    query: str
    default_db: Optional[str]
    default_schema: Optional[str]
    override_dialect: Optional[str] = None
    generate_column_lineage: bool = True


@dataclass
class ParseOutcome:
    """The result of parsing a single :class:`ParseTask`.

    Exactly one of ``result`` / ``error`` is meaningful: a populated ``result``
    (even one whose ``debug_info`` records a normal parse failure) means the
    worker ran to completion; ``error`` is set only when the worker itself raised
    (or its process died) so the caller can decide how to handle it."""

    key: Any
    result: Optional[SqlParsingResult]
    error: Optional[str]


# ---------------------------------------------------------------------------
# Worker side. These are module-level so the spawn start method can pickle them.
# ---------------------------------------------------------------------------

# Populated once per worker process by _worker_init, then reused for every task
# that worker handles. Process-local, so no cross-process sharing concerns.
_WORKER_RESOLVER: Optional[SchemaResolverInterface] = None


def _worker_init(
    snapshot_path: pathlib.Path,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    graph_config: Optional[DatahubClientConfig],
) -> None:
    global _WORKER_RESOLVER
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
        # Normal parse failures are already captured inside result.debug_info,
        # so they are returned as a result, not an error.
        return ParseOutcome(key=task.key, result=result, error=None)
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

    The pool is created lazily on first use. Each worker opens the schema
    snapshot read-only (and, if ``graph_config`` is provided, rebuilds a live
    graph client for cache-miss hydration). Results are yielded unordered; the
    caller correlates them via :attr:`ParseTask.key`.
    """

    num_workers: int
    snapshot_path: pathlib.Path
    platform: str
    platform_instance: Optional[str]
    env: str = DEFAULT_ENV
    graph_config: Optional[DatahubClientConfig] = None
    max_pending: Optional[int] = None

    _executor: Optional[ProcessPoolExecutor] = field(
        default=None, init=False, repr=False
    )
    _closed: bool = field(default=False, init=False, repr=False)

    def _ensure_executor(self) -> ProcessPoolExecutor:
        if self._executor is not None:
            return self._executor
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
                ),
            )
        except Exception as e:
            raise ParallelParserUnavailable(
                f"Failed to create parallel SQL parser process pool: {e!r}"
            ) from e
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
        except Exception as e:
            return ParseOutcome(key=task.key, result=None, error=repr(e))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
