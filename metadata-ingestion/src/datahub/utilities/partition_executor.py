from __future__ import annotations

import collections
import functools
import logging
import queue
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import BoundedSemaphore
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

from datahub.ingestion.api.closeable import Closeable

logger = logging.getLogger(__name__)
_R = TypeVar("_R")
_Args = TypeVar("_Args", bound=tuple)
_PARTITION_EXECUTOR_FLUSH_SLEEP_INTERVAL = 0.05
_DEFAULT_BATCHER_MIN_PROCESS_INTERVAL = timedelta(seconds=30)


class PartitionExecutor(Closeable):
    def __init__(self, max_workers: int, max_pending: int) -> None:
        """A thread pool executor with partitioning and a pending request bound.

        It works similarly to a ThreadPoolExecutor, with the following changes:
        - At most one request per partition key will be executing at a time.
        - If the number of pending requests exceeds the threshold, the submit() call
          will block until the number of pending requests drops below the threshold.

        Due to the interaction between max_workers and max_pending, it is possible
        for execution to effectively be serialized when there's a large influx of
        requests with the same key. This can be mitigated by setting a reasonably
        large max_pending value.

        Args:
            max_workers: The maximum number of threads to use for executing requests.
            max_pending: The maximum number of pending (e.g. non-executing) requests to allow.
        """
        self.max_workers = max_workers
        self.max_pending = max_pending

        self._executor = ThreadPoolExecutor(max_workers=max_workers)

        # Each pending or executing request will acquire a permit from this semaphore.
        self._semaphore = BoundedSemaphore(max_pending + max_workers)

        # A key existing in this dict means that there is a submitted request for that key.
        # Any entries in the key's value e.g. the deque are requests that are waiting
        # to be submitted once the current request for that key completes.
        self._pending_by_key: Dict[
            str, Deque[Tuple[Callable, tuple, dict, Optional[Callable[[Future], None]]]]
        ] = {}

    def submit(
        self,
        key: str,
        fn: Callable[..., _R],
        *args: Any,
        # Ideally, we would've used ParamSpec to annotate this method. However,
        # due to the limitations of PEP 612, we can't add a keyword argument here.
        # See https://peps.python.org/pep-0612/#concatenating-keyword-parameters
        # As such, we're using Any here, and won't validate the args to this method.
        # We might be able to work around it by moving the done_callback arg to be before
        # the *args, but that would mean making done_callback a required arg instead of
        # optional as it is now.
        done_callback: Optional[Callable[[Future], None]] = None,
        **kwargs: Any,
    ) -> None:
        """See concurrent.futures.Executor#submit"""

        self._semaphore.acquire()

        if key in self._pending_by_key:
            self._pending_by_key[key].append((fn, args, kwargs, done_callback))

        else:
            self._pending_by_key[key] = collections.deque()
            self._submit_nowait(key, fn, args, kwargs, done_callback=done_callback)

    def _submit_nowait(
        self,
        key: str,
        fn: Callable[..., _R],
        args: tuple,
        kwargs: dict,
        done_callback: Optional[Callable[[Future], None]],
    ) -> Future:
        future = self._executor.submit(fn, *args, **kwargs)

        def _system_done_callback(future: Future) -> None:
            self._semaphore.release()

            # If there is another pending request for this key, submit it now.
            # The key must exist in the map.
            if self._pending_by_key[key]:
                fn, args, kwargs, user_done_callback = self._pending_by_key[
                    key
                ].popleft()

                try:
                    self._submit_nowait(key, fn, args, kwargs, user_done_callback)
                except RuntimeError as e:
                    if self._executor._shutdown:
                        # If we're in shutdown mode, then we can't submit any more requests.
                        # That means we'll need to drop requests on the floor, which is to
                        # be expected in shutdown mode.
                        # The only reason we'd normally be in shutdown here is during
                        # Python exit (e.g. KeyboardInterrupt), so this is reasonable.
                        logger.debug("Dropping request due to shutdown")
                    else:
                        raise e

            else:
                # If there are no pending requests for this key, mark the key
                # as no longer in progress.
                del self._pending_by_key[key]

        if done_callback:
            future.add_done_callback(done_callback)
        future.add_done_callback(_system_done_callback)
        return future

    def flush(self) -> None:
        """Wait for all pending requests to complete."""

        # Acquire all the semaphore permits so that no more requests can be submitted.
        for _i in range(self.max_pending):
            self._semaphore.acquire()

        # Now, wait for all the pending requests to complete.
        while len(self._pending_by_key) > 0:
            # TODO: There should be a better way to wait for all executor threads to be idle.
            # One option would be to just shutdown the existing executor and create a new one.
            time.sleep(_PARTITION_EXECUTOR_FLUSH_SLEEP_INTERVAL)

        # Now allow new requests to be submitted.
        # TODO: With Python 3.9, release() can take a count argument.
        for _i in range(self.max_pending):
            self._semaphore.release()

    def shutdown(self) -> None:
        """See concurrent.futures.Executor#shutdown. Behaves as if wait=True."""

        self.flush()
        assert len(self._pending_by_key) == 0

        self._executor.shutdown(wait=True)

    def close(self) -> None:
        self.shutdown()


class _BatchPartitionWorkItem(NamedTuple):
    key: str
    args: tuple
    done_callback: Optional[Callable[[Future], None]]


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class BatchPartitionExecutor(Closeable):
    def __init__(
        self,
        max_workers: int,
        max_pending: int,
        # Due to limitations of Python's typing, we can't express the type of the list
        # effectively. Ideally we'd use ParamSpec here, but that's not allowed in a
        # class context like this.
        process_batch: Callable[[List], None],
        max_per_batch: int = 100,
        min_process_interval: timedelta = _DEFAULT_BATCHER_MIN_PROCESS_INTERVAL,
    ) -> None:
        """Similar to PartitionExecutor, but with batching.

        This takes in the stream of requests, automatically segments them into partition-aware
        batches, and schedules them across a pool of worker threads.

        It maintains the invariant that multiple requests with the same key will not be in
        flight concurrently, except when part of the same batch. Requests for a given key
        will also be executed in the order they were submitted.

        Unlike the PartitionExecutor, this does not support return values or kwargs.

        Args:
            max_workers: The maximum number of threads to use for executing requests.
            max_pending: The maximum number of pending (e.g. non-executing) requests to allow.
            max_per_batch: The maximum number of requests to include in a batch.
            min_process_interval: When requests are coming in slowly, we will wait at least this long
                before submitting a non-full batch.
            process_batch: A function that takes in a list of argument tuples.
        """
        self.max_workers = max_workers
        self.max_pending = max_pending
        self.max_per_batch = max_per_batch
        self.process_batch = process_batch
        self.min_process_interval = min_process_interval
        assert self.max_workers > 1

        # We add one here to account for the clearinghouse worker thread.
        self._executor = ThreadPoolExecutor(max_workers=max_workers + 1)
        self._clearinghouse_started = False

        self._pending_count = BoundedSemaphore(max_pending)
        self._pending: "queue.Queue[Optional[_BatchPartitionWorkItem]]" = queue.Queue(
            maxsize=max_pending
        )

        # If this is true, that means shutdown() has been called and self._pending is empty.
        self._queue_empty_for_shutdown = False

    def _clearinghouse_worker(self) -> None:  # noqa: C901
        # This worker will pull items off the queue, and submit them into the executor
        # in batches. Only this worker will submit process commands to the executor thread pool.

        # The lock protects the function's internal state.
        clearinghouse_state_lock = threading.Lock()
        workers_available = self.max_workers
        keys_in_flight: Set[str] = set()
        keys_no_longer_in_flight: Set[str] = set()
        pending_key_completion: List[_BatchPartitionWorkItem] = []

        last_submit_time = _now()

        def _handle_batch_completion(
            batch: List[_BatchPartitionWorkItem], future: Future
        ) -> None:
            with clearinghouse_state_lock:
                for item in batch:
                    keys_no_longer_in_flight.add(item.key)
                    self._pending_count.release()

            # Separate from the above loop to avoid holding the lock while calling the callbacks.
            for item in batch:
                if item.done_callback:
                    item.done_callback(future)

        def _find_ready_items() -> List[_BatchPartitionWorkItem]:
            with clearinghouse_state_lock:
                # First, update the keys in flight.
                for key in keys_no_longer_in_flight:
                    keys_in_flight.remove(key)
                keys_no_longer_in_flight.clear()

                # Then, update the pending key completion and build the ready list.
                pending = pending_key_completion.copy()
                pending_key_completion.clear()

                ready: List[_BatchPartitionWorkItem] = []
                for item in pending:
                    if (
                        len(ready) < self.max_per_batch
                        and item.key not in keys_in_flight
                    ):
                        ready.append(item)
                    else:
                        pending_key_completion.append(item)

                return ready

        def _build_batch() -> List[_BatchPartitionWorkItem]:
            next_batch = _find_ready_items()

            while (
                not self._queue_empty_for_shutdown
                and len(next_batch) < self.max_per_batch
            ):
                blocking = True
                if (
                    next_batch
                    and _now() - last_submit_time > self.min_process_interval
                    and workers_available > 0
                ):
                    # If we're past the submit deadline, pull from the queue
                    # in a non-blocking way, and submit the batch once the queue
                    # is empty.
                    blocking = False

                try:
                    next_item: Optional[_BatchPartitionWorkItem] = self._pending.get(
                        block=blocking,
                        timeout=self.min_process_interval.total_seconds(),
                    )
                    if next_item is None:
                        self._queue_empty_for_shutdown = True
                        break

                    with clearinghouse_state_lock:
                        if next_item.key in keys_in_flight:
                            pending_key_completion.append(next_item)
                        else:
                            next_batch.append(next_item)
                except queue.Empty:
                    if not blocking:
                        break

            return next_batch

        def _submit_batch(next_batch: List[_BatchPartitionWorkItem]) -> None:
            with clearinghouse_state_lock:
                for item in next_batch:
                    keys_in_flight.add(item.key)

                nonlocal workers_available
                workers_available -= 1

                nonlocal last_submit_time
                last_submit_time = _now()

            future = self._executor.submit(
                self.process_batch, [item.args for item in next_batch]
            )
            future.add_done_callback(
                functools.partial(_handle_batch_completion, next_batch)
            )

        try:
            # Normal operation - submit batches as they become available.
            while not self._queue_empty_for_shutdown:
                next_batch = _build_batch()
                if next_batch:
                    _submit_batch(next_batch)

            # Shutdown time.
            # Invariant - at this point, we know self._pending is empty.
            # We just need to wait for the in-flight items to complete,
            # and submit any currently pending items once possible.
            while pending_key_completion:
                next_batch = _build_batch()
                if next_batch:
                    _submit_batch(next_batch)
                time.sleep(_PARTITION_EXECUTOR_FLUSH_SLEEP_INTERVAL)

            # At this point, there are no more things to submit.
            # We could wait for the in-flight items to complete,
            # but the executor will take care of waiting for them to complete.
        except Exception as e:
            # This represents a fatal error that makes the entire executor defunct.
            logger.exception(
                "Threaded executor's clearinghouse worker failed.", exc_info=e
            )
        finally:
            self._clearinghouse_started = False

    def _ensure_clearinghouse_started(self) -> None:
        # Lazily start the clearinghouse worker.
        if not self._clearinghouse_started:
            self._clearinghouse_started = True
            self._executor.submit(self._clearinghouse_worker)

    def submit(
        self,
        key: str,
        *args: Any,
        done_callback: Optional[Callable[[Future], None]] = None,
    ) -> None:
        """See concurrent.futures.Executor#submit"""

        self._ensure_clearinghouse_started()

        self._pending_count.acquire()
        self._pending.put(_BatchPartitionWorkItem(key, args, done_callback))

    def shutdown(self) -> None:
        if not self._clearinghouse_started:
            # This is required to make shutdown() idempotent, which is important
            # when it's called explicitly and then also by a context manager.
            logger.debug("Shutting down: clearinghouse not started")
            return

        logger.debug(f"Shutting down {self.__class__.__name__}")

        # Send the shutdown signal.
        self._pending.put(None)

        # By acquiring all the permits, we ensure that no more tasks will be scheduled
        # and automatically wait until all existing tasks have completed.
        for _ in range(self.max_pending):
            self._pending_count.acquire()

        # We must wait for the clearinghouse worker to exit before calling shutdown
        # on the thread pool. Without this, the clearinghouse worker might fail to
        # enqueue pending tasks into the pool.
        while self._clearinghouse_started:
            time.sleep(_PARTITION_EXECUTOR_FLUSH_SLEEP_INTERVAL)

        self._executor.shutdown(wait=False)

    def close(self) -> None:
        self.shutdown()
