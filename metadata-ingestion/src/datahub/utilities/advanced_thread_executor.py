import collections
import time
from concurrent.futures import Future, ThreadPoolExecutor
from threading import BoundedSemaphore
from typing import Any, Callable, Deque, Dict, Optional, Tuple, TypeVar

from datahub.ingestion.api.closeable import Closeable

_R = TypeVar("_R")
_PARTITION_EXECUTOR_FLUSH_SLEEP_INTERVAL = 0.05


class PartitionExecutor(Closeable):
    def __init__(self, max_workers: int, max_pending: int) -> None:
        """A thread pool executor with partitioning and a pending request bound.

        It works similarly to a ThreadPoolExecutor, with the following changes:
        - At most one request per partition key will be executing at a time.
        - If the number of pending requests exceeds the threshold, the submit call
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
                self._submit_nowait(key, fn, args, kwargs, user_done_callback)

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
        self._semaphore.release(self.max_pending)

    def shutdown(self) -> None:
        """See concurrent.futures.Executor#shutdown. Behaves as if wait=True."""

        self.flush()
        assert len(self._pending_by_key) == 0

        # Technically, the wait=True here is redundant, since all the threads should
        # be idle now.
        self._executor.shutdown(wait=True)

    def close(self) -> None:
        self.shutdown()
