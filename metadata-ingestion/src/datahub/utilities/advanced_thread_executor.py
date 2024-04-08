from __future__ import annotations

import collections
import concurrent.futures
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from threading import BoundedSemaphore
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

from datahub.ingestion.api.closeable import Closeable

logger = logging.getLogger(__name__)
_R = TypeVar("_R")
_PARTITION_EXECUTOR_FLUSH_SLEEP_INTERVAL = 0.05


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

        # Technically, the wait=True here is redundant, since all the threads should
        # be idle now.
        self._executor.shutdown(wait=True)

    def close(self) -> None:
        self.shutdown()


class BackpressureAwareExecutor:
    # This couldn't be a real executor because the semantics of submit wouldn't really make sense.
    # In this variant, if we blocked on submit, then we would also be blocking the thread that
    # we expect to be consuming the results. As such, I made it accept the full list of args
    # up front, and that way the consumer can read results at its own pace.

    @classmethod
    def map(
        cls,
        fn: Callable[..., _R],
        args_list: Iterable[Tuple[Any, ...]],
        max_workers: int,
        max_pending: Optional[int] = None,
    ) -> Iterator[Future[_R]]:
        """Similar to concurrent.futures.ThreadPoolExecutor#map, except that it won't run ahead of the consumer.

        The main benefit is that the ThreadPoolExecutor isn't stuck holding a ton of result
        objects in memory if the consumer is slow. Instead, the consumer can read the results
        at its own pace and the executor threads will idle if they need to.

        Args:
            fn: The function to apply to each input.
            args_list: The list of inputs. In contrast to the builtin map, this is a list
                of tuples, where each tuple is the arguments to fn.
            max_workers: The maximum number of threads to use.
            max_pending: The maximum number of pending results to keep in memory.
                If not set, it will be set to 2*max_workers.

        Returns:
            An iterable of futures.

            This differs from a traditional map because it returns futures
            instead of the actual results, so that the caller is required
            to handle exceptions.

            Additionally, it does not maintain the order of the arguments.
            If you want to know which result corresponds to which input,
            the mapped function should return some form of an identifier.
        """

        if max_pending is None:
            max_pending = 2 * max_workers
        assert max_pending >= max_workers

        pending_futures: Set[Future] = set()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for args in args_list:
                # If the pending list is full, wait until one is done.
                if len(pending_futures) >= max_pending:
                    (done, _) = concurrent.futures.wait(
                        pending_futures, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for future in done:
                        pending_futures.remove(future)

                        # We don't want to call result() here because we want the caller
                        # to handle exceptions/cancellation.
                        yield future

                # Now that there's space in the pending list, enqueue the next task.
                pending_futures.add(executor.submit(fn, *args))

            # Wait for all the remaining tasks to complete.
            for future in concurrent.futures.as_completed(pending_futures):
                pending_futures.remove(future)
                yield future

            assert not pending_futures
