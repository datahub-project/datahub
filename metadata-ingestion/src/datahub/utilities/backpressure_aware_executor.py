from __future__ import annotations

import concurrent.futures
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Iterable, Iterator, Optional, Set, Tuple, TypeVar

_R = TypeVar("_R")


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
