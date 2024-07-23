import concurrent.futures
import contextlib
import queue
from typing import Any, Callable, Generator, Iterable, Tuple, TypeVar

T = TypeVar("T")


class ThreadedIteratorExecutor:
    """
    Executes worker functions of type `Callable[..., Iterable[T]]` in parallel threads,
    yielding items of type `T` as they become available.
    """

    @classmethod
    def process(
        cls,
        worker_func: Callable[..., Iterable[T]],
        args_list: Iterable[Tuple[Any, ...]],
        max_workers: int,
    ) -> Generator[T, None, None]:

        out_q: queue.Queue[T] = queue.Queue()

        def _worker_wrapper(
            worker_func: Callable[..., Iterable[T]], *args: Any
        ) -> None:
            for item in worker_func(*args):
                out_q.put(item)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for args in args_list:
                future = executor.submit(_worker_wrapper, worker_func, *args)
                futures.append(future)
            # Read from the queue and yield the work units until all futures are done.
            while True:
                if not out_q.empty():
                    while not out_q.empty():
                        yield out_q.get_nowait()
                else:
                    with contextlib.suppress(queue.Empty):
                        yield out_q.get(timeout=0.2)

                # Filter out the done futures.
                futures = [f for f in futures if not f.done()]
                if not futures:
                    break

        # Yield the remaining work units. This theoretically should not happen, but adding it just in case.
        while not out_q.empty():
            yield out_q.get_nowait()
