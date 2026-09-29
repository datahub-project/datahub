import concurrent.futures
import logging
import queue
import threading
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

logger = logging.getLogger(__name__)

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
        max_backpressure: Optional[int] = None,
    ) -> Iterator[T]:
        if max_backpressure is None:
            max_backpressure = 10 * max_workers
        if max_backpressure < max_workers:
            raise ValueError(
                f"max_backpressure ({max_backpressure}) must be >= max_workers ({max_workers})"
            )

        out_q: queue.Queue[T] = queue.Queue(maxsize=max_backpressure)
        stop_event = threading.Event()

        def _worker_wrapper(
            worker_func: Callable[..., Iterable[T]], *args: Any
        ) -> None:
            for item in worker_func(*args):
                if stop_event.is_set():
                    return
                out_q.put(item)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: List[concurrent.futures.Future] = []
            for args in args_list:
                future = executor.submit(_worker_wrapper, worker_func, *args)
                futures.append(future)

            while True:
                # Drain all immediately available items from the queue.
                drained = False
                while True:
                    try:
                        yield out_q.get_nowait()
                        drained = True
                    except queue.Empty:
                        break

                # If nothing was available, block briefly to avoid busy-waiting.
                if not drained:
                    try:
                        yield out_q.get(timeout=0.2)
                    except queue.Empty:
                        pass

                # Check done futures for exceptions, then filter them out.
                remaining: List[concurrent.futures.Future] = []
                for f in futures:
                    if f.done():
                        exc = f.exception()
                        if exc is not None:
                            logger.error(
                                "Worker thread raised an exception",
                                exc_info=exc,
                            )
                            # Signal workers to stop and cancel pending futures.
                            # Start a daemon thread to drain the queue so any
                            # workers blocked on out_q.put() can finish,
                            # preventing deadlock in executor.shutdown().
                            stop_event.set()
                            for pending in [*remaining, *futures]:
                                if not pending.done():
                                    pending.cancel()
                            drain_thread = threading.Thread(
                                target=_drain_queue,
                                args=(out_q, stop_event),
                                daemon=True,
                            )
                            drain_thread.start()
                            raise exc
                    else:
                        remaining.append(f)
                futures = remaining
                if not futures:
                    break

        # Drain any items enqueued after the last check.
        while True:
            try:
                yield out_q.get_nowait()
            except queue.Empty:
                break


def _drain_queue(q: queue.Queue, stop_event: threading.Event) -> None:
    """Continuously drain items from a queue until the stop event is cleared.

    Runs as a daemon thread to unblock workers stuck on q.put() during
    error-driven shutdown.
    """
    while stop_event.is_set():
        try:
            q.get(timeout=0.1)
        except queue.Empty:
            pass
