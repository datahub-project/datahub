import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable


class BoundedThreadPoolExecutor:
    """ThreadPoolExecutor with backpressure control"""

    def __init__(self, max_workers: int | None, max_pending: int):
        self.max_workers = max_workers
        self.max_pending = max_pending
        self.executor: ThreadPoolExecutor | None = None
        self.semaphore: threading.Semaphore | None = None

    def __enter__(self) -> "BoundedThreadPoolExecutor":
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.semaphore = threading.Semaphore(self.max_pending)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.shutdown()

    def submit(self, fn: Callable, *args: Any, **kwargs: Any) -> Future[Any]:
        """Submit with backpressure - blocks when too many pending"""
        if not self.executor or not self.semaphore:
            raise RuntimeError("Executor must be initialized before submitting tasks.")

        self.semaphore.acquire()

        def wrapped_fn(*args: Any, **kwargs: Any) -> Any:
            try:
                return fn(*args, **kwargs)
            finally:
                if self.semaphore:
                    self.semaphore.release()

        return self.executor.submit(wrapped_fn, *args, **kwargs)

    def shutdown(self, wait: bool = True) -> None:
        if self.executor:
            self.executor.shutdown(wait=wait)
            self.executor = None
            self.semaphore = None
