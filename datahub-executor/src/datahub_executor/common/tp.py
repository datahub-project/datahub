from concurrent.futures import Future, ThreadPoolExecutor
from threading import Condition
from typing import Callable, Optional

from datahub_executor.common.monitoring.metrics import (
    STATS_THREAD_POOL_ACTIVE_WEIGHT,
    STATS_THREAD_POOL_ACTIVE_WORKERS,
    STATS_THREAD_POOL_MAX_WEIGHT,
    STATS_THREAD_POOL_MAX_WORKERS,
)


class ThreadPoolExecutorWithQueueSizeLimit:
    def __init__(self, max_workers: int, name: str):

        self.shutdown_flag = False
        self.name = name

        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.cond = Condition()
        self.max_threads = max_workers

        self.active_threads: int = 0
        self.active_weight: float = 0.0

        STATS_THREAD_POOL_MAX_WORKERS.labels(name).set(max_workers)
        STATS_THREAD_POOL_MAX_WEIGHT.labels(name).set(1.0)

    def get_active_thread_count(self) -> int:
        return self.active_threads

    def get_active_weight(self) -> float:
        return self.active_weight

    def shutdown(self, wait: bool = True) -> None:
        self.shutdown_flag = True
        self.executor.shutdown(wait)

    def submit(self, fn: Callable, *args, **kwargs) -> None:  # type: ignore
        self.submit_weighted(None, fn, *args, **kwargs)

    def submit_weighted(self, weight: Optional[float], fn: Callable, *args, **kwargs) -> None:  # type: ignore

        if self.shutdown_flag:
            return None

        if weight is None:
            current_weight = 1.0 / self.max_threads
        elif weight > 1.0:
            current_weight = 1.0
        else:
            current_weight = weight

        def release(future: Future) -> None:
            with self.cond:
                self.active_threads -= 1
                self.active_weight -= current_weight
                self.cond.notify()

            STATS_THREAD_POOL_ACTIVE_WORKERS.labels(self.name).set(self.active_threads)
            STATS_THREAD_POOL_ACTIVE_WEIGHT.labels(self.name).set(self.active_weight)

        with self.cond:
            while (self.active_threads + 1) > self.max_threads or (
                self.active_weight + current_weight
            ) > 1.0:
                self.cond.wait()
            self.active_threads += 1
            self.active_weight += current_weight

        STATS_THREAD_POOL_ACTIVE_WORKERS.labels(self.name).set(self.active_threads)
        STATS_THREAD_POOL_ACTIVE_WEIGHT.labels(self.name).set(self.active_weight)

        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except Exception as e:
            release(future)
            raise (e)
        else:
            future.add_done_callback(release)
