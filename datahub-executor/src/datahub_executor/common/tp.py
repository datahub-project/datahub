from concurrent.futures import Future, ThreadPoolExecutor
from threading import BoundedSemaphore

from datahub_executor.common.monitoring.metrics import (
    STATS_THREAD_POOL_ACTIVE_WORKERS,
    STATS_THREAD_POOL_MAX_WORKERS,
)


class ThreadPoolExecutorWithQueueSizeLimit:
    def __init__(self, max_workers: int, name: str):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = BoundedSemaphore(max_workers)
        self.shutdown_flag = False
        self.name = name
        STATS_THREAD_POOL_MAX_WORKERS.labels(name).set(max_workers)

    def submit(self, fn, *args, **kwargs):  # type: ignore
        if self.shutdown_flag:
            return None

        def sem_release(future: Future) -> None:
            self.semaphore.release()
            STATS_THREAD_POOL_ACTIVE_WORKERS.labels(self.name).dec()
            return

        self.semaphore.acquire()
        STATS_THREAD_POOL_ACTIVE_WORKERS.labels(self.name).inc()

        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except:
            sem_release(future)
            raise
        else:
            future.add_done_callback(sem_release)
            return future

    def shutdown(self, wait=True):  # type: ignore
        self.shutdown_flag = True
        self.executor.shutdown(wait)
