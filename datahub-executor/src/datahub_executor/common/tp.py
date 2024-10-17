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
        self.active_threads = 0

        STATS_THREAD_POOL_MAX_WORKERS.labels(name).set(max_workers)

    def submit(self, fn, *args, **kwargs):  # type: ignore
        if self.shutdown_flag:
            return None

        def sem_release(future: Future) -> None:
            self.semaphore.release()
            self.active_threads -= 1
            STATS_THREAD_POOL_ACTIVE_WORKERS.labels(self.name).set(self.active_threads)
            return

        self.semaphore.acquire()
        self.active_threads += 1
        STATS_THREAD_POOL_ACTIVE_WORKERS.labels(self.name).set(self.active_threads)

        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except:
            sem_release(future)
            raise
        else:
            future.add_done_callback(sem_release)
            return future

    def get_active_thread_count(self) -> int:
        return self.active_threads

    def shutdown(self, wait=True):  # type: ignore
        self.shutdown_flag = True
        self.executor.shutdown(wait)
