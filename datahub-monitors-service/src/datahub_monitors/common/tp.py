from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore

class ThreadPoolExecutorWithQueueSizeLimit:
    def __init__(self, max_workers):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = BoundedSemaphore(max_workers)
        self.shutdown_flag = False

    def submit(self, fn, *args, **kwargs):
        if self.shutdown_flag:
            return None

        self.semaphore.acquire()
        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except:
            self.semaphore.release()
            raise
        else:
            future.add_done_callback(lambda x: self.semaphore.release())
            return future

    def shutdown(self, wait=True):
        self.shutdown_flag = True
        self.executor.shutdown(wait)
