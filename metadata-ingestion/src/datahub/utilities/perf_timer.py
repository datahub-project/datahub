import time
from contextlib import AbstractContextManager
from typing import Any, Optional


class PerfTimer(AbstractContextManager):
    """
    A context manager that gives easy access to elapsed time for performance measurement.
    """

    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def start(self) -> None:
        self.start_time = time.perf_counter()
        self.end_time = None

    def finish(self) -> None:
        assert self.start_time is not None
        self.end_time = time.perf_counter()

    def __enter__(self) -> "PerfTimer":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> Optional[bool]:
        self.finish()
        return None

    def elapsed_seconds(self) -> float:
        """
        Returns the elapsed time in seconds.
        """

        assert self.start_time is not None
        if self.end_time is None:
            return time.perf_counter() - self.start_time
        else:
            return self.end_time - self.start_time
