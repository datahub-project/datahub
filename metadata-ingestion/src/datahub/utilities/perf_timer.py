import time
from contextlib import AbstractContextManager
from typing import Any, Optional


class PerfTimer(AbstractContextManager):
    """
    A context manager that gives easy access to elapsed time for performance measurement.

    """

    def __init__(self) -> None:
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self._past_active_time: float = 0
        self.paused: Optional[bool] = None

    def start(self) -> None:
        # TODO
        # assert (
        #    self.end_time is None
        # ), "Can not start a finished timer. Did you accidentally re-use this timer ?"

        if self.end_time is not None:
            self._past_active_time = self.elapsed_seconds()

        self.start_time = time.perf_counter()
        self.end_time = None
        if self.paused:
            self.paused = False

    def pause_timer(self) -> "PerfTimer":
        assert (
            not self.paused and not self.end_time
        ), "Can not pause a paused/stopped timer"
        assert (
            self.start_time is not None
        ), "Can not pause a timer that hasn't started. Did you forget to start the timer ?"
        self._past_active_time = self.elapsed_seconds()
        self.start_time = None
        self.end_time = None
        self.paused = True
        return self

    def finish(self) -> None:
        assert (
            self.start_time is not None
        ), "Can not stop a timer that hasn't started. Did you forget to start the timer ?"
        self.end_time = time.perf_counter()

    def __enter__(self) -> "PerfTimer":
        if self.paused:  # Entering paused timer context, NO OP
            pass
        else:
            self.start()
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> Optional[bool]:
        if self.paused:  # Exiting paused timer context, resume timer
            self.start()
        else:
            self.finish()
        return None

    def elapsed_seconds(self) -> float:
        """
        Returns the elapsed time in seconds.
        """
        if self.paused:
            return self._past_active_time

        assert self.start_time is not None, "Did you forget to start the timer ?"
        if self.end_time is None:
            return (time.perf_counter() - self.start_time) + (self._past_active_time)
        else:
            return (self.end_time - self.start_time) + self._past_active_time

    def __repr__(self) -> str:
        return repr(self.as_obj())

    def __str__(self) -> str:
        return self.__repr__()

    def as_obj(self) -> Optional[str]:
        if self.start_time is None:
            return None
        else:
            time_taken = self.elapsed_seconds()
            return f"{time_taken:.3f} seconds"
