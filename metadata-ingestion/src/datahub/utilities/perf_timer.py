import logging
import time
from contextlib import AbstractContextManager
from typing import Any, Optional

logger: logging.Logger = logging.getLogger(__name__)


class PerfTimer(AbstractContextManager):
    """
    A context manager that gives easy access to elapsed time for performance measurement.
    """

    def __init__(self) -> None:
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self._past_active_time: float = 0
        self.paused: bool = False
        self._error_state = False

    def start(self) -> None:
        if self.end_time is not None:
            self._past_active_time = self.elapsed_seconds()

        self.start_time = time.perf_counter()
        self.end_time = None
        self.paused = False

    def pause(self) -> "PerfTimer":
        self.assert_timer_is_running()
        self._past_active_time = self.elapsed_seconds()
        self.start_time = None
        self.end_time = None
        self.paused = True
        return self

    def finish(self) -> None:
        self.assert_timer_is_running()
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

    def elapsed_seconds(self, digits: int = 4) -> float:
        """
        Returns the elapsed time in seconds.
        """
        if self.paused or not self.start_time:
            return self._past_active_time

        if self.end_time is None:
            elapsed = (time.perf_counter() - self.start_time) + (self._past_active_time)
        else:
            elapsed = (self.end_time - self.start_time) + self._past_active_time

        return round(elapsed, digits)

    def assert_timer_is_running(self) -> None:
        if not self.is_running():
            self._error_state = True
            logger.warning("Did you forget to start the timer ?")

    def is_running(self) -> bool:
        """
        Returns true if timer is in running state.
        Timer is in NOT in running state if
        1. it has never been started.
        2. it is in paused state.
        3. it had been started and finished in the past but not started again.
        """
        return self.start_time is not None and not self.paused and self.end_time is None

    def __repr__(self) -> str:
        return repr(self.as_obj())

    def __str__(self) -> str:
        return self.__repr__()

    def as_obj(self) -> Optional[str]:
        if self.start_time is None:
            return None
        else:
            time_taken = self.elapsed_seconds()
            state = " (error)" if self._error_state else ""
            return f"{time_taken:.3f} seconds{state}"
