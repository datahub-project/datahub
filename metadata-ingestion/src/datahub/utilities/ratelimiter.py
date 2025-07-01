import collections
import threading
import time
from contextlib import AbstractContextManager
from typing import Any, Deque


# Modified version of https://github.com/RazerM/ratelimiter/blob/master/ratelimiter/_sync.py
class RateLimiter(AbstractContextManager):
    """Provides rate limiting for an operation with a configurable number of
    requests for a time period.
    """

    def __init__(self, max_calls: int, period: float = 1.0) -> None:
        """Initialize a RateLimiter object which enforces as much as max_calls
        operations on period (eventually floating) number of seconds.
        """
        if period <= 0:
            raise ValueError("Rate limiting period should be > 0")
        if max_calls <= 0:
            raise ValueError("Rate limiting number of calls should be > 0")

        # We're using a deque to store the last execution timestamps, not for
        # its maxlen attribute, but to allow constant time front removal.
        self.calls: Deque = collections.deque()

        self.period = period
        self.max_calls = max_calls
        self._lock = threading.Lock()

    def __enter__(self) -> "RateLimiter":
        with self._lock:
            # We want to ensure that no more than max_calls were run in the allowed
            # period. For this, we store the last timestamps of each call and run
            # the rate verification upon each __enter__ call.
            if len(self.calls) >= self.max_calls:
                until = time.time() + self.period - self._timespan
                sleeptime = until - time.time()
                if sleeptime > 0:
                    time.sleep(sleeptime)
            return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        with self._lock:
            # Store the last operation timestamp.
            self.calls.append(time.time())

            # Pop the timestamp list front (ie: the older calls) until the sum goes
            # back below the period. This is our 'sliding period' window.
            while self._timespan >= self.period:
                self.calls.popleft()

    @property
    def _timespan(self) -> float:
        return self.calls[-1] - self.calls[0]
