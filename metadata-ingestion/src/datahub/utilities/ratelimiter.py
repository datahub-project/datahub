import collections
import threading
import time
from contextlib import AbstractContextManager
from datetime import datetime, timezone
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


class TokenBucket:
    """Classic token bucket: ``capacity`` tokens max, refilling continuously at
    ``rate`` tokens/second. ``acquire()`` blocks (sleeps) until a token is
    available rather than raising, since pacing — not rejecting — is the point.

    Complements ``RateLimiter`` (a sliding window over the last N calls): this
    paces to a sustained rate while allowing short bursts up to ``capacity``.
    """

    def __init__(self, rate: float, capacity: float) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._last_refill = now
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            if self._tokens < 1:
                wait = (1 - self._tokens) / self.rate
                time.sleep(wait)
                self._tokens = 0.0
                self._last_refill = time.monotonic()
            else:
                self._tokens -= 1


class DailyCallBudgetExceeded(RuntimeError):
    """Raised when DailyCallBudget's ceiling for the current UTC day is
    exhausted. A distinct type (rather than a bare RuntimeError) so callers
    can propagate it unwrapped, distinguishing it from a generic API failure
    or a transient rate-limit (429) error."""


class DailyCallBudget:
    """Tracks calls against a fixed daily ceiling, resetting at UTC midnight.
    Exceeding the budget raises ``DailyCallBudgetExceeded`` rather than sleeping
    until the next day — suited to APIs whose own daily quota resets at UTC
    midnight, where blocking for hours is worse than failing the run.
    """

    def __init__(self, daily_limit: int) -> None:
        if daily_limit <= 0:
            raise ValueError("daily_limit must be > 0")
        self.daily_limit = daily_limit
        self._count = 0
        self._day = datetime.now(timezone.utc).date()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            today = datetime.now(timezone.utc).date()
            if today != self._day:
                self._day = today
                self._count = 0
            if self._count >= self.daily_limit:
                raise DailyCallBudgetExceeded(
                    f"Daily call budget ({self.daily_limit}) exhausted "
                    f"for {today.isoformat()} (UTC)."
                )
            self._count += 1
