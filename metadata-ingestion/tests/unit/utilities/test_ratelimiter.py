import time
from collections import defaultdict
from datetime import datetime
from typing import Dict

import pytest
import time_machine

from datahub.utilities.ratelimiter import (
    DailyCallBudget,
    DailyCallBudgetExceeded,
    RateLimiter,
    TokenBucket,
)


def test_rate_is_limited():
    MAX_CALLS_PER_SEC = 5
    TOTAL_CALLS = 18
    actual_calls: Dict[float, int] = defaultdict(int)

    ratelimiter = RateLimiter(max_calls=MAX_CALLS_PER_SEC, period=1)
    for _ in range(TOTAL_CALLS):
        with ratelimiter:
            actual_calls[datetime.now().replace(microsecond=0).timestamp()] += 1

    assert len(actual_calls) == round(TOTAL_CALLS / MAX_CALLS_PER_SEC)
    assert all(calls <= MAX_CALLS_PER_SEC for calls in actual_calls.values())
    assert sum(actual_calls.values()) == TOTAL_CALLS


def test_token_bucket_allows_burst_then_paces_to_rate() -> None:
    bucket = TokenBucket(rate=1000.0, capacity=2)
    start = time.monotonic()
    bucket.acquire()  # burst token 1, instant
    bucket.acquire()  # burst token 2, instant
    burst_elapsed = time.monotonic() - start
    assert burst_elapsed < 0.05

    bucket.acquire()  # bucket empty, must wait ~1/rate = 1ms
    paced_elapsed = time.monotonic() - start
    assert paced_elapsed >= 0.001


def test_token_bucket_rejects_non_positive_params() -> None:
    with pytest.raises(ValueError):
        TokenBucket(rate=0, capacity=1)
    with pytest.raises(ValueError):
        TokenBucket(rate=1, capacity=0)


def test_daily_call_budget_raises_once_exhausted() -> None:
    budget = DailyCallBudget(daily_limit=2)
    budget.acquire()
    budget.acquire()
    with pytest.raises(DailyCallBudgetExceeded, match="call budget"):
        budget.acquire()


def test_daily_call_budget_resets_at_utc_midnight() -> None:
    budget = DailyCallBudget(daily_limit=1)
    with time_machine.travel("2026-06-01 23:59:00 +0000", tick=False):
        budget.acquire()
        with pytest.raises(DailyCallBudgetExceeded):
            budget.acquire()
    with time_machine.travel("2026-06-02 00:01:00 +0000", tick=False):
        budget.acquire()  # new UTC day -> budget replenished
