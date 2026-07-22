import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

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


def test_token_bucket_wait_duration_matches_deficit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once the bucket is empty, the wait must be exactly the time needed to
    accumulate the missing token at the configured rate — not a fixed or
    arbitrary backoff."""
    sleep_calls: List[float] = []
    monkeypatch.setattr(time, "sleep", sleep_calls.append)

    bucket = TokenBucket(rate=2.0, capacity=1)
    bucket.acquire()  # consumes the single burst token, no wait
    bucket.acquire()  # empty -> waits (1 - 0) / 2.0 = 0.5s

    assert sleep_calls == [pytest.approx(0.5, abs=0.01)]


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


def test_daily_call_budget_warns_once_past_threshold(
    caplog: pytest.LogCaptureFixture,
) -> None:
    daily_limit = 100
    threshold = DailyCallBudget._WARNING_THRESHOLD
    calls_to_cross_threshold = int(daily_limit * threshold) + 1

    budget = DailyCallBudget(daily_limit=daily_limit)
    with caplog.at_level(logging.WARNING):
        for _ in range(calls_to_cross_threshold - 1):
            budget.acquire()
        assert not caplog.records

        budget.acquire()  # this call crosses the threshold
        assert len(caplog.records) == 1
        assert f"{threshold * 100:.0f}%" in caplog.records[0].message

        budget.acquire()
        assert len(caplog.records) == 1  # only warns once per day
