import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, List

import pytest
import time_machine

from datahub.utilities.ratelimiter import (
    DailyCallBudget,
    DailyCallBudgetExceeded,
    RateLimiter,
    TokenBucket,
)


def _make_deterministic_sleep(
    clock: List[float], sleep_calls: List[float]
) -> Callable[[float], None]:
    """Return a ``time.sleep`` replacement that records the requested sleep and
    advances a deterministic ``time.monotonic`` clock by the same amount, so the
    token-bucket wait math is exercised without real sleeping."""

    def _sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds

    return _sleep


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


def test_token_bucket_rejects_sub_unit_capacity() -> None:
    # Each acquire() consumes exactly 1 token and refills are capped at
    # ``capacity``, so a sub-1 capacity can never satisfy a call (the bucket
    # would oscillate around zero and every call would block). Reject it up
    # front with a clear error rather than misbehaving at runtime.
    with pytest.raises(ValueError, match="capacity must be >= 1"):
        TokenBucket(rate=10.0, capacity=0.5)


def test_token_bucket_empty_branch_fires_under_burst(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When calls arrive faster than the refill rate, the bucket drains below 1
    and the wait branch must fire (it is reachable, not dead code). Drains the
    burst capacity, then the next acquire sees tokens < 1 and waits for the
    missing token at the configured rate."""
    sleep_calls: List[float] = []
    # Deterministic clock: advance only when we say so.
    clock = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", _make_deterministic_sleep(clock, sleep_calls))

    bucket = TokenBucket(rate=2.0, capacity=2)
    bucket.acquire()  # tokens 2 -> 1 (burst)
    bucket.acquire()  # tokens 1 -> 0 (burst)
    # Bucket empty; next acquire must enter the < 1 branch and wait for one token.
    bucket.acquire()
    # wait = (1 - 0) / 2.0 = 0.5s for the missing token.
    assert sleep_calls[-1] == pytest.approx(0.5, abs=0.01)


def test_token_bucket_preserves_fractional_leftover_across_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fractional balance carried into the sleep must be preserved on wake,
    not discarded. The post-sleep block must credit ``self._tokens + elapsed*rate``
    (then consume one), not ``elapsed*rate`` (then consume one): the latter drops
    the leftover and drives ``_tokens`` negative, making the next caller over-wait.
    """
    clock = [0.0]
    sleep_calls: List[float] = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", _make_deterministic_sleep(clock, sleep_calls))

    bucket = TokenBucket(rate=1.0, capacity=2)
    bucket.acquire()  # tokens 2 -> 1
    clock[0] = 0.5  # partial refill: 1 + 0.5*1.0 = 1.5
    bucket.acquire()  # tokens 1.5 -> 0.5 (fractional leftover carried)
    # Next acquire: tokens=0.5 < 1 -> wait = (1 - 0.5)/1.0 = 0.5s.
    bucket.acquire()
    assert sleep_calls[-1] == pytest.approx(0.5, abs=0.01)
    # On wake, elapsed=0.5 -> refill 0.5*1.0=0.5; tokens = min(2, 0.5 + 0.5) - 1 = 0.0.
    # The buggy version (discarding the leftover) would compute min(2, 0.5) - 1 = -0.5.
    assert bucket._tokens == 0.0


def test_token_bucket_releases_lock_during_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lock must NOT be held while time.sleep runs — holding it serializes
    concurrent callers and defeats the point of a token bucket (bursts would
    serialize rather than pace). Catches the sleep-under-lock regression."""
    clock = [0.0]
    sleep_calls: List[float] = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", _make_deterministic_sleep(clock, sleep_calls))

    bucket = TokenBucket(rate=2.0, capacity=1)
    bucket.acquire()  # consume the single burst token

    lock_held_during_sleep = []

    def _checking_sleep(seconds: float) -> None:
        lock_held_during_sleep.append(bucket._lock.locked())
        _make_deterministic_sleep(clock, sleep_calls)(seconds)

    monkeypatch.setattr(time, "sleep", _checking_sleep)
    bucket.acquire()  # empty -> waits 0.5s
    assert sleep_calls[-1] == pytest.approx(0.5, abs=0.01)
    assert lock_held_during_sleep == [False], "lock must be released before sleep"


def test_token_bucket_does_not_overshoot_when_sleep_overruns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If time.sleep takes longer than the computed wait (scheduler latency,
    GC pause, etc.), the bucket must recompute tokens from the ACTUAL elapsed
    time on wake — not credit exactly wait*rate and risk overshooting
    capacity. Catches the overshoot regression where the old code set
    tokens=0.0 unconditionally after sleep."""
    clock = [0.0]
    sleep_calls: List[float] = []

    # Sleep advances the clock by MORE than requested (simulating scheduler lag).
    def _laggy_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds * 3  # 3x the requested wait

    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", _laggy_sleep)

    bucket = TokenBucket(rate=2.0, capacity=2)
    bucket.acquire()  # tokens 2 -> 1
    bucket.acquire()  # tokens 1 -> 0
    # Empty -> wait = (1-0)/2.0 = 0.5s, but sleep advances clock by 1.5s.
    bucket.acquire()
    assert sleep_calls[-1] == pytest.approx(0.5, abs=0.01)
    # On wake, elapsed=1.5s -> refill = 1.5*2.0 = 3.0, capped at capacity=2,
    # then one consumed -> tokens=1.0. The old code set tokens=0.0 (ignoring
    # the overrun), which would under-credit and pace too slowly.
    assert bucket._tokens == 1.0

    assert bucket._tokens == 1.0


def test_token_bucket_resets_token_state_after_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After the wait branch fires, ``_tokens`` is reset to 0.0 (one token refilled
    during the sleep and immediately consumed) and ``_last_refill`` is advanced to
    post-sleep time, so the next acquire does not double-count the wait."""
    clock = [0.0]
    sleep_calls: List[float] = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", _make_deterministic_sleep(clock, sleep_calls))

    bucket = TokenBucket(rate=1.0, capacity=1)
    bucket.acquire()  # burst token consumed, tokens -> 0
    assert bucket._tokens == 0.0

    bucket.acquire()  # empty -> waits 1.0s for one token, consumes it
    assert sleep_calls[-1] == pytest.approx(1.0, abs=0.01)
    assert bucket._tokens == 0.0  # refilled to 1 during sleep, then consumed


def test_token_bucket_caps_at_capacity_no_overfill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long gap between calls cannot refill tokens above ``capacity`` — the
    ``min(capacity, ...)`` cap bounds the bucket, so a burst after idle is at
    most ``capacity`` tokens, not elapsed*rate."""
    clock = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", lambda s: None)

    bucket = TokenBucket(rate=1.0, capacity=2)
    clock[0] = 100.0  # long idle gap; naive refill would be 100 tokens
    bucket.acquire()  # capped at capacity=2, consumes one -> tokens=1
    assert bucket._tokens == 1.0


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
