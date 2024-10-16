from collections import defaultdict
from datetime import datetime
from typing import Dict

from datahub.utilities.ratelimiter import RateLimiter


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
