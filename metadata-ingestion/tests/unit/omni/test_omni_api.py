import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import pydantic

from datahub.ingestion.source.omni.omni_api import OmniClient


def test_throttle_enforces_minimum_interval() -> None:
    """Test that _throttle enforces the minimum interval between requests."""
    api = OmniClient(
        base_url="https://test.omni.co",
        api_key=pydantic.SecretStr("test-token"),
        timeout_seconds=30,
    )
    api._min_interval = 0.1

    # Make two sequential requests and measure the time between them
    api._throttle()
    first_request_time = time.monotonic()

    api._throttle()
    second_request_time = time.monotonic()

    # The second request should have been delayed by at least _min_interval
    elapsed = second_request_time - first_request_time
    assert elapsed >= api._min_interval, (
        f"Expected at least {api._min_interval}s between requests, got {elapsed}s"
    )


def test_throttle_thread_safety() -> None:
    """Test that _throttle is thread-safe and enforces rate limiting with concurrent threads."""
    api = OmniClient(
        base_url="https://test.omni.co",
        api_key=pydantic.SecretStr("test-token"),
        timeout_seconds=30,
    )
    api._min_interval = 0.05

    num_threads = 10
    request_times: List[float] = []

    def make_throttled_request() -> float:
        """Execute _throttle and record the time."""
        api._throttle()
        return time.monotonic()

    # Execute throttled requests in parallel
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(make_throttled_request) for _ in range(num_threads)]
        for future in as_completed(futures):
            request_times.append(future.result())

    # Sort times to verify sequential ordering
    request_times.sort()

    # Verify that each request is separated by at least _min_interval
    for i in range(1, len(request_times)):
        elapsed = request_times[i] - request_times[i - 1]
        assert elapsed >= api._min_interval - 0.003, (  # Allow 3ms tolerance
            f"Requests {i - 1} and {i} were only {elapsed}s apart, "
            f"expected at least {api._min_interval}s"
        )
