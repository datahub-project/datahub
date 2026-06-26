import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import pydantic

from datahub.ingestion.source.omni.omni_api import OmniClient


def test_throttle_enforces_minimum_interval() -> None:
    """Test that _throttle enforces the minimum interval between requests."""
    # Create an OmniClient instance with 0.1s minimum interval
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
    """Test that _throttle is thread-safe and serializes requests properly."""
    # Create an OmniClient instance with 0.05s minimum interval
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
        assert elapsed >= api._min_interval - 0.001, (  # Allow 1ms tolerance
            f"Requests {i - 1} and {i} were only {elapsed}s apart, "
            f"expected at least {api._min_interval}s"
        )


def test_throttle_no_sleep_on_first_request() -> None:
    """Test that the first request doesn't sleep."""
    api = OmniClient(
        base_url="https://test.omni.co",
        api_key=pydantic.SecretStr("test-token"),
        timeout_seconds=30,
    )
    api._min_interval = 0.1

    # First request should complete immediately
    start = time.monotonic()
    api._throttle()
    elapsed = time.monotonic() - start

    # Should complete in under 10ms (much less than the 100ms interval)
    assert elapsed < 0.01, f"First request took {elapsed}s, expected < 0.01s"


def test_throttle_respects_existing_delay() -> None:
    """Test that if enough time has passed, no additional sleep is needed."""
    api = OmniClient(
        base_url="https://test.omni.co",
        api_key=pydantic.SecretStr("test-token"),
        timeout_seconds=30,
    )
    api._min_interval = 0.05

    # Make first request
    api._throttle()

    # Wait longer than min_interval
    time.sleep(0.1)

    # Second request should not need to sleep
    start = time.monotonic()
    api._throttle()
    elapsed = time.monotonic() - start

    # Should complete quickly since we already waited
    assert elapsed < 0.01, (
        f"Second request took {elapsed}s after waiting {api._min_interval * 2}s, "
        f"expected < 0.01s"
    )
