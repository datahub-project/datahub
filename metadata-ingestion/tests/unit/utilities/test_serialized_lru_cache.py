import threading
import time

from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.serialized_lru_cache import serialized_lru_cache


def test_cache_hit() -> None:
    @serialized_lru_cache(maxsize=2)
    def fetch_data(x):
        return x * 2

    assert fetch_data(1) == 2  # Cache miss
    assert fetch_data(1) == 2  # Cache hit
    assert fetch_data.cache_info().hits == 1  # type: ignore
    assert fetch_data.cache_info().misses == 1  # type: ignore


def test_cache_eviction() -> None:
    @serialized_lru_cache(maxsize=2)
    def compute(x):
        return x * 2

    compute(1)
    compute(2)
    compute(3)  # Should evict the first entry (1)
    assert compute.cache_info().currsize == 2  # type: ignore
    assert compute.cache_info().misses == 3  # type: ignore
    assert compute(1) == 2  # Cache miss, since it was evicted
    assert compute.cache_info().misses == 4  # type: ignore


def test_thread_safety() -> None:
    @serialized_lru_cache(maxsize=5)
    def compute(x):
        time.sleep(0.2)  # Simulate some delay
        return x * 2

    threads = []
    results = [None] * 10

    def thread_func(index, arg):
        results[index] = compute(arg)

    with PerfTimer() as timer:
        for i in range(10):
            thread = threading.Thread(target=thread_func, args=(i, i % 5))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    assert len(set(results)) == 5  # Only 5 unique results should be there
    assert compute.cache_info().currsize <= 5  # type: ignore
    # Only 5 unique calls should miss the cache
    assert compute.cache_info().misses == 5  # type: ignore

    # Should take less than 1 second. If not, it means all calls were run serially.
    assert timer.elapsed_seconds() < 1


def test_concurrent_access_to_same_key() -> None:
    @serialized_lru_cache(maxsize=3)
    def compute(x: int) -> int:
        time.sleep(0.2)  # Simulate some delay
        return x * 2

    threads = []
    results = []

    def thread_func():
        results.append(compute(1))

    with PerfTimer() as timer:
        for _ in range(10):
            thread = threading.Thread(target=thread_func)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    assert all(result == 2 for result in results)  # All should compute the same result

    # 9 hits, as the first one is a miss
    assert compute.cache_info().hits == 9  # type: ignore
    # Only the first call is a miss
    assert compute.cache_info().misses == 1  # type: ignore

    # Should take less than 1 second. If not, it means all calls were run serially.
    assert timer.elapsed_seconds() < 1
