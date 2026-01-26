"""Unit tests for StripedLock utility."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from datahub_integrations.utils.striped_lock import StripedLock


class TestStripedLock:
    """Tests for the StripedLock class."""

    def test_initialization_default_stripes(self) -> None:
        """Test that default initialization creates 64 stripes."""
        lock_pool = StripedLock()
        assert lock_pool.num_stripes == 64

    def test_initialization_custom_stripes(self) -> None:
        """Test that custom stripe count is respected."""
        lock_pool = StripedLock(num_stripes=128)
        assert lock_pool.num_stripes == 128

    def test_initialization_invalid_stripes(self) -> None:
        """Test that invalid stripe count raises ValueError."""
        with pytest.raises(ValueError, match="at least 1"):
            StripedLock(num_stripes=0)

        with pytest.raises(ValueError, match="at least 1"):
            StripedLock(num_stripes=-1)

    def test_get_lock_returns_lock(self) -> None:
        """Test that get_lock returns a threading.Lock."""
        lock_pool = StripedLock(num_stripes=8)
        lock = lock_pool.get_lock("test_key")
        assert isinstance(lock, type(threading.Lock()))

    def test_same_key_returns_same_lock(self) -> None:
        """Test that the same key always maps to the same lock."""
        lock_pool = StripedLock(num_stripes=8)
        lock1 = lock_pool.get_lock("my_key")
        lock2 = lock_pool.get_lock("my_key")
        assert lock1 is lock2

    def test_different_keys_may_share_lock(self) -> None:
        """Test that different keys may hash to the same stripe."""
        # With only 2 stripes, many keys will collide
        lock_pool = StripedLock(num_stripes=2)

        # Collect locks for many keys
        locks = [lock_pool.get_lock(f"key_{i}") for i in range(100)]

        # All locks should be one of the two stripes
        unique_locks = set(id(lock) for lock in locks)
        assert len(unique_locks) <= 2

    def test_bounded_memory(self) -> None:
        """Test that memory is bounded regardless of key count."""
        lock_pool = StripedLock(num_stripes=8)

        # Access many different keys
        for i in range(10000):
            lock_pool.get_lock(f"user_{i}__plugin_{i % 10}")

        # Should still have only 8 locks
        assert len(lock_pool._locks) == 8

    def test_lock_provides_mutual_exclusion(self) -> None:
        """Test that locks actually provide mutual exclusion."""
        lock_pool = StripedLock(num_stripes=8)
        counter = {"value": 0}
        iterations = 1000

        def increment() -> None:
            for _ in range(iterations):
                with lock_pool.get_lock("shared_counter"):
                    # Read-modify-write without lock would cause race
                    current = counter["value"]
                    counter["value"] = current + 1

        # Run multiple threads
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(increment) for _ in range(4)]
            for f in futures:
                f.result()

        # If locking works, counter should be exactly 4 * iterations
        assert counter["value"] == 4 * iterations

    def test_repr(self) -> None:
        """Test string representation."""
        lock_pool = StripedLock(num_stripes=32)
        assert repr(lock_pool) == "StripedLock(num_stripes=32)"
