"""
Striped lock utility for bounded-memory concurrent locking.

This module provides a StripedLock class that maps arbitrary keys to a fixed
pool of locks using hashing. This prevents unbounded memory growth while still
providing good concurrency for different keys.

Usage:
    lock_pool = StripedLock(num_stripes=64)

    # Acquire lock for a specific key
    with lock_pool.get_lock("user_123__plugin_456"):
        # Critical section - only one thread with this key at a time
        refresh_token()

Design:
- Fixed number of locks (stripes) allocated at initialization
- Keys are hashed to select which stripe to use
- Hash collisions cause serialization but are harmless (just slower)
- Memory usage is O(num_stripes), not O(num_keys)
"""

import threading
from typing import List


class StripedLock:
    """
    A pool of locks that maps arbitrary string keys to a fixed set of stripes.

    This provides similar semantics to per-key locking but with bounded memory.
    Multiple keys may hash to the same stripe, causing them to serialize,
    but this is harmless - it just reduces concurrency slightly.

    Attributes:
        num_stripes: Number of lock stripes in the pool.
    """

    def __init__(self, num_stripes: int = 64) -> None:
        """
        Initialize the striped lock pool.

        Args:
            num_stripes: Number of lock stripes. Higher values reduce contention
                         but use more memory. 64 is a reasonable default.
                         Should be a power of 2 for efficient modulo operation.
        """
        if num_stripes < 1:
            raise ValueError("num_stripes must be at least 1")

        self._num_stripes = num_stripes
        self._locks: List[threading.Lock] = [
            threading.Lock() for _ in range(num_stripes)
        ]

    @property
    def num_stripes(self) -> int:
        """Return the number of stripes in this lock pool."""
        return self._num_stripes

    def get_lock(self, key: str) -> threading.Lock:
        """
        Get the lock for a given key.

        The key is hashed to select one of the stripe locks. Different keys
        may map to the same lock (hash collision), which is safe but reduces
        concurrency for those specific keys.

        Args:
            key: The key to get a lock for (e.g., "user_urn__plugin_id")

        Returns:
            The threading.Lock for this key's stripe.

        Example:
            with lock_pool.get_lock("my_key"):
                # Critical section
                do_work()
        """
        stripe_index = hash(key) % self._num_stripes
        return self._locks[stripe_index]

    def __repr__(self) -> str:
        return f"StripedLock(num_stripes={self._num_stripes})"
