"""
Cache Manager for Snowplow connector.

Centralizes caching patterns to improve performance and reduce redundant API calls.
"""

import logging
from typing import Any, Optional, TypeVar

import cachetools

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CacheManager:
    """
    Manages multiple named caches for performance optimization.

    Provides simple get/set interface for caching expensive operations like:
    - API calls to Snowplow BDP
    - URN construction
    - Data structure listings

    Uses cachetools.LRUCache internally to prevent unbounded memory growth.

    All caches are cleared between ingestion runs to ensure fresh data.
    """

    def __init__(self, maxsize: int = 1000):
        """
        Initialize cache manager with LRU cache.

        Args:
            maxsize: Maximum number of cache entries before LRU eviction (default: 1000)
        """
        self._caches: cachetools.LRUCache = cachetools.LRUCache(maxsize=maxsize)

    def get(self, cache_name: str) -> Optional[Any]:
        """
        Get cached value by name.

        Args:
            cache_name: Name of the cache (e.g., "warehouse_urn", "data_structures")

        Returns:
            Cached value if exists, None otherwise
        """
        value = self._caches.get(cache_name)
        if value is not None:
            logger.debug(f"Cache hit for '{cache_name}'")
        return value

    def set(self, cache_name: str, value: Any) -> None:
        """
        Set cached value by name.

        Args:
            cache_name: Name of the cache
            value: Value to cache
        """
        self._caches[cache_name] = value
        logger.debug(f"Cached value for '{cache_name}'")

    def has(self, cache_name: str) -> bool:
        """
        Check if cache exists and is not None.

        Args:
            cache_name: Name of the cache

        Returns:
            True if cache exists and has a value, False otherwise
        """
        return cache_name in self._caches and self._caches[cache_name] is not None

    def clear(self, cache_name: Optional[str] = None) -> None:
        """
        Clear cache(s).

        Args:
            cache_name: If provided, clear only this cache. Otherwise clear all caches.
        """
        if cache_name:
            if cache_name in self._caches:
                del self._caches[cache_name]
                logger.debug(f"Cleared cache '{cache_name}'")
        else:
            self._caches.clear()
            logger.debug("Cleared all caches")

    def get_or_compute(self, cache_name: str, compute_fn: callable) -> Any:
        """
        Get cached value or compute if not cached.

        Convenience method that combines get + compute + set in one call.

        Args:
            cache_name: Name of the cache
            compute_fn: Function to call if cache miss (should return the value to cache)

        Returns:
            Cached or newly computed value
        """
        cached = self.get(cache_name)
        if cached is not None:
            return cached

        # Compute value
        value = compute_fn()

        # Cache it
        if value is not None:
            self.set(cache_name, value)

        return value

    def __repr__(self) -> str:
        """String representation showing cache contents."""
        cache_keys = list(self._caches.keys())
        return f"CacheManager(caches={cache_keys})"
