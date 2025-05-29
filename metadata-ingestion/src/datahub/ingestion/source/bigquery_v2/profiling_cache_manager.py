import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class BigQueryCacheManager:
    """
    Centralized cache management for BigQuery operations.
    Handles query result caching to avoid redundant API calls during profiling.
    """

    def __init__(self, max_cache_size: int = 1000):
        """
        Initialize the cache manager with size limits.

        Args:
            max_cache_size: Maximum number of items in each cache
        """
        self._max_cache_size = max_cache_size
        # Initialize cache for query results to avoid redundant queries
        self._query_cache: Dict[str, Any] = {}

    def add_query_result(self, cache_key: str, results: List[Any]) -> None:
        """
        Add query results to the query cache.

        Args:
            cache_key: Cache key for the query
            results: Query results to cache
        """
        self._query_cache[cache_key] = results
        self._manage_cache_size()

    def get_query_result(self, cache_key: str) -> List[Any]:
        """
        Get cached query results if available.

        Args:
            cache_key: Cache key for the query

        Returns:
            Cached query results or empty list if not in cache
        """
        result = self._query_cache.get(cache_key)
        return [] if result is None else result

    def _manage_cache_size(self) -> None:
        """
        Manage cache size to prevent memory issues.
        When cache exceeds max_size, remove oldest entries.
        """
        if len(self._query_cache) > self._max_cache_size:
            # Remove oldest 20% of entries
            items_to_remove = int(self._max_cache_size * 0.2)
            for _ in range(items_to_remove):
                if self._query_cache:
                    self._query_cache.pop(next(iter(self._query_cache)))
