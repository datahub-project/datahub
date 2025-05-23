import logging
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class BigQueryCacheManager:
    """
    Centralized cache management for BigQuery operations.
    Handles various caches used during profiling to avoid redundant API calls.
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
        # Cache for partition information with better organization
        self._partition_info_cache: Dict[str, Dict[str, Any]] = {}
        # Enhanced cache for table metadata
        self._table_metadata_cache: Dict[str, Dict[str, Any]] = {}
        # Track queried tables to avoid redundant API calls
        self._queried_tables: Set[str] = set()
        # Cache for successful partition filters - project.dataset.table -> filters
        self._successful_filters_cache: Dict[str, List[str]] = {}
        # Cache for known column types to reduce schema queries - project.dataset.table.column -> type
        self._column_type_cache: Dict[str, str] = {}
        # Cache for verified filter combinations - filters hash -> success/failure
        self._filter_verification_cache: Dict[str, bool] = {}
        # Cache for problematic tables to avoid repeated failures
        self._problematic_tables_cache: Dict[str, Dict[str, Any]] = {}
        # Track tables that have been profiled with simplified queries
        self._simplified_profile_tables: Set[str] = set()

    def clear_all_caches(self) -> None:
        """Clear all caches."""
        self._query_cache.clear()
        self._partition_info_cache.clear()
        self._table_metadata_cache.clear()
        self._queried_tables.clear()
        self._successful_filters_cache.clear()
        self._column_type_cache.clear()
        self._filter_verification_cache.clear()

    def _manage_cache_size(self, cache: Dict, max_size: Optional[int] = None) -> None:
        """
        Manage cache size to prevent memory issues.
        When cache exceeds max_size, remove oldest entries.

        Args:
            cache: Cache dictionary to manage
            max_size: Maximum size for this specific cache, defaults to class default
        """
        if max_size is None:
            max_size = self._max_cache_size

        if len(cache) > max_size:
            # Remove oldest 20% of entries
            items_to_remove = int(max_size * 0.2)
            for _ in range(items_to_remove):
                if cache:
                    cache.pop(next(iter(cache)))

    def add_to_cache(
        self, cache: Dict, key: str, value: Any, max_size: Optional[int] = None
    ) -> None:
        """
        Add item to cache with size management.

        Args:
            cache: Cache dictionary to add to
            key: Cache key
            value: Value to cache
            max_size: Maximum size for this specific cache
        """
        cache[key] = value
        self._manage_cache_size(cache, max_size)

    def get_from_cache(self, cache: Dict, key: str) -> Optional[Any]:
        """
        Get item from cache if it exists.

        Args:
            cache: Cache dictionary to get from
            key: Cache key to retrieve

        Returns:
            Cached value or None if not in cache
        """
        return cache.get(key)

    def add_query_result(self, cache_key: str, results: List[Any]) -> None:
        """
        Add query results to the query cache.

        Args:
            cache_key: Cache key for the query
            results: Query results to cache
        """
        self.add_to_cache(self._query_cache, cache_key, results)

    def get_query_result(self, cache_key: str) -> List[Any]:
        """
        Get cached query results if available.

        Args:
            cache_key: Cache key for the query

        Returns:
            Cached query results or empty list if not in cache
        """
        result = self.get_from_cache(self._query_cache, cache_key)
        return [] if result is None else result

    def add_table_metadata(self, table_key: str, metadata: Dict[str, Any]) -> None:
        """
        Add table metadata to the cache.

        Args:
            table_key: Table identifier (project.dataset.table)
            metadata: Table metadata to cache
        """
        self.add_to_cache(self._table_metadata_cache, table_key, metadata)

    def get_table_metadata(self, table_key: str) -> Optional[Dict[str, Any]]:
        """
        Get cached table metadata if available.

        Args:
            table_key: Table identifier (project.dataset.table)

        Returns:
            Cached table metadata or None if not in cache
        """
        return self.get_from_cache(self._table_metadata_cache, table_key)

    def add_successful_filters(self, table_key: str, filters: List[str]) -> None:
        """
        Cache successful partition filters for a table.

        Args:
            table_key: Table identifier (project.dataset.table)
            filters: List of successful filter strings
        """
        self.add_to_cache(self._successful_filters_cache, table_key, filters)

    def get_successful_filters(self, table_key: str) -> Optional[List[str]]:
        """
        Get cached successful filters for a table if available.

        Args:
            table_key: Table identifier (project.dataset.table)

        Returns:
            List of filter strings or None if not in cache
        """
        return self.get_from_cache(self._successful_filters_cache, table_key)

    def add_problematic_table(
        self, table_key: str, error: str, timestamp: float
    ) -> None:
        """
        Mark a table as problematic to avoid repeated failures.

        Args:
            table_key: Table identifier (project.dataset.table)
            error: Error message
            timestamp: Time when the error occurred
        """
        self.add_to_cache(
            self._problematic_tables_cache,
            table_key,
            {"error": error, "timestamp": timestamp},
        )

    def is_problematic_table(
        self, table_key: str, max_age_seconds: float = 86400
    ) -> bool:
        """
        Check if a table is marked as problematic and the entry is still valid.

        Args:
            table_key: Table identifier (project.dataset.table)
            max_age_seconds: Maximum age for the problematic entry to be considered valid

        Returns:
            True if the table is marked as problematic and the entry is not expired
        """
        entry = self.get_from_cache(self._problematic_tables_cache, table_key)
        if entry:
            import time

            # Check if the entry is still valid (not expired)
            return time.time() - entry.get("timestamp", 0) < max_age_seconds
        return False

    def mark_table_as_queried(self, table_key: str) -> None:
        """
        Mark a table as queried to avoid redundant processing.

        Args:
            table_key: Table identifier (project.dataset.table)
        """
        self._queried_tables.add(table_key)

    def is_table_queried(self, table_key: str) -> bool:
        """
        Check if a table has already been queried.

        Args:
            table_key: Table identifier (project.dataset.table)

        Returns:
            True if the table has been queried
        """
        return table_key in self._queried_tables

    def mark_simplified_profile(self, table_key: str) -> None:
        """
        Mark a table as having been profiled with simplified queries.

        Args:
            table_key: Table identifier (project.dataset.table)
        """
        self._simplified_profile_tables.add(table_key)

    def used_simplified_profile(self, table_key: str) -> bool:
        """
        Check if a table was profiled with simplified queries.

        Args:
            table_key: Table identifier (project.dataset.table)

        Returns:
            True if the table was profiled with simplified queries
        """
        return table_key in self._simplified_profile_tables
