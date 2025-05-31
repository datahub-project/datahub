import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.bigquery_v2.profiler import BigqueryTable

logger = logging.getLogger(__name__)


class PartitionCacheManager:
    """
    Manages caching of partition filters to avoid repeated discovery.
    """

    def __init__(self, cache_ttl_seconds: int = 3600):
        """
        Initialize the cache manager

        Args:
            cache_ttl_seconds: Time to live for cache entries in seconds (default 1 hour)
        """
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl_seconds = cache_ttl_seconds

    def _generate_cache_key(self, project: str, schema: str, table_name: str) -> str:
        """
        Generate a cache key for a table.

        Args:
            project: The project ID
            schema: The dataset name
            table_name: The table name

        Returns:
            Cache key string
        """
        return f"{project}:{schema}:{table_name}"

    def get_cached_filters(
        self, project: str, schema: str, table: BigqueryTable
    ) -> Optional[List[str]]:
        """
        Get cached partition filters if available.

        Args:
            project: The project ID
            schema: The dataset name
            table: The BigQuery table object

        Returns:
            List of partition filter strings if cached, None otherwise
        """
        cache_key = self._generate_cache_key(project, schema, table.name)

        if cache_key in self._cache:
            cache_entry = self._cache[cache_key]

            # Check if entry is still valid
            cached_time = cache_entry.get("timestamp")
            if (
                cached_time
                and (datetime.now() - cached_time).total_seconds()
                <= self._cache_ttl_seconds
            ):
                return cache_entry.get("filters")

        return None

    def cache_filters(
        self, project: str, schema: str, table: BigqueryTable, filters: List[str]
    ) -> None:
        """
        Cache partition filters for a table.

        Args:
            project: The project ID
            schema: The dataset name
            table: The BigQuery table object
            filters: List of partition filter strings
        """
        cache_key = self._generate_cache_key(project, schema, table.name)

        self._cache[cache_key] = {
            "filters": filters,
            "timestamp": datetime.now(),
            "table_id": table.id,
        }

        logger.debug(f"Cached partition filters for {table.name}: {filters}")

    def invalidate_cache(self, project: str, schema: str, table_name: str) -> None:
        """
        Invalidate cache entry for a table.

        Args:
            project: The project ID
            schema: The dataset name
            table_name: The table name
        """
        cache_key = self._generate_cache_key(project, schema, table_name)

        if cache_key in self._cache:
            del self._cache[cache_key]
            logger.debug(f"Invalidated cache for {table_name}")

    def cleanup_expired_entries(self) -> None:
        """
        Clean up expired cache entries.
        """
        now = datetime.now()
        keys_to_remove = []

        for key, entry in self._cache.items():
            cached_time = entry.get("timestamp")
            if (
                cached_time
                and (now - cached_time).total_seconds() > self._cache_ttl_seconds
            ):
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._cache[key]

        if keys_to_remove:
            logger.debug(f"Cleaned up {len(keys_to_remove)} expired cache entries")
