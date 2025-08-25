"""
File-backed caching utilities for Dremio source to prevent OOM errors.

This module provides optional file-backed caching that can be enabled via configuration
to handle large datasets without running out of memory.
"""

import logging
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class DremioFileBackedCache:
    """
    A file-backed cache using SQLite to store intermediate results and prevent OOM errors.

    This cache is designed to be a drop-in replacement for in-memory storage when
    processing large numbers of containers and datasets from Dremio.
    """

    def __init__(
        self,
        cache_size: int = 1000,
        eviction_batch_size: int = 100,
        temp_dir: Optional[str] = None,
    ):
        """
        Initialize the file-backed cache.

        Args:
            cache_size: Maximum number of items to keep in memory before writing to disk
            eviction_batch_size: Number of items to evict at once when cache is full
            temp_dir: Directory for temporary files (uses system temp if None)
        """
        self.cache_size = cache_size
        self.eviction_batch_size = eviction_batch_size
        self.temp_dir = Path(temp_dir) if temp_dir else Path(tempfile.gettempdir())

        # Create temporary SQLite database
        self.db_path = self.temp_dir / f"dremio_cache_{id(self)}.db"
        self.connection = sqlite3.connect(str(self.db_path))
        self.connection.execute("""
            CREATE TABLE cache_items (
                key TEXT PRIMARY KEY,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.connection.commit()

        # In-memory cache for frequently accessed items
        self._memory_cache: Dict[str, Any] = {}

        logger.info(
            f"Initialized DremioFileBackedCache with cache_size={cache_size}, "
            f"db_path={self.db_path}"
        )

    def put(self, key: str, value: Any) -> None:
        """Store an item in the cache."""

        # Store in memory cache first
        self._memory_cache[key] = value

        # If memory cache is full, evict some items to disk
        if len(self._memory_cache) > self.cache_size:
            self._evict_to_disk()

    def get(self, key: str) -> Optional[Any]:
        """Retrieve an item from the cache."""
        # Check memory cache first
        if key in self._memory_cache:
            return self._memory_cache[key]

        # Check disk cache
        import json

        cursor = self.connection.execute(
            "SELECT value FROM cache_items WHERE key = ?", (key,)
        )
        row = cursor.fetchone()
        if row:
            try:
                value = json.loads(row[0])
                # Promote back to memory cache
                self._memory_cache[key] = value
                return value
            except json.JSONDecodeError:
                logger.warning(f"Failed to deserialize cached value for key: {key}")

        return None

    def _evict_to_disk(self) -> None:
        """Evict items from memory cache to disk storage."""
        import json

        # Evict oldest items (simple FIFO for now)
        items_to_evict = list(self._memory_cache.items())[: self.eviction_batch_size]

        for key, value in items_to_evict:
            try:
                serialized_value = json.dumps(value)
                self.connection.execute(
                    "INSERT OR REPLACE INTO cache_items (key, value) VALUES (?, ?)",
                    (key, serialized_value),
                )
                del self._memory_cache[key]
            except (TypeError, ValueError) as e:
                logger.warning(f"Failed to serialize value for key {key}: {e}")

        self.connection.commit()
        logger.debug(f"Evicted {len(items_to_evict)} items to disk")

    def clear(self) -> None:
        """Clear all cached items."""
        self._memory_cache.clear()
        self.connection.execute("DELETE FROM cache_items")
        self.connection.commit()

    def close(self) -> None:
        """Clean up resources."""
        try:
            self.connection.close()
            if self.db_path.exists():
                self.db_path.unlink()
                logger.debug(f"Cleaned up cache database: {self.db_path}")
        except Exception as e:
            logger.warning(f"Error cleaning up cache: {e}")


class DremioChunkedProcessor:
    """
    Processes Dremio entities in chunks to prevent memory issues.

    This processor works with the file-backed cache to handle large numbers
    of containers and datasets by processing them in manageable batches.
    """

    def __init__(
        self,
        cache: DremioFileBackedCache,
        max_containers_per_batch: int = 10,
    ):
        """
        Initialize the chunked processor.

        Args:
            cache: File-backed cache instance
            max_containers_per_batch: Maximum containers to process in one batch
        """
        self.cache = cache
        self.max_containers_per_batch = max_containers_per_batch

        logger.info(
            f"Initialized DremioChunkedProcessor with "
            f"max_containers_per_batch={max_containers_per_batch}"
        )

    def process_containers_chunked(
        self, containers: List[Dict[str, Any]]
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Process containers in chunks.

        Args:
            containers: List of container dictionaries

        Yields:
            Batches of containers for processing
        """
        for i in range(0, len(containers), self.max_containers_per_batch):
            batch = containers[i : i + self.max_containers_per_batch]

            # Cache the batch for potential reuse
            batch_key = f"container_batch_{i}_{i + len(batch)}"
            self.cache.put(batch_key, batch)

            yield batch

            logger.debug(
                f"Processed container batch {i // self.max_containers_per_batch + 1}"
            )

    def process_datasets_chunked(
        self, datasets: List[Dict[str, Any]], batch_size: int = 50
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Process datasets in chunks.

        Args:
            datasets: List of dataset dictionaries
            batch_size: Number of datasets per batch

        Yields:
            Batches of datasets for processing
        """
        for i in range(0, len(datasets), batch_size):
            batch = datasets[i : i + batch_size]

            # Cache the batch
            batch_key = f"dataset_batch_{i}_{i + len(batch)}"
            self.cache.put(batch_key, batch)

            yield batch

            logger.debug(f"Processed dataset batch {i // batch_size + 1}")
