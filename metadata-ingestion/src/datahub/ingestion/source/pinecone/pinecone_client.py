"""Pinecone API client wrapper for metadata extraction."""

import logging
import time
from dataclasses import dataclass
from functools import lru_cache, wraps
from typing import Any, Callable, Dict, List, Optional

from pinecone import Pinecone

from datahub.ingestion.source.pinecone.config import PineconeConfig

logger = logging.getLogger(__name__)

# Default namespace constant to avoid empty string URN issues
DEFAULT_NAMESPACE = "__default__"


def with_retry(max_retries: int = 3, backoff_factor: float = 2.0) -> Callable:
    """
    Decorator for exponential backoff on API calls.
    
    Retries on rate limit errors with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for wait time between retries
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_msg = str(e).lower()
                    is_rate_limit = any(
                        phrase in error_msg
                        for phrase in ["rate limit", "too many requests", "429"]
                    )
                    
                    if attempt == max_retries - 1:
                        # Last attempt, re-raise the error
                        raise
                    
                    if is_rate_limit:
                        wait_time = backoff_factor ** attempt
                        logger.warning(
                            f"Rate limited on {func.__name__}, "
                            f"waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}"
                        )
                        time.sleep(wait_time)
                    else:
                        # Not a rate limit error, re-raise immediately
                        raise
            return None  # Should never reach here
        return wrapper
    return decorator


@dataclass
class IndexInfo:
    """Information about a Pinecone index."""

    name: str
    dimension: int
    metric: str
    host: str
    status: str
    spec: Dict[str, Any]  # Contains pod or serverless spec details


@dataclass
class NamespaceStats:
    """Statistics for a namespace within an index."""

    name: str
    vector_count: int


@dataclass
class VectorRecord:
    """A vector record with metadata."""

    id: str
    values: Optional[List[float]]
    metadata: Optional[Dict[str, Any]]


class PineconeClient:
    """
    Wrapper around Pinecone SDK for metadata extraction.
    
    Handles both serverless and pod-based indexes.
    """

    def __init__(self, config: PineconeConfig):
        """
        Initialize Pinecone client.
        
        Args:
            config: PineconeConfig with API key and optional environment
        """
        self.config = config
        
        # Initialize Pinecone client
        init_kwargs: Dict[str, Any] = {
            "api_key": config.api_key.get_secret_value()
        }
        
        # Add environment for pod-based indexes if specified
        if config.environment:
            init_kwargs["environment"] = config.environment
        
        self.pc = Pinecone(**init_kwargs)
        logger.info("Initialized Pinecone client")

    @lru_cache(maxsize=10)
    def _get_index(self, index_name: str) -> Any:
        """
        Get or create cached index connection.
        
        Uses LRU cache to reuse index connections and improve performance.
        
        Args:
            index_name: Name of the index
            
        Returns:
            Pinecone index object
        """
        logger.debug(f"Getting index connection for {index_name}")
        return self.pc.Index(index_name)

    @with_retry()
    def list_indexes(self) -> List[IndexInfo]:
        """
        List all indexes in the Pinecone account.
        
        Returns:
            List of IndexInfo objects
        """
        try:
            indexes = self.pc.list_indexes()
            result = []
            
            for index in indexes:
                # Extract index information
                index_name = index.get("name")
                if not index_name:
                    logger.warning(f"Index without name found: {index}")
                    continue
                
                # Get detailed index description
                try:
                    description = self.pc.describe_index(index_name)
                    
                    index_info = IndexInfo(
                        name=index_name,
                        dimension=description.get("dimension", 0),
                        metric=description.get("metric", "unknown"),
                        host=description.get("host", ""),
                        status=description.get("status", {}).get("state", "unknown"),
                        spec=description.get("spec", {}),
                    )
                    result.append(index_info)
                    logger.info(f"Found index: {index_name}")
                    
                except Exception as e:
                    logger.warning(f"Failed to describe index {index_name}: {e}")
                    continue
            
            logger.info(f"Listed {len(result)} indexes")
            return result
            
        except Exception as e:
            logger.error(f"Failed to list indexes: {e}")
            raise

    @with_retry()
    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        """
        Get statistics for an index including namespace information.
        
        Args:
            index_name: Name of the index
            
        Returns:
            Dictionary with index statistics including namespaces
        """
        try:
            # Use cached index connection
            index = self._get_index(index_name)
            
            # Get index statistics
            stats = index.describe_index_stats()
            
            logger.debug(f"Retrieved stats for index {index_name}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get stats for index {index_name}: {e}")
            raise

    def list_namespaces(self, index_name: str) -> List[NamespaceStats]:
        """
        List all namespaces in an index with their vector counts.
        
        Works for both serverless and pod-based indexes by using describe_index_stats.
        
        Args:
            index_name: Name of the index
            
        Returns:
            List of NamespaceStats objects
        """
        try:
            stats = self.get_index_stats(index_name)
            
            # Extract namespace information from stats
            namespaces_dict = stats.get("namespaces", {})
            
            result = []
            for namespace_name, namespace_data in namespaces_dict.items():
                vector_count = namespace_data.get("vector_count", 0)
                
                namespace_stats = NamespaceStats(
                    name=namespace_name,
                    vector_count=vector_count,
                )
                result.append(namespace_stats)
            
            # If no namespaces found, check if there's a default namespace
            if not result:
                total_vector_count = stats.get("total_vector_count", 0)
                if total_vector_count > 0:
                    # There are vectors but no namespace info - use default namespace constant
                    logger.info(
                        f"Index {index_name} has {total_vector_count} vectors "
                        f"but no namespace information. Using default namespace '{DEFAULT_NAMESPACE}'."
                    )
                    result.append(
                        NamespaceStats(
                            name=DEFAULT_NAMESPACE,
                            vector_count=total_vector_count,
                        )
                    )
            
            logger.info(f"Found {len(result)} namespaces in index {index_name}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to list namespaces for index {index_name}: {e}")
            raise

    def sample_vectors(
        self, index_name: str, namespace: str, limit: int
    ) -> List[VectorRecord]:
        """
        Sample vectors from a namespace for schema inference.
        
        Uses the list() and fetch() approach for deterministic sampling.
        Falls back to query() if list() is not available.
        
        Args:
            index_name: Name of the index
            namespace: Namespace to sample from (empty string for default)
            limit: Maximum number of vectors to sample
            
        Returns:
            List of VectorRecord objects with metadata
        """
        try:
            # Use cached index connection
            index = self._get_index(index_name)
            
            # Try list() + fetch() approach first (more deterministic)
            vector_ids = self._list_vector_ids(index, namespace, limit)
            
            if vector_ids:
                return self._fetch_vectors(index, namespace, vector_ids)
            
            # Fallback to query() approach if list() didn't work
            logger.info(
                f"list() approach failed for {index_name}/{namespace}, "
                "trying query() fallback"
            )
            return self._query_sample_vectors(index, namespace, limit)
            
        except Exception as e:
            logger.error(
                f"Failed to sample vectors from {index_name}/{namespace}: {e}"
            )
            return []

    def _list_vector_ids(
        self, index: Any, namespace: str, limit: int
    ) -> List[str]:
        """
        List vector IDs from an index namespace.
        
        Args:
            index: Pinecone index object
            namespace: Namespace name
            limit: Maximum number of IDs to retrieve
            
        Returns:
            List of vector IDs
        """
        try:
            vector_ids = []
            
            # Use list() to get vector IDs
            list_result = index.list(namespace=namespace, limit=limit)
            
            # Handle different response formats
            if hasattr(list_result, "vectors"):
                # Response has vectors attribute
                vector_ids = [v.id for v in list_result.vectors[:limit]]
            elif isinstance(list_result, list):
                # Response is a list
                vector_ids = list_result[:limit]
            else:
                # Try to iterate (generator)
                for i, vector_id in enumerate(list_result):
                    if i >= limit:
                        break
                    vector_ids.append(vector_id)
            
            logger.debug(f"Listed {len(vector_ids)} vector IDs")
            return vector_ids
            
        except Exception as e:
            logger.debug(f"Failed to list vector IDs: {e}")
            return []

    def _fetch_vectors(
        self, index: Any, namespace: str, vector_ids: List[str]
    ) -> List[VectorRecord]:
        """
        Fetch vectors by IDs with metadata.
        
        Args:
            index: Pinecone index object
            namespace: Namespace name
            vector_ids: List of vector IDs to fetch
            
        Returns:
            List of VectorRecord objects
        """
        try:
            fetch_response = index.fetch(ids=vector_ids, namespace=namespace)
            
            result = []
            vectors_dict = fetch_response.get("vectors", {})
            
            for vector_id, vector_data in vectors_dict.items():
                record = VectorRecord(
                    id=vector_id,
                    values=vector_data.get("values"),
                    metadata=vector_data.get("metadata"),
                )
                result.append(record)
            
            logger.info(f"Fetched {len(result)} vectors with metadata")
            return result
            
        except Exception as e:
            logger.warning(f"Failed to fetch vectors: {e}")
            return []

    def _query_sample_vectors(
        self, index: Any, namespace: str, limit: int
    ) -> List[VectorRecord]:
        """
        Sample vectors using query() as a fallback.
        
        Uses a dummy/random vector to query and get samples.
        
        Args:
            index: Pinecone index object
            namespace: Namespace name
            limit: Maximum number of vectors to sample
            
        Returns:
            List of VectorRecord objects
        """
        try:
            # Get index stats to determine dimension
            stats = index.describe_index_stats()
            dimension = stats.get("dimension")
            
            # Validate dimension
            if not dimension:
                logger.error(f"Cannot determine dimension for index, skipping query sampling")
                return []
            
            # Create a dummy query vector (all zeros)
            dummy_vector = [0.0] * dimension
            
            # Query with the dummy vector
            query_response = index.query(
                vector=dummy_vector,
                top_k=min(limit, 100),  # Pinecone has limits on top_k
                namespace=namespace,
                include_metadata=True,
                include_values=False,  # We don't need the actual vector values
            )
            
            result = []
            matches = query_response.get("matches", [])
            
            for match in matches:
                record = VectorRecord(
                    id=match.get("id"),
                    values=None,  # Not included in query response
                    metadata=match.get("metadata"),
                )
                result.append(record)
            
            logger.info(f"Sampled {len(result)} vectors via query()")
            return result
            
        except Exception as e:
            logger.warning(f"Failed to sample vectors via query: {e}")
            return []

    def get_index_host(self, index_name: str) -> Optional[str]:
        """
        Get the host URL for an index.
        
        Args:
            index_name: Name of the index
            
        Returns:
            Host URL or None if not found
        """
        # Check manual mapping first
        if self.config.index_host_mapping and index_name in self.config.index_host_mapping:
            return self.config.index_host_mapping[index_name]
        
        # Otherwise get from describe_index
        try:
            description = self.pc.describe_index(index_name)
            return description.get("host")
        except Exception as e:
            logger.warning(f"Failed to get host for index {index_name}: {e}")
            return None
