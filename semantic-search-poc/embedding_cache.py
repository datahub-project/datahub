"""
Caching system for embeddings to avoid redundant API calls.
"""
import hashlib
import json
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Dict, Any, TypedDict
from embeddings_base import BaseEmbeddings


class CacheStats(TypedDict):
    cache_hits: int
    cache_misses: int


class EmbeddingCache(ABC):
    """Abstract base class for embedding cache implementations."""
    
    @abstractmethod
    def try_get(self, text: str, model_id: str) -> Optional[List[float]]:
        """
        Try to retrieve a cached embedding.
        
        Args:
            text: The text that was embedded
            model_id: The model used for embedding
            
        Returns:
            The cached embedding vector if found, None otherwise
        """
        pass
    
    @abstractmethod
    def store(self, text: str, model_id: str, embedding: List[float]) -> None:
        """
        Store an embedding in the cache.
        
        Args:
            text: The text that was embedded
            model_id: The model used for embedding
            embedding: The embedding vector to cache
        """
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all cached embeddings."""
        pass

    @abstractmethod
    def get_cache_stats(self) -> CacheStats:
        """Return lightweight cache stats used by wrappers and UIs."""
        pass
    
    def _generate_key(self, text: str, model_id: str) -> str:
        """
        Generate a unique cache key for a text/model combination.
        
        Args:
            text: The text that was embedded
            model_id: The model used for embedding
            
        Returns:
            A unique hash key
        """
        combined = f"{model_id}:{text}"
        return hashlib.sha256(combined.encode()).hexdigest()


class LocalFileEmbeddingCache(EmbeddingCache):
    """
    File-based embedding cache that stores embeddings in /tmp.
    Each embedding is stored as a separate JSON file.
    """
    
    def __init__(self, cache_dir: Optional[str] = None):
        """
        Initialize the file-based cache.
        
        Args:
            cache_dir: Optional custom cache directory. 
                      Defaults to /tmp/datahub_embeddings_cache
        """
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            # Use system temp directory with a specific subfolder
            temp_base = Path(tempfile.gettempdir())
            self.cache_dir = temp_base / "datahub_embeddings_cache"
        
        # Create cache directory if it doesn't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Track statistics (private)
        self._hits = 0
        self._misses = 0
    
    def try_get(self, text: str, model_id: str) -> Optional[List[float]]:
        """
        Try to retrieve a cached embedding from disk.
        
        Args:
            text: The text that was embedded
            model_id: The model used for embedding
            
        Returns:
            The cached embedding vector if found, None otherwise
        """
        cache_key = self._generate_key(text, model_id)
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    
                # Validate the cached data
                if 'embedding' in data and 'model_id' in data:
                    # Double-check model matches (for safety)
                    if data['model_id'] == model_id:
                        self._hits += 1
                        return data['embedding']
            except (json.JSONDecodeError, IOError):
                # Corrupted cache file, remove it
                cache_file.unlink(missing_ok=True)
        
        self._misses += 1
        return None
    
    def store(self, text: str, model_id: str, embedding: List[float]) -> None:
        """
        Store an embedding to disk.
        
        Args:
            text: The text that was embedded
            model_id: The model used for embedding
            embedding: The embedding vector to cache
        """
        cache_key = self._generate_key(text, model_id)
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        # Store embedding with metadata
        data = {
            'model_id': model_id,
            'embedding': embedding,
            'text_length': len(text),  # For debugging/stats
            'text_preview': text[:100] if len(text) > 100 else text  # For debugging
        }
        
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
        except IOError as e:
            # Log error but don't fail - caching is optional
            print(f"Warning: Failed to cache embedding: {e}")
    
    def clear(self) -> None:
        """Remove all cached embeddings."""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink(missing_ok=True)
        self._hits = 0
        self._misses = 0

    def get_cache_stats(self) -> CacheStats:
        """Return minimal stats for public consumption."""
        return {"cache_hits": int(self._hits), "cache_misses": int(self._misses)}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        cache_files = list(self.cache_dir.glob("*.json"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            'cache_dir': str(self.cache_dir),
            'num_cached': len(cache_files),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate': round(self._hits / (self._hits + self._misses), 3) if (self._hits + self._misses) > 0 else 0
        }
    
    def __repr__(self) -> str:
        stats = self.get_stats()
        return (
            f"LocalFileEmbeddingCache("
            f"cached={stats['num_cached']}, "
            f"hits={stats['hits']}, "
            f"misses={stats['misses']}, "
            f"hit_rate={stats['hit_rate']:.1%})"
        )


class NoOpEmbeddingCache(EmbeddingCache):
    """A no-op cache implementation that never caches anything."""
    
    def try_get(self, text: str, model_id: str) -> Optional[List[float]]:
        """Always returns None (cache miss)."""
        return None
    
    def store(self, text: str, model_id: str, embedding: List[float]) -> None:
        """Does nothing."""
        pass
    
    def clear(self) -> None:
        """Does nothing."""
        pass
    
    def __repr__(self) -> str:
        return "NoOpEmbeddingCache()"

    def get_cache_stats(self) -> CacheStats:
        return {"cache_hits": 0, "cache_misses": 0}


class CachedEmbeddings:
    """Wrapper for embeddings with caching support."""

    def __init__(self, embeddings: BaseEmbeddings, cache: EmbeddingCache):
        """
        Initialize cached embeddings wrapper.
        
        Args:
            embeddings: The base embeddings implementation
            cache: The cache implementation to use
        """
        self._embeddings = embeddings
        self._cache = cache
        # Derive a stable cache key root from provider class + required model_name
        provider = self._embeddings.__class__.__name__
        self.model_id = f"{provider}:{self._embeddings.model_name}"
        
        # Distinguish cache entries by usage to avoid collisions where
        # providers produce different vectors for the same text depending
        # on whether it's a document or a query (e.g., Cohere input_type).
        # We keep the public cache API unchanged by namespacing the model_id.
        self._doc_model_id = f"{self.model_id}|usage=document"
        self._query_model_id = f"{self.model_id}|usage=query"

    @property
    def model_name(self) -> str:
        return self._embeddings.model_name

    def get_cache_stats(self) -> CacheStats:
        """Return current cache hits/misses in a stable named dict shape."""
        return self._cache.get_cache_stats()

    @staticmethod
    def diff_stats(before: CacheStats, after: CacheStats) -> CacheStats:
        """Compute the difference between two CacheStats snapshots."""
        return {
            "cache_hits": int(after.get("cache_hits", 0)) - int(before.get("cache_hits", 0)),
            "cache_misses": int(after.get("cache_misses", 0)) - int(before.get("cache_misses", 0)),
        }
    
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Embed multiple documents with caching.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embedding vectors
        """
        results = []
        texts_to_embed = []
        text_indices = []
        
        # Check cache for each text
        for i, text in enumerate(texts):
            cached = self._cache.try_get(text, self._doc_model_id)
            if cached is not None:
                results.append((i, cached))
            else:
                texts_to_embed.append(text)
                text_indices.append(i)
        
        # Embed uncached texts
        if texts_to_embed:
            new_embeddings = self._embeddings.embed_documents(texts_to_embed)
            
            # Store in cache and add to results
            for text, embedding, idx in zip(texts_to_embed, new_embeddings, text_indices):
                self._cache.store(text, self._doc_model_id, embedding)
                results.append((idx, embedding))
        
        # Sort results by original index and extract embeddings
        results.sort(key=lambda x: x[0])
        return [emb for _, emb in results]
    
    def embed_query(self, text: str) -> List[float]:
        """
        Embed a single query with caching.
        
        Args:
            text: Query text to embed
            
        Returns:
            Embedding vector
        """
        # Check cache first
        cached = self._cache.try_get(text, self._query_model_id)
        if cached is not None:
            return cached
        
        # Generate new embedding
        embedding = self._embeddings.embed_query(text)
        
        # Store in cache
        self._cache.store(text, self._query_model_id, embedding)
        
        return embedding
