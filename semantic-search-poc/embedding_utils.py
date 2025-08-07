"""
Utilities for working with embeddings using Bedrock and Cohere.

This module now returns LiteLLM-based embeddings via `LiteLLMEmbeddings` while
keeping the original factory function names and signatures. Call sites can
continue to use `embed_documents` and `embed_query` unchanged.

Type hinting uses a small `EmbeddingsLike` Protocol to avoid importing
LangChain types while preserving IDE/typing support.
"""
import os
from typing import List, Dict, Any, Optional

from embedding_cache import LocalFileEmbeddingCache, CachedEmbeddings
from litellm_embeddings import LiteLLMEmbeddings
from embeddings_base import BaseEmbeddings


def create_bedrock_embeddings(
    model_id: str = "cohere.embed-english-v3",
    *,
    region: str = "us-west-2",
) -> BaseEmbeddings:
    """
    Create a Bedrock embeddings instance (LiteLLM-backed).

    Args:
        model_id: Bedrock model ID (default: "cohere.embed-english-v3")
            - "cohere.embed-english-v3" (1024 dims)
            - "cohere.embed-multilingual-v3" (1024 dims)
            - "amazon.titan-embed-text-v1" (1536 dims)
            - "amazon.titan-embed-text-v2:0" (256/512/1024 dims, default 1024)
        region: AWS region (default: us-west-2)

    Returns:
        Configured embeddings instance implementing `embed_documents` and `embed_query`
    """
    # Map Bedrock model id to LiteLLM model string by prefixing with "bedrock/" if missing
    litellm_model = model_id if "/" in model_id else f"bedrock/{model_id}"

    return LiteLLMEmbeddings(
        model=litellm_model,
        aws_region_name=region,
    )


def create_cohere_embeddings(
    api_key: Optional[str] = None,
    model: str = "embed-english-v3.0",
) -> BaseEmbeddings:
    """
    Create a Cohere embeddings instance (LiteLLM-backed).

    Args:
        api_key: Cohere API key (or set COHERE_API_KEY env var). LiteLLM will read
            COHERE_API_KEY from the environment. This parameter is accepted for
            backward compatibility; if provided, it's validated but not passed directly.
        model: Model to use
            - "embed-english-v3.0" (1024 dims) - Best for English
            - "embed-multilingual-v3.0" (1024 dims) - For multiple languages
            - "embed-english-light-v3.0" (384 dims) - Faster, smaller
            - "embed-multilingual-light-v3.0" (384 dims) - Faster, multilingual

    Returns:
        Configured embeddings instance implementing `embed_documents` and `embed_query`
    """
    # Validate API key presence (LiteLLM will use env var at runtime)
    api_key = api_key or os.environ.get("COHERE_API_KEY")
    if not api_key:
        raise ValueError(
            "Cohere API key required. Set COHERE_API_KEY environment variable or pass api_key parameter"
        )

    # Map to LiteLLM model string
    litellm_model = model if "/" in model else f"cohere/{model}"

    return LiteLLMEmbeddings(model=litellm_model)


def get_model_info(model_id: str) -> Dict[str, Any]:
    """
    Get information about a Bedrock embedding model.
    
    Returns:
        Dict with model information (dimensions, max_tokens, etc.)
    """
    models = {
        "amazon.titan-embed-text-v1": {
            "dimensions": 1536,
            "max_tokens": 8192,
            "configurable_dims": False
        },
        "amazon.titan-embed-text-v2:0": {
            "dimensions": 1024,  # default
            "max_tokens": 8192,
            "configurable_dims": True,
            "allowed_dims": [256, 512, 1024]
        },
        "cohere.embed-english-v3": {
            "dimensions": 1024,
            "max_tokens": 512,
            "configurable_dims": False
        },
        "cohere.embed-multilingual-v3": {
            "dimensions": 1024,
            "max_tokens": 512,
            "configurable_dims": False
        }
    }
    
    return models.get(model_id, {
        "dimensions": "unknown",
        "max_tokens": "unknown",
        "configurable_dims": False
    })


def batch_embed_texts(
    embeddings: BaseEmbeddings,
    texts: List[str],
    batch_size: int = 25
) -> List[List[float]]:
    """
    Embed texts in batches to avoid rate limits.
    
    Args:
        embeddings: Configured embeddings instance
        texts: List of texts to embed
        batch_size: Number of texts per batch (Bedrock typically allows 25)
    
    Returns:
        List of embedding vectors
    """
    all_embeddings = []
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        batch_embeddings = embeddings.embed_documents(batch)
        all_embeddings.extend(batch_embeddings)
    
    return all_embeddings


def create_cached_embeddings(
    embeddings: BaseEmbeddings,
    cache_dir: Optional[str] = None,
) -> CachedEmbeddings:
    """
    Wrap any embeddings implementation with caching.
    
    Args:
        embeddings: The base embeddings implementation
        cache_dir: Optional custom cache directory
        model_id: Optional model identifier for cache keys
    
    Returns:
        Cached embeddings wrapper
    """
    cache = LocalFileEmbeddingCache(cache_dir)

    return CachedEmbeddings(embeddings, cache)


def create_embeddings(*, use_cache: bool = True, cache_dir: Optional[str] = None) -> BaseEmbeddings:
    """
    Factory that selects provider (bedrock | cohere) from environment, creates the
    appropriate embeddings instance, and optionally wraps it with a cache.

    Environment variables
    - EMBED_PROVIDER: "bedrock" (default) or "cohere"
    - When EMBED_PROVIDER=bedrock:
        - BEDROCK_MODEL: Bedrock model id (default: "cohere.embed-english-v3")
        - AWS_REGION: AWS region (default: "us-west-2")
    - When EMBED_PROVIDER=cohere:
        - COHERE_MODEL: Cohere model (default: "embed-english-v3.0")
        - COHERE_API_KEY must be present in the environment

    Args:
        use_cache: If True, wraps the embeddings with a local file cache
        cache_dir: Optional custom cache directory when caching is enabled

    Returns:
        BaseEmbeddings implementation. May be a cached wrapper with the same interface.
    """
    provider = os.environ.get("EMBED_PROVIDER", "bedrock").lower()
    if provider == "cohere":
        base = create_cohere_embeddings(
            model=os.environ.get("COHERE_MODEL", "embed-english-v3.0"),
        )
    else:
        base = create_bedrock_embeddings(
            model_id=os.environ.get("BEDROCK_MODEL", "cohere.embed-english-v3"),
            region=os.environ.get("AWS_REGION", "us-west-2"),
        )

    if use_cache:
        return create_cached_embeddings(base, cache_dir=cache_dir)
    return base
