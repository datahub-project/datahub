"""
Utilities for working with embeddings using Bedrock.

This module provides LiteLLM-based embeddings for Bedrock models.
Uses standard AWS credential chain (AWS_PROFILE).
"""

import os
from typing import Any, Dict, List, Optional

from .litellm_wrapper import LiteLLMEmbeddings


def create_bedrock_embeddings(
    model_id: str = "cohere.embed-english-v3",
    *,
    region: str = "us-west-2",
) -> LiteLLMEmbeddings:
    """Create a Bedrock embeddings instance using LiteLLM.

    Args:
        model_id: Bedrock model ID (default: "cohere.embed-english-v3")
            - "cohere.embed-english-v3" (1024 dims)
            - "cohere.embed-multilingual-v3" (1024 dims)
            - "amazon.titan-embed-text-v1" (1536 dims)
            - "amazon.titan-embed-text-v2:0" (256/512/1024 dims, default 1024)
        region: AWS region (default: us-west-2)

    Returns:
        LiteLLMEmbeddings instance

    Authentication:
        Uses standard AWS credential chain. Set AWS_PROFILE environment variable
        to use a specific AWS profile.

    Note:
        LiteLLM creates its own internal Bedrock client using the standard AWS
        credential chain. For custom client configuration, consider using direct
        Bedrock API calls instead.
    """
    # Map Bedrock model id to LiteLLM model string by prefixing with "bedrock/" if missing
    litellm_model = model_id if "/" in model_id else f"bedrock/{model_id}"

    # Hard cap to satisfy Bedrock Cohere request validator (separate from token window)
    # Cohere embed v3 models enforce a 2048-character limit on the 'texts' field. If exceeded,
    # Bedrock returns ValidationException regardless of `truncate`. See discussion:
    # https://github.com/deepset-ai/haystack-core-integrations/issues/912
    return LiteLLMEmbeddings(
        model_id=litellm_model,
        aws_region_name=region,
        max_character_length=2048,
    )


def get_model_info(model_id: str) -> Dict[str, Any]:
    """
    Get information about a Bedrock embedding model.

    Returns:
        Dict with model information (dimensions, max_tokens, etc.)
    """
    # Remove bedrock/ prefix if present for lookup
    if model_id.startswith("bedrock/"):
        model_id = model_id[8:]

    models: Dict[str, Dict[str, Any]] = {
        "amazon.titan-embed-text-v1": {
            "dimensions": 1536,
            "max_tokens": 8192,
            "configurable_dims": False,
            "allowed_dims": None,
        },
        "amazon.titan-embed-text-v2:0": {
            "dimensions": 1024,  # default
            "max_tokens": 8192,
            "configurable_dims": True,
            "allowed_dims": [256, 512, 1024],
        },
        "cohere.embed-english-v3": {
            "dimensions": 1024,
            "max_tokens": 512,
            "configurable_dims": False,
            "allowed_dims": None,
        },
        "cohere.embed-multilingual-v3": {
            "dimensions": 1024,
            "max_tokens": 512,
            "configurable_dims": False,
            "allowed_dims": None,
        },
    }

    return models.get(
        model_id,
        {
            "dimensions": -1,
            "max_tokens": -1,
            "configurable_dims": False,
            "allowed_dims": None,
        },
    )


def batch_embed_texts(
    embeddings: LiteLLMEmbeddings, texts: List[str], batch_size: int = 25
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
        batch = texts[i : i + batch_size]
        batch_embeddings = embeddings.embed_documents(batch)
        all_embeddings.extend(batch_embeddings)

    return all_embeddings


def create_embeddings(
    provider: Optional[str] = None,
    model_id: Optional[str] = None,
    *,
    aws_region: Optional[str] = None,
) -> LiteLLMEmbeddings:
    """Factory that creates embeddings instance with explicit parameters.

    Args:
        provider: Embedding provider - currently only "bedrock" is supported
        model_id: Model identifier (defaults to BEDROCK_MODEL env var or "cohere.embed-english-v3")
        aws_region: AWS region (defaults to BEDROCK_AWS_REGION or AWS_REGION or "us-west-2")

    Returns:
        LiteLLMEmbeddings instance

    Authentication:
        Uses standard AWS credential chain. Set AWS_PROFILE environment variable
        to use a specific AWS profile.

    Raises:
        ValueError: If provider is not "bedrock"
    """
    provider = provider or os.environ.get("EMBED_PROVIDER", "bedrock")

    if provider != "bedrock":
        raise ValueError(
            f"Unsupported embedding provider: {provider}. Only 'bedrock' is currently supported."
        )

    model_id = model_id or os.environ.get("BEDROCK_MODEL", "cohere.embed-english-v3")
    aws_region = (
        aws_region
        or os.environ.get("BEDROCK_AWS_REGION")
        or os.environ.get("AWS_REGION", "us-west-2")
    )

    # These should always be strings due to fallback values above
    assert model_id is not None
    assert aws_region is not None

    return create_bedrock_embeddings(
        model_id=model_id,
        region=aws_region,
    )
