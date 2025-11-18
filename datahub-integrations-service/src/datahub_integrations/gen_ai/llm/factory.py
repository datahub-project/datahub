"""
Factory functions for creating LLM wrappers.

LLM Provider Abstraction Layer
==============================

Provides a unified interface for multiple LLM providers (Bedrock, OpenAI, Gemini)
while maintaining compatibility with Bedrock's converse() API.

Key features:
- Provider detection from model string prefix (bedrock/, openai/, gemini/)
- Proper type hints for responses (ConverseResponse, TokenUsage)
- Provider-specific prompt caching when available
- Tool calling support via langchain
- Minimal changes to existing code

Usage:
    >>> from datahub_integrations.gen_ai.llm import get_llm_client
    >>>
    >>> # Bedrock (no prefix = default to bedrock)
    >>> client = get_llm_client("us.anthropic.claude-3-7-sonnet-20250219-v1:0")
    >>>
    >>> # OpenAI (requires OPENAI_API_KEY env var)
    >>> client = get_llm_client("openai/gpt-4o")
    >>>
    >>> # Gemini (requires GOOGLE_CLOUD_PROJECT env var)
    >>> client = get_llm_client("gemini/gemini-1.5-pro")
    >>>
    >>> # Use the same interface for all providers
    >>> response = client.converse(
    ...     modelId=model_id,
    ...     system=[{"text": "You are a helpful assistant"}],
    ...     messages=[{"role": "user", "content": [{"text": "Hello"}]}],
    ...     toolConfig={"tools": tools},
    ...     inferenceConfig={"temperature": 0.5, "maxTokens": 4096}
    ... )

Caching:
- Bedrock: Explicit cachePoint markers (preserved as-is)
- OpenAI: Automatic prompt caching for prefixes ≥1024 tokens
- Gemini: No caching (graceful degradation)
"""

import functools

from loguru import logger

from datahub_integrations.gen_ai.llm.base import LLMWrapper
from datahub_integrations.gen_ai.llm.bedrock import BedrockLLMWrapper
from datahub_integrations.gen_ai.llm.openai import OpenAILLMWrapper

# Lazy import for Gemini to avoid segfault at module load time


def _parse_model_id(model_id: str) -> tuple[str, str]:
    """
    Parse model ID to extract provider and model name.

    Args:
        model_id: Full model identifier (e.g., "bedrock/claude-3-5-sonnet")

    Returns:
        Tuple of (provider, model_name)
    """
    if "/" in model_id:
        provider, model_name = model_id.split("/", 1)
        return provider.lower(), model_name
    else:
        # Default to bedrock for backward compatibility
        return "bedrock", model_id


def _create_llm_wrapper(
    provider: str,
    model_name: str,
    read_timeout: int,
    connect_timeout: int,
    max_attempts: int,
) -> LLMWrapper:
    """
    Factory function to create the appropriate LLM wrapper based on provider.

    Args:
        provider: Provider name (bedrock, openai, gemini, vertex_ai)
        model_name: Model name (without provider prefix)
        read_timeout: Read timeout in seconds
        connect_timeout: Connection timeout in seconds
        max_attempts: Maximum retry attempts

    Returns:
        Concrete LLMWrapper instance for the provider

    Raises:
        ValueError: If provider is not supported
    """
    if provider == "bedrock":
        return BedrockLLMWrapper(
            model_name=model_name,
            read_timeout=read_timeout,
            connect_timeout=connect_timeout,
            max_attempts=max_attempts,
        )
    elif provider == "openai":
        return OpenAILLMWrapper(
            model_name=model_name,
            read_timeout=read_timeout,
            connect_timeout=connect_timeout,
            max_attempts=max_attempts,
        )
    elif provider in ("gemini", "vertex_ai"):
        # Lazy import to avoid segfault at module load time
        from datahub_integrations.gen_ai.llm.gemini import GeminiLLMWrapper

        return GeminiLLMWrapper(
            model_name=model_name,
            read_timeout=read_timeout,
            connect_timeout=connect_timeout,
            max_attempts=max_attempts,
        )
    else:
        raise ValueError(
            f"Unsupported LLM provider: {provider}. "
            f"Supported providers: bedrock, openai, gemini, vertex_ai"
        )


@functools.cache
def get_llm_client(
    model_id: str,
    read_timeout: int = 60,
    connect_timeout: int = 60,
    max_attempts: int = 10,
) -> LLMWrapper:
    """
    Get a cached LLM client for the specified model.

    This is the main entry point for getting an LLM client. It caches clients
    by model_id for efficiency.

    Args:
        model_id: Model identifier with provider prefix (e.g., "bedrock/claude-3-5-sonnet")
        read_timeout: Read timeout in seconds
        connect_timeout: Connection timeout in seconds
        max_attempts: Maximum retry attempts

    Returns:
        LLMWrapper instance configured for the provider

    Examples:
        >>> # Bedrock (no prefix or explicit bedrock/ prefix)
        >>> client = get_llm_client("us.anthropic.claude-3-7-sonnet-20250219-v1:0")
        >>> client = get_llm_client("bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0")
        >>>
        >>> # OpenAI
        >>> client = get_llm_client("openai/gpt-4o")
        >>>
        >>> # Gemini
        >>> client = get_llm_client("gemini/gemini-1.5-pro")
    """
    provider, model_name = _parse_model_id(model_id)
    logger.info(f"Initializing LLMWrapper with provider={provider}, model={model_name}")

    return _create_llm_wrapper(
        provider=provider,
        model_name=model_name,
        read_timeout=read_timeout,
        connect_timeout=connect_timeout,
        max_attempts=max_attempts,
    )
