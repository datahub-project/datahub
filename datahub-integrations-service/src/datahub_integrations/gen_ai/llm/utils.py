"""
Utility functions for LLM operations.
"""

import os


def is_verbose_llm_logging_enabled() -> bool:
    """
    Check if verbose LLM logging is enabled via environment variable.

    Returns:
        True if LLM_VERBOSE_LOGGING is set to 'true', '1', or 'yes' (case-insensitive)
    """
    return os.getenv("LLM_VERBOSE_LOGGING", "false").lower() in ("true", "1", "yes")


def parse_model_id(model_id: str) -> tuple[str, str]:
    """
    Parse model ID to extract provider and model name.

    Args:
        model_id: Full model identifier (e.g., "bedrock/claude-3-5-sonnet")

    Returns:
        Tuple of (provider, model_name)

    Examples:
        >>> parse_model_id("bedrock/claude-3-5-sonnet")
        ('bedrock', 'claude-3-5-sonnet')
        >>> parse_model_id("openai/gpt-4o")
        ('openai', 'gpt-4o')
        >>> parse_model_id("claude-3-5-sonnet")
        ('bedrock', 'claude-3-5-sonnet')
    """
    if "/" in model_id:
        provider, model_name = model_id.split("/", 1)
        return provider.lower(), model_name
    else:
        # Default to bedrock for backward compatibility
        return "bedrock", model_id
