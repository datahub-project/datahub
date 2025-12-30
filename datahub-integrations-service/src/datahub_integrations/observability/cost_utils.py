"""Utility functions for cost tracking."""


def detect_provider_and_normalize_model(model_id: str) -> tuple[str, str]:
    """
    Detect provider from model ID and normalize model name.

    Args:
        model_id: Model identifier (e.g., "bedrock/anthropic.claude-3-5-sonnet-20241022-v2:0")

    Returns:
        Tuple of (provider, normalized_model_name)
        - provider: "bedrock", "anthropic", "openai", etc.
        - normalized_model_name: Model ID with provider prefix stripped

    Examples:
        >>> detect_provider_and_normalize_model("bedrock/anthropic.claude-3")
        ("bedrock", "anthropic.claude-3")

        >>> detect_provider_and_normalize_model("anthropic.claude-3")
        ("anthropic", "anthropic.claude-3")

        >>> detect_provider_and_normalize_model("gpt-4")
        ("openai", "gpt-4")
    """
    provider = "bedrock"  # Default
    model_name = model_id

    # Detect provider from model ID
    if "anthropic" in model_id.lower() and "bedrock" not in model_id.lower():
        provider = "anthropic"
    elif "gpt" in model_id.lower():
        provider = "openai"

    # Strip provider prefix (e.g., "bedrock/" -> "")
    if "/" in model_name:
        model_name = model_name.split("/", 1)[1]

    return provider, model_name
