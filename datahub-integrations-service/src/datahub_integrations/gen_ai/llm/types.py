"""Type definitions for LLM provider abstraction."""

from typing import Any, Dict

from typing_extensions import NotRequired, TypedDict


class TokenUsage(TypedDict):
    """Token usage information from LLM response."""

    inputTokens: int
    outputTokens: int
    cacheReadInputTokens: NotRequired[int]
    cacheWriteInputTokens: NotRequired[int]


class ConverseResponse(TypedDict):
    """
    Typed response structure matching Bedrock's converse API.

    This ensures type safety and IDE autocomplete for all providers.
    """

    output: Dict[str, Any]  # Contains message with content blocks
    stopReason: str  # "tool_use", "end_turn", "max_tokens"
    usage: TokenUsage
