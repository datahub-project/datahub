"""Standardized exceptions for LLM provider abstraction."""


class LlmException(Exception):
    """Base exception for all LLM-related errors."""

    pass


class LlmInputTooLongException(LlmException):
    """
    Exception raised when input exceeds the model's context window.

    This can happen when:
    - The conversation history is too long
    - Input tokens + max_tokens exceeds context limit
    - A single message is too large
    """

    pass


class LlmOutputTooLongException(LlmException):
    """
    Exception raised when the model hits max_tokens during generation.

    The response was truncated because it reached the maximum output token limit.

    Note: Currently not used. When stopReason == "max_tokens", this is returned as a
    valid response (not an exception), and the calling code checks the stopReason
    field to handle it. This exception is defined for future use cases where we might
    want to fail fast on truncated outputs.
    """

    pass


class LlmValidationException(LlmException):
    """
    Exception raised for validation errors in the request.

    This includes:
    - Invalid model ID
    - Invalid tool specifications
    - Invalid inference configuration
    """

    pass


class LlmRateLimitException(LlmException):
    """Exception raised when rate limits are exceeded."""

    pass


class LlmAuthenticationException(LlmException):
    """Exception raised for authentication/authorization errors."""

    pass
