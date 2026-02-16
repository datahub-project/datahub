from typing import Optional


class VertexAISourceError(Exception):
    """Base exception for all Vertex AI source errors."""

    def __init__(
        self,
        message: str,
        resource_type: Optional[str] = None,
        resource_name: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        self.message = message
        self.resource_type = resource_type
        self.resource_name = resource_name
        self.cause = cause
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message with context."""
        parts = [self.message]
        if self.resource_type:
            parts.append(f"resource_type={self.resource_type}")
        if self.resource_name:
            parts.append(f"resource_name={self.resource_name}")
        if self.cause:
            parts.append(f"cause={type(self.cause).__name__}: {str(self.cause)}")
        return " | ".join(parts)


class VertexAIAPIError(VertexAISourceError):
    """Wrapped Google API error for Vertex AI operations."""

    pass


class VertexAIPermissionError(VertexAISourceError):
    """Permission denied or authentication failed for Vertex AI resource."""

    pass


class VertexAIQuotaExceededError(VertexAISourceError):
    """API quota or rate limit exceeded for Vertex AI."""

    pass


class VertexAITimeoutError(VertexAISourceError):
    """Timeout while calling Vertex AI API."""

    pass


class VertexAIResourceNotFoundError(VertexAISourceError):
    """Vertex AI resource not found."""

    pass


class VertexAIDataError(VertexAISourceError):
    """Data parsing or validation error for Vertex AI metadata."""

    pass


class VertexAIConfigurationError(VertexAISourceError):
    """Configuration error for Vertex AI source."""

    pass
