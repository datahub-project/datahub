from enum import Enum
from typing import Any, Dict, Optional

from loguru import logger


class TeamsErrorCategory(Enum):
    """Categories of Teams notification errors for better handling."""

    AUTHENTICATION_ERROR = "authentication_error"
    PERMISSION_ERROR = "permission_error"
    RATE_LIMIT_ERROR = "rate_limit_error"
    NETWORK_ERROR = "network_error"
    RECIPIENT_ERROR = "recipient_error"
    CONFIGURATION_ERROR = "configuration_error"
    MESSAGE_FORMAT_ERROR = "message_format_error"
    UNKNOWN_ERROR = "unknown_error"


class TeamsError(Exception):
    """Custom exception for Teams notification errors."""

    def __init__(
        self,
        message: str,
        category: TeamsErrorCategory = TeamsErrorCategory.UNKNOWN_ERROR,
        details: Optional[Dict[str, Any]] = None,
        retryable: bool = True,
    ):
        super().__init__(message)
        self.category = category
        self.details = details or {}
        self.retryable = retryable


def categorize_teams_error(exception: Exception, context: str = "") -> TeamsError:
    """Categorize a Teams API error and return a structured TeamsError."""
    error_message = str(exception)
    error_type = type(exception).__name__

    details = {
        "original_error_type": error_type,
        "original_error_message": error_message,
        "context": context,
    }

    # Authentication/Authorization errors
    if any(
        keyword in error_message.lower()
        for keyword in [
            "unauthorized",
            "401",
            "403",
            "forbidden",
            "token",
            "authentication",
        ]
    ):
        return TeamsError(
            f"Teams authentication/authorization failed: {error_message}",
            category=TeamsErrorCategory.AUTHENTICATION_ERROR,
            details=details,
            retryable=False,
        )

    # Permission errors
    if any(
        keyword in error_message.lower()
        for keyword in ["permission", "access denied", "not allowed"]
    ):
        return TeamsError(
            f"Teams permission denied: {error_message}",
            category=TeamsErrorCategory.PERMISSION_ERROR,
            details=details,
            retryable=False,
        )

    # Rate limiting errors
    if any(
        keyword in error_message.lower()
        for keyword in ["rate limit", "429", "throttle", "too many requests"]
    ):
        return TeamsError(
            f"Teams rate limit exceeded: {error_message}",
            category=TeamsErrorCategory.RATE_LIMIT_ERROR,
            details=details,
            retryable=True,
        )

    # Network/connectivity errors
    if any(
        keyword in error_message.lower()
        for keyword in ["connection", "timeout", "network", "dns", "unreachable"]
    ):
        return TeamsError(
            f"Teams network error: {error_message}",
            category=TeamsErrorCategory.NETWORK_ERROR,
            details=details,
            retryable=True,
        )

    # Recipient/user not found errors
    if any(
        keyword in error_message.lower()
        for keyword in ["not found", "404", "user not found", "recipient"]
    ):
        return TeamsError(
            f"Teams recipient error: {error_message}",
            category=TeamsErrorCategory.RECIPIENT_ERROR,
            details=details,
            retryable=False,
        )

    # Configuration errors
    if any(
        keyword in error_message.lower()
        for keyword in ["configuration", "config", "invalid app", "app not found"]
    ):
        return TeamsError(
            f"Teams configuration error: {error_message}",
            category=TeamsErrorCategory.CONFIGURATION_ERROR,
            details=details,
            retryable=False,
        )

    # Message format errors
    if any(
        keyword in error_message.lower()
        for keyword in ["invalid format", "malformed", "bad request", "400"]
    ):
        return TeamsError(
            f"Teams message format error: {error_message}",
            category=TeamsErrorCategory.MESSAGE_FORMAT_ERROR,
            details=details,
            retryable=False,
        )

    # Default to unknown error
    return TeamsError(
        f"Teams unknown error: {error_message}",
        category=TeamsErrorCategory.UNKNOWN_ERROR,
        details=details,
        retryable=True,
    )


def log_teams_error(
    teams_error: TeamsError,
    recipient: Optional[str] = None,
    operation: Optional[str] = None,
    additional_context: Optional[Dict[str, Any]] = None,
) -> None:
    """Log a Teams error with structured information."""
    log_data = {
        "error_category": teams_error.category.value,
        "error_message": str(teams_error),
        "retryable": teams_error.retryable,
        "details": teams_error.details,
    }

    if recipient:
        log_data["recipient"] = recipient
    if operation:
        log_data["operation"] = operation
    if additional_context:
        log_data.update(additional_context)

    # Use different log levels based on error category
    if teams_error.category in [
        TeamsErrorCategory.AUTHENTICATION_ERROR,
        TeamsErrorCategory.CONFIGURATION_ERROR,
    ]:
        logger.error("Critical Teams error", extra=log_data)
    elif teams_error.category in [
        TeamsErrorCategory.PERMISSION_ERROR,
        TeamsErrorCategory.RECIPIENT_ERROR,
    ]:
        logger.warning("Teams error - check configuration", extra=log_data)
    elif teams_error.category == TeamsErrorCategory.RATE_LIMIT_ERROR:
        logger.warning("Teams rate limit - will retry", extra=log_data)
    elif teams_error.category == TeamsErrorCategory.NETWORK_ERROR:
        logger.warning("Teams network issue - will retry", extra=log_data)
    else:
        logger.error("Unknown Teams error", extra=log_data)


def should_retry_teams_error(
    teams_error: TeamsError, attempt: int, max_attempts: int
) -> bool:
    """Determine if a Teams error should be retried based on its category and attempt count."""
    if not teams_error.retryable:
        return False

    if attempt >= max_attempts:
        return False

    # Special handling for rate limit errors - always retry if within max attempts
    if teams_error.category == TeamsErrorCategory.RATE_LIMIT_ERROR:
        return True

    # Network errors - retry with exponential backoff
    if teams_error.category == TeamsErrorCategory.NETWORK_ERROR:
        return True

    # Unknown errors - retry to be safe
    if teams_error.category == TeamsErrorCategory.UNKNOWN_ERROR:
        return True

    return False


def get_retry_delay_for_error(
    teams_error: TeamsError, attempt: int, base_delay: float = 1.0
) -> float:
    """Get the appropriate retry delay based on error category."""
    # Rate limit errors need longer delays
    if teams_error.category == TeamsErrorCategory.RATE_LIMIT_ERROR:
        return min(base_delay * (3**attempt), 300)  # Max 5 minutes

    # Network errors use standard exponential backoff
    if teams_error.category == TeamsErrorCategory.NETWORK_ERROR:
        return min(base_delay * (2**attempt), 120)  # Max 2 minutes

    # Default exponential backoff
    return min(base_delay * (2**attempt), 60)  # Max 1 minute
