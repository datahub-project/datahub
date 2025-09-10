"""
Exceptions for Teams integration OAuth and webhook handling.
"""

import urllib.parse
from typing import Optional

from fastapi.responses import RedirectResponse
from loguru import logger


class TeamsOAuthException(Exception):
    """
    Exception for Teams OAuth flow failures with standardized error handling.

    This exception enables:
    - Consistent user experience with proper redirects
    - Structured error logging for metrics and monitoring
    - Categorized error codes for failure analysis
    """

    def __init__(
        self,
        message: str,
        error_code: str,
        redirect_url: Optional[str] = None,
        user_friendly_message: Optional[str] = None,
    ):
        """
        Initialize Teams OAuth exception.

        Args:
            message: Technical error message for logging
            error_code: Categorized error code for metrics (e.g., "missing_user_urn", "oauth_token_exchange_failed")
            redirect_url: URL to redirect user back to (defaults to personal notifications)
            user_friendly_message: User-facing error message (defaults to generic message)
        """
        self.message = message
        self.error_code = error_code
        self.redirect_url = redirect_url
        self.user_friendly_message = (
            user_friendly_message or "Teams integration setup failed. Please try again."
        )
        super().__init__(message)

    def to_redirect_response(
        self, default_redirect: str = "/settings/personal-notifications"
    ) -> RedirectResponse:
        """
        Convert exception to user-friendly redirect response.

        Args:
            default_redirect: Default redirect URL if none specified

        Returns:
            RedirectResponse with error parameters
        """
        # Log the technical error for monitoring/metrics
        logger.error(f"Teams OAuth failure [{self.error_code}]: {self.message}")

        # TODO: Add metrics tracking here
        # metrics.counter("teams_oauth_failures", tags={"error_code": self.error_code}).increment()

        # Create user-friendly redirect
        final_redirect = self.redirect_url or default_redirect
        error_params = f"teams_oauth=error&message={urllib.parse.quote(self.user_friendly_message)}"

        # Add error_code as a query parameter for debugging/support
        error_params += f"&error_code={self.error_code}"

        # Construct final URL with error parameters
        separator = "&" if "?" in final_redirect else "?"
        redirect_with_error = f"{final_redirect}{separator}{error_params}"

        logger.info(f"Redirecting user to: {redirect_with_error}")

        return RedirectResponse(url=redirect_with_error, status_code=302)


class TeamsWebhookException(Exception):
    """
    Exception for Teams webhook processing failures.

    Unlike OAuth exceptions, webhook failures typically don't need user redirects
    but should be logged and tracked for reliability metrics.
    """

    def __init__(
        self, message: str, error_code: str, webhook_data: Optional[dict] = None
    ):
        """
        Initialize Teams webhook exception.

        Args:
            message: Technical error message for logging
            error_code: Categorized error code for metrics
            webhook_data: Original webhook payload for debugging
        """
        self.message = message
        self.error_code = error_code
        self.webhook_data = webhook_data
        super().__init__(message)

    def log_and_track(self) -> None:
        """Log the webhook failure and track metrics."""
        logger.error(f"Teams webhook failure [{self.error_code}]: {self.message}")

        # Log webhook data for debugging (be careful with PII)
        if self.webhook_data:
            # Only log non-sensitive fields
            safe_data = {
                "type": self.webhook_data.get("type"),
                "channelId": self.webhook_data.get("channelId"),
                "conversation_type": self.webhook_data.get("conversation", {}).get(
                    "conversationType"
                ),
                "tenant_id": self.webhook_data.get("channelData", {})
                .get("tenant", {})
                .get("id"),
            }
            logger.debug(f"Webhook context: {safe_data}")

        # TODO: Add metrics tracking
        # metrics.counter("teams_webhook_failures", tags={"error_code": self.error_code}).increment()


class TeamsMessageSendException(Exception):
    """
    Exception for Teams message sending failures with user-friendly error mapping.

    This exception provides specific handling for common Teams API errors,
    especially 403 Forbidden errors that indicate the bot needs to be invited.
    """

    def __init__(
        self,
        message: str,
        error_code: str,
        status_code: Optional[int] = None,
        response_text: Optional[str] = None,
        user_friendly_message: Optional[str] = None,
    ):
        """
        Initialize Teams message send exception.

        Args:
            message: Technical error message for logging
            error_code: Categorized error code for metrics
            status_code: HTTP status code from Teams API
            response_text: Response body from Teams API
            user_friendly_message: User-facing error message
        """
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.response_text = response_text
        self.user_friendly_message = (
            user_friendly_message or self._get_default_user_message()
        )
        super().__init__(message)

    def _get_default_user_message(self) -> str:
        """Get default user-friendly message based on error code."""
        if self.error_code == TeamsErrorCodes.MESSAGE_SEND_FORBIDDEN:
            return "Please invite the DataHub App to the channel."
        elif self.error_code == TeamsErrorCodes.MESSAGE_SEND_UNAUTHORIZED:
            return "DataHub App is not authorized to send messages. Please check permissions."
        elif self.error_code == TeamsErrorCodes.MESSAGE_SEND_NOT_FOUND:
            return "Channel or conversation not found. Please check the channel exists."
        else:
            return "Failed to send message to Teams. Please try again."

    def log_and_track(self) -> None:
        """Log the message send failure and track metrics."""
        logger.error(f"Teams message send failure [{self.error_code}]: {self.message}")

        if self.status_code:
            logger.error(f"HTTP Status: {self.status_code}")
        if self.response_text:
            logger.error(f"Response: {self.response_text}")

        # TODO: Add metrics tracking
        # metrics.counter("teams_message_send_failures", tags={"error_code": self.error_code}).increment()


# Common error codes for standardization
class TeamsErrorCodes:
    """Standard error codes for Teams integration failures."""

    # OAuth flow errors
    MISSING_USER_URN = "missing_user_urn"
    MISSING_OAUTH_CODE = "missing_oauth_code"
    MISSING_APP_CREDENTIALS = "missing_app_credentials"
    OAUTH_INVALID_STATE = "oauth_invalid_state"
    OAUTH_TOKEN_EXCHANGE_FAILED = "oauth_token_exchange_failed"
    MICROSOFT_GRAPH_API_FAILED = "microsoft_graph_api_failed"
    TEAMS_USER_ID_NOT_FOUND = "teams_user_id_not_found"
    DATAHUB_SETTINGS_UPDATE_FAILED = "datahub_settings_update_failed"

    # Webhook processing errors
    WEBHOOK_AUTH_FAILED = "webhook_auth_failed"
    INVALID_WEBHOOK_PAYLOAD = "invalid_webhook_payload"
    TEAMS_CONFIG_NOT_FOUND = "teams_config_not_found"
    TEAMS_RESPONSE_FAILED = "teams_response_failed"
    MESSAGE_PROCESSING_FAILED = "message_processing_failed"

    # Message sending errors
    MESSAGE_SEND_FORBIDDEN = "message_send_forbidden"
    MESSAGE_SEND_UNAUTHORIZED = "message_send_unauthorized"
    MESSAGE_SEND_NOT_FOUND = "message_send_not_found"
    MESSAGE_SEND_FAILED = "message_send_failed"
