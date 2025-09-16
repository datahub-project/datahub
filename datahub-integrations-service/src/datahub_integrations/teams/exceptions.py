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
        self.user_friendly_message = user_friendly_message or getattr(
            TeamsErrorMessages,
            error_code.upper(),
            "Teams integration setup failed. Please try again.",
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
        error_params = f"teams_oauth=error&message={urllib.parse.quote(str(self.user_friendly_message))}"

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
        # Use centralized error messages
        return getattr(
            TeamsErrorMessages,
            self.error_code.upper(),
            "Teams operation failed. Please try again.",
        )

    def log_and_track(self) -> None:
        """Log the message send failure and track metrics."""
        logger.error(f"Teams message send failure [{self.error_code}]: {self.message}")

        if self.status_code:
            logger.error(f"HTTP Status: {self.status_code}")
        if self.response_text:
            logger.error(f"Response: {self.response_text}")

        # TODO: Add metrics tracking
        # metrics.counter("teams_message_send_failures", tags={"error_code": self.error_code}).increment()


# User-friendly error messages mapped to error codes
class TeamsErrorMessages:
    """User-friendly error messages for Teams integration failures."""

    # OAuth flow errors
    MISSING_USER_URN = (
        "User authentication information is missing. Please log in and try again."
    )
    MISSING_OAUTH_CODE = (
        "OAuth authorization failed. Please try the setup process again."
    )
    MISSING_APP_CREDENTIALS = "Teams integration is not properly configured. Please contact your administrator."
    OAUTH_INVALID_STATE = (
        "OAuth state validation failed. Please try the setup process again."
    )
    OAUTH_TOKEN_EXCHANGE_FAILED = (
        "Failed to exchange OAuth tokens with Microsoft. Please try again."
    )
    MICROSOFT_GRAPH_API_FAILED = "Microsoft Graph API is temporarily unavailable. Please try again in a few minutes."
    TEAMS_USER_ID_NOT_FOUND = "Could not find your Teams user ID. Please ensure you have a Microsoft Teams account."
    DATAHUB_SETTINGS_UPDATE_FAILED = "Failed to save Teams settings. Please try again."

    # Webhook processing errors
    WEBHOOK_AUTH_FAILED = "Teams webhook authentication failed."
    INVALID_WEBHOOK_PAYLOAD = "Invalid Teams webhook data received."
    TEAMS_CONFIG_NOT_FOUND = (
        "Teams configuration not found. Please contact your administrator."
    )
    TEAMS_RESPONSE_FAILED = "Failed to respond to Teams message. Please try again."
    MESSAGE_PROCESSING_FAILED = "Failed to process Teams message. Please try again."

    # Message sending errors
    MESSAGE_SEND_FORBIDDEN = (
        "Please invite the DataHub App to the channel or conversation."
    )
    MESSAGE_SEND_UNAUTHORIZED = (
        "DataHub App is not authorized to send messages. Please check permissions."
    )
    MESSAGE_SEND_NOT_FOUND = (
        "Channel or conversation not found. Please check the channel exists."
    )
    MESSAGE_SEND_FAILED = "Failed to send message to Teams. Please try again."

    # Configuration and setup errors
    TEAMS_CONFIG_INVALID = "Teams integration is not properly configured. Please contact your administrator."
    TEAMS_TENANT_ID_INVALID = "Teams tenant configuration is invalid. Please contact your administrator to fix the tenant ID setup."
    BOT_FRAMEWORK_TOKEN_FAILED = "Failed to authenticate with Teams Bot Framework. Please contact your administrator."
    CONVERSATION_NOT_FOUND = "Teams conversation not found. You may need to install the DataHub bot or message it first."


def _map_azure_ad_error_to_code(error_json: dict) -> Optional[str]:
    """
    Map Azure AD specific errors to Teams error codes using dictionary-based approach.

    Args:
        error_json: Parsed JSON response from Azure AD

    Returns:
        Teams error code if mapping found, None otherwise
    """
    error_description = error_json.get("error_description", "").lower()
    error_code = error_json.get("error", "").lower()

    # Dictionary-based mapping for Azure AD errors
    azure_error_mappings = {
        # Tenant ID errors
        "tenant_identifier_none": (
            "tenant identifier" in error_description
            and ("none" in error_description or "null" in error_description)
        ),
        # Application errors
        "app_not_found": "aadsts700016" in error_description,
        "invalid_client_secret": "aadsts7000215" in error_description,
        "invalid_client": error_code == "invalid_client"
        or "client credentials" in error_description,
        "unauthorized_client": error_code == "unauthorized_client",
    }

    # Map conditions to error codes
    error_code_mapping = {
        "tenant_identifier_none": TeamsErrorCodes.TEAMS_TENANT_ID_INVALID,
        "app_not_found": TeamsErrorCodes.TEAMS_CONFIG_INVALID,
        "invalid_client_secret": TeamsErrorCodes.TEAMS_CONFIG_INVALID,
        "invalid_client": TeamsErrorCodes.TEAMS_CONFIG_INVALID,
        "unauthorized_client": TeamsErrorCodes.TEAMS_CONFIG_INVALID,
    }

    # Find first matching error condition
    for condition, error_code in error_code_mapping.items():
        if azure_error_mappings.get(condition, False):
            return error_code

    return None


def _map_http_status_to_code(status_code: int, error_context: str) -> str:
    """
    Map HTTP status codes to Teams error codes.

    Args:
        status_code: HTTP status code
        error_context: Context of the error

    Returns:
        Appropriate Teams error code
    """
    # Direct status code mappings
    status_mappings = {
        403: TeamsErrorCodes.MESSAGE_SEND_FORBIDDEN,
        401: TeamsErrorCodes.MESSAGE_SEND_UNAUTHORIZED,
        404: TeamsErrorCodes.MESSAGE_SEND_NOT_FOUND,
    }

    # Check direct mappings first
    if status_code in status_mappings:
        return status_mappings[status_code]

    # Context-specific mappings for 400 and other codes
    if status_code == 400:
        return (
            TeamsErrorCodes.BOT_FRAMEWORK_TOKEN_FAILED
            if error_context == "token_request"
            else TeamsErrorCodes.MESSAGE_SEND_FAILED
        )

    # Context-appropriate fallback error codes
    fallback_mappings = {
        "token_request": TeamsErrorCodes.BOT_FRAMEWORK_TOKEN_FAILED,
        "graph_api": TeamsErrorCodes.MICROSOFT_GRAPH_API_FAILED,
    }

    return fallback_mappings.get(error_context, TeamsErrorCodes.MESSAGE_SEND_FAILED)


def map_teams_api_error_to_error_code(
    status_code: int, response_text: str, error_context: str = "message_send"
) -> str:
    """
    Map Teams API errors to appropriate error codes.

    Centralizes error mapping logic to avoid duplication across Teams integration modules.
    Handles both HTTP status codes and Azure AD specific error descriptions.

    Args:
        status_code: HTTP status code from Teams/Azure API
        response_text: Response body text (may contain JSON error details)
        error_context: Context of the error - determines fallback behavior
            - "message_send": For message posting failures
            - "token_request": For Bot Framework/Azure AD token requests
            - "graph_api": For Microsoft Graph API calls

    Returns:
        Appropriate TeamsErrorCodes constant for the error
    """
    import json

    # Handle Azure AD specific errors in response body for token requests
    if error_context == "token_request":
        try:
            error_json = json.loads(response_text)
            azure_error_code = _map_azure_ad_error_to_code(error_json)
            if azure_error_code:
                return azure_error_code
        except (json.JSONDecodeError, KeyError, TypeError):
            # If we can't parse the JSON, fall through to status code handling
            pass

    # Handle HTTP status codes
    return _map_http_status_to_code(status_code, error_context)


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

    # Configuration and setup errors
    TEAMS_CONFIG_INVALID = "teams_config_invalid"
    TEAMS_TENANT_ID_INVALID = "teams_tenant_id_invalid"
    BOT_FRAMEWORK_TOKEN_FAILED = "bot_framework_token_failed"
    CONVERSATION_NOT_FOUND = "conversation_not_found"
