"""Authentication helpers for Microsoft Fabric REST APIs."""

import logging
from typing import Optional

from azure.core.credentials import TokenCredential

from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig

logger = logging.getLogger(__name__)

# Fabric REST API scope - using Power BI scope as Fabric is part of Power BI ecosystem
# This may need to be updated if a Fabric-specific scope becomes available
FABRIC_API_SCOPE = "https://analysis.windows.net/powerbi/api/.default"


class FabricAuthHelper:
    """Helper class for authenticating with Microsoft Fabric REST APIs.

    Converts Azure TokenCredential to Bearer tokens for REST API calls.
    Handles token refresh and caching.
    """

    def __init__(self, credential_config: AzureCredentialConfig):
        """Initialize the authentication helper.

        Args:
            credential_config: Azure credential configuration
        """
        self.credential_config = credential_config
        self._credential: Optional[TokenCredential] = None
        self._cached_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None

    def get_credential(self) -> TokenCredential:
        """Get the Azure TokenCredential.

        Returns:
            TokenCredential instance
        """
        if self._credential is None:
            self._credential = self.credential_config.get_credential()
        return self._credential

    def get_bearer_token(self) -> str:
        """Get a Bearer token for REST API authentication.

        Returns:
            Bearer token string (without "Bearer " prefix)

        Raises:
            Exception: If token acquisition fails
        """
        import time

        # Check if cached token is still valid (with 5 minute buffer)
        if (
            self._cached_token is not None
            and self._token_expires_at is not None
            and time.time() < (self._token_expires_at - 300)
        ):
            return self._cached_token

        try:
            credential = self.get_credential()
            token_response = credential.get_token(FABRIC_API_SCOPE)

            self._cached_token = token_response.token
            # Token expires_at is a datetime, convert to timestamp
            if hasattr(token_response, "expires_on"):
                # expires_on can be an int (Unix timestamp) or datetime
                expires_on = token_response.expires_on
                if isinstance(expires_on, (int, float)):
                    self._token_expires_at = float(expires_on)
                else:
                    # It's a datetime, convert to timestamp
                    self._token_expires_at = expires_on.timestamp()
            else:
                # Fallback: assume 1 hour validity if expires_on not available
                self._token_expires_at = time.time() + 3600

            logger.debug("Successfully acquired Fabric API token")
            return self._cached_token

        except Exception as e:
            logger.error(f"Failed to acquire Fabric API token: {e}")
            raise

    def get_authorization_header(self) -> str:
        """Get the full Authorization header value.

        Returns:
            Authorization header value: "Bearer {token}"
        """
        token = self.get_bearer_token()
        return f"Bearer {token}"
