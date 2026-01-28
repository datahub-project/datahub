"""Authentication helpers for Microsoft Fabric REST APIs."""

import logging
from typing import Optional

from azure.core.credentials import TokenCredential

from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig

logger = logging.getLogger(__name__)

# Fabric REST API scope - using Power BI scope as Fabric is part of Power BI ecosystem
# This may need to be updated if a Fabric-specific scope becomes available
FABRIC_API_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# OneLake Table APIs require Storage audience token
ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"

# SQL Analytics Endpoint requires database scope for Azure AD authentication
SQL_ANALYTICS_ENDPOINT_SCOPE = "https://database.windows.net/.default"


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
        self._cached_scope: Optional[str] = None
        self._token_expires_at: Optional[float] = None

    def get_credential(self) -> TokenCredential:
        """Get the Azure TokenCredential.

        Returns:
            TokenCredential instance
        """
        if self._credential is None:
            self._credential = self.credential_config.get_credential()
        return self._credential

    def get_bearer_token(self, scope: Optional[str] = None) -> str:
        """Get a Bearer token for REST API authentication.

        Args:
            scope: Optional scope/audience. Defaults to FABRIC_API_SCOPE.
                  Use ONELAKE_STORAGE_SCOPE for OneLake Delta Table APIs.
                  Use SQL_ANALYTICS_ENDPOINT_SCOPE for SQL Analytics Endpoint connections.

        Returns:
            Bearer token string (without "Bearer " prefix)

        Raises:
            Exception: If token acquisition fails
        """
        import time

        target_scope = scope or FABRIC_API_SCOPE

        # Check if cached token is still valid and for the same scope
        if (
            self._cached_token is not None
            and self._cached_scope == target_scope
            and self._token_expires_at is not None
            and time.time() < (self._token_expires_at - 300)
        ):
            return self._cached_token

        try:
            credential = self.get_credential()
            token_response = credential.get_token(target_scope)

            self._cached_token = token_response.token
            self._cached_scope = target_scope
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

            logger.debug(f"Successfully acquired token for scope: {target_scope}")
            return self._cached_token

        except Exception as e:
            logger.error(f"Failed to acquire token for scope {target_scope}: {e}")
            raise

    def get_authorization_header(self, scope: Optional[str] = None) -> str:
        """Get the full Authorization header value.

        Args:
            scope: Optional scope/audience. Defaults to FABRIC_API_SCOPE.

        Returns:
            Authorization header value: "Bearer {token}"
        """
        token = self.get_bearer_token(scope=scope)
        return f"Bearer {token}"
