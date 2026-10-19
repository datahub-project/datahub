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

# Microsoft Graph, used to resolve a sensitivity label id to a name and description.
GRAPH_API_SCOPE = "https://graph.microsoft.com/.default"


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
        # scope -> (token, expires_at). Fabric and Graph tokens are cached separately.
        self._token_cache: dict[str, tuple[str, float]] = {}

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
                  Use GRAPH_API_SCOPE to read sensitivity labels.

        Returns:
            Bearer token string (without "Bearer " prefix)

        Raises:
            Exception: If token acquisition fails
        """
        import time

        target_scope = scope or FABRIC_API_SCOPE

        cached = self._token_cache.get(target_scope)
        if cached is not None and time.time() < (cached[1] - 300):
            return cached[0]

        try:
            credential = self.get_credential()
            token_response = credential.get_token(target_scope)

            # expires_on can be an int (Unix timestamp) or datetime
            if hasattr(token_response, "expires_on"):
                expires_on = token_response.expires_on
                if isinstance(expires_on, (int, float)):
                    expires_at = float(expires_on)
                else:
                    expires_at = expires_on.timestamp()
            else:
                # Fallback: assume 1 hour validity if expires_on not available
                expires_at = time.time() + 3600

            self._token_cache[target_scope] = (token_response.token, expires_at)
            logger.debug(f"Successfully acquired token for scope: {target_scope}")
            return token_response.token

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
