"""
Credential storage for AI Plugin OAuth and API key credentials.

This module provides:
- UserCredentials: Data model for user credentials (OAuth tokens or API keys)
- CredentialStore: Protocol interface for credential storage implementations
- DataHubConnectionCredentialStore: Implementation using DataHubConnection entities

Design Notes:
- Each user's credentials for a plugin are stored in a separate DataHubConnection
- URN format: urn:li:dataHubConnection:<sanitized_user>__<sanitized_plugin>
  (Uses double-underscore separator since DataHubConnectionKey has a single id field)
- Credentials are stored as encrypted JSON blobs by DataHub's connection system
- OAuth tokens include access_token, refresh_token, expires_at, and token_type
- API keys are stored with just the api_key field

Security:
- DataHubConnection provides encryption at rest
- Access is controlled via DataHub's authorization system
- Token refresh uses in-memory locking to prevent race conditions
"""

import base64
import json
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Protocol

import httpx
from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from datahub_integrations.utils.striped_lock import StripedLock


@dataclass
class OAuthTokens:
    """
    OAuth tokens for a user's plugin connection.

    Attributes:
        access_token: The OAuth access token for API calls.
        refresh_token: The refresh token for obtaining new access tokens.
        expires_at: Unix timestamp when the access token expires.
        token_type: Token type (usually "Bearer").
        scope: Optional space-separated list of granted scopes.
    """

    access_token: str
    refresh_token: Optional[str]
    expires_at: Optional[float]
    token_type: str = "Bearer"
    scope: Optional[str] = None

    def is_expired(self, buffer_seconds: int = 60) -> bool:
        """
        Check if the access token is expired or about to expire.

        Args:
            buffer_seconds: Consider expired if within this many seconds of expiry.

        Returns:
            True if the token is expired or will expire soon.
        """
        if self.expires_at is None:
            return False
        return time.time() >= (self.expires_at - buffer_seconds)

    def to_dict(self) -> dict:
        """
        Convert to a dictionary for JSON serialization.

        Returns:
            Dictionary representation of the tokens.
        """
        result: dict[str, str | float | None] = {
            "access_token": self.access_token,
            "token_type": self.token_type,
        }
        if self.refresh_token is not None:
            result["refresh_token"] = self.refresh_token
        if self.expires_at is not None:
            result["expires_at"] = self.expires_at
        if self.scope is not None:
            result["scope"] = self.scope
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "OAuthTokens":
        """
        Create from a dictionary.

        Args:
            data: Dictionary with token data.

        Returns:
            OAuthTokens instance.
        """
        return cls(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            expires_at=data.get("expires_at"),
            token_type=data.get("token_type", "Bearer"),
            scope=data.get("scope"),
        )


@dataclass
class ApiKeyCredential:
    """
    API key credential for a user's plugin connection.

    Attributes:
        api_key: The API key for authentication.
    """

    api_key: str

    def to_dict(self) -> dict:
        """
        Convert to a dictionary for JSON serialization.

        Returns:
            Dictionary representation of the credential.
        """
        return {"api_key": self.api_key}

    @classmethod
    def from_dict(cls, data: dict) -> "ApiKeyCredential":
        """
        Create from a dictionary.

        Args:
            data: Dictionary with API key data.

        Returns:
            ApiKeyCredential instance.
        """
        return cls(api_key=data["api_key"])


@dataclass
class UserCredentials:
    """
    User credentials for an AI plugin.

    Contains either OAuth tokens or an API key, never both.

    Attributes:
        oauth_tokens: OAuth tokens if using OAuth authentication.
        api_key: API key credential if using API key authentication.
    """

    oauth_tokens: Optional[OAuthTokens] = None
    api_key: Optional[ApiKeyCredential] = None

    def to_dict(self) -> dict:
        """
        Convert to a dictionary for JSON serialization.

        Returns:
            Dictionary representation of the credentials.
        """
        if self.oauth_tokens is not None:
            return {"type": "oauth", **self.oauth_tokens.to_dict()}
        elif self.api_key is not None:
            return {"type": "api_key", **self.api_key.to_dict()}
        else:
            return {"type": "none"}

    @classmethod
    def from_dict(cls, data: dict) -> "UserCredentials":
        """
        Create from a dictionary.

        Handles multiple formats:
        - Standard format: {"type": "api_key", "api_key": "value"}
        - GMS format (from UpsertAiPluginResolver): {"apiKey": "value"}
        - OAuth format: {"type": "oauth", "access_token": ..., ...}

        Args:
            data: Dictionary with credential data.

        Returns:
            UserCredentials instance.
        """
        cred_type = data.get("type")

        # Handle GMS format: {"apiKey": "value"} (no type field, camelCase key)
        if cred_type is None and "apiKey" in data:
            return cls(api_key=ApiKeyCredential(api_key=data["apiKey"]))

        # Handle standard api_key format
        if cred_type == "api_key":
            return cls(api_key=ApiKeyCredential.from_dict(data))

        # Handle OAuth format (default for backwards compat when type is "oauth" or missing with OAuth fields)
        if cred_type == "oauth" or (cred_type is None and "access_token" in data):
            return cls(oauth_tokens=OAuthTokens.from_dict(data))

        return cls()


class CredentialStore(Protocol):
    """
    Protocol interface for credential storage.

    Implementations handle storing and retrieving user credentials
    for AI plugins.
    """

    def get_credentials(
        self, user_urn: str, plugin_id: str
    ) -> Optional[UserCredentials]:
        """
        Get user credentials for a plugin.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.

        Returns:
            UserCredentials if found, None otherwise.
        """
        ...

    def save_oauth_tokens(
        self, user_urn: str, plugin_id: str, tokens: OAuthTokens
    ) -> str:
        """
        Save OAuth tokens for a user's plugin connection.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.
            tokens: The OAuth tokens to save.

        Returns:
            The URN of the created/updated DataHubConnection.
        """
        ...

    def save_api_key(self, user_urn: str, plugin_id: str, api_key: str) -> str:
        """
        Save an API key for a user's plugin connection.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.
            api_key: The API key to save.

        Returns:
            The URN of the created/updated DataHubConnection.
        """
        ...

    def delete_credentials(self, user_urn: str, plugin_id: str) -> bool:
        """
        Delete user credentials for a plugin.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.

        Returns:
            True if credentials were deleted, False if not found.
        """
        ...

    def get_access_token(
        self, user_urn: str, plugin_id: str, oauth_server_urn: str
    ) -> str:
        """
        Get a valid access token, refreshing if needed.

        Uses on-demand refresh with a 5-minute buffer.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.
            oauth_server_urn: The URN of the OAuth server for refresh.

        Returns:
            A valid access token.

        Raises:
            TokenRefreshError: If no credentials exist or refresh fails.
        """
        ...


# Platform URN for AI plugin credentials
AI_PLUGIN_PLATFORM_URN = "urn:li:dataPlatform:datahub"


def build_connection_urn(user_urn: str, plugin_id: str) -> str:
    """
    Build a DataHubConnection URN for user credentials.

    Format: urn:li:dataHubConnection:<sanitized_user>__<sanitized_plugin>

    The ID uses `__` as separator (not a tuple) because DataHubConnection
    expects a single-string key. Using tuple format (a,b) causes DataHub
    to interpret it as a multi-part key which fails validation.

    Args:
        user_urn: The URN of the user (e.g., urn:li:corpuser:johndoe).
        plugin_id: The ID of the AI plugin (e.g., urn:li:service:glean).

    Returns:
        The connection URN.
    """
    connection_id = get_connection_id(user_urn, plugin_id)
    return f"urn:li:dataHubConnection:{connection_id}"


def get_connection_id(user_urn: str, plugin_id: str) -> str:
    """
    Get the connection ID portion of the URN.

    Creates a single-string ID by joining sanitized user and plugin IDs
    with `__` separator. All special characters (colons, commas, parentheses)
    are replaced with underscores for URN safety.

    Args:
        user_urn: The URN of the user.
        plugin_id: The ID of the AI plugin.

    Returns:
        The connection ID (without urn:li:dataHubConnection: prefix).
    """
    # Sanitize both parts - replace colons, commas, and other special chars
    sanitized_user = (
        user_urn.replace(":", "_").replace(",", "_").replace("(", "_").replace(")", "_")
    )
    sanitized_plugin = (
        plugin_id.replace(":", "_")
        .replace(",", "_")
        .replace("(", "_")
        .replace(")", "_")
    )
    return f"{sanitized_user}__{sanitized_plugin}"


class TokenRefreshError(Exception):
    """Raised when OAuth token refresh fails."""

    pass


# Default buffer for proactive token refresh (5 minutes)
TOKEN_REFRESH_BUFFER_SECONDS = 300


class DataHubConnectionCredentialStore:
    """
    Credential store implementation using DataHubConnection entities.

    Each user's credentials for a plugin are stored in a separate
    DataHubConnection entity. The credentials are encrypted at rest
    by DataHub's connection storage system.

    Thread Safety:
        Uses per-connection locks for token refresh to prevent race conditions.
        Multiple threads refreshing the same token will serialize through the lock.
    """

    # Striped lock pool for per-connection locking during token refresh.
    # Uses 64 stripes to balance memory usage vs. concurrency.
    # Keys are hashed to select a stripe, so different connections may share
    # a lock (harmless - just serializes those specific refreshes).
    _refresh_locks = StripedLock(num_stripes=64)

    def __init__(self, graph: DataHubGraph) -> None:
        """
        Initialize the credential store.

        Args:
            graph: DataHubGraph client for API access.
        """
        self._graph = graph

    def _get_refresh_lock(self, connection_key: str) -> "threading.Lock":
        """Get a lock for a specific connection from the striped lock pool."""
        return self._refresh_locks.get_lock(connection_key)

    def get_credentials(
        self, user_urn: str, plugin_id: str
    ) -> Optional[UserCredentials]:
        """
        Get user credentials for a plugin.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.

        Returns:
            UserCredentials if found, None otherwise.
        """
        urn = build_connection_urn(user_urn, plugin_id)

        try:
            result = self._graph.execute_graphql(
                query="""
query GetConnection($urn: String!) {
  connection(urn: $urn) {
    urn
    details {
      type
      json {
        blob
      }
    }
  }
}
""".strip(),
                variables={"urn": urn},
            )

            if not result.get("connection"):
                return None

            details = result["connection"]["details"]
            if details["type"] != "JSON":
                logger.error(f"Unexpected connection type: {details['type']}")
                return None

            blob = details["json"]["blob"]
            data = json.loads(blob)
            return UserCredentials.from_dict(data)

        except Exception as e:
            logger.warning(f"Failed to get credentials for {user_urn}/{plugin_id}: {e}")
            return None

    def get_credentials_by_urn(self, connection_urn: str) -> Optional[UserCredentials]:
        """
        Get credentials by direct DataHubConnection URN.

        Use this for shared API keys where the credential_urn is already known
        and should not be transformed through build_connection_urn().

        Uses the REST API directly since GraphQL `connection` query may not find
        connections created via GMS (e.g., by UpsertAiPluginResolver).

        Args:
            connection_urn: The full URN of the DataHubConnection entity.

        Returns:
            UserCredentials if found, None otherwise.
        """
        try:
            # Use GraphQL which auto-decrypts the blob
            result = self._graph.execute_graphql(
                query="""
query GetConnection($urn: String!) {
  connection(urn: $urn) {
    urn
    details {
      type
      json {
        blob
      }
    }
  }
}
""",
                variables={"urn": connection_urn},
            )

            connection = result.get("connection")
            if not connection:
                logger.debug(f"Connection not found via GraphQL: {connection_urn}")
                return None

            details = connection.get("details")
            if not details or details.get("type") != "JSON":
                logger.debug(f"Connection has no JSON details: {connection_urn}")
                return None

            json_details = details.get("json", {})
            blob = json_details.get("blob")
            if not blob:
                logger.debug(f"Connection has no blob: {connection_urn}")
                return None

            data = json.loads(blob)
            return UserCredentials.from_dict(data)

        except Exception as e:
            logger.bind(
                connection_urn=connection_urn,
                error_type=type(e).__name__,
            ).opt(exception=True).warning("Failed to get credentials by URN")
            return None

    def save_oauth_tokens(
        self, user_urn: str, plugin_id: str, tokens: OAuthTokens
    ) -> str:
        """
        Save OAuth tokens for a user's plugin connection.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.
            tokens: The OAuth tokens to save.

        Returns:
            The URN of the created/updated DataHubConnection.
        """
        credentials = UserCredentials(oauth_tokens=tokens)
        return self._save_credentials(user_urn, plugin_id, credentials)

    def save_api_key(self, user_urn: str, plugin_id: str, api_key: str) -> str:
        """
        Save an API key for a user's plugin connection.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.
            api_key: The API key to save.

        Returns:
            The URN of the created/updated DataHubConnection.
        """
        credentials = UserCredentials(api_key=ApiKeyCredential(api_key=api_key))
        return self._save_credentials(user_urn, plugin_id, credentials)

    def delete_credentials(self, user_urn: str, plugin_id: str) -> bool:
        """
        Delete user credentials for a plugin.

        Note: This soft-deletes the DataHubConnection entity.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.

        Returns:
            True if credentials were deleted, False if not found.
        """
        urn = build_connection_urn(user_urn, plugin_id)

        try:
            result = self._graph.execute_graphql(
                query="""
mutation DeleteConnection($urn: String!) {
  deleteConnection(urn: $urn)
}
""".strip(),
                variables={"urn": urn},
            )

            return result.get("deleteConnection", False)

        except Exception as e:
            logger.warning(
                f"Failed to delete credentials for {user_urn}/{plugin_id}: {e}"
            )
            return False

    def _save_credentials(
        self, user_urn: str, plugin_id: str, credentials: UserCredentials
    ) -> str:
        """
        Save credentials to DataHubConnection.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.
            credentials: The credentials to save.

        Returns:
            The URN of the created/updated DataHubConnection.
        """
        connection_id = get_connection_id(user_urn, plugin_id)
        blob = json.dumps(credentials.to_dict())

        # Generate a human-readable name
        user_name = user_urn.split(":")[-1] if ":" in user_urn else user_urn
        plugin_name = plugin_id.split(":")[-1] if ":" in plugin_id else plugin_id
        name = f"AI Plugin Credentials - {user_name} - {plugin_name}"

        result = self._graph.execute_graphql(
            query="""
mutation UpsertConnection($id: String!, $platformUrn: String!, $blob: String!, $name: String) {
  upsertConnection(
    input: {
      id: $id,
      type: JSON,
      name: $name,
      platformUrn: $platformUrn,
      json: {
        blob: $blob
      }
    }
  ) {
    urn
  }
}
""".strip(),
            variables={
                "id": connection_id,
                "platformUrn": AI_PLUGIN_PLATFORM_URN,
                "name": name,
                "blob": blob,
            },
        )

        return result["upsertConnection"]["urn"]

    def get_access_token(
        self,
        user_urn: str,
        plugin_id: str,
        oauth_server_urn: str,
        on_refresh: Optional[Callable[[float, bool], None]] = None,
    ) -> str:
        """
        Get a valid access token, refreshing if needed.

        Uses on-demand refresh with a 5-minute buffer: if the token expires
        within 5 minutes, it's refreshed proactively.

        Args:
            user_urn: The URN of the user.
            plugin_id: The ID of the AI plugin.
            oauth_server_urn: The URN of the OAuth server for refresh.
            on_refresh: Optional callback invoked only when a token refresh
                actually occurs. Called with (duration_seconds, success).
                Callers can use this for metrics without coupling the store
                to a specific observability system.

        Returns:
            A valid access token.

        Raises:
            TokenRefreshError: If no credentials exist or refresh fails.
        """
        connection_key = get_connection_id(user_urn, plugin_id)

        # Load current tokens
        credentials = self.get_credentials(user_urn, plugin_id)
        if not credentials or not credentials.oauth_tokens:
            raise TokenRefreshError(
                f"No OAuth credentials found for {user_urn}/{plugin_id}"
            )

        tokens = credentials.oauth_tokens

        # Check if refresh is needed
        if not tokens.is_expired(buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS):
            return tokens.access_token

        # Need to refresh - acquire lock to prevent concurrent refreshes
        with self._get_refresh_lock(connection_key):
            # Re-check after acquiring lock (another thread may have refreshed)
            credentials = self.get_credentials(user_urn, plugin_id)
            if not credentials or not credentials.oauth_tokens:
                raise TokenRefreshError(
                    f"Credentials disappeared during refresh for {user_urn}/{plugin_id}"
                )

            tokens = credentials.oauth_tokens
            if not tokens.is_expired(buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS):
                return tokens.access_token

            # Still need refresh - do it now
            if not tokens.refresh_token:
                raise TokenRefreshError(
                    f"Token expired but no refresh token available for {user_urn}/{plugin_id}"
                )

            refresh_start = time.perf_counter()
            try:
                new_tokens = self._refresh_oauth_token(
                    tokens.refresh_token, oauth_server_urn
                )
            except Exception:
                if on_refresh:
                    on_refresh(time.perf_counter() - refresh_start, False)
                raise
            else:
                if on_refresh:
                    on_refresh(time.perf_counter() - refresh_start, True)

            # Save the new tokens
            self.save_oauth_tokens(user_urn, plugin_id, new_tokens)

            return new_tokens.access_token

    def _refresh_oauth_token(
        self, refresh_token: str, oauth_server_urn: str
    ) -> OAuthTokens:
        """
        Refresh an OAuth access token using the refresh token.

        Args:
            refresh_token: The refresh token to use.
            oauth_server_urn: The URN of the OAuth authorization server.

        Returns:
            New OAuth tokens with updated access token.

        Raises:
            TokenRefreshError: If the refresh fails.
        """
        # Load OAuth server config (raises TokenRefreshError if not found)
        server_config = self._get_oauth_server_config(oauth_server_urn)

        token_url = server_config.get("tokenUrl")
        client_id = server_config.get("clientId")
        client_secret = server_config.get("clientSecret")
        token_auth_method = server_config.get("tokenAuthMethod", "POST_BODY")
        auth_scheme = server_config.get("authScheme")
        auth_header_name = server_config.get("authHeaderName", "Authorization")

        if not token_url or not client_id:
            raise TokenRefreshError(
                f"Invalid OAuth server config (missing tokenUrl or clientId): {oauth_server_urn}"
            )

        # Build request
        data: Dict[str, str] = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        # Handle authentication method (same logic as router.py token exchange)
        if token_auth_method == "BASIC" and client_secret:
            # HTTP Basic Auth: credentials in Authorization header
            credentials = base64.b64encode(
                f"{client_id}:{client_secret}".encode()
            ).decode()
            headers[auth_header_name] = f"Basic {credentials}"
            data["client_id"] = client_id
        elif token_auth_method == "CUSTOM" and auth_scheme and client_secret:
            # Custom auth scheme (e.g., "Token" for dbt Cloud)
            headers[auth_header_name] = f"{auth_scheme} {client_secret}"
            data["client_id"] = client_id
        elif token_auth_method == "NONE":
            # No client authentication (public clients)
            data["client_id"] = client_id
        else:
            # POST_BODY (default): credentials in request body
            data["client_id"] = client_id
            if client_secret:
                data["client_secret"] = client_secret

        try:
            with httpx.Client() as client:
                response = client.post(token_url, data=data, headers=headers)

                if response.status_code != 200:
                    logger.error(
                        f"Token refresh failed: {response.status_code} - {response.text}"
                    )
                    raise TokenRefreshError(
                        f"Token refresh failed with status {response.status_code}"
                    )

                token_data = response.json()

                # Calculate expires_at from expires_in if provided
                expires_at = None
                if "expires_in" in token_data:
                    expires_at = time.time() + token_data["expires_in"]

                return OAuthTokens(
                    access_token=token_data["access_token"],
                    refresh_token=token_data.get("refresh_token", refresh_token),
                    expires_at=expires_at,
                    token_type=token_data.get("token_type", "Bearer"),
                    scope=token_data.get("scope"),
                )

        except TokenRefreshError:
            raise
        except Exception as e:
            logger.bind(
                oauth_server_urn=oauth_server_urn,
                token_url=token_url,
                error_type=type(e).__name__,
            ).opt(exception=True).error("Token refresh error")
            raise TokenRefreshError(f"Token refresh failed: {e}") from e

    def _get_oauth_server_config(self, server_urn: str) -> Dict:
        """
        Get OAuth server configuration including resolved client secret.

        Args:
            server_urn: The URN of the OAuth authorization server.

        Returns:
            Server configuration dict with properties.

        Raises:
            TokenRefreshError: If the server config cannot be retrieved.
        """
        try:
            result = self._graph.execute_graphql(
                query="""
query GetOAuthServer($urn: String!) {
  oauthAuthorizationServer(urn: $urn) {
    urn
    properties {
      tokenUrl
      clientId
      clientSecretUrn
      tokenAuthMethod
      authHeaderName
      authScheme
    }
  }
}
""".strip(),
                variables={"urn": server_urn},
            )

            server = result.get("oauthAuthorizationServer")
            if not server:
                raise TokenRefreshError(f"OAuth server not found: {server_urn}")

            properties = server["properties"]

            # Resolve client secret if URN is provided
            client_secret_urn = properties.get("clientSecretUrn")
            if client_secret_urn:
                client_secret = self._resolve_secret_value(client_secret_urn)
                properties["clientSecret"] = client_secret

            return properties

        except TokenRefreshError:
            raise
        except Exception as e:
            logger.bind(
                oauth_server_urn=server_urn,
                error_type=type(e).__name__,
            ).opt(exception=True).error("Failed to get OAuth server config")
            raise TokenRefreshError(
                f"Failed to get OAuth server config for {server_urn}: {e}"
            ) from e

    def _resolve_secret_value(self, secret_urn: str) -> str:
        """
        Resolve a secret value from its URN.

        Args:
            secret_urn: The URN of the DataHubSecret entity.

        Returns:
            The secret value.

        Raises:
            TokenRefreshError: If the secret cannot be resolved.
        """
        prefix = "urn:li:dataHubSecret:"
        if not secret_urn.startswith(prefix):
            raise TokenRefreshError(f"Invalid secret URN format: {secret_urn}")

        secret_name = secret_urn[len(prefix) :]

        try:
            result = self._graph.execute_graphql(
                query="""
query GetSecretValues($input: GetSecretValuesInput!) {
    getSecretValues(input: $input) {
        name
        value
    }
}
""".strip(),
                variables={"input": {"secrets": [secret_name]}},
            )

            secret_values = result.get("getSecretValues", [])
            if secret_values:
                value = secret_values[0].get("value")
                if value is not None:
                    return value

            raise TokenRefreshError(f"Secret not found: {secret_name}")

        except TokenRefreshError:
            raise
        except Exception as e:
            logger.bind(
                secret_urn=secret_urn,
                secret_name=secret_name,
                error_type=type(e).__name__,
            ).opt(exception=True).error("Failed to resolve secret")
            raise TokenRefreshError(
                f"Failed to resolve secret {secret_name}: {e}"
            ) from e
