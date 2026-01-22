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
- Token refresh should be done with distributed locking (Phase 3 with Redis)
"""

import json
import time
from dataclasses import dataclass
from typing import Optional, Protocol

from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger


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

        Args:
            data: Dictionary with credential data.

        Returns:
            UserCredentials instance.
        """
        cred_type = data.get("type", "oauth")  # Default to oauth for backwards compat
        if cred_type == "api_key":
            return cls(api_key=ApiKeyCredential.from_dict(data))
        elif cred_type == "oauth":
            return cls(oauth_tokens=OAuthTokens.from_dict(data))
        else:
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


class DataHubConnectionCredentialStore:
    """
    Credential store implementation using DataHubConnection entities.

    Each user's credentials for a plugin are stored in a separate
    DataHubConnection entity. The credentials are encrypted at rest
    by DataHub's connection storage system.

    Thread Safety:
        This implementation relies on DataHub's API for atomicity.
        For token refresh with distributed systems, additional locking
        is needed (planned for Phase 3 with Redis).
    """

    def __init__(self, graph: DataHubGraph) -> None:
        """
        Initialize the credential store.

        Args:
            graph: DataHubGraph client for API access.
        """
        self._graph = graph

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
