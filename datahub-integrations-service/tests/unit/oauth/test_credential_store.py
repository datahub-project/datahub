"""Unit tests for OAuth credential store."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.oauth.credential_store import (
    AI_PLUGIN_PLATFORM_URN,
    ApiKeyCredential,
    DataHubConnectionCredentialStore,
    OAuthTokens,
    TokenRefreshError,
    UserCredentials,
    build_connection_urn,
    get_connection_id,
)


class TestBuildConnectionUrn:
    """Test connection URN construction."""

    def test_basic_urn_construction(self) -> None:
        """Test basic URN construction with simple IDs."""
        urn = build_connection_urn(
            user_urn="urn:li:corpuser:admin",
            plugin_id="urn:li:service:test-plugin",
        )

        # Should be a valid DataHubConnection URN
        assert urn.startswith("urn:li:dataHubConnection:")

    def test_urn_sanitizes_special_chars(self) -> None:
        """Test that special characters are sanitized in the connection ID."""
        urn = build_connection_urn(
            user_urn="urn:li:corpuser:admin",
            plugin_id="urn:li:service:my-plugin",
        )

        # After urn:li:dataHubConnection:, there should be no colons or commas
        connection_id = urn.split("urn:li:dataHubConnection:")[1]
        assert ":" not in connection_id
        assert "," not in connection_id

    def test_urn_uses_double_underscore_separator(self) -> None:
        """Test that user and plugin are joined with double underscore."""
        urn = build_connection_urn(
            user_urn="urn:li:corpuser:admin",
            plugin_id="urn:li:service:plugin",
        )

        connection_id = urn.split("urn:li:dataHubConnection:")[1]
        assert "__" in connection_id

    def test_deterministic_urn_generation(self) -> None:
        """Test that same inputs produce same URN."""
        urn1 = build_connection_urn(
            user_urn="urn:li:corpuser:admin",
            plugin_id="urn:li:service:test-plugin",
        )
        urn2 = build_connection_urn(
            user_urn="urn:li:corpuser:admin",
            plugin_id="urn:li:service:test-plugin",
        )

        assert urn1 == urn2

    def test_different_users_get_different_urns(self) -> None:
        """Test that different users get different URNs for same plugin."""
        urn1 = build_connection_urn(
            user_urn="urn:li:corpuser:user1",
            plugin_id="urn:li:service:test-plugin",
        )
        urn2 = build_connection_urn(
            user_urn="urn:li:corpuser:user2",
            plugin_id="urn:li:service:test-plugin",
        )

        assert urn1 != urn2

    def test_different_plugins_get_different_urns(self) -> None:
        """Test that same user gets different URNs for different plugins."""
        urn1 = build_connection_urn(
            user_urn="urn:li:corpuser:admin",
            plugin_id="urn:li:service:plugin1",
        )
        urn2 = build_connection_urn(
            user_urn="urn:li:corpuser:admin",
            plugin_id="urn:li:service:plugin2",
        )

        assert urn1 != urn2


class TestGetConnectionId:
    """Test connection ID extraction from URN."""

    def test_get_connection_id_roundtrip(self) -> None:
        """Test that build_connection_urn and get_connection_id are consistent."""
        user_urn = "urn:li:corpuser:admin"
        plugin_id = "urn:li:service:test-plugin"

        urn = build_connection_urn(user_urn, plugin_id)
        connection_id = get_connection_id(user_urn, plugin_id)

        assert urn == f"urn:li:dataHubConnection:{connection_id}"


class TestCredentialDataClasses:
    """Test credential data classes."""

    def test_api_key_credential(self) -> None:
        """Test ApiKeyCredential dataclass."""
        cred = ApiKeyCredential(api_key="my-secret-key")
        assert cred.api_key == "my-secret-key"

    def test_oauth_tokens(self) -> None:
        """Test OAuthTokens dataclass."""
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=1234567890,
            token_type="Bearer",
            scope="read write",
        )

        assert tokens.access_token == "access-123"
        assert tokens.refresh_token == "refresh-456"
        assert tokens.expires_at == 1234567890
        assert tokens.token_type == "Bearer"
        assert tokens.scope == "read write"

    def test_oauth_tokens_minimal(self) -> None:
        """Test OAuthTokens with minimal fields."""
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token=None,
            expires_at=None,
        )

        assert tokens.access_token == "access-123"
        assert tokens.refresh_token is None
        assert tokens.expires_at is None
        assert tokens.token_type == "Bearer"  # Default value

    def test_user_credentials_with_api_key(self) -> None:
        """Test UserCredentials with API key only."""
        creds = UserCredentials(
            api_key=ApiKeyCredential(api_key="my-key"),
            oauth_tokens=None,
        )

        assert creds.api_key is not None
        assert creds.api_key.api_key == "my-key"
        assert creds.oauth_tokens is None

    def test_user_credentials_with_oauth(self) -> None:
        """Test UserCredentials with OAuth tokens only."""
        creds = UserCredentials(
            api_key=None,
            oauth_tokens=OAuthTokens(
                access_token="token-123",
                refresh_token="refresh-456",
                expires_at=1234567890.0,
            ),
        )

        assert creds.api_key is None
        assert creds.oauth_tokens is not None
        assert creds.oauth_tokens.access_token == "token-123"


class TestOAuthTokensExpiration:
    """Test OAuth token expiration checking."""

    def test_is_expired_returns_false_for_future_expiry(self) -> None:
        """Test that token is not expired when expiry is in the future."""
        future_time = time.time() + 3600  # 1 hour from now
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=future_time,
        )

        assert not tokens.is_expired()

    def test_is_expired_returns_true_for_past_expiry(self) -> None:
        """Test that token is expired when expiry is in the past."""
        past_time = time.time() - 3600  # 1 hour ago
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=past_time,
        )

        assert tokens.is_expired()

    def test_is_expired_uses_buffer(self) -> None:
        """Test that expiration check uses buffer for near-expiry tokens."""
        # Token expires in 30 seconds, but buffer is 60 seconds
        near_future = time.time() + 30
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=near_future,
        )

        # With default 60 second buffer, should be considered expired
        assert tokens.is_expired(buffer_seconds=60)

        # With smaller buffer, should not be expired
        assert not tokens.is_expired(buffer_seconds=10)

    def test_is_expired_returns_false_when_no_expiry(self) -> None:
        """Test that token is not expired when expires_at is None."""
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=None,
        )

        assert not tokens.is_expired()


class TestOAuthTokensSerialization:
    """Test OAuth token serialization and deserialization."""

    def test_to_dict_includes_all_fields(self) -> None:
        """Test that to_dict includes all set fields."""
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=1234567890.0,
            token_type="Bearer",
            scope="read write",
        )

        data = tokens.to_dict()

        assert data["access_token"] == "access-123"
        assert data["refresh_token"] == "refresh-456"
        assert data["expires_at"] == 1234567890.0
        assert data["token_type"] == "Bearer"
        assert data["scope"] == "read write"

    def test_to_dict_excludes_none_fields(self) -> None:
        """Test that to_dict excludes None fields."""
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token=None,
            expires_at=None,
        )

        data = tokens.to_dict()

        assert "access_token" in data
        assert "refresh_token" not in data
        assert "expires_at" not in data

    def test_from_dict_creates_token(self) -> None:
        """Test that from_dict creates tokens from dictionary."""
        data = {
            "access_token": "access-123",
            "refresh_token": "refresh-456",
            "expires_at": 1234567890.0,
            "token_type": "Bearer",
            "scope": "read write",
        }

        tokens = OAuthTokens.from_dict(data)

        assert tokens.access_token == "access-123"
        assert tokens.refresh_token == "refresh-456"
        assert tokens.expires_at == 1234567890.0
        assert tokens.token_type == "Bearer"
        assert tokens.scope == "read write"

    def test_from_dict_handles_missing_optional_fields(self) -> None:
        """Test that from_dict handles missing optional fields."""
        data = {
            "access_token": "access-123",
        }

        tokens = OAuthTokens.from_dict(data)

        assert tokens.access_token == "access-123"
        assert tokens.refresh_token is None
        assert tokens.expires_at is None
        assert tokens.token_type == "Bearer"  # Default
        assert tokens.scope is None

    def test_roundtrip_serialization(self) -> None:
        """Test that to_dict and from_dict are inverses."""
        original = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=1234567890.0,
            token_type="Bearer",
            scope="read write admin",
        )

        data = original.to_dict()
        restored = OAuthTokens.from_dict(data)

        assert restored.access_token == original.access_token
        assert restored.refresh_token == original.refresh_token
        assert restored.expires_at == original.expires_at
        assert restored.token_type == original.token_type
        assert restored.scope == original.scope


class TestApiKeyCredentialSerialization:
    """Test API key credential serialization."""

    def test_to_dict(self) -> None:
        """Test ApiKeyCredential to_dict."""
        cred = ApiKeyCredential(api_key="my-secret-key")
        data = cred.to_dict()

        assert data == {"api_key": "my-secret-key"}

    def test_from_dict(self) -> None:
        """Test ApiKeyCredential from_dict."""
        data = {"api_key": "my-secret-key"}
        cred = ApiKeyCredential.from_dict(data)

        assert cred.api_key == "my-secret-key"

    def test_roundtrip_serialization(self) -> None:
        """Test roundtrip serialization."""
        original = ApiKeyCredential(api_key="secret-api-key-123")
        data = original.to_dict()
        restored = ApiKeyCredential.from_dict(data)

        assert restored.api_key == original.api_key


class TestUserCredentialsSerialization:
    """Test UserCredentials serialization.

    Note: UserCredentials uses a flat structure with a 'type' field to distinguish
    between OAuth and API key credentials in to_dict/from_dict.
    """

    def test_to_dict_with_api_key(self) -> None:
        """Test UserCredentials to_dict with API key."""
        creds = UserCredentials(
            api_key=ApiKeyCredential(api_key="my-key"),
            oauth_tokens=None,
        )

        data = creds.to_dict()

        # Uses flat structure with type field
        assert data["type"] == "api_key"
        assert data["api_key"] == "my-key"

    def test_to_dict_with_oauth(self) -> None:
        """Test UserCredentials to_dict with OAuth tokens."""
        creds = UserCredentials(
            api_key=None,
            oauth_tokens=OAuthTokens(
                access_token="access-123",
                refresh_token="refresh-456",
                expires_at=1234567890.0,
            ),
        )

        data = creds.to_dict()

        # Uses flat structure with type field
        assert data["type"] == "oauth"
        assert data["access_token"] == "access-123"
        assert data["refresh_token"] == "refresh-456"

    def test_to_dict_with_neither(self) -> None:
        """Test UserCredentials to_dict with no credentials."""
        creds = UserCredentials(api_key=None, oauth_tokens=None)

        data = creds.to_dict()

        assert data["type"] == "none"

    def test_from_dict_with_api_key(self) -> None:
        """Test UserCredentials from_dict with API key."""
        data = {
            "type": "api_key",
            "api_key": "my-secret-key",
        }

        creds = UserCredentials.from_dict(data)

        assert creds.api_key is not None
        assert creds.api_key.api_key == "my-secret-key"
        assert creds.oauth_tokens is None

    def test_from_dict_with_oauth(self) -> None:
        """Test UserCredentials from_dict with OAuth tokens."""
        data = {
            "type": "oauth",
            "access_token": "access-123",
            "refresh_token": "refresh-456",
            "expires_at": 1234567890.0,
        }

        creds = UserCredentials.from_dict(data)

        assert creds.api_key is None
        assert creds.oauth_tokens is not None
        assert creds.oauth_tokens.access_token == "access-123"

    def test_from_dict_defaults_to_oauth(self) -> None:
        """Test UserCredentials from_dict defaults to oauth when type missing."""
        data = {
            "access_token": "access-123",
        }

        creds = UserCredentials.from_dict(data)

        # Default is oauth for backwards compatibility
        assert creds.oauth_tokens is not None
        assert creds.oauth_tokens.access_token == "access-123"

    def test_roundtrip_with_api_key(self) -> None:
        """Test roundtrip serialization with API key."""
        original = UserCredentials(
            api_key=ApiKeyCredential(api_key="secret-key"),
            oauth_tokens=None,
        )

        data = original.to_dict()
        restored = UserCredentials.from_dict(data)

        assert original.api_key is not None  # Type narrowing
        assert restored.api_key is not None
        assert restored.api_key.api_key == original.api_key.api_key

    def test_roundtrip_with_oauth(self) -> None:
        """Test roundtrip serialization with OAuth tokens."""
        original = UserCredentials(
            api_key=None,
            oauth_tokens=OAuthTokens(
                access_token="access-123",
                refresh_token="refresh-456",
                expires_at=1234567890.0,
            ),
        )

        data = original.to_dict()
        restored = UserCredentials.from_dict(data)

        assert original.oauth_tokens is not None  # Type narrowing
        assert restored.oauth_tokens is not None
        assert restored.oauth_tokens.access_token == original.oauth_tokens.access_token


class TestBuildConnectionUrnEdgeCases:
    """Test edge cases for connection URN construction."""

    def test_handles_special_characters_in_user_urn(self) -> None:
        """Test that special characters in user URN are properly sanitized."""
        urn = build_connection_urn(
            user_urn="urn:li:corpuser:user@example.com",
            plugin_id="urn:li:service:test",
        )

        # Should be a valid URN
        assert urn.startswith("urn:li:dataHubConnection:")
        # Email @ sign should be preserved (only : and , are sanitized)
        connection_id = urn.split("urn:li:dataHubConnection:")[1]
        assert ":" not in connection_id
        assert "," not in connection_id

    def test_handles_uuid_in_plugin_id(self) -> None:
        """Test handling of UUID-based plugin IDs."""
        urn = build_connection_urn(
            user_urn="urn:li:corpuser:admin",
            plugin_id="urn:li:service:a5f9fde9-8cdc-4051-8fc3-1d10016ceb15",
        )

        assert urn.startswith("urn:li:dataHubConnection:")
        connection_id = urn.split("urn:li:dataHubConnection:")[1]
        # Hyphens in UUID should be preserved
        assert "-" in connection_id

    def test_handles_empty_strings(self) -> None:
        """Test handling of empty strings (edge case)."""
        # Empty strings should produce deterministic but different URNs
        urn_empty = build_connection_urn(user_urn="", plugin_id="")
        urn_non_empty = build_connection_urn(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:test",
        )

        assert urn_empty != urn_non_empty
        assert urn_empty.startswith("urn:li:dataHubConnection:")


class TestConnectionIdConsistency:
    """Test connection ID consistency between functions."""

    def test_connection_id_matches_urn_suffix(self) -> None:
        """Test that get_connection_id returns the suffix of build_connection_urn."""
        user_urn = "urn:li:corpuser:admin"
        plugin_id = "urn:li:service:github"

        full_urn = build_connection_urn(user_urn, plugin_id)
        connection_id = get_connection_id(user_urn, plugin_id)

        assert full_urn.endswith(connection_id)

    def test_multiple_calls_produce_same_id(self) -> None:
        """Test that multiple calls produce the same connection ID."""
        user_urn = "urn:li:corpuser:test"
        plugin_id = "urn:li:service:plugin"

        id1 = get_connection_id(user_urn, plugin_id)
        id2 = get_connection_id(user_urn, plugin_id)
        id3 = get_connection_id(user_urn, plugin_id)

        assert id1 == id2 == id3


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for DataHubConnectionCredentialStore
# ═══════════════════════════════════════════════════════════════════════════════


class TestDataHubConnectionCredentialStoreGetCredentials:
    """Test DataHubConnectionCredentialStore.get_credentials."""

    def test_get_credentials_returns_oauth_tokens(self) -> None:
        """Test get_credentials returns OAuth tokens when found."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:dataHubConnection:test",
                "details": {
                    "type": "JSON",
                    "json": {
                        "blob": json.dumps(
                            {
                                "type": "oauth",
                                "access_token": "test-access-token",
                                "refresh_token": "test-refresh-token",
                                "expires_at": 1234567890.0,
                                "token_type": "Bearer",
                            }
                        )
                    },
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_credentials("urn:li:corpuser:admin", "urn:li:service:github")

        assert result is not None
        assert result.oauth_tokens is not None
        assert result.oauth_tokens.access_token == "test-access-token"
        assert result.oauth_tokens.refresh_token == "test-refresh-token"
        assert result.api_key is None

    def test_get_credentials_returns_api_key(self) -> None:
        """Test get_credentials returns API key when found."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:dataHubConnection:test",
                "details": {
                    "type": "JSON",
                    "json": {
                        "blob": json.dumps(
                            {"type": "api_key", "api_key": "my-secret-key"}
                        )
                    },
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_credentials("urn:li:corpuser:admin", "urn:li:service:dbt")

        assert result is not None
        assert result.api_key is not None
        assert result.api_key.api_key == "my-secret-key"
        assert result.oauth_tokens is None

    def test_get_credentials_returns_none_when_not_found(self) -> None:
        """Test get_credentials returns None when connection doesn't exist."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {"connection": None}

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_credentials(
            "urn:li:corpuser:admin", "urn:li:service:nonexistent"
        )

        assert result is None

    def test_get_credentials_returns_none_on_unexpected_type(self) -> None:
        """Test get_credentials returns None when connection type is unexpected."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:dataHubConnection:test",
                "details": {
                    "type": "OTHER_TYPE",  # Not JSON
                    "json": {"blob": "{}"},
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_credentials("urn:li:corpuser:admin", "urn:li:service:github")

        assert result is None

    def test_get_credentials_returns_none_on_graphql_error(self) -> None:
        """Test get_credentials returns None when GraphQL query fails."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_credentials("urn:li:corpuser:admin", "urn:li:service:github")

        assert result is None

    def test_get_credentials_returns_none_on_invalid_json(self) -> None:
        """Test get_credentials returns None when JSON blob is invalid."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:dataHubConnection:test",
                "details": {
                    "type": "JSON",
                    "json": {"blob": "not-valid-json{"},
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_credentials("urn:li:corpuser:admin", "urn:li:service:github")

        assert result is None

    def test_get_credentials_builds_correct_urn(self) -> None:
        """Test get_credentials builds correct connection URN."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {"connection": None}

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        store.get_credentials("urn:li:corpuser:admin", "urn:li:service:github")

        # Verify the URN passed to GraphQL
        call_args = mock_graph.execute_graphql.call_args
        variables = call_args.kwargs.get("variables", {})
        expected_urn = build_connection_urn(
            "urn:li:corpuser:admin", "urn:li:service:github"
        )
        assert variables["urn"] == expected_urn


class TestDataHubConnectionCredentialStoreSaveOAuthTokens:
    """Test DataHubConnectionCredentialStore.save_oauth_tokens."""

    def test_save_oauth_tokens_returns_connection_urn(self) -> None:
        """Test save_oauth_tokens returns the connection URN."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "upsertConnection": {"urn": "urn:li:dataHubConnection:test-connection"}
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=1234567890.0,
        )

        result = store.save_oauth_tokens(
            "urn:li:corpuser:admin", "urn:li:service:github", tokens
        )

        assert result == "urn:li:dataHubConnection:test-connection"

    def test_save_oauth_tokens_sends_correct_mutation(self) -> None:
        """Test save_oauth_tokens sends correct GraphQL mutation."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "upsertConnection": {"urn": "urn:li:dataHubConnection:test"}
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        tokens = OAuthTokens(
            access_token="access-123",
            refresh_token="refresh-456",
            expires_at=1234567890.0,
            token_type="Bearer",
            scope="read write",
        )

        store.save_oauth_tokens(
            "urn:li:corpuser:admin", "urn:li:service:github", tokens
        )

        # Verify GraphQL call
        call_args = mock_graph.execute_graphql.call_args
        variables = call_args.kwargs.get("variables", {})

        assert variables["platformUrn"] == AI_PLUGIN_PLATFORM_URN
        assert "admin" in variables["name"]
        assert "github" in variables["name"]

        # Verify the blob contains OAuth tokens
        blob_data = json.loads(variables["blob"])
        assert blob_data["type"] == "oauth"
        assert blob_data["access_token"] == "access-123"
        assert blob_data["refresh_token"] == "refresh-456"


class TestDataHubConnectionCredentialStoreSaveApiKey:
    """Test DataHubConnectionCredentialStore.save_api_key."""

    def test_save_api_key_returns_connection_urn(self) -> None:
        """Test save_api_key returns the connection URN."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "upsertConnection": {"urn": "urn:li:dataHubConnection:test-api-key"}
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.save_api_key(
            "urn:li:corpuser:admin", "urn:li:service:dbt", "my-secret-api-key"
        )

        assert result == "urn:li:dataHubConnection:test-api-key"

    def test_save_api_key_sends_correct_mutation(self) -> None:
        """Test save_api_key sends correct GraphQL mutation."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "upsertConnection": {"urn": "urn:li:dataHubConnection:test"}
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        store.save_api_key(
            "urn:li:corpuser:admin", "urn:li:service:dbt", "my-secret-api-key"
        )

        # Verify GraphQL call
        call_args = mock_graph.execute_graphql.call_args
        variables = call_args.kwargs.get("variables", {})

        assert variables["platformUrn"] == AI_PLUGIN_PLATFORM_URN
        assert "admin" in variables["name"]
        assert "dbt" in variables["name"]

        # Verify the blob contains API key
        blob_data = json.loads(variables["blob"])
        assert blob_data["type"] == "api_key"
        assert blob_data["api_key"] == "my-secret-api-key"


class TestDataHubConnectionCredentialStoreDeleteCredentials:
    """Test DataHubConnectionCredentialStore.delete_credentials."""

    def test_delete_credentials_returns_true_on_success(self) -> None:
        """Test delete_credentials returns True when deletion succeeds."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {"deleteConnection": True}

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.delete_credentials(
            "urn:li:corpuser:admin", "urn:li:service:github"
        )

        assert result is True

    def test_delete_credentials_returns_false_when_not_found(self) -> None:
        """Test delete_credentials returns False when connection not found."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {"deleteConnection": False}

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.delete_credentials(
            "urn:li:corpuser:admin", "urn:li:service:nonexistent"
        )

        assert result is False

    def test_delete_credentials_returns_false_on_error(self) -> None:
        """Test delete_credentials returns False when GraphQL fails."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.delete_credentials(
            "urn:li:corpuser:admin", "urn:li:service:github"
        )

        assert result is False

    def test_delete_credentials_builds_correct_urn(self) -> None:
        """Test delete_credentials builds correct connection URN."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {"deleteConnection": True}

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        store.delete_credentials("urn:li:corpuser:admin", "urn:li:service:github")

        # Verify the URN passed to GraphQL
        call_args = mock_graph.execute_graphql.call_args
        variables = call_args.kwargs.get("variables", {})
        expected_urn = build_connection_urn(
            "urn:li:corpuser:admin", "urn:li:service:github"
        )
        assert variables["urn"] == expected_urn


class TestDataHubConnectionCredentialStoreSaveCredentials:
    """Test DataHubConnectionCredentialStore._save_credentials."""

    def test_save_credentials_generates_readable_name(self) -> None:
        """Test _save_credentials generates human-readable name."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "upsertConnection": {"urn": "urn:li:dataHubConnection:test"}
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        credentials = UserCredentials(
            oauth_tokens=OAuthTokens(
                access_token="test",
                refresh_token=None,
                expires_at=None,
            )
        )

        store._save_credentials(
            "urn:li:corpuser:johndoe", "urn:li:service:github-plugin", credentials
        )

        call_args = mock_graph.execute_graphql.call_args
        variables = call_args.kwargs.get("variables", {})

        # Name should include user and plugin names
        assert "johndoe" in variables["name"]
        assert "github-plugin" in variables["name"]
        assert "AI Plugin Credentials" in variables["name"]

    def test_save_credentials_uses_correct_connection_id(self) -> None:
        """Test _save_credentials uses correct connection ID format."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "upsertConnection": {"urn": "urn:li:dataHubConnection:test"}
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        credentials = UserCredentials(api_key=ApiKeyCredential(api_key="test"))

        store._save_credentials(
            "urn:li:corpuser:admin", "urn:li:service:test", credentials
        )

        call_args = mock_graph.execute_graphql.call_args
        variables = call_args.kwargs.get("variables", {})

        expected_id = get_connection_id("urn:li:corpuser:admin", "urn:li:service:test")
        assert variables["id"] == expected_id

    def test_save_credentials_raises_on_graphql_error(self) -> None:
        """Test _save_credentials raises exception on GraphQL error."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.side_effect = RuntimeError("GraphQL error")

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        credentials = UserCredentials(api_key=ApiKeyCredential(api_key="test"))

        with pytest.raises(RuntimeError, match="GraphQL error"):
            store._save_credentials(
                "urn:li:corpuser:admin", "urn:li:service:test", credentials
            )


class TestDataHubConnectionCredentialStoreEndToEnd:
    """End-to-end tests for DataHubConnectionCredentialStore."""

    def test_save_and_get_oauth_tokens_roundtrip(self) -> None:
        """Test saving and getting OAuth tokens (conceptual roundtrip)."""
        # First call: save tokens
        mock_graph = MagicMock()
        connection_urn = "urn:li:dataHubConnection:test-roundtrip"
        mock_graph.execute_graphql.return_value = {
            "upsertConnection": {"urn": connection_urn}
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        tokens = OAuthTokens(
            access_token="access-roundtrip",
            refresh_token="refresh-roundtrip",
            expires_at=1234567890.0,
        )

        save_result = store.save_oauth_tokens(
            "urn:li:corpuser:admin", "urn:li:service:github", tokens
        )
        assert save_result == connection_urn

        # Verify save was called with correct blob
        save_call = mock_graph.execute_graphql.call_args
        saved_blob = json.loads(save_call.kwargs["variables"]["blob"])
        assert saved_blob["access_token"] == "access-roundtrip"

        # Second call: get tokens back
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": connection_urn,
                "details": {
                    "type": "JSON",
                    "json": {"blob": json.dumps(saved_blob)},
                },
            }
        }

        get_result = store.get_credentials(
            "urn:li:corpuser:admin", "urn:li:service:github"
        )

        assert get_result is not None
        assert get_result.oauth_tokens is not None
        assert get_result.oauth_tokens.access_token == "access-roundtrip"

    def test_save_and_get_api_key_roundtrip(self) -> None:
        """Test saving and getting API key (conceptual roundtrip)."""
        mock_graph = MagicMock()
        connection_urn = "urn:li:dataHubConnection:test-api-roundtrip"
        mock_graph.execute_graphql.return_value = {
            "upsertConnection": {"urn": connection_urn}
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)

        save_result = store.save_api_key(
            "urn:li:corpuser:admin", "urn:li:service:dbt", "my-api-key-roundtrip"
        )
        assert save_result == connection_urn

        # Verify save was called with correct blob
        save_call = mock_graph.execute_graphql.call_args
        saved_blob = json.loads(save_call.kwargs["variables"]["blob"])
        assert saved_blob["api_key"] == "my-api-key-roundtrip"

        # Second call: get API key back
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": connection_urn,
                "details": {
                    "type": "JSON",
                    "json": {"blob": json.dumps(saved_blob)},
                },
            }
        }

        get_result = store.get_credentials(
            "urn:li:corpuser:admin", "urn:li:service:dbt"
        )

        assert get_result is not None
        assert get_result.api_key is not None
        assert get_result.api_key.api_key == "my-api-key-roundtrip"


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for Token Refresh
# ═══════════════════════════════════════════════════════════════════════════════


class TestGetAccessToken:
    """Test DataHubConnectionCredentialStore.get_access_token."""

    def test_returns_token_when_not_expired(self) -> None:
        """Test that valid token is returned without refresh."""
        mock_graph = MagicMock()
        future_time = time.time() + 3600  # 1 hour from now
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:dataHubConnection:test",
                "details": {
                    "type": "JSON",
                    "json": {
                        "blob": json.dumps(
                            {
                                "type": "oauth",
                                "access_token": "valid-token",
                                "refresh_token": "refresh-token",
                                "expires_at": future_time,
                            }
                        )
                    },
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_access_token(
            "urn:li:corpuser:admin",
            "urn:li:service:plugin",
            "urn:li:oauthServer:test",
        )

        assert result == "valid-token"

    def test_raises_when_no_credentials(self) -> None:
        """Test that TokenRefreshError is raised when no credentials exist."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {"connection": None}

        store = DataHubConnectionCredentialStore(graph=mock_graph)

        with pytest.raises(TokenRefreshError, match="No OAuth credentials found"):
            store.get_access_token(
                "urn:li:corpuser:admin",
                "urn:li:service:plugin",
                "urn:li:oauthServer:test",
            )

    def test_raises_when_no_oauth_tokens(self) -> None:
        """Test that TokenRefreshError is raised when credentials are API key."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:dataHubConnection:test",
                "details": {
                    "type": "JSON",
                    "json": {"blob": json.dumps({"type": "api_key", "api_key": "key"})},
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)

        with pytest.raises(TokenRefreshError, match="No OAuth credentials found"):
            store.get_access_token(
                "urn:li:corpuser:admin",
                "urn:li:service:plugin",
                "urn:li:oauthServer:test",
            )

    def test_raises_when_no_refresh_token_and_expired(self) -> None:
        """Test that TokenRefreshError is raised when token is expired but no refresh token."""
        mock_graph = MagicMock()
        past_time = time.time() - 3600  # 1 hour ago
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:dataHubConnection:test",
                "details": {
                    "type": "JSON",
                    "json": {
                        "blob": json.dumps(
                            {
                                "type": "oauth",
                                "access_token": "expired-token",
                                "refresh_token": None,
                                "expires_at": past_time,
                            }
                        )
                    },
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)

        with pytest.raises(TokenRefreshError, match="no refresh token available"):
            store.get_access_token(
                "urn:li:corpuser:admin",
                "urn:li:service:plugin",
                "urn:li:oauthServer:test",
            )

    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_refreshes_token_when_expired(self, mock_httpx_client: MagicMock) -> None:
        """Test that expired token triggers refresh."""
        mock_graph = MagicMock()
        past_time = time.time() - 3600  # 1 hour ago

        # First call: get expired credentials
        # Second call: get OAuth server config
        # Third call: save new tokens
        # Fourth call: (if re-checking) get new credentials
        call_count = [0]

        def graphql_side_effect(query: str, variables: dict | None = None):
            call_count[0] += 1
            if "GetConnection" in query or "connection(" in query:
                return {
                    "connection": {
                        "urn": "urn:li:dataHubConnection:test",
                        "details": {
                            "type": "JSON",
                            "json": {
                                "blob": json.dumps(
                                    {
                                        "type": "oauth",
                                        "access_token": "expired-token",
                                        "refresh_token": "refresh-token",
                                        "expires_at": past_time,
                                    }
                                )
                            },
                        },
                    }
                }
            elif "oauthAuthorizationServer" in query:
                return {
                    "oauthAuthorizationServer": {
                        "urn": "urn:li:oauthServer:test",
                        "properties": {
                            "tokenUrl": "https://oauth.example.com/token",
                            "clientId": "client-id",
                            "clientSecretUrn": None,
                        },
                    }
                }
            elif "upsertConnection" in query:
                return {"upsertConnection": {"urn": "urn:li:dataHubConnection:test"}}
            return {}

        mock_graph.execute_graphql.side_effect = graphql_side_effect

        # Mock HTTP response for token refresh
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new-access-token",
            "refresh_token": "new-refresh-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        mock_client_instance = MagicMock()
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=None)
        mock_client_instance.post.return_value = mock_response
        mock_httpx_client.return_value = mock_client_instance

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_access_token(
            "urn:li:corpuser:admin",
            "urn:li:service:plugin",
            "urn:li:oauthServer:test",
        )

        assert result == "new-access-token"

        # Verify HTTP call was made with correct params
        mock_client_instance.post.assert_called_once()
        call_args = mock_client_instance.post.call_args
        assert call_args[0][0] == "https://oauth.example.com/token"
        assert "grant_type" in call_args[1]["data"]
        assert call_args[1]["data"]["grant_type"] == "refresh_token"


class TestRefreshOAuthToken:
    """Test DataHubConnectionCredentialStore._refresh_oauth_token."""

    def test_raises_when_oauth_server_not_found(self) -> None:
        """Test that TokenRefreshError is raised when OAuth server doesn't exist."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {"oauthAuthorizationServer": None}

        store = DataHubConnectionCredentialStore(graph=mock_graph)

        with pytest.raises(TokenRefreshError, match="OAuth server not found"):
            store._refresh_oauth_token(
                "refresh-token", "urn:li:oauthServer:nonexistent"
            )

    def test_raises_when_missing_token_url(self) -> None:
        """Test that TokenRefreshError is raised when tokenUrl is missing."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "oauthAuthorizationServer": {
                "urn": "urn:li:oauthServer:test",
                "properties": {
                    "tokenUrl": None,
                    "clientId": "client-id",
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)

        with pytest.raises(TokenRefreshError, match="Invalid OAuth server config"):
            store._refresh_oauth_token("refresh-token", "urn:li:oauthServer:test")

    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_raises_on_http_error(self, mock_httpx_client: MagicMock) -> None:
        """Test that TokenRefreshError is raised on HTTP error."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "oauthAuthorizationServer": {
                "urn": "urn:li:oauthServer:test",
                "properties": {
                    "tokenUrl": "https://oauth.example.com/token",
                    "clientId": "client-id",
                },
            }
        }

        # Mock HTTP error response
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Invalid refresh token"
        mock_client_instance = MagicMock()
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=None)
        mock_client_instance.post.return_value = mock_response
        mock_httpx_client.return_value = mock_client_instance

        store = DataHubConnectionCredentialStore(graph=mock_graph)

        with pytest.raises(TokenRefreshError, match="Token refresh failed with status"):
            store._refresh_oauth_token(
                "invalid-refresh-token", "urn:li:oauthServer:test"
            )

    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_expired_refresh_token_invalid_grant(
        self, mock_httpx_client: MagicMock
    ) -> None:
        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = {
            "oauthAuthorizationServer": {
                "urn": "urn:li:oauthServer:test",
                "properties": {
                    "tokenUrl": "https://oauth.example.com/token",
                    "clientId": "client-id",
                },
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = (
            '{"error":"invalid_grant","error_description":"Refresh token expired"}'
        )
        mock_client_instance = MagicMock()
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=None)
        mock_client_instance.post.return_value = mock_response
        mock_httpx_client.return_value = mock_client_instance

        store = DataHubConnectionCredentialStore(graph=mock_graph)

        with pytest.raises(TokenRefreshError, match="status 400"):
            store._refresh_oauth_token("stale-refresh-token", "urn:li:oauthServer:test")


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for Token Refresh -- Rotation
# ═══════════════════════════════════════════════════════════════════════════════


def _make_oauth_graphql_side_effect(past_time: float):
    """Build a graphql side_effect that returns expired credentials and a valid OAuth server."""

    def side_effect(query: str, variables=None):
        if "GetConnection" in query or "connection(" in query:
            return {
                "connection": {
                    "urn": "urn:li:dataHubConnection:test",
                    "details": {
                        "type": "JSON",
                        "json": {
                            "blob": json.dumps(
                                {
                                    "type": "oauth",
                                    "access_token": "expired-token",
                                    "refresh_token": "original-refresh-token",
                                    "expires_at": past_time,
                                }
                            )
                        },
                    },
                }
            }
        elif "oauthAuthorizationServer" in query:
            return {
                "oauthAuthorizationServer": {
                    "urn": "urn:li:oauthServer:test",
                    "properties": {
                        "tokenUrl": "https://oauth.example.com/token",
                        "clientId": "client-id",
                        "clientSecretUrn": None,
                    },
                }
            }
        elif "upsertConnection" in query:
            return {"upsertConnection": {"urn": "urn:li:dataHubConnection:test"}}
        return {}

    return side_effect


def _make_httpx_mock(token_response: dict) -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = token_response
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=None)
    mock_client.post.return_value = mock_response
    return mock_client


class TestTokenRefreshRotation:
    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_provider_returns_new_refresh_token(
        self, mock_httpx_client: MagicMock
    ) -> None:
        mock_graph = MagicMock()
        past_time = time.time() - 3600
        mock_graph.execute_graphql.side_effect = _make_oauth_graphql_side_effect(
            past_time
        )
        mock_httpx_client.return_value = _make_httpx_mock(
            {
                "access_token": "new-at",
                "refresh_token": "rotated-rt",
                "expires_in": 3600,
            }
        )

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        new_tokens = store._refresh_oauth_token(
            "original-refresh-token", "urn:li:oauthServer:test"
        )

        assert new_tokens.access_token == "new-at"
        assert new_tokens.refresh_token == "rotated-rt"

    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_provider_omits_refresh_token_preserves_original(
        self, mock_httpx_client: MagicMock
    ) -> None:
        mock_graph = MagicMock()
        past_time = time.time() - 3600
        mock_graph.execute_graphql.side_effect = _make_oauth_graphql_side_effect(
            past_time
        )
        mock_httpx_client.return_value = _make_httpx_mock(
            {
                "access_token": "new-at",
                "expires_in": 3600,
            }
        )

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        new_tokens = store._refresh_oauth_token(
            "original-refresh-token", "urn:li:oauthServer:test"
        )

        assert new_tokens.access_token == "new-at"
        assert new_tokens.refresh_token == "original-refresh-token"


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for Token Refresh -- 5-min Buffer Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestTokenRefreshBufferEdgeCases:
    def test_buffer_constant_is_300(self) -> None:
        from datahub_integrations.oauth.credential_store import (
            TOKEN_REFRESH_BUFFER_SECONDS,
        )

        assert TOKEN_REFRESH_BUFFER_SECONDS == 300

    def test_token_expiring_at_boundary_triggers_refresh(self) -> None:
        now = time.time()
        tokens = OAuthTokens(
            access_token="at",
            refresh_token="rt",
            expires_at=now + 300,
        )
        # time.time() >= (now + 300) - 300 => time.time() >= now => True
        assert tokens.is_expired(buffer_seconds=300) is True

    def test_token_just_outside_buffer_does_not_trigger_refresh(self) -> None:
        now = time.time()
        tokens = OAuthTokens(
            access_token="at",
            refresh_token="rt",
            # +301 means: time.time() >= (now + 301) - 300 => time.time() >= now + 1 => False
            expires_at=now + 301,
        )
        assert tokens.is_expired(buffer_seconds=300) is False

    def test_token_just_inside_buffer_triggers_refresh(self) -> None:
        now = time.time()
        tokens = OAuthTokens(
            access_token="at",
            refresh_token="rt",
            # +299 means: time.time() >= (now + 299) - 300 => time.time() >= now - 1 => True
            expires_at=now + 299,
        )
        assert tokens.is_expired(buffer_seconds=300) is True

    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_get_access_token_skips_refresh_when_outside_buffer(
        self, mock_httpx_client: MagicMock
    ) -> None:
        mock_graph = MagicMock()
        future_time = time.time() + 301
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:dataHubConnection:test",
                "details": {
                    "type": "JSON",
                    "json": {
                        "blob": json.dumps(
                            {
                                "type": "oauth",
                                "access_token": "still-valid",
                                "refresh_token": "rt",
                                "expires_at": future_time,
                            }
                        )
                    },
                },
            }
        }

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_access_token(
            "urn:li:corpuser:admin",
            "urn:li:service:plugin",
            "urn:li:oauthServer:test",
        )

        assert result == "still-valid"
        mock_httpx_client.assert_not_called()

    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_get_access_token_refreshes_when_inside_buffer(
        self, mock_httpx_client: MagicMock
    ) -> None:
        mock_graph = MagicMock()
        near_time = time.time() + 299
        mock_graph.execute_graphql.side_effect = _make_oauth_graphql_side_effect(
            near_time
        )
        mock_httpx_client.return_value = _make_httpx_mock(
            {
                "access_token": "refreshed-at",
                "refresh_token": "new-rt",
                "expires_in": 3600,
            }
        )

        store = DataHubConnectionCredentialStore(graph=mock_graph)
        result = store.get_access_token(
            "urn:li:corpuser:admin",
            "urn:li:service:plugin",
            "urn:li:oauthServer:test",
        )

        assert result == "refreshed-at"
        mock_httpx_client.return_value.post.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for on_refresh Callback
# ═══════════════════════════════════════════════════════════════════════════════


class TestOnRefreshCallback:
    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_callback_on_successful_refresh(self, mock_httpx_client: MagicMock) -> None:
        mock_graph = MagicMock()
        past_time = time.time() - 3600
        mock_graph.execute_graphql.side_effect = _make_oauth_graphql_side_effect(
            past_time
        )
        mock_httpx_client.return_value = _make_httpx_mock(
            {"access_token": "new-at", "expires_in": 3600}
        )

        callback = MagicMock()
        store = DataHubConnectionCredentialStore(graph=mock_graph)
        store.get_access_token(
            "urn:li:corpuser:admin",
            "urn:li:service:plugin",
            "urn:li:oauthServer:test",
            on_refresh=callback,
        )

        callback.assert_called_once()
        duration, success = callback.call_args[0]
        assert isinstance(duration, float)
        assert duration >= 0
        assert success is True

    @patch("datahub_integrations.oauth.credential_store.httpx.Client")
    def test_callback_on_failed_refresh(self, mock_httpx_client: MagicMock) -> None:
        mock_graph = MagicMock()
        past_time = time.time() - 3600
        mock_graph.execute_graphql.side_effect = _make_oauth_graphql_side_effect(
            past_time
        )

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=None)
        mock_client.post.return_value = mock_response
        mock_httpx_client.return_value = mock_client

        callback = MagicMock()
        store = DataHubConnectionCredentialStore(graph=mock_graph)

        with pytest.raises(TokenRefreshError):
            store.get_access_token(
                "urn:li:corpuser:admin",
                "urn:li:service:plugin",
                "urn:li:oauthServer:test",
                on_refresh=callback,
            )

        callback.assert_called_once()
        duration, success = callback.call_args[0]
        assert isinstance(duration, float)
        assert success is False
