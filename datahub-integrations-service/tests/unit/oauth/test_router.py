"""Unit tests for OAuth router functionality."""

import base64
import time
from typing import Union
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.responses import HTMLResponse

from datahub_integrations.oauth.router import (
    _resolve_secret_value,
    build_authorization_url,
    build_oauth_callback_url,
    get_oauth_server_config,
    get_plugin_config,
    get_user_urn_from_token,
)


class TestBuildAuthorizationUrl:
    """Test authorization URL construction."""

    def test_basic_authorization_url(self) -> None:
        """Test building a basic authorization URL with required params."""
        server_config = {
            "clientId": "test-client-id",
            "authorizationUrl": "https://provider.com/oauth/authorize",
            "scopes": ["read", "write"],
        }

        url = build_authorization_url(
            server_config=server_config,
            redirect_uri="https://example.com/callback",
            code_challenge="test-challenge",
        )

        assert "https://provider.com/oauth/authorize?" in url
        assert "client_id=test-client-id" in url
        assert "redirect_uri=https%3A%2F%2Fexample.com%2Fcallback" in url
        assert "code_challenge=test-challenge" in url
        assert "code_challenge_method=S256" in url
        assert "response_type=code" in url
        assert "scope=read+write" in url or "scope=write+read" in url

    def test_authorization_url_without_scopes(self) -> None:
        """Test building authorization URL when no scopes are configured."""
        server_config = {
            "clientId": "test-client-id",
            "authorizationUrl": "https://provider.com/oauth/authorize",
        }

        url = build_authorization_url(
            server_config=server_config,
            redirect_uri="https://example.com/callback",
            code_challenge="test-challenge",
        )

        assert "scope=" not in url

    def test_authorization_url_with_additional_scopes(self) -> None:
        """Test building authorization URL with additional scopes merged."""
        server_config = {
            "clientId": "test-client-id",
            "authorizationUrl": "https://provider.com/oauth/authorize",
            "scopes": ["read"],
        }

        url = build_authorization_url(
            server_config=server_config,
            redirect_uri="https://example.com/callback",
            code_challenge="test-challenge",
            additional_scopes=["write", "admin"],
        )

        # Should contain all scopes (deduplicated)
        assert "read" in url
        assert "write" in url
        assert "admin" in url


class TestBuildOAuthCallbackUrl:
    """Test OAuth callback URL construction."""

    @patch(
        "datahub_integrations.oauth.router.DATAHUB_FRONTEND_URL",
        "https://datahub.example.com",
    )
    def test_callback_url_with_frontend_url(self) -> None:
        """Test callback URL uses DATAHUB_FRONTEND_URL."""
        url = build_oauth_callback_url()

        assert url == "https://datahub.example.com/integrations/oauth/callback"

    @patch(
        "datahub_integrations.oauth.router.DATAHUB_FRONTEND_URL",
        "https://datahub.example.com",
    )
    def test_callback_url_ignores_plugin_id(self) -> None:
        """Test callback URL is the same regardless of plugin_id (single callback pattern)."""
        url_with_plugin = build_oauth_callback_url(plugin_id="urn:li:service:test")
        url_without_plugin = build_oauth_callback_url()

        assert url_with_plugin == url_without_plugin


class TestGetUserUrnFromToken:
    """Test JWT token parsing for user URN extraction."""

    def test_extracts_full_urn_from_sub(self) -> None:
        """Test extracting user URN when sub already contains full URN."""
        import jwt

        # Create a JWT with full URN in sub claim (unsigned for testing)
        token = jwt.encode(
            {"sub": "urn:li:corpuser:testuser"}, "secret", algorithm="HS256"
        )

        result = get_user_urn_from_token(token)

        assert result == "urn:li:corpuser:testuser"

    def test_converts_username_to_full_urn(self) -> None:
        """Test converting plain username to full URN format."""
        import jwt

        # Create a JWT with plain username in sub claim
        token = jwt.encode({"sub": "admin"}, "secret", algorithm="HS256")

        result = get_user_urn_from_token(token)

        assert result == "urn:li:corpuser:admin"

    def test_raises_on_missing_sub_claim(self) -> None:
        """Test error when token has no sub claim."""
        import jwt
        from fastapi import HTTPException

        # Create a JWT without sub claim
        token = jwt.encode({"other": "claim"}, "secret", algorithm="HS256")

        with pytest.raises(HTTPException) as exc_info:
            get_user_urn_from_token(token)

        assert exc_info.value.status_code == 401


class TestTokenAuthMethodHandling:
    """Test token exchange with different authentication methods."""

    @pytest.mark.asyncio
    async def test_basic_auth_method_sends_header(self) -> None:
        """Test that BASIC auth method sends credentials in Authorization header."""
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
            "tokenAuthMethod": "BASIC",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "test-access-token",
                "token_type": "bearer",
            }
            mock_client.post.return_value = mock_response

            await exchange_code_for_tokens(
                server_config=server_config,
                code="auth-code",
                redirect_uri="https://example.com/callback",
                code_verifier="test-verifier",
            )

            # Verify the call was made with Basic auth header
            call_kwargs = mock_client.post.call_args
            headers = call_kwargs.kwargs.get("headers", {})

            expected_credentials = base64.b64encode(b"test-client:test-secret").decode()
            assert headers.get("Authorization") == f"Basic {expected_credentials}"

    @pytest.mark.asyncio
    async def test_post_body_auth_method_sends_in_data(self) -> None:
        """Test that POST_BODY auth method sends credentials in request body."""
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
            "tokenAuthMethod": "POST_BODY",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "test-access-token",
                "token_type": "bearer",
            }
            mock_client.post.return_value = mock_response

            await exchange_code_for_tokens(
                server_config=server_config,
                code="auth-code",
                redirect_uri="https://example.com/callback",
                code_verifier="test-verifier",
            )

            # Verify credentials are in the data, not header
            call_kwargs = mock_client.post.call_args
            data = call_kwargs.kwargs.get("data", {})
            headers = call_kwargs.kwargs.get("headers", {})

            assert data.get("client_id") == "test-client"
            assert data.get("client_secret") == "test-secret"
            assert "Authorization" not in headers or "Basic" not in headers.get(
                "Authorization", ""
            )

    @pytest.mark.asyncio
    async def test_default_auth_method_is_post_body(self) -> None:
        """Test that default auth method (when not specified) is POST_BODY."""
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
            # No tokenAuthMethod specified
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "test-access-token",
                "token_type": "bearer",
            }
            mock_client.post.return_value = mock_response

            await exchange_code_for_tokens(
                server_config=server_config,
                code="auth-code",
                redirect_uri="https://example.com/callback",
                code_verifier="test-verifier",
            )

            # Verify credentials are in the data (POST_BODY default)
            call_kwargs = mock_client.post.call_args
            data = call_kwargs.kwargs.get("data", {})

            assert data.get("client_id") == "test-client"
            assert data.get("client_secret") == "test-secret"

    @pytest.mark.asyncio
    async def test_custom_auth_scheme(self) -> None:
        """Test custom auth scheme (e.g., Token for dbt Cloud)."""
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "my-api-token",
            "tokenAuthMethod": "CUSTOM",  # Must explicitly set CUSTOM
            "authScheme": "Token",  # Custom scheme like dbt Cloud
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "test-access-token",
                "token_type": "bearer",
            }
            mock_client.post.return_value = mock_response

            await exchange_code_for_tokens(
                server_config=server_config,
                code="auth-code",
                redirect_uri="https://example.com/callback",
                code_verifier="test-verifier",
            )

            # Verify custom auth scheme is used
            call_kwargs = mock_client.post.call_args
            headers = call_kwargs.kwargs.get("headers", {})

            assert headers.get("Authorization") == "Token my-api-token"

    @pytest.mark.asyncio
    async def test_auth_scheme_without_custom_method_uses_post_body(self) -> None:
        """Test that authScheme alone doesn't trigger custom auth - must use tokenAuthMethod=CUSTOM.

        This tests the fix for the bug where authScheme (meant for API calls) was
        incorrectly used for token endpoint auth. For example, GitHub OAuth uses
        authScheme="Bearer" for API calls but tokenAuthMethod="POST_BODY" for token endpoint.
        """
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
            "tokenAuthMethod": "POST_BODY",  # Explicit POST_BODY
            "authScheme": "Bearer",  # This is for API calls, not token endpoint
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "test-access-token",
                "token_type": "bearer",
            }
            mock_client.post.return_value = mock_response

            await exchange_code_for_tokens(
                server_config=server_config,
                code="auth-code",
                redirect_uri="https://example.com/callback",
                code_verifier="test-verifier",
            )

            # Verify credentials are in POST body, NOT using authScheme in header
            call_kwargs = mock_client.post.call_args
            data = call_kwargs.kwargs.get("data", {})
            headers = call_kwargs.kwargs.get("headers", {})

            assert data.get("client_id") == "test-client"
            assert data.get("client_secret") == "test-secret"
            # Should NOT have Bearer auth header (that's the bug we fixed)
            assert headers.get("Authorization") != "Bearer test-secret"

    @pytest.mark.asyncio
    async def test_none_auth_method_no_secret(self) -> None:
        """Test that NONE auth method only sends client_id, no secret."""
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",  # Even if provided, should not be sent
            "tokenAuthMethod": "NONE",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "test-access-token",
                "token_type": "bearer",
            }
            mock_client.post.return_value = mock_response

            await exchange_code_for_tokens(
                server_config=server_config,
                code="auth-code",
                redirect_uri="https://example.com/callback",
                code_verifier="test-verifier",
            )

            # Verify only client_id is sent, no secret
            call_kwargs = mock_client.post.call_args
            data = call_kwargs.kwargs.get("data", {})
            headers = call_kwargs.kwargs.get("headers", {})

            assert data.get("client_id") == "test-client"
            assert "client_secret" not in data
            assert "Authorization" not in headers or "Basic" not in headers.get(
                "Authorization", ""
            )


class TestTokenExchangeErrorHandling:
    """Test error handling in token exchange."""

    @pytest.mark.asyncio
    async def test_token_exchange_handles_http_error_response(self) -> None:
        """Test that token exchange raises HTTPException on HTTP error responses."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.text = '{"error": "invalid_grant"}'
            mock_response.headers = {}
            mock_client.post.return_value = mock_response

            # Should raise HTTPException on HTTP error
            with pytest.raises(HTTPException) as exc_info:
                await exchange_code_for_tokens(
                    server_config=server_config,
                    code="expired-code",
                    redirect_uri="https://example.com/callback",
                    code_verifier="test-verifier",
                )

            assert exc_info.value.status_code == 400
            assert "Failed to exchange" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_token_exchange_handles_oauth_error_in_body(self) -> None:
        """Test that token exchange handles OAuth error in 200 response body."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            # Some OAuth providers return errors with 200 status
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "error": "invalid_grant",
                "error_description": "The authorization code has expired",
            }
            mock_client.post.return_value = mock_response

            # Should raise HTTPException when error is in response body
            with pytest.raises(HTTPException) as exc_info:
                await exchange_code_for_tokens(
                    server_config=server_config,
                    code="expired-code",
                    redirect_uri="https://example.com/callback",
                    code_verifier="test-verifier",
                )

            assert exc_info.value.status_code == 400
            assert "invalid_grant" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_token_exchange_handles_network_error(self) -> None:
        """Test that token exchange raises HTTPException on network errors."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            # Simulate network error
            mock_client.post.side_effect = ConnectionError("Connection refused")

            # Should raise HTTPException on network error
            with pytest.raises(HTTPException) as exc_info:
                await exchange_code_for_tokens(
                    server_config=server_config,
                    code="auth-code",
                    redirect_uri="https://example.com/callback",
                    code_verifier="test-verifier",
                )

            assert exc_info.value.status_code == 500
            assert "Token exchange failed" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_token_exchange_returns_oauth_tokens(self) -> None:
        """Test that token exchange returns OAuthTokens object."""
        from datahub_integrations.oauth.credential_store import OAuthTokens
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "access-token-123",
                "refresh_token": "refresh-token-456",
                "expires_in": 3600,
                "token_type": "Bearer",
                "scope": "read write",
            }
            mock_client.post.return_value = mock_response

            result = await exchange_code_for_tokens(
                server_config=server_config,
                code="auth-code",
                redirect_uri="https://example.com/callback",
                code_verifier="test-verifier",
            )

            assert result is not None
            assert isinstance(result, OAuthTokens)
            assert result.access_token == "access-token-123"
            assert result.refresh_token == "refresh-token-456"
            assert result.token_type == "Bearer"
            assert result.scope == "read write"

    @pytest.mark.asyncio
    async def test_token_exchange_without_client_secret(self) -> None:
        """Test token exchange for public clients (no client secret)."""
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "public-client",
            # No clientSecret - public client
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "test-access-token",
                "token_type": "bearer",
            }
            mock_client.post.return_value = mock_response

            result = await exchange_code_for_tokens(
                server_config=server_config,
                code="auth-code",
                redirect_uri="https://example.com/callback",
                code_verifier="test-verifier",
            )

            assert result is not None
            # Verify client_id is in data but no client_secret
            call_kwargs = mock_client.post.call_args
            data = call_kwargs.kwargs.get("data", {})
            assert data.get("client_id") == "public-client"
            assert "client_secret" not in data or data.get("client_secret") is None


class TestGetAuthToken:
    """Test auth token extraction from requests."""

    def test_get_auth_token_extracts_bearer_token(self) -> None:
        """Test extracting bearer token from Authorization header."""
        from datahub_integrations.oauth.router import get_auth_token

        token = get_auth_token("Bearer my-jwt-token")
        assert token == "my-jwt-token"

    def test_get_auth_token_raises_on_missing_header(self) -> None:
        """Test error when Authorization header is missing."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import get_auth_token

        with pytest.raises(HTTPException) as exc_info:
            get_auth_token(None)

        assert exc_info.value.status_code == 401
        assert "Missing Authorization header" in str(exc_info.value.detail)

    def test_get_auth_token_raises_on_invalid_format(self) -> None:
        """Test error when Authorization header has wrong format."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import get_auth_token

        with pytest.raises(HTTPException) as exc_info:
            get_auth_token("Basic abc123")

        assert exc_info.value.status_code == 401
        assert "expected 'Bearer" in str(exc_info.value.detail)

    def test_get_auth_token_raises_on_empty_token(self) -> None:
        """Test error when token is empty after Bearer prefix."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import get_auth_token

        with pytest.raises(HTTPException) as exc_info:
            get_auth_token("Bearer ")

        assert exc_info.value.status_code == 401


class TestBuildAuthorizationUrlEdgeCases:
    """Test edge cases for authorization URL construction."""

    def test_authorization_url_preserves_all_params(self) -> None:
        """Test that all required OAuth params are included."""
        server_config = {
            "clientId": "test-client-id",
            "authorizationUrl": "https://provider.com/oauth/authorize",
            "scopes": ["openid", "profile"],
        }

        url = build_authorization_url(
            server_config=server_config,
            redirect_uri="https://example.com/callback",
            code_challenge="test-challenge",
        )

        # Verify all standard OAuth params are present
        assert "response_type=code" in url
        assert "client_id=test-client-id" in url
        assert "code_challenge=test-challenge" in url
        assert "code_challenge_method=S256" in url
        assert "scope=" in url

    def test_authorization_url_handles_special_characters_in_redirect_uri(self) -> None:
        """Test URL encoding of special characters in redirect URI."""
        server_config = {
            "clientId": "test-client-id",
            "authorizationUrl": "https://provider.com/oauth/authorize",
        }

        url = build_authorization_url(
            server_config=server_config,
            redirect_uri="https://example.com/callback?param=value&other=test",
            code_challenge="test-challenge",
        )

        # The redirect_uri should be URL-encoded
        assert "redirect_uri=" in url
        # The ? and & should be encoded
        assert "%3F" in url or "%26" in url or "callback%3F" in url


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for _resolve_secret_value
# ═══════════════════════════════════════════════════════════════════════════════


class TestResolveSecretValue:
    """Test secret value resolution from DataHub."""

    @patch("datahub_integrations.oauth.router.graph")
    def test_resolves_secret_successfully(self, mock_graph: MagicMock) -> None:
        """Test successful secret resolution."""
        mock_graph.execute_graphql.return_value = {
            "getSecretValues": [{"name": "my-secret", "value": "secret-value-123"}]
        }

        result = _resolve_secret_value("urn:li:dataHubSecret:my-secret")

        assert result == "secret-value-123"
        mock_graph.execute_graphql.assert_called_once()

    @patch("datahub_integrations.oauth.router.graph")
    def test_returns_none_for_invalid_urn_format(self, mock_graph: MagicMock) -> None:
        """Test returns None for invalid secret URN format."""
        result = _resolve_secret_value("invalid-urn-format")

        assert result is None
        mock_graph.execute_graphql.assert_not_called()

    @patch("datahub_integrations.oauth.router.graph")
    def test_returns_none_when_secret_not_found(self, mock_graph: MagicMock) -> None:
        """Test returns None when secret is not found."""
        mock_graph.execute_graphql.return_value = {"getSecretValues": []}

        result = _resolve_secret_value("urn:li:dataHubSecret:nonexistent")

        assert result is None

    @patch("datahub_integrations.oauth.router.graph")
    def test_returns_none_on_graphql_error(self, mock_graph: MagicMock) -> None:
        """Test returns None when GraphQL query fails."""
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        result = _resolve_secret_value("urn:li:dataHubSecret:my-secret")

        assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for get_plugin_config
# ═══════════════════════════════════════════════════════════════════════════════


class TestGetPluginConfig:
    """Test plugin configuration retrieval."""

    @patch("datahub_integrations.oauth.router.graph")
    def test_returns_plugin_config_when_found(self, mock_graph: MagicMock) -> None:
        """Test successful plugin config retrieval."""
        mock_graph.execute_graphql.return_value = {
            "globalSettings": {
                "aiPlugins": [
                    {
                        "id": "urn:li:service:github",
                        "authType": "USER_OAUTH",
                        "oauthConfig": {"serverUrn": "urn:li:oauthServer:github"},
                    },
                    {
                        "id": "urn:li:service:other",
                        "authType": "USER_API_KEY",
                    },
                ]
            }
        }

        result = get_plugin_config("urn:li:service:github")

        assert result["id"] == "urn:li:service:github"
        assert result["authType"] == "USER_OAUTH"

    @patch("datahub_integrations.oauth.router.graph")
    def test_raises_404_when_plugin_not_found(self, mock_graph: MagicMock) -> None:
        """Test raises HTTPException 404 when plugin is not found."""
        mock_graph.execute_graphql.return_value = {
            "globalSettings": {"aiPlugins": [{"id": "urn:li:service:other"}]}
        }

        with pytest.raises(HTTPException) as exc_info:
            get_plugin_config("urn:li:service:nonexistent")

        assert exc_info.value.status_code == 404
        assert "Plugin not found" in str(exc_info.value.detail)

    @patch("datahub_integrations.oauth.router.graph")
    def test_raises_404_when_no_plugins_configured(self, mock_graph: MagicMock) -> None:
        """Test raises HTTPException 404 when no plugins are configured."""
        mock_graph.execute_graphql.return_value = {
            "globalSettings": {"aiPlugins": None}
        }

        with pytest.raises(HTTPException) as exc_info:
            get_plugin_config("urn:li:service:github")

        assert exc_info.value.status_code == 404

    @patch("datahub_integrations.oauth.router.graph")
    def test_raises_500_on_graphql_error(self, mock_graph: MagicMock) -> None:
        """Test raises HTTPException 500 on GraphQL error."""
        mock_graph.execute_graphql.side_effect = Exception("Connection failed")

        with pytest.raises(HTTPException) as exc_info:
            get_plugin_config("urn:li:service:github")

        assert exc_info.value.status_code == 500
        assert "Failed to retrieve plugin configuration" in str(exc_info.value.detail)


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for get_oauth_server_config
# ═══════════════════════════════════════════════════════════════════════════════


class TestGetOAuthServerConfig:
    """Test OAuth server configuration retrieval."""

    @patch("datahub_integrations.oauth.router._resolve_secret_value")
    @patch("datahub_integrations.oauth.router.graph")
    def test_returns_server_config_with_resolved_secret(
        self, mock_graph: MagicMock, mock_resolve: MagicMock
    ) -> None:
        """Test successful server config retrieval with secret resolution."""
        mock_graph.execute_graphql.return_value = {
            "oauthAuthorizationServer": {
                "urn": "urn:li:oauthServer:github",
                "properties": {
                    "displayName": "GitHub",
                    "authorizationUrl": "https://github.com/login/oauth/authorize",
                    "tokenUrl": "https://github.com/login/oauth/access_token",
                    "clientId": "client-123",
                    "clientSecretUrn": "urn:li:dataHubSecret:github-secret",
                    "scopes": ["repo", "read:user"],
                },
            }
        }
        mock_resolve.return_value = "resolved-secret-value"

        result = get_oauth_server_config("urn:li:oauthServer:github")

        assert result["displayName"] == "GitHub"
        assert result["clientId"] == "client-123"
        assert result["clientSecret"] == "resolved-secret-value"
        mock_resolve.assert_called_once_with("urn:li:dataHubSecret:github-secret")

    @patch("datahub_integrations.oauth.router.graph")
    def test_returns_config_without_secret_when_no_urn(
        self, mock_graph: MagicMock
    ) -> None:
        """Test server config retrieval when no clientSecretUrn is provided."""
        mock_graph.execute_graphql.return_value = {
            "oauthAuthorizationServer": {
                "urn": "urn:li:oauthServer:public",
                "properties": {
                    "displayName": "Public OAuth",
                    "authorizationUrl": "https://provider.com/oauth/authorize",
                    "tokenUrl": "https://provider.com/oauth/token",
                    "clientId": "public-client",
                    # No clientSecretUrn
                },
            }
        }

        result = get_oauth_server_config("urn:li:oauthServer:public")

        assert result["clientId"] == "public-client"
        assert "clientSecret" not in result

    @patch("datahub_integrations.oauth.router.graph")
    def test_raises_404_when_server_not_found(self, mock_graph: MagicMock) -> None:
        """Test raises HTTPException 404 when OAuth server is not found."""
        mock_graph.execute_graphql.return_value = {"oauthAuthorizationServer": None}

        with pytest.raises(HTTPException) as exc_info:
            get_oauth_server_config("urn:li:oauthServer:nonexistent")

        assert exc_info.value.status_code == 404
        assert "OAuth server not found" in str(exc_info.value.detail)

    @patch("datahub_integrations.oauth.router.graph")
    def test_raises_500_on_graphql_error(self, mock_graph: MagicMock) -> None:
        """Test raises HTTPException 500 on GraphQL error."""
        mock_graph.execute_graphql.side_effect = Exception("Connection failed")

        with pytest.raises(HTTPException) as exc_info:
            get_oauth_server_config("urn:li:oauthServer:github")

        assert exc_info.value.status_code == 500
        assert "Failed to retrieve OAuth server configuration" in str(
            exc_info.value.detail
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for exchange_code_for_tokens - additional error cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestTokenExchangeMissingAccessToken:
    """Test token exchange when access_token is missing from response."""

    @pytest.mark.asyncio
    async def test_raises_500_when_access_token_missing(self) -> None:
        """Test raises HTTPException 500 when access_token is missing."""
        from datahub_integrations.oauth.router import exchange_code_for_tokens

        server_config = {
            "tokenUrl": "https://provider.com/oauth/token",
            "clientId": "test-client",
            "clientSecret": "test-secret",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            # Response is 200 but missing access_token
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "token_type": "Bearer",
                # Missing: "access_token"
            }
            mock_client.post.return_value = mock_response

            with pytest.raises(HTTPException) as exc_info:
                await exchange_code_for_tokens(
                    server_config=server_config,
                    code="auth-code",
                    redirect_uri="https://example.com/callback",
                    code_verifier="test-verifier",
                )

            assert exc_info.value.status_code == 500
            assert "missing access_token" in str(exc_info.value.detail)


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for OAuth callback endpoints
# ═══════════════════════════════════════════════════════════════════════════════


def _get_response_body_str(response: HTMLResponse) -> str:
    """Helper to safely decode response body for assertions."""
    body: Union[bytes, memoryview] = response.body
    if isinstance(body, memoryview):
        return bytes(body).decode("utf-8")
    return body.decode("utf-8")


class TestOAuthCallbackUnified:
    """Test handle_oauth_callback_unified endpoint."""

    @pytest.mark.asyncio
    async def test_returns_error_page_on_oauth_error(self) -> None:
        """Test returns error page when OAuth provider returns error."""
        from datahub_integrations.oauth.router import handle_oauth_callback_unified

        mock_state_store = MagicMock()
        mock_credential_store = MagicMock()

        response = await handle_oauth_callback_unified(
            state_store=mock_state_store,
            credential_store=mock_credential_store,
            code="ignored",
            state="ignored",
            error="access_denied",
            error_description="User denied access",
        )

        body_str = _get_response_body_str(response)
        assert "Connection Failed" in body_str
        assert "User denied access" in body_str
        # State should not be consumed on OAuth error
        mock_state_store.get_and_consume_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_error_page_on_invalid_state(self) -> None:
        """Test returns error page when state is invalid or expired."""
        from datahub_integrations.oauth.router import handle_oauth_callback_unified

        mock_state_store = MagicMock()
        mock_state_store.get_and_consume_state.return_value = None
        mock_credential_store = MagicMock()

        response = await handle_oauth_callback_unified(
            state_store=mock_state_store,
            credential_store=mock_credential_store,
            code="auth-code",
            state="invalid-or-expired-state",
            error=None,
            error_description=None,
        )

        body_str = _get_response_body_str(response)
        assert "Connection Failed" in body_str
        assert "Invalid or expired OAuth state" in body_str

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router.exchange_code_for_tokens")
    @patch("datahub_integrations.oauth.router.get_oauth_server_config")
    @patch("datahub_integrations.oauth.router.get_plugin_config")
    async def test_returns_error_page_on_token_exchange_failure(
        self,
        mock_get_plugin: MagicMock,
        mock_get_server: MagicMock,
        mock_exchange: MagicMock,
    ) -> None:
        """Test returns error page when token exchange fails."""
        from datahub_integrations.oauth.router import handle_oauth_callback_unified
        from datahub_integrations.oauth.state_store import OAuthState

        mock_state_store = MagicMock()
        mock_state_store.get_and_consume_state.return_value = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:github",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=time.time(),
            auth_token="jwt-token",
        )
        mock_credential_store = MagicMock()

        mock_get_plugin.return_value = {
            "oauthConfig": {"serverUrn": "urn:li:oauthServer:github"}
        }
        mock_get_server.return_value = {"tokenUrl": "https://github.com/oauth/token"}
        mock_exchange.side_effect = HTTPException(
            status_code=400, detail="Token exchange failed"
        )

        response = await handle_oauth_callback_unified(
            state_store=mock_state_store,
            credential_store=mock_credential_store,
            code="auth-code",
            state="valid-state",
            error=None,
            error_description=None,
        )

        body_str = _get_response_body_str(response)
        assert "Connection Failed" in body_str
        assert "Token exchange failed" in body_str

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._update_user_plugin_settings")
    @patch("datahub_integrations.oauth.router.exchange_code_for_tokens")
    @patch("datahub_integrations.oauth.router.get_oauth_server_config")
    @patch("datahub_integrations.oauth.router.get_plugin_config")
    async def test_returns_success_page_on_successful_flow(
        self,
        mock_get_plugin: MagicMock,
        mock_get_server: MagicMock,
        mock_exchange: MagicMock,
        mock_update_settings: MagicMock,
    ) -> None:
        """Test returns success page on successful OAuth flow."""
        from datahub_integrations.oauth.credential_store import OAuthTokens
        from datahub_integrations.oauth.router import handle_oauth_callback_unified
        from datahub_integrations.oauth.state_store import OAuthState

        mock_state_store = MagicMock()
        mock_state_store.get_and_consume_state.return_value = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:github",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=time.time(),
            auth_token="jwt-token",
        )
        mock_credential_store = MagicMock()
        mock_credential_store.save_oauth_tokens.return_value = (
            "urn:li:dataHubConnection:test"
        )

        mock_get_plugin.return_value = {
            "oauthConfig": {"serverUrn": "urn:li:oauthServer:github"}
        }
        mock_get_server.return_value = {"tokenUrl": "https://github.com/oauth/token"}
        mock_exchange.return_value = OAuthTokens(
            access_token="access-token-123",
            refresh_token="refresh-token-456",
            expires_at=None,
        )
        mock_update_settings.return_value = None

        response = await handle_oauth_callback_unified(
            state_store=mock_state_store,
            credential_store=mock_credential_store,
            code="auth-code",
            state="valid-state",
            error=None,
            error_description=None,
        )

        body_str = _get_response_body_str(response)
        assert "Connected Successfully" in body_str
        mock_credential_store.save_oauth_tokens.assert_called_once()


class TestOAuthCallbackLegacy:
    """Test handle_oauth_callback_legacy endpoint."""

    @pytest.mark.asyncio
    async def test_returns_error_page_on_oauth_error(self) -> None:
        """Test returns error page when OAuth provider returns error."""
        from datahub_integrations.oauth.router import handle_oauth_callback_legacy

        mock_state_store = MagicMock()
        mock_credential_store = MagicMock()

        response = await handle_oauth_callback_legacy(
            plugin_id="urn:li:service:github",
            state_store=mock_state_store,
            credential_store=mock_credential_store,
            code="ignored",
            state="ignored",
            error="access_denied",
            error_description="User denied access",
        )

        body_str = _get_response_body_str(response)
        assert "Connection Failed" in body_str
        assert "User denied access" in body_str

    @pytest.mark.asyncio
    async def test_returns_error_page_on_plugin_id_mismatch(self) -> None:
        """Test returns error page when plugin ID doesn't match state."""
        from datahub_integrations.oauth.router import handle_oauth_callback_legacy
        from datahub_integrations.oauth.state_store import OAuthState

        mock_state_store = MagicMock()
        mock_state_store.get_and_consume_state.return_value = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:github",  # Different from URL
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=time.time(),
            auth_token="jwt-token",
        )
        mock_credential_store = MagicMock()

        response = await handle_oauth_callback_legacy(
            plugin_id="urn:li:service:different-plugin",  # Mismatch!
            state_store=mock_state_store,
            credential_store=mock_credential_store,
            code="auth-code",
            state="valid-state",
            error=None,
            error_description=None,
        )

        body_str = _get_response_body_str(response)
        assert "Connection Failed" in body_str
        assert "Plugin ID mismatch" in body_str

    @pytest.mark.asyncio
    async def test_returns_error_page_on_invalid_state(self) -> None:
        """Test returns error page when state is invalid or expired."""
        from datahub_integrations.oauth.router import handle_oauth_callback_legacy

        mock_state_store = MagicMock()
        mock_state_store.get_and_consume_state.return_value = None
        mock_credential_store = MagicMock()

        response = await handle_oauth_callback_legacy(
            plugin_id="urn:li:service:github",
            state_store=mock_state_store,
            credential_store=mock_credential_store,
            code="auth-code",
            state="invalid-state",
            error=None,
            error_description=None,
        )

        body_str = _get_response_body_str(response)
        assert "Connection Failed" in body_str
        assert "Invalid or expired OAuth state" in body_str

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router.get_plugin_config")
    async def test_returns_error_page_on_unexpected_exception(
        self, mock_get_plugin: MagicMock
    ) -> None:
        """Test returns error page on unexpected exceptions."""
        from datahub_integrations.oauth.router import handle_oauth_callback_legacy
        from datahub_integrations.oauth.state_store import OAuthState

        mock_state_store = MagicMock()
        mock_state_store.get_and_consume_state.return_value = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:github",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=time.time(),
            auth_token="jwt-token",
        )
        mock_credential_store = MagicMock()

        # Simulate unexpected error
        mock_get_plugin.side_effect = RuntimeError("Unexpected database error")

        response = await handle_oauth_callback_legacy(
            plugin_id="urn:li:service:github",
            state_store=mock_state_store,
            credential_store=mock_credential_store,
            code="auth-code",
            state="valid-state",
            error=None,
            error_description=None,
        )

        body_str = _get_response_body_str(response)
        assert "Connection Failed" in body_str
        assert "unexpected error" in body_str.lower()
