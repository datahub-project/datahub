"""Unit tests for OAuth router functionality."""

import base64
import time
from typing import Union
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException
from fastapi.responses import HTMLResponse

from datahub_integrations.oauth.router import (
    _resolve_secret_value,
    build_authorization_url,
    build_oauth_callback_url,
    get_auth_token,
    get_oauth_server_config,
    get_plugin_config,
    get_user_urn_from_token,
    validate_token_and_get_user,
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


class TestValidateTokenAndGetUser:
    """Test token validation via GMS GraphQL API.

    The validate_token_and_get_user function validates JWT tokens by making
    a GraphQL call to GMS. This ensures tokens are properly signed and valid,
    preventing forged tokens from being accepted.
    """

    def test_valid_token_returns_user_urn(self) -> None:
        """Test that a valid token returns the user URN from GMS."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"me": {"corpUser": {"urn": "urn:li:corpuser:validuser"}}}
        }

        with patch("httpx.post", return_value=mock_response):
            result = validate_token_and_get_user("valid-token")

        assert result == "urn:li:corpuser:validuser"

    def test_invalid_token_returns_401(self) -> None:
        """Test that an invalid token results in 401 Unauthorized."""
        mock_response = MagicMock()
        mock_response.status_code = 401

        with patch("httpx.post", return_value=mock_response):
            with pytest.raises(HTTPException) as exc_info:
                validate_token_and_get_user("invalid-token")

        assert exc_info.value.status_code == 401
        assert "Invalid or expired token" in exc_info.value.detail

    def test_forbidden_returns_403(self) -> None:
        """Test that a forbidden response results in 403."""
        mock_response = MagicMock()
        mock_response.status_code = 403

        with patch("httpx.post", return_value=mock_response):
            with pytest.raises(HTTPException) as exc_info:
                validate_token_and_get_user("forbidden-token")

        assert exc_info.value.status_code == 403
        assert "Access denied" in exc_info.value.detail

    def test_graphql_error_returns_401(self) -> None:
        """Test that GraphQL errors result in 401."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "errors": [{"message": "Unauthorized"}],
            "data": None,
        }

        with patch("httpx.post", return_value=mock_response):
            with pytest.raises(HTTPException) as exc_info:
                validate_token_and_get_user("bad-token")

        assert exc_info.value.status_code == 401
        assert "Token validation failed" in exc_info.value.detail

    def test_missing_user_urn_returns_401(self) -> None:
        """Test that missing user URN in response results in 401."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"me": {"corpUser": {"urn": None}}}}

        with patch("httpx.post", return_value=mock_response):
            with pytest.raises(HTTPException) as exc_info:
                validate_token_and_get_user("token-with-no-user")

        assert exc_info.value.status_code == 401
        assert "Could not determine user identity" in exc_info.value.detail

    def test_network_error_returns_503(self) -> None:
        """Test that network errors result in 503 Service Unavailable."""
        import httpx

        with patch("httpx.post", side_effect=httpx.RequestError("Connection failed")):
            with pytest.raises(HTTPException) as exc_info:
                validate_token_and_get_user("any-token")

        assert exc_info.value.status_code == 503
        assert "Authentication service unavailable" in exc_info.value.detail

    def test_makes_correct_graphql_request(self) -> None:
        """Test that the correct GraphQL query is sent to GMS."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"me": {"corpUser": {"urn": "urn:li:corpuser:testuser"}}}
        }

        with patch("httpx.post", return_value=mock_response) as mock_post:
            validate_token_and_get_user("test-token")

        # Verify the request was made with correct parameters
        call_args = mock_post.call_args
        assert "Bearer test-token" in str(call_args)
        assert "GetMe" in str(call_args.kwargs["json"]["query"])

    def test_forged_token_rejected(self) -> None:
        """Test that a forged JWT token is rejected by GMS.

        This is the key security test - even if an attacker crafts a JWT
        with arbitrary claims, GMS will reject it because the signature
        is invalid.
        """
        import jwt

        # Create a forged token with a fake signature
        forged_token = jwt.encode(
            {"sub": "admin", "role": "superuser"},
            "wrong-secret",  # Attacker doesn't know the real secret
            algorithm="HS256",
        )

        # GMS would reject this token
        mock_response = MagicMock()
        mock_response.status_code = 401

        with patch("httpx.post", return_value=mock_response):
            with pytest.raises(HTTPException) as exc_info:
                validate_token_and_get_user(forged_token)

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

    def test_get_auth_token_raises_on_missing_header_and_cookie(self) -> None:
        """Test error when both Authorization header and PLAY_SESSION cookie are missing."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import get_auth_token

        with pytest.raises(HTTPException) as exc_info:
            get_auth_token(authorization=None, play_session=None)

        assert exc_info.value.status_code == 401
        assert "Missing Authorization header or PLAY_SESSION cookie" in str(
            exc_info.value.detail
        )

    def test_get_auth_token_raises_on_invalid_format(self) -> None:
        """Test error when Authorization header has wrong format."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import get_auth_token

        with pytest.raises(HTTPException) as exc_info:
            get_auth_token(authorization="Basic abc123", play_session=None)

        assert exc_info.value.status_code == 401
        assert "expected 'Bearer" in str(exc_info.value.detail)

    def test_get_auth_token_raises_on_empty_token(self) -> None:
        """Test error when token is empty after Bearer prefix."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import get_auth_token

        with pytest.raises(HTTPException) as exc_info:
            get_auth_token(authorization="Bearer ", play_session=None)

        assert exc_info.value.status_code == 401

    def test_get_auth_token_extracts_token_from_play_session_cookie(self) -> None:
        """Test extracting token from valid PLAY_SESSION cookie."""
        import base64
        import json

        # Create a valid JWT payload structure matching PLAY_SESSION cookie
        payload_data = {
            "data": {
                "actor": "urn:li:corpuser:admin",
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test.signature",
            },
            "exp": 1769539919,
            "nbf": 1769453519,
            "iat": 1769453519,
        }

        # Encode as JWT (header.payload.signature)
        header = (
            base64.urlsafe_b64encode(
                json.dumps({"alg": "HS256", "typ": "JWT"}).encode()
            )
            .decode()
            .rstrip("=")
        )
        payload = (
            base64.urlsafe_b64encode(json.dumps(payload_data).encode())
            .decode()
            .rstrip("=")
        )
        signature = "fake_signature"

        play_session_cookie = f"{header}.{payload}.{signature}"

        # Should extract token from cookie
        token = get_auth_token(authorization=None, play_session=play_session_cookie)
        assert token == "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test.signature"

    def test_get_auth_token_authorization_header_takes_precedence_over_cookie(
        self,
    ) -> None:
        """Test that Authorization header is used when both header and cookie are present."""
        import base64
        import json

        # Create a valid PLAY_SESSION cookie
        payload_data = {
            "data": {
                "actor": "urn:li:corpuser:admin",
                "token": "cookie_token_should_not_be_used",
            },
            "exp": 1769539919,
        }
        header = (
            base64.urlsafe_b64encode(json.dumps({"alg": "HS256"}).encode())
            .decode()
            .rstrip("=")
        )
        payload = (
            base64.urlsafe_b64encode(json.dumps(payload_data).encode())
            .decode()
            .rstrip("=")
        )
        play_session_cookie = f"{header}.{payload}.signature"

        # Should use Authorization header token, not cookie token
        token = get_auth_token(
            authorization="Bearer header_token_should_be_used",
            play_session=play_session_cookie,
        )
        assert token == "header_token_should_be_used"

    def test_get_auth_token_raises_on_invalid_play_session_format(self) -> None:
        """Test error when PLAY_SESSION cookie is not a valid JWT format."""
        # Not a JWT (missing parts)
        with pytest.raises(HTTPException) as exc_info:
            get_auth_token(authorization=None, play_session="not.a.valid.jwt.format")

        assert exc_info.value.status_code == 401
        assert "Invalid PLAY_SESSION cookie format" in exc_info.value.detail

    def test_get_auth_token_raises_on_play_session_without_token(self) -> None:
        """Test error when PLAY_SESSION cookie doesn't contain token in data field."""
        import base64
        import json

        # Create JWT without token in data field
        payload_data = {
            "data": {
                "actor": "urn:li:corpuser:admin",
                # Missing "token" field
            },
            "exp": 1769539919,
        }
        header = (
            base64.urlsafe_b64encode(json.dumps({"alg": "HS256"}).encode())
            .decode()
            .rstrip("=")
        )
        payload = (
            base64.urlsafe_b64encode(json.dumps(payload_data).encode())
            .decode()
            .rstrip("=")
        )
        play_session_cookie = f"{header}.{payload}.signature"

        with pytest.raises(HTTPException) as exc_info:
            get_auth_token(authorization=None, play_session=play_session_cookie)

        assert exc_info.value.status_code == 401
        assert "No token found in PLAY_SESSION cookie" in exc_info.value.detail

    def test_get_auth_token_raises_on_malformed_play_session_json(self) -> None:
        """Test error when PLAY_SESSION cookie payload is not valid JSON."""
        import base64

        # Create JWT with invalid JSON in payload
        header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode().rstrip("=")
        payload = base64.urlsafe_b64encode(b"not valid json{{{").decode().rstrip("=")
        play_session_cookie = f"{header}.{payload}.signature"

        with pytest.raises(HTTPException) as exc_info:
            get_auth_token(authorization=None, play_session=play_session_cookie)

        assert exc_info.value.status_code == 401
        assert "Invalid PLAY_SESSION cookie payload" in exc_info.value.detail


class TestGetAuthenticatedUser:
    """Test get_authenticated_user function with Authorization header and PLAY_SESSION cookie."""

    def test_get_authenticated_user_with_authorization_header(
        self, monkeypatch
    ) -> None:
        """Test get_authenticated_user extracts token from Authorization header."""
        from unittest.mock import MagicMock

        from datahub_integrations.oauth.router import get_authenticated_user

        # Mock validate_token_and_get_user to avoid actual GMS call
        mock_validate = MagicMock(return_value="urn:li:corpuser:testuser")
        monkeypatch.setattr(
            "datahub_integrations.oauth.router.validate_token_and_get_user",
            mock_validate,
        )

        # Call with Authorization header
        user_urn = get_authenticated_user(
            authorization="Bearer test_token_from_header", play_session=None
        )

        assert user_urn == "urn:li:corpuser:testuser"
        # Verify the token was extracted correctly
        mock_validate.assert_called_once_with("test_token_from_header")

    def test_get_authenticated_user_with_play_session_cookie(self, monkeypatch) -> None:
        """Test get_authenticated_user extracts token from PLAY_SESSION cookie."""
        import base64
        import json
        from unittest.mock import MagicMock

        from datahub_integrations.oauth.router import get_authenticated_user

        # Create a valid PLAY_SESSION cookie
        payload_data = {
            "data": {
                "actor": "urn:li:corpuser:admin",
                "token": "test_token_from_cookie",
            },
            "exp": 1769539919,
            "nbf": 1769453519,
            "iat": 1769453519,
        }
        header = (
            base64.urlsafe_b64encode(json.dumps({"alg": "HS256"}).encode())
            .decode()
            .rstrip("=")
        )
        payload = (
            base64.urlsafe_b64encode(json.dumps(payload_data).encode())
            .decode()
            .rstrip("=")
        )
        play_session_cookie = f"{header}.{payload}.signature"

        # Mock validate_token_and_get_user to avoid actual GMS call
        mock_validate = MagicMock(return_value="urn:li:corpuser:cookieuser")
        monkeypatch.setattr(
            "datahub_integrations.oauth.router.validate_token_and_get_user",
            mock_validate,
        )

        # Call with PLAY_SESSION cookie only
        user_urn = get_authenticated_user(
            authorization=None, play_session=play_session_cookie
        )

        assert user_urn == "urn:li:corpuser:cookieuser"
        # Verify the token was extracted from cookie correctly
        mock_validate.assert_called_once_with("test_token_from_cookie")

    def test_get_authenticated_user_header_takes_precedence(self, monkeypatch) -> None:
        """Test that Authorization header takes precedence over PLAY_SESSION cookie."""
        import base64
        import json
        from unittest.mock import MagicMock

        from datahub_integrations.oauth.router import get_authenticated_user

        # Create a valid PLAY_SESSION cookie
        payload_data = {
            "data": {
                "actor": "urn:li:corpuser:admin",
                "token": "cookie_token_should_not_be_used",
            },
            "exp": 1769539919,
        }
        header = (
            base64.urlsafe_b64encode(json.dumps({"alg": "HS256"}).encode())
            .decode()
            .rstrip("=")
        )
        payload = (
            base64.urlsafe_b64encode(json.dumps(payload_data).encode())
            .decode()
            .rstrip("=")
        )
        play_session_cookie = f"{header}.{payload}.signature"

        # Mock validate_token_and_get_user to avoid actual GMS call
        mock_validate = MagicMock(return_value="urn:li:corpuser:headeruser")
        monkeypatch.setattr(
            "datahub_integrations.oauth.router.validate_token_and_get_user",
            mock_validate,
        )

        # Call with both Authorization header and cookie
        user_urn = get_authenticated_user(
            authorization="Bearer header_token_used",
            play_session=play_session_cookie,
        )

        # Should use header token, so get headeruser
        assert user_urn == "urn:li:corpuser:headeruser"
        # Verify header token was used, not cookie token
        mock_validate.assert_called_once_with("header_token_used")

    def test_get_authenticated_user_raises_on_missing_both(self) -> None:
        """Test error when both Authorization header and PLAY_SESSION cookie are missing."""
        from fastapi import HTTPException

        from datahub_integrations.oauth.router import get_authenticated_user

        with pytest.raises(HTTPException) as exc_info:
            get_authenticated_user(authorization=None, play_session=None)

        assert exc_info.value.status_code == 401
        assert (
            "Missing Authorization header or PLAY_SESSION cookie"
            in exc_info.value.detail
        )

    def test_get_authenticated_user_raises_on_invalid_token(self, monkeypatch) -> None:
        """Test error when GMS rejects the token."""

        from fastapi import HTTPException

        from datahub_integrations.oauth.router import get_authenticated_user

        # Mock validate_token_and_get_user to raise HTTPException
        def mock_validate_raises(token):
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        monkeypatch.setattr(
            "datahub_integrations.oauth.router.validate_token_and_get_user",
            mock_validate_raises,
        )

        with pytest.raises(HTTPException) as exc_info:
            get_authenticated_user(
                authorization="Bearer invalid_token", play_session=None
            )

        assert exc_info.value.status_code == 401
        assert "Invalid or expired token" in exc_info.value.detail


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


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for initiate_oauth_connect endpoint
# ═══════════════════════════════════════════════════════════════════════════════


class TestInitiateOAuthConnect:
    """Test initiate_oauth_connect endpoint."""

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router.build_authorization_url")
    @patch("datahub_integrations.oauth.router.build_oauth_callback_url")
    @patch("datahub_integrations.oauth.router.get_oauth_server_config")
    @patch("datahub_integrations.oauth.router.get_plugin_config")
    async def test_successful_oauth_connect(
        self,
        mock_get_plugin: MagicMock,
        mock_get_server: MagicMock,
        mock_build_callback: MagicMock,
        mock_build_auth_url: MagicMock,
    ) -> None:
        """Test successful OAuth connect returns authorization URL."""
        from datahub_integrations.oauth.router import initiate_oauth_connect

        mock_get_plugin.return_value = {
            "authType": "USER_OAUTH",
            "oauthConfig": {"serverUrn": "urn:li:oauthServer:github"},
        }
        mock_get_server.return_value = {
            "clientId": "client-123",
            "authorizationUrl": "https://github.com/login/oauth/authorize",
        }
        mock_build_callback.return_value = (
            "https://datahub.example.com/integrations/oauth/callback"
        )
        mock_build_auth_url.return_value = (
            "https://github.com/login/oauth/authorize?client_id=client-123"
        )

        mock_state_store = MagicMock()
        mock_state_store.create_state.return_value = MagicMock(
            authorization_url="https://github.com/login/oauth/authorize?client_id=client-123&state=nonce123"
        )

        response = await initiate_oauth_connect(
            plugin_id="urn:li:service:github",
            user_urn="urn:li:corpuser:testuser",
            auth_token="jwt-token",
            state_store=mock_state_store,
        )

        assert response.authorization_url is not None
        mock_state_store.create_state.assert_called_once()

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router.get_plugin_config")
    async def test_rejects_non_oauth_plugin(self, mock_get_plugin: MagicMock) -> None:
        """Test rejects connect for non-OAuth plugins."""
        from datahub_integrations.oauth.router import initiate_oauth_connect

        mock_get_plugin.return_value = {"authType": "USER_API_KEY"}
        mock_state_store = MagicMock()

        with pytest.raises(HTTPException) as exc_info:
            await initiate_oauth_connect(
                plugin_id="urn:li:service:api-plugin",
                user_urn="urn:li:corpuser:testuser",
                auth_token="jwt-token",
                state_store=mock_state_store,
            )

        assert exc_info.value.status_code == 400
        assert "does not use OAuth" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router.get_plugin_config")
    async def test_rejects_oauth_plugin_without_config(
        self, mock_get_plugin: MagicMock
    ) -> None:
        """Test rejects connect when OAuth config is missing."""
        from datahub_integrations.oauth.router import initiate_oauth_connect

        mock_get_plugin.return_value = {
            "authType": "USER_OAUTH",
            "oauthConfig": None,
        }
        mock_state_store = MagicMock()

        with pytest.raises(HTTPException) as exc_info:
            await initiate_oauth_connect(
                plugin_id="urn:li:service:broken",
                user_urn="urn:li:corpuser:testuser",
                auth_token="jwt-token",
                state_store=mock_state_store,
            )

        assert exc_info.value.status_code == 400
        assert "not properly configured" in str(exc_info.value.detail)


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for save_api_key endpoint
# ═══════════════════════════════════════════════════════════════════════════════


class TestSaveApiKey:
    """Test save_api_key endpoint."""

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._update_user_plugin_settings")
    @patch("datahub_integrations.oauth.router.get_plugin_config")
    async def test_successful_api_key_save(
        self,
        mock_get_plugin: MagicMock,
        mock_update_settings: MagicMock,
    ) -> None:
        """Test successful API key save."""
        from datahub_integrations.oauth.router import ApiKeyRequest, save_api_key

        mock_get_plugin.return_value = {"authType": "USER_API_KEY"}
        mock_update_settings.return_value = None

        mock_credential_store = MagicMock()
        mock_credential_store.save_api_key.return_value = (
            "urn:li:dataHubConnection:test"
        )

        response = await save_api_key(
            plugin_id="urn:li:service:api-plugin",
            body=ApiKeyRequest(api_key="my-secret-key"),
            user_urn="urn:li:corpuser:testuser",
            auth_token="jwt-token",
            credential_store=mock_credential_store,
        )

        assert response.success is True
        assert response.connection_urn == "urn:li:dataHubConnection:test"
        mock_credential_store.save_api_key.assert_called_once_with(
            user_urn="urn:li:corpuser:testuser",
            plugin_id="urn:li:service:api-plugin",
            api_key="my-secret-key",
        )
        mock_update_settings.assert_called_once()

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router.get_plugin_config")
    async def test_rejects_non_api_key_plugin(self, mock_get_plugin: MagicMock) -> None:
        """Test rejects API key save for non-API-key plugins."""
        from datahub_integrations.oauth.router import ApiKeyRequest, save_api_key

        mock_get_plugin.return_value = {"authType": "USER_OAUTH"}
        mock_credential_store = MagicMock()

        with pytest.raises(HTTPException) as exc_info:
            await save_api_key(
                plugin_id="urn:li:service:oauth-plugin",
                body=ApiKeyRequest(api_key="my-key"),
                user_urn="urn:li:corpuser:testuser",
                auth_token="jwt-token",
                credential_store=mock_credential_store,
            )

        assert exc_info.value.status_code == 400
        assert "does not use API key" in str(exc_info.value.detail)


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for disconnect_plugin endpoint
# ═══════════════════════════════════════════════════════════════════════════════


class TestDisconnectPlugin:
    """Test disconnect_plugin endpoint."""

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._remove_user_plugin_connection")
    async def test_successful_disconnect(
        self, mock_remove_connection: MagicMock
    ) -> None:
        """Test successful plugin disconnect."""
        from datahub_integrations.oauth.router import disconnect_plugin

        mock_remove_connection.return_value = None
        mock_credential_store = MagicMock()
        mock_credential_store.delete_credentials.return_value = True

        response = await disconnect_plugin(
            plugin_id="urn:li:service:github",
            user_urn="urn:li:corpuser:testuser",
            auth_token="jwt-token",
            credential_store=mock_credential_store,
        )

        assert response.success is True
        mock_credential_store.delete_credentials.assert_called_once_with(
            user_urn="urn:li:corpuser:testuser",
            plugin_id="urn:li:service:github",
        )
        mock_remove_connection.assert_called_once()

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._remove_user_plugin_connection")
    async def test_disconnect_when_no_credentials_exist(
        self, mock_remove_connection: MagicMock
    ) -> None:
        """Test disconnect succeeds even when no credentials exist."""
        from datahub_integrations.oauth.router import disconnect_plugin

        mock_remove_connection.return_value = None
        mock_credential_store = MagicMock()
        mock_credential_store.delete_credentials.return_value = (
            False  # Nothing to delete
        )

        response = await disconnect_plugin(
            plugin_id="urn:li:service:already-disconnected",
            user_urn="urn:li:corpuser:testuser",
            auth_token="jwt-token",
            credential_store=mock_credential_store,
        )

        assert response.success is True
        # Should still try to clean up user settings
        mock_remove_connection.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for _update_user_plugin_settings helper
# ═══════════════════════════════════════════════════════════════════════════════


class TestUpdateUserPluginSettings:
    """Test _update_user_plugin_settings helper."""

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._execute_graphql_as_user")
    async def test_oauth_connection_sets_correct_mutation_input(
        self, mock_execute: MagicMock
    ) -> None:
        """Test OAuth connection builds correct GraphQL mutation input."""
        from datahub_integrations.oauth.router import _update_user_plugin_settings

        mock_execute.return_value = {"updateUserAiPluginSettings": True}

        await _update_user_plugin_settings(
            user_urn="urn:li:corpuser:testuser",
            plugin_id="urn:li:service:github",
            connection_urn="urn:li:dataHubConnection:test",
            is_oauth=True,
            auth_token="jwt-token",
        )

        call_kwargs = mock_execute.call_args.kwargs
        variables = call_kwargs["variables"]
        assert variables["input"]["pluginId"] == "urn:li:service:github"
        assert (
            variables["input"]["oauthConnectionUrn"] == "urn:li:dataHubConnection:test"
        )
        assert variables["input"]["enabled"] is True
        assert "apiKey" not in variables["input"]

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._execute_graphql_as_user")
    async def test_api_key_connection_sets_correct_mutation_input(
        self, mock_execute: MagicMock
    ) -> None:
        """Test API key connection builds correct GraphQL mutation input."""
        from datahub_integrations.oauth.router import _update_user_plugin_settings

        mock_execute.return_value = {"updateUserAiPluginSettings": True}

        await _update_user_plugin_settings(
            user_urn="urn:li:corpuser:testuser",
            plugin_id="urn:li:service:api-plugin",
            connection_urn="urn:li:dataHubConnection:test",
            is_oauth=False,
            auth_token="jwt-token",
        )

        call_kwargs = mock_execute.call_args.kwargs
        variables = call_kwargs["variables"]
        assert variables["input"]["pluginId"] == "urn:li:service:api-plugin"
        assert variables["input"]["apiKey"] == "connected"
        assert variables["input"]["enabled"] is True
        assert "oauthConnectionUrn" not in variables["input"]

    @pytest.mark.asyncio
    async def test_raises_on_missing_auth_token(self) -> None:
        """Test raises ValueError when auth_token is missing."""
        from datahub_integrations.oauth.router import _update_user_plugin_settings

        with pytest.raises(ValueError, match="auth_token is required"):
            await _update_user_plugin_settings(
                user_urn="urn:li:corpuser:testuser",
                plugin_id="urn:li:service:github",
                connection_urn="urn:li:dataHubConnection:test",
                is_oauth=True,
                auth_token="",
            )

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._execute_graphql_as_user")
    async def test_raises_on_mutation_returning_false(
        self, mock_execute: MagicMock
    ) -> None:
        """Test raises Exception when mutation returns False."""
        from datahub_integrations.oauth.router import _update_user_plugin_settings

        mock_execute.return_value = {"updateUserAiPluginSettings": False}

        with pytest.raises(Exception, match="returned False"):
            await _update_user_plugin_settings(
                user_urn="urn:li:corpuser:testuser",
                plugin_id="urn:li:service:github",
                connection_urn="urn:li:dataHubConnection:test",
                is_oauth=True,
                auth_token="jwt-token",
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for _remove_user_plugin_connection helper
# ═══════════════════════════════════════════════════════════════════════════════


class TestRemoveUserPluginConnection:
    """Test _remove_user_plugin_connection helper."""

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._execute_graphql_as_user")
    async def test_oauth_disconnect_sets_disconnect_flag(
        self, mock_execute: MagicMock
    ) -> None:
        """Test OAuth disconnect sends disconnectOAuth=True."""
        from datahub_integrations.oauth.router import _remove_user_plugin_connection

        mock_execute.return_value = {"updateUserAiPluginSettings": True}

        await _remove_user_plugin_connection(
            user_urn="urn:li:corpuser:testuser",
            plugin_id="urn:li:service:github",
            is_oauth=True,
            auth_token="jwt-token",
        )

        call_kwargs = mock_execute.call_args.kwargs
        variables = call_kwargs["variables"]
        assert variables["input"]["disconnectOAuth"] is True

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router._execute_graphql_as_user")
    async def test_api_key_disconnect_sends_empty_api_key(
        self, mock_execute: MagicMock
    ) -> None:
        """Test API key disconnect sends empty apiKey string."""
        from datahub_integrations.oauth.router import _remove_user_plugin_connection

        mock_execute.return_value = {"updateUserAiPluginSettings": True}

        await _remove_user_plugin_connection(
            user_urn="urn:li:corpuser:testuser",
            plugin_id="urn:li:service:api-plugin",
            is_oauth=False,
            auth_token="jwt-token",
        )

        call_kwargs = mock_execute.call_args.kwargs
        variables = call_kwargs["variables"]
        assert variables["input"]["apiKey"] == ""

    @pytest.mark.asyncio
    async def test_raises_on_missing_auth_token(self) -> None:
        """Test raises ValueError when auth_token is missing."""
        from datahub_integrations.oauth.router import _remove_user_plugin_connection

        with pytest.raises(ValueError, match="auth_token is required"):
            await _remove_user_plugin_connection(
                user_urn="urn:li:corpuser:testuser",
                plugin_id="urn:li:service:github",
                auth_token="",
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for _execute_graphql_as_user helper
# ═══════════════════════════════════════════════════════════════════════════════


class TestExecuteGraphqlAsUser:
    """Test _execute_graphql_as_user helper."""

    @patch("datahub_integrations.oauth.router.graph")
    def test_sends_request_with_user_token(self, mock_graph: MagicMock) -> None:
        """Test sends GraphQL request with user's auth token."""
        from datahub_integrations.oauth.router import _execute_graphql_as_user

        mock_graph._gms_server = "http://gms:8080"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"result": True}}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.post", return_value=mock_response) as mock_post:
            result = _execute_graphql_as_user(
                auth_token="user-jwt-token",
                query="mutation { doThing }",
                variables={"input": {"key": "value"}},
            )

        assert result == {"result": True}
        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["headers"]["Authorization"] == "Bearer user-jwt-token"
        assert call_kwargs.kwargs["json"]["query"] == "mutation { doThing }"
        assert call_kwargs.kwargs["json"]["variables"] == {"input": {"key": "value"}}

    @patch("datahub_integrations.oauth.router.graph")
    def test_raises_on_graphql_errors(self, mock_graph: MagicMock) -> None:
        """Test raises Exception on GraphQL errors in response."""
        from datahub_integrations.oauth.router import _execute_graphql_as_user

        mock_graph._gms_server = "http://gms:8080"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "errors": [{"message": "Unauthorized"}],
            "data": None,
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.post", return_value=mock_response):
            with pytest.raises(Exception, match="GraphQL errors"):
                _execute_graphql_as_user(
                    auth_token="user-jwt-token",
                    query="mutation { doThing }",
                )

    @patch("datahub_integrations.oauth.router.graph")
    def test_raises_on_http_error(self, mock_graph: MagicMock) -> None:
        """Test raises on HTTP error status."""
        from datahub_integrations.oauth.router import _execute_graphql_as_user

        mock_graph._gms_server = "http://gms:8080"

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error",
            request=MagicMock(),
            response=MagicMock(status_code=500),
        )

        with patch("httpx.post", return_value=mock_response):
            with pytest.raises(httpx.HTTPStatusError):
                _execute_graphql_as_user(
                    auth_token="user-jwt-token",
                    query="mutation { doThing }",
                )


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for test_oauth_connect endpoint
# ═══════════════════════════════════════════════════════════════════════════════


class TestTestOAuthConnect:
    """Test test_oauth_connect endpoint."""

    @pytest.mark.asyncio
    async def test_successful_test_connect(self) -> None:
        """Test successful test OAuth connect returns authorization URL."""
        from datahub_integrations.oauth.router import (
            TestOAuthConnectRequest,
            test_oauth_connect,
        )
        from datahub_integrations.oauth.state_store import InMemoryOAuthStateStore

        state_store = InMemoryOAuthStateStore(ttl_seconds=60)

        body = TestOAuthConnectRequest(
            oauth_config={
                "clientId": "test-client",
                "clientSecret": "test-secret",
                "authorizationUrl": "https://provider.com/authorize",
                "tokenUrl": "https://provider.com/token",
            },
            mcp_config={"url": "https://mcp.example.com", "transport": "HTTP"},
        )

        with patch(
            "datahub_integrations.oauth.router.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        ):
            response = await test_oauth_connect(
                body=body,
                user_urn="urn:li:corpuser:admin",
                auth_token="jwt-token",
                state_store=state_store,
            )

        assert response.authorization_url is not None
        assert "client_id=test-client" in response.authorization_url
        assert "state=" in response.authorization_url
        assert len(state_store) == 1

    @pytest.mark.asyncio
    async def test_rejects_missing_client_id(self) -> None:
        """Test rejects request without clientId."""
        from datahub_integrations.oauth.router import (
            TestOAuthConnectRequest,
            test_oauth_connect,
        )

        body = TestOAuthConnectRequest(
            oauth_config={"authorizationUrl": "https://provider.com/authorize"},
            mcp_config={"url": "https://mcp.example.com"},
        )

        with pytest.raises(HTTPException) as exc_info:
            await test_oauth_connect(
                body=body,
                user_urn="urn:li:corpuser:admin",
                auth_token="jwt-token",
                state_store=MagicMock(),
            )

        assert exc_info.value.status_code == 400
        assert "clientId" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_rejects_missing_authorization_url(self) -> None:
        """Test rejects request without authorizationUrl."""
        from datahub_integrations.oauth.router import (
            TestOAuthConnectRequest,
            test_oauth_connect,
        )

        body = TestOAuthConnectRequest(
            oauth_config={"clientId": "test-client"},
            mcp_config={"url": "https://mcp.example.com"},
        )

        with pytest.raises(HTTPException) as exc_info:
            await test_oauth_connect(
                body=body,
                user_urn="urn:li:corpuser:admin",
                auth_token="jwt-token",
                state_store=MagicMock(),
            )

        assert exc_info.value.status_code == 400
        assert "clientId" in str(exc_info.value.detail) or "authorizationUrl" in str(
            exc_info.value.detail
        )

    @pytest.mark.asyncio
    async def test_rejects_missing_mcp_url(self) -> None:
        """Test rejects request without MCP URL."""
        from datahub_integrations.oauth.router import (
            TestOAuthConnectRequest,
            test_oauth_connect,
        )

        body = TestOAuthConnectRequest(
            oauth_config={
                "clientId": "test-client",
                "authorizationUrl": "https://provider.com/authorize",
            },
            mcp_config={},
        )

        with pytest.raises(HTTPException) as exc_info:
            await test_oauth_connect(
                body=body,
                user_urn="urn:li:corpuser:admin",
                auth_token="jwt-token",
                state_store=MagicMock(),
            )

        assert exc_info.value.status_code == 400
        assert "url" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_stores_mcp_discovery_flow_mode(self) -> None:
        """Test that state is stored with McpDiscoveryFlow mode."""
        from datahub_integrations.oauth.router import (
            TestOAuthConnectRequest,
            test_oauth_connect,
        )
        from datahub_integrations.oauth.state_store import (
            InMemoryOAuthStateStore,
            McpDiscoveryFlow,
        )

        state_store = InMemoryOAuthStateStore(ttl_seconds=60)
        oauth_config = {
            "clientId": "test-client",
            "authorizationUrl": "https://provider.com/authorize",
        }
        mcp_config = {"url": "https://mcp.example.com"}

        body = TestOAuthConnectRequest(oauth_config=oauth_config, mcp_config=mcp_config)

        with patch(
            "datahub_integrations.oauth.router.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        ):
            response = await test_oauth_connect(
                body=body,
                user_urn="urn:li:corpuser:admin",
                auth_token="jwt-token",
                state_store=state_store,
            )

        # Extract nonce from URL and consume the state
        url = response.authorization_url
        nonce = url.split("state=")[1]
        state = state_store.get_and_consume_state(nonce)

        assert state is not None
        assert isinstance(state.flow_mode, McpDiscoveryFlow)
        assert state.flow_mode.oauth_config == oauth_config
        assert state.flow_mode.mcp_config == mcp_config
        assert state.plugin_id == "__test__"


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for corrupt_credentials endpoint
# ═══════════════════════════════════════════════════════════════════════════════


class TestCorruptCredentials:
    """Test corrupt_credentials endpoint."""

    @pytest.mark.asyncio
    async def test_admin_can_corrupt_credentials(self) -> None:
        """Test admin user can corrupt credentials."""
        from datahub_integrations.oauth.credential_store import OAuthTokens
        from datahub_integrations.oauth.router import corrupt_credentials

        mock_creds = MagicMock()
        mock_creds.oauth_tokens = OAuthTokens(
            access_token="valid-token", refresh_token="valid-refresh", expires_at=None
        )

        mock_credential_store = MagicMock()
        mock_credential_store.get_credentials.return_value = mock_creds

        response = await corrupt_credentials(
            plugin_id="test-plugin",
            user_urn="urn:li:corpuser:admin",
            credential_store=mock_credential_store,
        )

        assert response.success is True
        assert "corrupted" in response.message.lower()
        mock_credential_store.save_oauth_tokens.assert_called_once()

    @pytest.mark.asyncio
    async def test_non_admin_rejected(self) -> None:
        """Test non-admin user is rejected with 403."""
        from datahub_integrations.oauth.router import corrupt_credentials

        with pytest.raises(HTTPException) as exc_info:
            await corrupt_credentials(
                plugin_id="test-plugin",
                user_urn="urn:li:corpuser:regularuser",
                credential_store=MagicMock(),
            )

        assert exc_info.value.status_code == 403
        assert "admin" in str(exc_info.value.detail).lower()

    @pytest.mark.asyncio
    async def test_no_credentials_returns_404(self) -> None:
        """Test returns 404 when no credentials exist."""
        from datahub_integrations.oauth.router import corrupt_credentials

        mock_credential_store = MagicMock()
        mock_credential_store.get_credentials.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await corrupt_credentials(
                plugin_id="nonexistent-plugin",
                user_urn="urn:li:corpuser:admin",
                credential_store=mock_credential_store,
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_no_oauth_tokens_returns_404(self) -> None:
        """Test returns 404 when credentials exist but no OAuth tokens."""
        from datahub_integrations.oauth.router import corrupt_credentials

        mock_creds = MagicMock()
        mock_creds.oauth_tokens = None

        mock_credential_store = MagicMock()
        mock_credential_store.get_credentials.return_value = mock_creds

        with pytest.raises(HTTPException) as exc_info:
            await corrupt_credentials(
                plugin_id="test-plugin",
                user_urn="urn:li:corpuser:admin",
                credential_store=mock_credential_store,
            )

        assert exc_info.value.status_code == 404


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for _handle_test_mcp_discovery_callback
# ═══════════════════════════════════════════════════════════════════════════════


class TestHandleTestMcpDiscoveryCallback:
    """Test _handle_test_mcp_discovery_callback helper."""

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router.exchange_code_for_tokens")
    async def test_successful_discovery(self, mock_exchange: MagicMock) -> None:
        """Test successful OAuth + MCP discovery."""
        from datahub_integrations.mcp_integration.connection_tester import McpTestResult
        from datahub_integrations.oauth.router import (
            _handle_test_mcp_discovery_callback,
        )
        from datahub_integrations.oauth.state_store import McpDiscoveryFlow, OAuthState

        mock_exchange.return_value = MagicMock(access_token="test-token")

        oauth_state = OAuthState(
            user_urn="urn:li:corpuser:admin",
            plugin_id="__test__",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=time.time(),
            flow_mode=McpDiscoveryFlow(
                oauth_config={
                    "tokenUrl": "https://provider.com/token",
                    "clientId": "test-client",
                },
                mcp_config={"url": "https://mcp.example.com"},
            ),
        )

        mock_mcp_result = McpTestResult(
            success=True,
            tool_count=3,
            tool_names=["search", "get", "list"],
            duration_seconds=1.0,
        )

        with patch(
            "datahub_integrations.mcp_integration.connection_tester.check_mcp_connection",
            return_value=mock_mcp_result,
        ):
            response = await _handle_test_mcp_discovery_callback(
                oauth_state=oauth_state, code="auth-code"
            )

        body = _get_response_body_str(response)
        assert "Discovered 3 tools" in body
        assert "search" in body

    @pytest.mark.asyncio
    @patch("datahub_integrations.oauth.router.exchange_code_for_tokens")
    async def test_mcp_connection_failure(self, mock_exchange: MagicMock) -> None:
        """Test MCP connection failure after successful token exchange."""
        from datahub_integrations.mcp_integration.connection_tester import McpTestResult
        from datahub_integrations.oauth.router import (
            _handle_test_mcp_discovery_callback,
        )
        from datahub_integrations.oauth.state_store import McpDiscoveryFlow, OAuthState

        mock_exchange.return_value = MagicMock(access_token="test-token")

        oauth_state = OAuthState(
            user_urn="urn:li:corpuser:admin",
            plugin_id="__test__",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=time.time(),
            flow_mode=McpDiscoveryFlow(
                oauth_config={
                    "tokenUrl": "https://provider.com/token",
                    "clientId": "c",
                },
                mcp_config={"url": "https://mcp.example.com"},
            ),
        )

        mock_mcp_result = McpTestResult(
            success=False,
            error="Connection refused",
            error_type="ConnectionError",
        )

        with patch(
            "datahub_integrations.mcp_integration.connection_tester.check_mcp_connection",
            return_value=mock_mcp_result,
        ):
            response = await _handle_test_mcp_discovery_callback(
                oauth_state=oauth_state, code="auth-code"
            )

        body = _get_response_body_str(response)
        assert "MCP Connection Failed" in body
        assert "Connection refused" in body

    @pytest.mark.asyncio
    async def test_token_exchange_failure(self) -> None:
        """Test failure during token exchange."""
        from datahub_integrations.oauth.router import (
            _handle_test_mcp_discovery_callback,
        )
        from datahub_integrations.oauth.state_store import McpDiscoveryFlow, OAuthState

        oauth_state = OAuthState(
            user_urn="urn:li:corpuser:admin",
            plugin_id="__test__",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=time.time(),
            flow_mode=McpDiscoveryFlow(
                oauth_config={
                    "tokenUrl": "https://provider.com/token",
                    "clientId": "c",
                },
                mcp_config={"url": "https://mcp.example.com"},
            ),
        )

        with patch(
            "datahub_integrations.oauth.router.exchange_code_for_tokens",
            side_effect=HTTPException(status_code=400, detail="Invalid grant"),
        ):
            response = await _handle_test_mcp_discovery_callback(
                oauth_state=oauth_state, code="expired-code"
            )

        body = _get_response_body_str(response)
        assert "Token Exchange Failed" in body
        assert "Invalid grant" in body

    @pytest.mark.asyncio
    async def test_unexpected_error(self) -> None:
        """Test unexpected error during callback."""
        from datahub_integrations.oauth.router import (
            _handle_test_mcp_discovery_callback,
        )
        from datahub_integrations.oauth.state_store import McpDiscoveryFlow, OAuthState

        oauth_state = OAuthState(
            user_urn="urn:li:corpuser:admin",
            plugin_id="__test__",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=time.time(),
            flow_mode=McpDiscoveryFlow(
                oauth_config={
                    "tokenUrl": "https://provider.com/token",
                    "clientId": "c",
                },
                mcp_config={"url": "https://mcp.example.com"},
            ),
        )

        with patch(
            "datahub_integrations.oauth.router.exchange_code_for_tokens",
            side_effect=RuntimeError("Unexpected database error"),
        ):
            response = await _handle_test_mcp_discovery_callback(
                oauth_state=oauth_state, code="code"
            )

        body = _get_response_body_str(response)
        assert "Test Failed" in body
        assert "unexpected" in body.lower()


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for _create_test_result_popup and _create_popup_response
# ═══════════════════════════════════════════════════════════════════════════════


class TestCreateTestResultPopup:
    """Test _create_test_result_popup HTML generation."""

    def test_success_popup_contains_tool_info(self) -> None:
        """Test success popup shows tool count and names."""
        from datahub_integrations.oauth.router import _create_test_result_popup

        response = _create_test_result_popup(
            success=True,
            message="Discovered 3 tools",
            details="search, get, list",
            result={"type": "oauth_test_result", "success": True, "toolCount": 3},
        )

        body = _get_response_body_str(response)
        assert "Discovered 3 tools" in body
        assert "search, get, list" in body
        assert "Test Passed" in body
        assert "postMessage" in body

    def test_failure_popup_contains_error(self) -> None:
        """Test failure popup shows error details."""
        from datahub_integrations.oauth.router import _create_test_result_popup

        response = _create_test_result_popup(
            success=False,
            message="Connection Failed",
            details="Server returned 401",
            result={"type": "oauth_test_result", "success": False},
        )

        body = _get_response_body_str(response)
        assert "Connection Failed" in body
        assert "Server returned 401" in body
        assert "Test Failed" in body

    def test_xss_escape_in_details(self) -> None:
        """Test XSS is escaped in tool names / details."""
        from datahub_integrations.oauth.router import _create_test_result_popup

        response = _create_test_result_popup(
            success=True,
            message="Test",
            details='<img src=x onerror="alert(1)">',
            result={"type": "oauth_test_result", "success": True},
        )

        body = _get_response_body_str(response)
        # The malicious input should be escaped
        assert 'onerror="alert(1)"' not in body
        assert "&lt;img" in body


class TestCreatePopupResponse:
    """Test _create_popup_response HTML generation."""

    def test_success_popup(self) -> None:
        """Test success popup content."""
        from datahub_integrations.oauth.router import _create_popup_response

        response = _create_popup_response(
            success=True,
            plugin_id="test-plugin",
            connection_urn="urn:li:dataHubConnection:test",
        )

        body = _get_response_body_str(response)
        assert "Connected Successfully" in body
        assert "postMessage" in body

    def test_failure_popup_with_error(self) -> None:
        """Test failure popup with error message."""
        from datahub_integrations.oauth.router import _create_popup_response

        response = _create_popup_response(
            success=False,
            plugin_id="test-plugin",
            error="Token exchange failed",
        )

        body = _get_response_body_str(response)
        assert "Connection Failed" in body
        assert "Token exchange failed" in body

    def test_xss_escape_in_error(self) -> None:
        """Test XSS is escaped in error messages."""
        from datahub_integrations.oauth.router import _create_popup_response

        response = _create_popup_response(
            success=False,
            plugin_id="test-plugin",
            error='<img src=x onerror="alert(1)">',
        )

        body = _get_response_body_str(response)
        assert 'onerror="alert(1)"' not in body
        assert "&lt;img" in body

    def test_failure_popup_without_error_shows_default(self) -> None:
        """Test failure popup shows default message when no error provided."""
        from datahub_integrations.oauth.router import _create_popup_response

        response = _create_popup_response(
            success=False,
            plugin_id="test-plugin",
        )

        body = _get_response_body_str(response)
        assert "Please try again" in body
