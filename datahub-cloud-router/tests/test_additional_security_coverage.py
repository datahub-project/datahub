"""
Additional security tests to improve coverage for critical security components.
"""

import base64
import hashlib
import hmac
import json
import time
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from datahub.cloud.router.security import (
    CSRFProtection,
    RequestSigningValidator,
    SecurityManager,
    SlackWebhookValidator,
)


class TestSlackWebhookValidator:
    """Test SlackWebhookValidator functionality."""

    def test_init_requires_signing_secret(self):
        """Test SlackWebhookValidator initialization requires signing secret."""
        with pytest.raises(ValueError, match="Slack signing secret is required"):
            SlackWebhookValidator("")

    def test_init_success(self):
        """Test successful SlackWebhookValidator initialization."""
        validator = SlackWebhookValidator("test_signing_secret")
        assert validator.signing_secret == "test_signing_secret"

    def test_get_integration_name(self):
        """Test SlackWebhookValidator integration name."""
        validator = SlackWebhookValidator("test_secret")
        assert validator.get_integration_name() == "Slack"

    @pytest.mark.asyncio
    async def test_validate_request_missing_signature_header(self):
        """Test Slack validation with missing signature header."""
        validator = SlackWebhookValidator("test_signing_secret")

        # Mock request without signature header
        request = Mock()
        request.headers = {}

        result = await validator.validate_request(request, b'{"test": "data"}')
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_request_missing_timestamp_header(self):
        """Test Slack validation with missing timestamp header."""
        validator = SlackWebhookValidator("test_signing_secret")

        # Mock request with signature but no timestamp
        request = Mock()
        request.headers = {"x-slack-signature": "v0=test_signature"}

        result = await validator.validate_request(request, b'{"test": "data"}')
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_request_invalid_signature_format(self):
        """Test Slack validation with invalid signature format."""
        validator = SlackWebhookValidator("test_signing_secret")

        request = Mock()
        request.headers = {
            "x-slack-signature": "invalid_format",
            "x-slack-request-timestamp": str(int(time.time())),
        }

        result = await validator.validate_request(request, b'{"test": "data"}')
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_request_expired_timestamp(self):
        """Test Slack validation with expired timestamp."""
        validator = SlackWebhookValidator("test_signing_secret")

        # Old timestamp (more than 5 minutes ago)
        old_timestamp = str(int(time.time()) - 400)

        request = Mock()
        request.headers = {
            "x-slack-signature": "v0=test_signature",
            "x-slack-request-timestamp": old_timestamp,
        }

        result = await validator.validate_request(request, b'{"test": "data"}')
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_request_invalid_signature(self):
        """Test Slack validation with invalid signature."""
        validator = SlackWebhookValidator("test_signing_secret")

        current_timestamp = str(int(time.time()))
        request = Mock()
        request.headers = {
            "x-slack-signature": "v0=invalid_signature_hash",
            "x-slack-request-timestamp": current_timestamp,
        }

        result = await validator.validate_request(request, b'{"test": "data"}')
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_request_valid_signature(self):
        """Test Slack validation with valid signature."""
        signing_secret = "test_signing_secret"
        validator = SlackWebhookValidator(signing_secret)

        current_timestamp = str(int(time.time()))
        body = b'{"test": "data"}'

        # Generate correct signature
        sig_basestring = f"v0:{current_timestamp}:{body.decode()}"
        expected_signature = (
            "v0="
            + hmac.new(
                signing_secret.encode(),
                sig_basestring.encode(),
                hashlib.sha256,
            ).hexdigest()
        )

        request = Mock()
        request.headers = Mock()
        request.headers.get = Mock(
            side_effect=lambda key, default=None: {
                "X-Slack-Signature": expected_signature,
                "X-Slack-Request-Timestamp": current_timestamp,
            }.get(key, default)
        )

        result = await validator.validate_request(request, body)
        assert result is True

    @pytest.mark.asyncio
    async def test_validate_request_exception_handling(self):
        """Test Slack validation handles exceptions gracefully."""
        validator = SlackWebhookValidator("test_signing_secret")

        # Mock request that will cause exception
        request = Mock()
        request.headers = Mock()
        request.headers.get = Mock(side_effect=Exception("Header access failed"))

        result = await validator.validate_request(request, b'{"test": "data"}')
        assert result is False


class TestCSRFProtection:
    """Test CSRFProtection functionality."""

    def test_init_generates_secret(self):
        """Test CSRFProtection initializes with secret."""
        manager = CSRFProtection()
        assert manager._secret is not None
        assert len(manager._secret) > 0

    def test_init_with_custom_secret(self):
        """Test CSRFProtection with custom secret."""
        custom_secret = "custom_csrf_secret"
        manager = CSRFProtection(custom_secret)
        assert manager._secret == custom_secret

    def test_generate_token(self):
        """Test CSRF token generation."""
        manager = CSRFProtection("test_secret")
        session_id = "test_session_123"

        token = manager.generate_token(session_id)

        assert isinstance(token, str)
        assert ":" in token  # Should have signature:data format
        assert token in manager._tokens
        assert manager._tokens[token]["session_id"] == session_id

    def test_validate_token_success(self):
        """Test successful CSRF token validation."""
        manager = CSRFProtection("test_secret")
        session_id = "test_session_123"

        # Generate token
        token = manager.generate_token(session_id)

        # Validate token
        result = manager.validate_token(token, session_id)
        assert result is True

        # Token should be consumed (removed after validation)
        assert token not in manager._tokens

    def test_validate_token_invalid_format(self):
        """Test CSRF validation with invalid token format."""
        manager = CSRFProtection("test_secret")

        result = manager.validate_token("invalid_token", "session_123")
        assert result is False

    def test_validate_token_invalid_signature(self):
        """Test CSRF validation with invalid signature."""
        manager = CSRFProtection("test_secret")

        # Create token with wrong signature
        token_data = {
            "session_id": "test_session",
            "timestamp": int(time.time()),
            "entropy": "test_entropy",
        }
        token_str = json.dumps(token_data, sort_keys=True)
        invalid_token = (
            f"wrong_signature:{base64.b64encode(token_str.encode()).decode()}"
        )

        result = manager.validate_token(invalid_token, "test_session")
        assert result is False

    def test_validate_token_wrong_session(self):
        """Test CSRF validation with wrong session ID."""
        manager = CSRFProtection("test_secret")
        session_id = "correct_session"

        token = manager.generate_token(session_id)

        # Try to validate with different session
        result = manager.validate_token(token, "wrong_session")
        assert result is False

    def test_validate_token_expired(self):
        """Test CSRF validation with expired token."""
        manager = CSRFProtection("test_secret")
        session_id = "test_session"

        token = manager.generate_token(session_id)

        # Manually expire the token
        manager._tokens[token]["expiry"] = int(time.time()) - 100

        result = manager.validate_token(token, session_id)
        assert result is False

    def test_cleanup_expired_tokens(self):
        """Test that expired tokens can be detected."""
        manager = CSRFProtection("test_secret")

        # Generate token and manually expire it
        token = manager.generate_token("test_session")
        manager._tokens[token]["expiry"] = int(time.time()) - 100  # Expire

        # Expired token should fail validation
        result = manager.validate_token(token, "test_session")
        assert result is False


class TestRequestSigningValidator:
    """Test RequestSigningValidator functionality."""

    def test_init_generates_secret(self):
        """Test RequestSigningValidator generates secret if none provided."""
        validator = RequestSigningValidator()
        assert validator._secret is not None
        assert len(validator._secret) > 0

    def test_init_with_custom_secret(self):
        """Test RequestSigningValidator with custom secret."""
        custom_secret = "custom_signing_secret"
        validator = RequestSigningValidator(custom_secret)
        assert validator._secret == custom_secret

    def test_sign_request_basic(self):
        """Test basic request signing."""
        validator = RequestSigningValidator("test_secret")

        headers = validator.sign_request("POST", "/api/test", b'{"data": "test"}')

        assert "X-DataHub-Signature" in headers
        assert "X-DataHub-Timestamp" in headers
        assert "X-DataHub-Nonce" in headers
        assert isinstance(headers["X-DataHub-Signature"], str)
        assert isinstance(headers["X-DataHub-Timestamp"], str)
        assert isinstance(headers["X-DataHub-Nonce"], str)


class TestSecurityManagerRateLimitingEdgeCases:
    """Test SecurityManager rate limiting edge cases and error conditions."""

    def test_security_manager_rate_limiter_cleanup_mechanism(self):
        """Test rate limiter cleanup mechanism."""
        security_manager = SecurityManager(enable_rate_limiting=True)
        rate_limiter = security_manager.rate_limiter

        # Add some old requests
        current_time = time.time()
        old_time = current_time - 7200  # 2 hours ago

        rate_limiter._requests["old_key"] = [old_time, old_time + 10]
        rate_limiter._requests["recent_key"] = [current_time - 30]

        # Force cleanup
        rate_limiter._last_cleanup = 0  # Force cleanup on next call

        # This should trigger cleanup
        is_allowed = rate_limiter.is_allowed("test_key", limit=10)

        # Old requests should be cleaned up
        assert "old_key" not in rate_limiter._requests
        assert "recent_key" in rate_limiter._requests
        assert is_allowed is True

    def test_security_manager_validation_error_paths(self):
        """Test SecurityManager webhook validation error paths."""
        security_manager = SecurityManager()

        # Test with non-existent integration
        with pytest.raises(HTTPException) as exc_info:
            import asyncio

            asyncio.run(
                security_manager.validate_webhook("nonexistent", Mock(), b"data")
            )

        assert exc_info.value.status_code == 500
        assert "Server configuration error" in exc_info.value.detail


class TestSecurityManagerIntegrationEdgeCases:
    """Test SecurityManager integration edge cases."""

    def test_register_slack_validator_success(self):
        """Test successful Slack validator registration."""
        security_manager = SecurityManager()

        security_manager.register_slack_validator("test_signing_secret")

        assert "slack" in security_manager.get_registered_integrations()
        assert security_manager._webhook_validators["slack"] is not None

    def test_register_slack_validator_failure(self):
        """Test Slack validator registration failure handling."""
        security_manager = SecurityManager()

        # This should fail gracefully with empty secret
        with pytest.raises(ValueError):
            security_manager.register_slack_validator("")

    @pytest.mark.asyncio
    async def test_validate_webhook_slack_success(self):
        """Test successful Slack webhook validation."""
        security_manager = SecurityManager()
        security_manager.register_slack_validator("test_secret")

        # Mock the validator to return True
        mock_validator = Mock()
        mock_validator.validate_request = AsyncMock(return_value=True)
        security_manager._webhook_validators["slack"] = mock_validator

        mock_request = Mock()

        # Should not raise exception
        await security_manager.validate_webhook("slack", mock_request, b"data")

        mock_validator.validate_request.assert_called_once_with(mock_request, b"data")

    @pytest.mark.asyncio
    async def test_validate_webhook_slack_failure(self):
        """Test failed Slack webhook validation."""
        security_manager = SecurityManager()
        security_manager.register_slack_validator("test_secret")

        # Mock the validator to return False
        mock_validator = Mock()
        mock_validator.validate_request = AsyncMock(return_value=False)
        security_manager._webhook_validators["slack"] = mock_validator

        mock_request = Mock()

        with pytest.raises(HTTPException) as exc_info:
            await security_manager.validate_webhook("slack", mock_request, b"data")

        assert exc_info.value.status_code == 401
        assert "Invalid slack webhook" in exc_info.value.detail
