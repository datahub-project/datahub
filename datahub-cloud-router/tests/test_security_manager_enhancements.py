"""
Tests for security manager enhancements and OAuth state validation.
"""

import base64
import json
import time
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from datahub.cloud.router.db.models import OAuthStateDecoded
from datahub.cloud.router.security import SecureOAuthState, SecurityManager


class TestSecureOAuthState:
    """Test SecureOAuthState functionality."""

    def test_init(self):
        """Test SecureOAuthState initialization."""
        oauth_state = SecureOAuthState()
        assert oauth_state._used_nonces == set()
        assert oauth_state._max_nonces == 10000
        assert isinstance(oauth_state._last_cleanup, float)

    def test_create_state_basic(self):
        """Test basic OAuth state creation."""
        oauth_state = SecureOAuthState()

        state = oauth_state.create_state(
            url="http://localhost:3000",
            flow_type="platform_integration",
            tenant_id="test_tenant",
        )

        assert isinstance(state, str)
        assert len(state) > 0

        # Should be valid base64
        try:
            # Add padding if needed
            missing_padding = len(state) % 4
            if missing_padding:
                padded_state = state + "=" * (4 - missing_padding)
            else:
                padded_state = state
            decoded = base64.urlsafe_b64decode(padded_state)
            data = json.loads(decoded.decode())

            assert data["url"] == "http://localhost:3000"
            assert data["flow_type"] == "platform_integration"
            assert data["tenant_id"] == "test_tenant"
            assert "nonce" in data
            assert "timestamp" in data
        except Exception as e:
            pytest.fail(f"Failed to decode OAuth state: {e}")

    def test_create_state_with_all_parameters(self):
        """Test OAuth state creation with all parameters."""
        oauth_state = SecureOAuthState()

        state = oauth_state.create_state(
            url="http://localhost:3000",
            flow_type="personal_notifications",
            tenant_id="test_tenant",
            user_urn="urn:li:corpuser:admin",
            redirect_path="/settings/personal-notifications",
        )

        # Decode and verify
        missing_padding = len(state) % 4
        if missing_padding:
            padded_state = state + "=" * (4 - missing_padding)
        else:
            padded_state = state
        decoded = base64.urlsafe_b64decode(padded_state)
        data = json.loads(decoded.decode())

        assert data["url"] == "http://localhost:3000"
        assert data["flow_type"] == "personal_notifications"
        assert data["tenant_id"] == "test_tenant"
        assert data["user_urn"] == "urn:li:corpuser:admin"
        assert data["redirect_path"] == "/settings/personal-notifications"

    def test_validate_state_success(self):
        """Test successful OAuth state validation."""
        oauth_state = SecureOAuthState()

        # Create state
        original_state = oauth_state.create_state(
            url="http://localhost:3000",
            flow_type="platform_integration",
            tenant_id="test_tenant",
        )

        # Validate state
        decoded = oauth_state.validate_state(original_state)

        assert isinstance(decoded, OAuthStateDecoded)
        assert decoded.url == "http://localhost:3000"
        assert decoded.flow_type == "platform_integration"
        assert decoded.tenant_id == "test_tenant"

    def test_validate_state_expired(self):
        """Test OAuth state validation with expired state."""
        oauth_state = SecureOAuthState()

        # Create state
        original_state = oauth_state.create_state(
            url="http://localhost:3000",
            flow_type="platform_integration",
            tenant_id="test_tenant",
        )

        # Mock time to make state appear expired (simulate 2 hours later)
        with patch("time.time", return_value=time.time() + 7200):  # 2 hours later
            with pytest.raises(ValueError, match="OAuth state expired"):
                oauth_state.validate_state(
                    original_state, max_age=3600
                )  # 1 hour max age

    def test_validate_state_replay_attack_protection(self):
        """Test OAuth state replay attack protection."""
        oauth_state = SecureOAuthState()

        # Create state
        original_state = oauth_state.create_state(
            url="http://localhost:3000",
            flow_type="platform_integration",
            tenant_id="test_tenant",
        )

        # First validation should succeed
        decoded1 = oauth_state.validate_state(original_state)
        assert decoded1.url == "http://localhost:3000"

        # Second validation should fail (replay attack)
        with pytest.raises(ValueError, match="OAuth state replay detected"):
            oauth_state.validate_state(original_state)

    def test_validate_state_invalid_format(self):
        """Test OAuth state validation with invalid format."""
        oauth_state = SecureOAuthState()

        with pytest.raises(ValueError, match="Invalid OAuth state format"):
            oauth_state.validate_state("invalid_base64!")

    def test_validate_state_invalid_json(self):
        """Test OAuth state validation with invalid JSON."""
        oauth_state = SecureOAuthState()

        # Create invalid base64 that decodes to invalid JSON
        invalid_json = base64.urlsafe_b64encode(b"not json").decode()

        with pytest.raises(ValueError, match="Invalid OAuth state format"):
            oauth_state.validate_state(invalid_json)

    def test_nonce_cleanup(self):
        """Test nonce cleanup functionality."""
        oauth_state = SecureOAuthState()
        oauth_state._max_nonces = 5  # Small limit for testing
        oauth_state._cleanup_interval = 0  # Force cleanup on every call

        # Create many states to trigger cleanup
        states = []
        for i in range(10):
            state = oauth_state.create_state(
                url=f"http://localhost:300{i}",
                flow_type="platform_integration",
                tenant_id=f"tenant_{i}",
            )
            states.append(state)
            # Validate to add nonce to used set
            oauth_state.validate_state(state)

        # Should have triggered cleanup when we exceeded max_nonces
        assert len(oauth_state._used_nonces) <= oauth_state._max_nonces


class TestSecurityManagerOAuthIntegration:
    """Test SecurityManager OAuth state integration."""

    def test_create_oauth_state(self):
        """Test SecurityManager OAuth state creation."""
        security_manager = SecurityManager()

        state = security_manager.create_oauth_state(
            url="http://localhost:3000",
            flow_type="platform_integration",
            tenant_id="test_tenant",
        )

        assert isinstance(state, str)
        assert len(state) > 0

    def test_validate_oauth_callback_success(self):
        """Test successful OAuth callback validation."""
        security_manager = SecurityManager()

        # Create state
        state = security_manager.create_oauth_state(
            url="http://localhost:3000",
            flow_type="platform_integration",
            tenant_id="test_tenant",
        )

        # Validate callback
        decoded = security_manager.validate_oauth_callback(state)

        assert isinstance(decoded, OAuthStateDecoded)
        assert decoded.url == "http://localhost:3000"
        assert decoded.flow_type == "platform_integration"
        assert decoded.tenant_id == "test_tenant"

    def test_validate_oauth_callback_invalid_state(self):
        """Test OAuth callback validation with invalid state."""
        security_manager = SecurityManager()

        with pytest.raises(HTTPException) as exc_info:
            security_manager.validate_oauth_callback("invalid_state")

        assert exc_info.value.status_code == 400
        assert "Invalid OAuth state" in exc_info.value.detail

    def test_validate_oauth_callback_expired_state(self):
        """Test OAuth callback validation with expired state."""
        security_manager = SecurityManager()

        # Create state
        state = security_manager.create_oauth_state(
            url="http://localhost:3000",
            flow_type="platform_integration",
            tenant_id="test_tenant",
        )

        # Mock time to make state appear expired
        with patch("time.time", return_value=time.time() + 7200):  # 2 hours later
            with pytest.raises(HTTPException) as exc_info:
                security_manager.validate_oauth_callback(state)

            assert exc_info.value.status_code == 400
            assert "OAuth state expired" in exc_info.value.detail


class TestSecurityManagerIntegrationRegistration:
    """Test SecurityManager integration validator registration."""

    def test_empty_integrations_initially(self):
        """Test that security manager starts with no integrations."""
        security_manager = SecurityManager()
        assert security_manager.get_registered_integrations() == []

    @patch("datahub.cloud.router.security.TeamsWebhookValidator")
    def test_register_teams_validator_updates_list(self, mock_validator_class):
        """Test that registering Teams validator updates integration list."""
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        security_manager = SecurityManager()
        security_manager.register_teams_validator("app_id", "password")

        assert security_manager.get_registered_integrations() == ["teams"]

    @patch("datahub.cloud.router.security.SlackWebhookValidator")
    def test_register_slack_validator_updates_list(self, mock_validator_class):
        """Test that registering Slack validator updates integration list."""
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        security_manager = SecurityManager()
        security_manager.register_slack_validator("signing_secret")

        assert security_manager.get_registered_integrations() == ["slack"]

    @patch("datahub.cloud.router.security.TeamsWebhookValidator")
    @patch("datahub.cloud.router.security.SlackWebhookValidator")
    def test_register_multiple_validators_updates_list(
        self, mock_slack_class, mock_teams_class
    ):
        """Test that registering multiple validators updates integration list."""
        mock_teams = Mock()
        mock_slack = Mock()
        mock_teams_class.return_value = mock_teams
        mock_slack_class.return_value = mock_slack

        security_manager = SecurityManager()
        security_manager.register_teams_validator("app_id", "password")
        security_manager.register_slack_validator("signing_secret")

        integrations = security_manager.get_registered_integrations()
        assert "teams" in integrations
        assert "slack" in integrations
        assert len(integrations) == 2

    def test_webhook_validator_storage(self):
        """Test that webhook validators are properly stored."""
        with patch(
            "datahub.cloud.router.security.TeamsWebhookValidator"
        ) as mock_validator_class:
            mock_validator = Mock()
            mock_validator_class.return_value = mock_validator

            security_manager = SecurityManager()
            security_manager.register_teams_validator("app_id", "password")

            # Should be stored in internal dict
            assert security_manager._webhook_validators["teams"] == mock_validator


class TestExceptionHandlingImprovements:
    """Test improved exception handling with proper error chaining."""

    def test_oauth_state_creation_validation_error(self):
        """Test proper exception chaining in OAuth state creation."""
        oauth_state = SecureOAuthState()

        # Test with invalid parameters that would cause ValidationError
        with patch(
            "datahub.cloud.router.security.OAuthStateV1",
            side_effect=Exception("Validation failed"),
        ):
            with pytest.raises(ValueError) as exc_info:
                oauth_state.create_state(url="invalid")

            # Should have proper exception chaining
            assert exc_info.value.__cause__ is not None
            assert "Failed to create OAuth state" in str(exc_info.value)

    def test_oauth_state_validation_error_chaining(self):
        """Test proper exception chaining in OAuth state validation."""
        oauth_state = SecureOAuthState()

        # Test with state that causes ValidationError during parsing
        with patch(
            "datahub.cloud.router.security.OAuthStateV1",
            side_effect=Exception("Parse failed"),
        ):
            with pytest.raises(ValueError) as exc_info:
                oauth_state.validate_state(
                    "dGVzdA=="
                )  # valid base64 but will fail parsing

            # Should have proper exception chaining
            assert exc_info.value.__cause__ is not None

    def test_security_manager_oauth_validation_error_chaining(self):
        """Test proper exception chaining in SecurityManager OAuth validation."""
        security_manager = SecurityManager()

        # Mock the underlying oauth_state to raise ValueError
        security_manager.oauth_state.validate_state = Mock(
            side_effect=ValueError("State invalid")
        )

        with pytest.raises(HTTPException) as exc_info:
            security_manager.validate_oauth_callback("invalid_state")

        # Should properly chain the ValueError as the cause
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, ValueError)


class TestRateLimitingIntegration:
    """Test rate limiting integration in security manager."""

    @pytest.mark.asyncio
    async def test_rate_limiting_enabled_by_default(self):
        """Test that rate limiting is enabled by default."""
        security_manager = SecurityManager()
        assert security_manager.rate_limiter is not None

    @pytest.mark.asyncio
    async def test_rate_limiting_can_be_disabled(self):
        """Test that rate limiting can be disabled."""
        security_manager = SecurityManager(enable_rate_limiting=False)
        assert security_manager.rate_limiter is None

    @pytest.mark.asyncio
    async def test_webhook_validation_uses_rate_limiting(self):
        """Test that webhook validation applies rate limiting."""
        with patch("datahub.cloud.router.security.TeamsWebhookValidator"):
            security_manager = SecurityManager()
            security_manager.register_teams_validator("app_id", "password")

            # Mock rate limiter to deny request
            security_manager.rate_limiter.is_allowed = Mock(return_value=False)

            mock_request = Mock()
            mock_request.client.host = "127.0.0.1"

            from fastapi import HTTPException

            with pytest.raises(HTTPException) as exc_info:
                await security_manager.validate_webhook(
                    "teams", mock_request, b'{"test": "data"}'
                )

            assert exc_info.value.status_code == 429
            assert "Rate limit exceeded" in exc_info.value.detail
