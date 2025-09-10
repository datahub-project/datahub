"""
Tests for Teams webhook validation functionality.
"""

import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from datahub.cloud.router.core import MultiTenantRouter
from datahub.cloud.router.security import SecurityManager, TeamsWebhookValidator
from datahub.cloud.router.server import DataHubMultiTenantRouter


class TestTeamsWebhookValidator:
    """Test Teams webhook validator."""

    def test_init_requires_credentials(self):
        """Test that validator initialization requires app_id and app_password."""
        with pytest.raises(
            ValueError, match="Teams app_id and app_password are required"
        ):
            TeamsWebhookValidator("", "password")

        with pytest.raises(
            ValueError, match="Teams app_id and app_password are required"
        ):
            TeamsWebhookValidator("app_id", "")

        with pytest.raises(
            ValueError, match="Teams app_id and app_password are required"
        ):
            TeamsWebhookValidator(None, "password")

    @patch("datahub.cloud.router.security.BotFrameworkAdapter")
    @patch("datahub.cloud.router.security.BotFrameworkAdapterSettings")
    def test_init_success(self, mock_settings, mock_adapter):
        """Test successful validator initialization."""
        validator = TeamsWebhookValidator("test_app_id", "test_password")

        assert validator.app_id == "test_app_id"
        assert validator.app_password == "test_password"
        mock_settings.assert_called_once_with(
            app_id="test_app_id", app_password="test_password"
        )
        mock_adapter.assert_called_once()

    def test_get_integration_name(self):
        """Test get_integration_name returns correct name."""
        with (
            patch("datahub.cloud.router.security.BotFrameworkAdapter"),
            patch("datahub.cloud.router.security.BotFrameworkAdapterSettings"),
        ):
            validator = TeamsWebhookValidator("test_app_id", "test_password")
            assert validator.get_integration_name() == "Teams"

    @pytest.mark.asyncio
    async def test_validate_request_missing_auth_header(self):
        """Test validation fails when Authorization header is missing."""
        with (
            patch("datahub.cloud.router.security.BotFrameworkAdapter"),
            patch("datahub.cloud.router.security.BotFrameworkAdapterSettings"),
        ):
            validator = TeamsWebhookValidator("test_app_id", "test_password")

            mock_request = Mock()
            mock_request.headers = {}

            result = await validator.validate_request(mock_request, b'{"test": "data"}')
            assert result is False

    @pytest.mark.asyncio
    async def test_validate_request_invalid_json(self):
        """Test validation fails with invalid JSON body."""
        with (
            patch("datahub.cloud.router.security.BotFrameworkAdapter"),
            patch("datahub.cloud.router.security.BotFrameworkAdapterSettings"),
        ):
            validator = TeamsWebhookValidator("test_app_id", "test_password")

            mock_request = Mock()
            mock_request.headers = {"Authorization": "Bearer test_token"}

            result = await validator.validate_request(mock_request, b"invalid json")
            assert result is False

    @pytest.mark.asyncio
    async def test_validate_request_bot_framework_validation_success(self):
        """Test successful validation using Bot Framework adapter."""
        mock_adapter = Mock()
        mock_adapter.process_activity = AsyncMock()

        with (
            patch(
                "datahub.cloud.router.security.BotFrameworkAdapter",
                return_value=mock_adapter,
            ),
            patch("datahub.cloud.router.security.BotFrameworkAdapterSettings"),
            patch("datahub.cloud.router.security.Activity") as mock_activity_class,
        ):
            mock_activity = Mock()
            mock_activity.type = "message"
            mock_activity_class.return_value.deserialize.return_value = mock_activity

            validator = TeamsWebhookValidator("test_app_id", "test_password")

            mock_request = Mock()
            mock_request.headers = {"Authorization": "Bearer test_token"}

            test_data = {"type": "message", "text": "hello"}
            body = json.dumps(test_data).encode()

            result = await validator.validate_request(mock_request, body)

            assert result is True
            mock_adapter.process_activity.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_request_bot_framework_validation_failure(self):
        """Test validation failure when Bot Framework adapter raises exception."""
        mock_adapter = Mock()
        mock_adapter.process_activity = AsyncMock(
            side_effect=Exception("JWT validation failed")
        )

        with (
            patch(
                "datahub.cloud.router.security.BotFrameworkAdapter",
                return_value=mock_adapter,
            ),
            patch("datahub.cloud.router.security.BotFrameworkAdapterSettings"),
            patch("datahub.cloud.router.security.Activity") as mock_activity_class,
        ):
            mock_activity = Mock()
            mock_activity.type = "message"
            mock_activity_class.return_value.deserialize.return_value = mock_activity

            validator = TeamsWebhookValidator("test_app_id", "test_password")

            mock_request = Mock()
            mock_request.headers = {"Authorization": "Bearer test_token"}

            test_data = {"type": "message", "text": "hello"}
            body = json.dumps(test_data).encode()

            result = await validator.validate_request(mock_request, body)

            assert result is False


class TestSecurityManagerTeamsIntegration:
    """Test SecurityManager integration with Teams validator."""

    def test_register_teams_validator_success(self):
        """Test successful Teams validator registration."""
        with patch(
            "datahub.cloud.router.security.TeamsWebhookValidator"
        ) as mock_validator_class:
            mock_validator = Mock()
            mock_validator_class.return_value = mock_validator

            security_manager = SecurityManager()
            security_manager.register_teams_validator("test_app_id", "test_password")

            mock_validator_class.assert_called_once_with("test_app_id", "test_password")
            assert security_manager._webhook_validators["teams"] == mock_validator

    def test_register_teams_validator_failure(self):
        """Test Teams validator registration failure handling."""
        with patch(
            "datahub.cloud.router.security.TeamsWebhookValidator",
            side_effect=ValueError("Invalid credentials"),
        ):
            security_manager = SecurityManager()

            with pytest.raises(ValueError, match="Invalid credentials"):
                security_manager.register_teams_validator("", "")

    @pytest.mark.asyncio
    async def test_validate_webhook_teams_success(self):
        """Test successful Teams webhook validation through SecurityManager."""
        mock_validator = Mock()
        mock_validator.validate_request = AsyncMock(return_value=True)

        with patch(
            "datahub.cloud.router.security.TeamsWebhookValidator",
            return_value=mock_validator,
        ):
            security_manager = SecurityManager()
            security_manager.register_teams_validator("test_app_id", "test_password")

            mock_request = Mock()
            mock_request.client.host = "127.0.0.1"

            # Should not raise exception
            await security_manager.validate_webhook(
                "teams", mock_request, b'{"test": "data"}'
            )

            mock_validator.validate_request.assert_called_once_with(
                mock_request, b'{"test": "data"}'
            )

    @pytest.mark.asyncio
    async def test_validate_webhook_teams_failure(self):
        """Test Teams webhook validation failure through SecurityManager."""
        mock_validator = Mock()
        mock_validator.validate_request = AsyncMock(return_value=False)

        with patch(
            "datahub.cloud.router.security.TeamsWebhookValidator",
            return_value=mock_validator,
        ):
            security_manager = SecurityManager()
            security_manager.register_teams_validator("test_app_id", "test_password")

            mock_request = Mock()
            mock_request.client.host = "127.0.0.1"

            from fastapi import HTTPException

            with pytest.raises(HTTPException) as exc_info:
                await security_manager.validate_webhook(
                    "teams", mock_request, b'{"test": "data"}'
                )

            assert exc_info.value.status_code == 401
            assert "Invalid teams webhook" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_validate_webhook_no_validator_registered(self):
        """Test webhook validation when no validator is registered."""
        security_manager = SecurityManager()

        mock_request = Mock()
        mock_request.client.host = "127.0.0.1"

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await security_manager.validate_webhook(
                "teams", mock_request, b'{"test": "data"}'
            )

        assert exc_info.value.status_code == 500
        assert "teams validation not available" in exc_info.value.detail

    def test_get_registered_integrations(self):
        """Test getting list of registered integrations."""
        with patch("datahub.cloud.router.security.TeamsWebhookValidator"):
            security_manager = SecurityManager()

            # Initially empty
            assert security_manager.get_registered_integrations() == []

            # After registering Teams
            security_manager.register_teams_validator("test_app_id", "test_password")
            assert security_manager.get_registered_integrations() == ["teams"]


class TestDataHubMultiTenantRouterTeamsIntegration:
    """Test DataHub Multi-Tenant Router Teams webhook integration."""

    @patch("datahub.cloud.router.security.TeamsWebhookValidator")
    def test_router_initialization_with_teams_credentials(self, mock_validator_class):
        """Test router initialization with Teams credentials."""
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        router = MultiTenantRouter()
        server = DataHubMultiTenantRouter(
            router,
            webhook_auth_enabled=True,
            teams_app_id="test_app_id",
            teams_app_password="test_password",
        )

        # Should have registered Teams validator
        mock_validator_class.assert_called_once_with("test_app_id", "test_password")
        assert "teams" in server.security_manager.get_registered_integrations()

    def test_router_initialization_without_teams_credentials(self):
        """Test router initialization without Teams credentials."""
        router = MultiTenantRouter()
        server = DataHubMultiTenantRouter(
            router,
            webhook_auth_enabled=True,
            teams_app_id=None,
            teams_app_password=None,
        )

        # Should not have registered Teams validator
        assert server.security_manager.get_registered_integrations() == []

    def test_router_initialization_webhook_auth_disabled(self):
        """Test router initialization with webhook auth disabled."""
        router = MultiTenantRouter()
        server = DataHubMultiTenantRouter(
            router,
            webhook_auth_enabled=False,
            teams_app_id="test_app_id",
            teams_app_password="test_password",
        )

        # Should not have registered Teams validator even with credentials
        assert server.security_manager.get_registered_integrations() == []
        assert server.webhook_auth_enabled is False

    @patch("datahub.cloud.router.security.TeamsWebhookValidator")
    def test_router_teams_validator_registration_failure(self, mock_validator_class):
        """Test router handling of Teams validator registration failure."""
        mock_validator_class.side_effect = ValueError("Invalid credentials")

        router = MultiTenantRouter()
        # Should not raise exception, just log warning
        server = DataHubMultiTenantRouter(
            router,
            webhook_auth_enabled=True,
            teams_app_id="invalid_id",
            teams_app_password="invalid_password",
        )

        # Should not have registered Teams validator
        assert server.security_manager.get_registered_integrations() == []

    @patch("datahub.cloud.router.security.TeamsWebhookValidator")
    def test_teams_webhook_endpoint_with_validation(self, mock_validator_class):
        """Test Teams webhook endpoint with validation enabled."""
        mock_validator = Mock()
        mock_validator.validate_request = AsyncMock(return_value=True)
        mock_validator_class.return_value = mock_validator

        router = MultiTenantRouter()
        server = DataHubMultiTenantRouter(
            router,
            webhook_auth_enabled=True,
            teams_app_id="test_app_id",
            teams_app_password="test_password",
        )

        client = TestClient(server.app)

        # Mock the router.route_event method
        router.route_event = AsyncMock(return_value={"status": "success"})

        response = client.post(
            "/public/teams/webhook",
            json={"tenantId": "test_tenant", "type": "message", "text": "hello"},
            headers={"Authorization": "Bearer test_token"},
        )

        assert response.status_code == 200
        assert response.json()["status"] == "success"

        # Verify validation was called
        mock_validator.validate_request.assert_called_once()

        # Verify routing was called
        router.route_event.assert_called_once()

    def test_teams_webhook_endpoint_without_validation(self):
        """Test Teams webhook endpoint with validation disabled."""
        router = MultiTenantRouter()
        server = DataHubMultiTenantRouter(router, webhook_auth_enabled=False)

        client = TestClient(server.app)

        # Mock the router.route_event method
        router.route_event = AsyncMock(return_value={"status": "success"})

        response = client.post(
            "/public/teams/webhook",
            json={"tenantId": "test_tenant", "type": "message", "text": "hello"},
        )

        assert response.status_code == 200
        assert response.json()["status"] == "success"

        # Verify routing was called without validation
        router.route_event.assert_called_once()
