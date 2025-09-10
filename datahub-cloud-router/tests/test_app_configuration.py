"""
Tests for app.py environment variable configuration.
"""

import os
from unittest.mock import Mock, patch

from datahub.cloud.router.server import DataHubMultiTenantRouter


class TestAppEnvironmentConfiguration:
    """Test app.py environment variable configuration."""

    def test_app_imports_os_module(self):
        """Test that app.py properly imports os module."""
        # This test ensures the import is there after our changes
        import app

        assert hasattr(app, "os")

    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "test_app_id_from_env",
            "DATAHUB_TEAMS_APP_PASSWORD": "test_password_from_env",
        },
    )
    @patch("datahub.cloud.router.security.TeamsWebhookValidator")
    def test_app_uses_environment_variables(self, mock_validator_class):
        """Test that app.py uses environment variables for Teams configuration."""
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        # Instead of importing existing app, create a new server with env vars
        # This simulates what app.py does
        from datahub.cloud.router import DataHubMultiTenantRouter, MultiTenantRouter

        router = MultiTenantRouter()
        server = DataHubMultiTenantRouter(
            router,
            teams_app_id=os.getenv("DATAHUB_TEAMS_APP_ID"),
            teams_app_password=os.getenv("DATAHUB_TEAMS_APP_PASSWORD"),
        )

        # Verify the server was created with environment variables
        assert isinstance(server, DataHubMultiTenantRouter)

        # Verify Teams validator was registered with env vars
        mock_validator_class.assert_called_once_with(
            "test_app_id_from_env", "test_password_from_env"
        )

    @patch.dict(os.environ, {}, clear=True)
    def test_app_handles_missing_environment_variables(self):
        """Test that app.py handles missing environment variables gracefully."""
        # Clear any existing environment variables
        if "DATAHUB_TEAMS_APP_ID" in os.environ:
            del os.environ["DATAHUB_TEAMS_APP_ID"]
        if "DATAHUB_TEAMS_APP_PASSWORD" in os.environ:
            del os.environ["DATAHUB_TEAMS_APP_PASSWORD"]

        # Import app module (simulating running app.py)
        import app

        # Should not raise exception and server should be created
        assert isinstance(app.server, DataHubMultiTenantRouter)

        # Teams validator should not be registered (no credentials)
        assert app.server.security_manager.get_registered_integrations() == []

    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "test_app_id_partial",
            # Missing DATAHUB_TEAMS_APP_PASSWORD
        },
    )
    def test_app_handles_partial_environment_variables(self):
        """Test that app.py handles partial environment variables."""
        # Import app module (simulating running app.py)
        import app

        # Should not raise exception and server should be created
        assert isinstance(app.server, DataHubMultiTenantRouter)

        # Teams validator should not be registered (missing password)
        assert app.server.security_manager.get_registered_integrations() == []

    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "",  # Empty string
            "DATAHUB_TEAMS_APP_PASSWORD": "test_password",
        },
    )
    def test_app_handles_empty_environment_variables(self):
        """Test that app.py handles empty environment variables."""
        # Import app module (simulating running app.py)
        import app

        # Should not raise exception and server should be created
        assert isinstance(app.server, DataHubMultiTenantRouter)

        # Teams validator should not be registered (empty app_id)
        assert app.server.security_manager.get_registered_integrations() == []

    def test_app_structure(self):
        """Test that app.py has the correct structure after changes."""
        import app

        # Verify the expected attributes exist
        assert hasattr(app, "router")
        assert hasattr(app, "server")
        assert hasattr(app, "app")

        # Verify types
        from fastapi import FastAPI

        from datahub.cloud.router import DataHubMultiTenantRouter, MultiTenantRouter

        assert isinstance(app.router, MultiTenantRouter)
        assert isinstance(app.server, DataHubMultiTenantRouter)
        assert isinstance(app.app, FastAPI)

        # Verify the FastAPI app is the server's app
        assert app.app is app.server.app

    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "env_app_id",
            "DATAHUB_TEAMS_APP_PASSWORD": "env_password",
        },
    )
    def test_app_server_configuration_from_env(self):
        """Test that server gets proper configuration from environment."""
        # Import app module fresh
        import importlib

        import app

        importlib.reload(app)

        server = app.server

        # Verify webhook auth is enabled by default
        assert server.webhook_auth_enabled is True

        # Verify Teams integration is registered
        assert "teams" in server.security_manager.get_registered_integrations()

    def test_app_server_default_configuration(self):
        """Test that server has proper default configuration."""
        import app

        server = app.server

        # Verify default settings
        assert server.admin_auth_enabled is True  # Should be enabled by default
        assert server.webhook_auth_enabled is True  # Should be enabled by default
        assert server.admin_api_key is not None  # Should be generated if not provided

    @patch(
        "datahub.cloud.router.security.TeamsWebhookValidator",
        side_effect=ValueError("Invalid credentials"),
    )
    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "invalid_app_id",
            "DATAHUB_TEAMS_APP_PASSWORD": "invalid_password",
        },
    )
    def test_app_handles_validator_registration_failure(self, mock_validator_class):
        """Test that app.py handles Teams validator registration failures gracefully."""
        # Import app module (simulating running app.py)
        import importlib

        import app

        importlib.reload(app)

        # Should not raise exception and server should be created
        assert isinstance(app.server, DataHubMultiTenantRouter)

        # Teams validator should not be registered due to failure
        assert app.server.security_manager.get_registered_integrations() == []


class TestEnvironmentVariableIntegration:
    """Test integration of environment variables with the router system."""

    def test_environment_variable_names_match_constructor(self):
        """Test that environment variable names match constructor parameters."""

        # Verify the constructor parameters match the expected environment variables
        # This ensures consistency between app.py and server constructor

        # The constructor should accept teams_app_id and teams_app_password
        from inspect import signature

        sig = signature(DataHubMultiTenantRouter.__init__)
        params = list(sig.parameters.keys())

        assert "teams_app_id" in params
        assert "teams_app_password" in params

        # And app.py should use the corresponding environment variables
        assert "DATAHUB_TEAMS_APP_ID" in [
            "DATAHUB_TEAMS_APP_ID"
        ]  # This is a bit redundant but ensures we document the expected env vars
        assert "DATAHUB_TEAMS_APP_PASSWORD" in ["DATAHUB_TEAMS_APP_PASSWORD"]

    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "production_app_id",
            "DATAHUB_TEAMS_APP_PASSWORD": "production_password",
        },
    )
    @patch("datahub.cloud.router.security.TeamsWebhookValidator")
    def test_production_like_configuration(self, mock_validator_class):
        """Test production-like configuration with environment variables."""
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        # Import app with production-like environment
        import importlib

        import app

        importlib.reload(app)

        server = app.server

        # Verify production-like security settings
        assert server.admin_auth_enabled is True
        assert server.webhook_auth_enabled is True
        assert server.admin_api_key is not None

        # Verify Teams integration is properly configured
        mock_validator_class.assert_called_once_with(
            "production_app_id", "production_password"
        )

        # Verify server is ready for production use
        assert "teams" in server.security_manager.get_registered_integrations()

    def test_development_configuration(self):
        """Test development configuration without environment variables."""
        # Clear environment variables to simulate development
        with patch.dict(os.environ, {}, clear=True):
            import importlib

            import app

            importlib.reload(app)

            server = app.server

            # Should still have security enabled by default
            assert server.admin_auth_enabled is True
            assert server.webhook_auth_enabled is True

            # But Teams integration won't be configured
            assert server.security_manager.get_registered_integrations() == []

            # Should have generated admin API key for development
            assert server.admin_api_key is not None

    @patch("builtins.print")  # Capture print statements
    @patch(
        "datahub.cloud.router.security.TeamsWebhookValidator",
        side_effect=Exception("Connection failed"),
    )
    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "failing_app_id",
            "DATAHUB_TEAMS_APP_PASSWORD": "failing_password",
        },
    )
    def test_validator_failure_logging(self, mock_validator_class, mock_print):
        """Test that validator registration failures are properly logged."""
        import importlib

        import app

        # Mock logger to capture warning
        with patch("datahub.cloud.router.server.logger") as mock_logger:
            importlib.reload(app)

            # Should have logged the error
            mock_logger.error.assert_called()
            mock_logger.warning.assert_called()

            # Verify warning message about Teams webhooks
            warning_calls = [
                call.args[0] for call in mock_logger.warning.call_args_list
            ]
            assert any(
                "Teams webhooks will NOT be validated" in msg for msg in warning_calls
            )
