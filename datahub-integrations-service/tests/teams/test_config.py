import os
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from datahub_integrations.teams.config import (
    TeamsAppDetails,
    TeamsConnection,
    _get_current_teams_config,
    _set_current_teams_config,
    _TeamsConfigManager,
)


class TestTeamsConfig:
    """Test cases for Teams configuration management."""

    def test_teams_app_details_creation(self) -> None:
        """Test creation of TeamsAppDetails with valid data."""
        details = TeamsAppDetails(
            app_id="test-app-id",
            app_password="test-password",
            app_tenant_id="test-tenant-id",
            tenant_id="test-tenant-id",
        )

        assert details.app_id == "test-app-id"
        assert details.app_password == "test-password"
        assert details.tenant_id == "test-tenant-id"

    def test_teams_connection_creation(self) -> None:
        """Test creation of TeamsConnection with valid data."""
        app_details = TeamsAppDetails(
            app_id="test-app-id",
            app_password="test-password",
            app_tenant_id="test-tenant-id",
            tenant_id="test-tenant-id",
        )

        connection = TeamsConnection(
            app_details=app_details, webhook_url="https://example.com/webhook"
        )

        assert connection.app_details == app_details
        assert connection.webhook_url == "https://example.com/webhook"

    def test_teams_connection_without_app_details(self) -> None:
        """Test TeamsConnection can be created without app_details."""
        connection = TeamsConnection(
            app_details=None, webhook_url="https://example.com/webhook"
        )

        assert connection.app_details is None
        assert connection.webhook_url == "https://example.com/webhook"

    @patch("datahub_integrations.teams.config.get_connection_json")
    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "test-app-id",
            "DATAHUB_TEAMS_APP_PASSWORD": "test-password",
            "DATAHUB_TEAMS_TENANT_ID": "test-app-tenant-id",
            "DATAHUB_TEAMS_WEBHOOK_URL": "https://example.com/webhook",
        },
    )
    def test_get_current_teams_config_with_existing_config(
        self, mock_get_connection: MagicMock
    ) -> None:
        """Test _get_current_teams_config when config exists."""
        mock_config_data = {
            "app_details": {
                "app_id": "test-app-id",
                "app_password": "test-password",
                "tenant_id": "test-customer-tenant-id",  # This is the customer's tenant_id from OAuth
            },
            "webhook_url": "https://example.com/webhook",
        }
        mock_get_connection.return_value = mock_config_data

        result = _get_current_teams_config()

        assert result is not None
        assert result.app_details is not None
        assert result.app_details.app_id == "test-app-id"  # From env var
        assert result.app_details.app_password == "test-password"  # From env var
        assert result.app_details.app_tenant_id == "test-app-tenant-id"  # From env var
        assert result.app_details.tenant_id == "test-customer-tenant-id"  # From DataHub
        assert result.webhook_url == "https://example.com/webhook"

    @patch("datahub_integrations.teams.config.get_connection_json")
    @patch.dict(
        os.environ,
        {
            "DATAHUB_TEAMS_APP_ID": "test-app-id",
            "DATAHUB_TEAMS_APP_PASSWORD": "test-password",
            "DATAHUB_TEAMS_TENANT_ID": "test-app-tenant-id",
            "DATAHUB_TEAMS_WEBHOOK_URL": "https://example.com/webhook",
        },
    )
    def test_get_current_teams_config_no_existing_config(
        self, mock_get_connection: MagicMock
    ) -> None:
        """Test _get_current_teams_config when no config exists."""
        mock_get_connection.return_value = None

        result = _get_current_teams_config()

        assert result is not None
        assert isinstance(result, TeamsConnection)
        assert result.app_details is not None
        assert result.app_details.app_id == "test-app-id"  # From env var
        assert result.app_details.app_password == "test-password"  # From env var
        assert result.app_details.app_tenant_id == "test-app-tenant-id"  # From env var
        assert result.app_details.tenant_id is None  # No customer OAuth yet
        assert result.webhook_url == "https://example.com/webhook"

    @patch("datahub_integrations.teams.config.save_connection_json")
    def test_set_current_teams_config(self, mock_save_connection: MagicMock) -> None:
        """Test _set_current_teams_config saves config."""
        config = TeamsConnection(
            app_details=TeamsAppDetails(
                app_id="test-app-id",
                app_password="test-password",
                app_tenant_id="test-tenant-id",
                tenant_id="test-tenant-id",
            ),
            webhook_url="https://example.com/webhook",
        )

        _set_current_teams_config(config)

        mock_save_connection.assert_called_once()
        call_args = mock_save_connection.call_args
        assert call_args[1]["config"] == config

    def test_teams_config_manager_initialization(self) -> None:
        """Test TeamsConfigManager can be initialized."""
        manager = _TeamsConfigManager()
        assert manager._config is None

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_teams_config_manager_get_config(self, mock_get_config: MagicMock) -> None:
        """Test TeamsConfigManager.get_config()."""
        mock_config = TeamsConnection()
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()
        result = manager.get_config()

        assert result == mock_config
        mock_get_config.assert_called_once()

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_teams_config_manager_get_config_cached(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test TeamsConfigManager.get_config() uses cache."""
        # Create a valid mock config with all required fields
        mock_config = TeamsConnection(
            app_details=TeamsAppDetails(
                app_id="test-app-id",
                app_password="test-password",
                app_tenant_id="test-app-tenant-id",
                tenant_id="test-customer-tenant-id",  # This makes it valid
            ),
            webhook_url="https://example.com/webhook",
        )
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()
        result1 = manager.get_config()
        result2 = manager.get_config()

        assert result1 == result2 == mock_config
        mock_get_config.assert_called_once()  # Should only call once due to caching

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_teams_config_manager_force_refresh(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test TeamsConfigManager.get_config(force_refresh=True)."""
        mock_config = TeamsConnection()
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()
        result1 = manager.get_config()
        result2 = manager.get_config(force_refresh=True)

        assert result1 == result2 == mock_config
        assert mock_get_config.call_count == 2  # Should call twice due to force_refresh

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_teams_config_manager_reload(self, mock_get_config: MagicMock) -> None:
        """Test TeamsConfigManager.reload()."""
        mock_config = TeamsConnection()
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()
        result = manager.reload()

        assert result == mock_config
        mock_get_config.assert_called_once()

    @patch("datahub_integrations.teams.config._set_current_teams_config")
    def test_teams_config_manager_save_config(self, mock_set_config: MagicMock) -> None:
        """Test TeamsConfigManager.save_config()."""
        config = TeamsConnection(webhook_url="https://example.com/webhook")

        manager = _TeamsConfigManager()
        manager.save_config(config)

        assert manager._config == config
        mock_set_config.assert_called_once_with(config)

    def test_teams_app_details_serialization(self) -> None:
        """Test TeamsAppDetails can be serialized/deserialized."""
        details = TeamsAppDetails(
            app_id="test-app-id",
            app_password="test-password",
            app_tenant_id="test-tenant-id",
            tenant_id="test-tenant-id",
        )

        # Test that it can be converted to dict (for JSON serialization)
        details_dict = details.model_dump()
        assert details_dict["app_id"] == "test-app-id"
        assert details_dict["app_password"] == "test-password"
        assert details_dict["tenant_id"] == "test-tenant-id"

        # Test that it can be recreated from dict
        recreated = TeamsAppDetails.model_validate(details_dict)
        assert recreated.app_id == details.app_id
        assert recreated.app_password == details.app_password
        assert recreated.tenant_id == details.tenant_id

    def test_teams_connection_serialization(self) -> None:
        """Test TeamsConnection can be serialized/deserialized."""
        connection = TeamsConnection(
            app_details=TeamsAppDetails(
                app_id="test-app-id",
                app_password="test-password",
                app_tenant_id="test-tenant-id",
                tenant_id="test-tenant-id",
            ),
            webhook_url="https://example.com/webhook",
        )

        # Test serialization
        connection_dict = connection.model_dump()
        assert connection_dict["webhook_url"] == "https://example.com/webhook"
        assert connection_dict["app_details"]["app_id"] == "test-app-id"

        # Test deserialization
        recreated = TeamsConnection.model_validate(connection_dict)
        assert recreated.webhook_url == connection.webhook_url
        assert recreated.app_details is not None
        assert connection.app_details is not None
        assert recreated.app_details.app_id == connection.app_details.app_id


class TestTeamsConfigTimeBasedThrottling:
    """Test cases for time-based throttling in Teams configuration management."""

    def _create_valid_mock_config(self) -> TeamsConnection:
        """Helper method to create a valid mock config for testing."""
        return TeamsConnection(
            app_details=TeamsAppDetails(
                app_id="test-app-id",
                app_password="test-password",
                app_tenant_id="test-app-tenant-id",
                tenant_id="test-customer-tenant-id",
            ),
            webhook_url="https://example.com/webhook",
        )

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_time_based_throttling_first_call(self, mock_get_config: MagicMock) -> None:
        """Test that first call to get_config() always loads from DataHub."""
        mock_config = self._create_valid_mock_config()
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()
        result = manager.get_config()

        assert result == mock_config
        assert manager._config == mock_config
        assert manager._last_credentials_refresh_attempt is not None
        mock_get_config.assert_called_once()

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_time_based_throttling_within_5_minutes(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test that calls within 5 minutes use cached config."""
        mock_config = self._create_valid_mock_config()
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()

        # First call - should load from DataHub
        result1 = manager.get_config()

        # Second call within 5 minutes - should use cache
        result2 = manager.get_config()

        assert result1 == result2 == mock_config
        assert (
            mock_get_config.call_count == 1
        )  # Should only call once due to throttling

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    @freeze_time("2024-01-01 12:00:00")
    def test_time_based_throttling_after_5_minutes(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test that calls after 5 minutes reload from DataHub."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()

        # First call at 12:00:00 - should load from DataHub
        result1 = manager.get_config()

        # Travel forward 6 minutes to 12:06:00
        with freeze_time("2024-01-01 12:06:00"):
            # Second call after 5 minutes - should reload from DataHub
            result2 = manager.get_config()

        assert result1 == result2 == mock_config
        assert (
            mock_get_config.call_count == 2
        )  # Should call twice due to time-based refresh

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_force_refresh_bypasses_throttling(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test that force_refresh=True bypasses time-based throttling."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()

        # First call - should load from DataHub
        result1 = manager.get_config()

        # Second call with force_refresh=True - should reload even within 5 minutes
        result2 = manager.get_config(force_refresh=True)

        assert result1 == result2 == mock_config
        assert mock_get_config.call_count == 2  # Should call twice due to force_refresh

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_force_refresh_updates_timestamp(self, mock_get_config: MagicMock) -> None:
        """Test that force_refresh updates the last refresh timestamp."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()

        # First call
        manager.get_config()
        first_timestamp = manager._last_credentials_refresh_attempt

        # Force refresh
        manager.get_config(force_refresh=True)
        second_timestamp = manager._last_credentials_refresh_attempt

        assert first_timestamp is not None
        assert second_timestamp is not None
        assert second_timestamp > first_timestamp

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_reload_method_updates_timestamp(self, mock_get_config: MagicMock) -> None:
        """Test that reload() method updates the last refresh timestamp."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()

        # First call
        manager.get_config()
        first_timestamp = manager._last_credentials_refresh_attempt

        # Reload
        manager.reload()
        second_timestamp = manager._last_credentials_refresh_attempt

        assert first_timestamp is not None
        assert second_timestamp is not None
        assert second_timestamp > first_timestamp

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_multiple_calls_within_throttle_window(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test that multiple calls within throttle window only load once."""
        mock_config = self._create_valid_mock_config()
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()

        # Make multiple calls within throttle window
        for _ in range(5):
            result = manager.get_config()
            assert result == mock_config

        assert mock_get_config.call_count == 1  # Should only call once

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    def test_config_manager_initialization_timestamp(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test that config manager initializes with None timestamp."""
        manager = _TeamsConfigManager()

        assert manager._last_credentials_refresh_attempt is None
        assert manager._config is None

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    @freeze_time("2024-01-01 12:00:00")
    def test_time_based_throttling_edge_case_exactly_5_minutes(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test edge case where exactly 5 minutes have passed."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()

        # First call at 12:00:00
        manager.get_config()

        # Travel forward exactly 5 minutes to 12:05:00
        with freeze_time("2024-01-01 12:05:00"):
            # Second call - should reload (5 minutes is the threshold)
            manager.get_config()

        assert mock_get_config.call_count == 2  # Should call twice

    @patch("datahub_integrations.teams.config._get_current_teams_config")
    @freeze_time("2024-01-01 12:00:00")
    def test_time_based_throttling_just_under_5_minutes(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test edge case where just under 5 minutes have passed."""
        mock_config = self._create_valid_mock_config()
        mock_get_config.return_value = mock_config

        manager = _TeamsConfigManager()

        # First call at 12:00:00
        manager.get_config()

        # Travel forward to 12:04:59 (just under 5 minutes)
        with freeze_time("2024-01-01 12:04:59"):
            # Second call - should use cache
            manager.get_config()

        assert mock_get_config.call_count == 1  # Should only call once
