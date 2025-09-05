from unittest.mock import MagicMock, patch

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
    def test_get_current_teams_config_with_existing_config(
        self, mock_get_connection: MagicMock
    ) -> None:
        """Test _get_current_teams_config when config exists."""
        mock_config_data = {
            "app_details": {
                "app_id": "test-app-id",
                "app_password": "test-password",
                "tenant_id": "test-tenant-id",
            },
            "webhook_url": "https://example.com/webhook",
        }
        mock_get_connection.return_value = mock_config_data

        result = _get_current_teams_config()

        assert result is not None
        assert result.app_details is not None
        assert result.app_details.app_id == "test-app-id"
        assert result.webhook_url == "https://example.com/webhook"

    @patch("datahub_integrations.teams.config.get_connection_json")
    def test_get_current_teams_config_no_existing_config(
        self, mock_get_connection: MagicMock
    ) -> None:
        """Test _get_current_teams_config when no config exists."""
        mock_get_connection.return_value = None

        result = _get_current_teams_config()

        assert result is not None
        assert isinstance(result, TeamsConnection)
        assert result.app_details is None
        assert result.webhook_url is None

    @patch("datahub_integrations.teams.config.save_connection_json")
    def test_set_current_teams_config(self, mock_save_connection: MagicMock) -> None:
        """Test _set_current_teams_config saves config."""
        config = TeamsConnection(
            app_details=TeamsAppDetails(
                app_id="test-app-id",
                app_password="test-password",
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
        mock_config = TeamsConnection()
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
