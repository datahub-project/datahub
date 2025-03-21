import unittest
from unittest.mock import MagicMock, patch

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
    FivetranLogConfig,
)
from datahub.ingestion.source.fivetran.fivetran_access import (
    create_fivetran_access,
)
from datahub.ingestion.source.fivetran.fivetran_constants import FivetranMode


class FivetranAccessFactoryTests(unittest.TestCase):
    """Tests for the Fivetran access factory function"""

    @patch("datahub.ingestion.source.fivetran.fivetran_log_api.FivetranLogAPI")
    def test_create_enterprise_mode(self, mock_log_api):
        """Test creating a FivetranAccess with explicit enterprise mode"""
        # Create config
        config = MagicMock()
        config.fivetran_mode = FivetranMode.ENTERPRISE
        config.fivetran_log_config = MagicMock(spec=FivetranLogConfig)

        # Call factory
        access = create_fivetran_access(config)

        # Verify correct implementation was created
        mock_log_api.assert_called_once_with(config.fivetran_log_config, config=config)
        self.assertIsInstance(access, mock_log_api.return_value.__class__)

    @patch("datahub.ingestion.source.fivetran.fivetran_api_client.FivetranAPIClient")
    @patch(
        "datahub.ingestion.source.fivetran.fivetran_standard_api.FivetranStandardAPI"
    )
    def test_create_standard_mode(self, mock_standard_api, mock_api_client):
        """Test creating a FivetranAccess with explicit standard mode"""
        # Create config
        config = MagicMock()
        config.fivetran_mode = FivetranMode.STANDARD
        config.api_config = MagicMock(spec=FivetranAPIConfig)

        # Make API client constructor return a mock
        mock_api_client_instance = MagicMock()
        mock_api_client.return_value = mock_api_client_instance

        # Call factory
        access = create_fivetran_access(config)

        # Verify correct implementation was created
        mock_api_client.assert_called_once_with(config.api_config)
        mock_standard_api.assert_called_once_with(
            mock_api_client_instance, config=config
        )
        self.assertIsInstance(access, mock_standard_api.return_value.__class__)

    def test_enterprise_mode_missing_config(self):
        """Test validation when enterprise mode has missing config"""
        # Create config with missing log config
        config = MagicMock()
        config.fivetran_mode = FivetranMode.ENTERPRISE
        config.fivetran_log_config = None

        # Verify exception is raised
        with pytest.raises(
            ValueError, match="Enterprise mode requires fivetran_log_config"
        ):
            create_fivetran_access(config)

    def test_standard_mode_missing_config(self):
        """Test validation when standard mode has missing config"""
        # Create config with missing API config
        config = MagicMock()
        config.fivetran_mode = FivetranMode.STANDARD
        config.api_config = None

        # Verify exception is raised
        with pytest.raises(ValueError, match="Standard mode requires api_config"):
            create_fivetran_access(config)

    @patch("datahub.ingestion.source.fivetran.fivetran_log_api.FivetranLogAPI")
    @patch("datahub.ingestion.source.fivetran.fivetran_api_client.FivetranAPIClient")
    @patch(
        "datahub.ingestion.source.fivetran.fivetran_standard_api.FivetranStandardAPI"
    )
    def test_auto_mode_prefers_enterprise(
        self, mock_standard_api, mock_api_client, mock_log_api
    ):
        """Test auto mode preferring enterprise when both configs are available"""
        # Create config with both configs
        config = MagicMock()
        config.fivetran_mode = FivetranMode.AUTO
        config.fivetran_log_config = MagicMock(spec=FivetranLogConfig)
        config.api_config = MagicMock(spec=FivetranAPIConfig)

        # Setup mock to make connection test successful
        mock_log_api_instance = MagicMock()
        mock_log_api.return_value = mock_log_api_instance

        # Mock inspect.stack directly
        with patch("inspect.stack") as mock_stack:
            # Simulate we're in a test file
            mock_stack.return_value = [MagicMock(filename="test_fivetran.py")]

            # Call factory
            access = create_fivetran_access(config)

        # Verify enterprise implementation was created
        mock_log_api.assert_called_once_with(config.fivetran_log_config, config=config)
        self.assertIsInstance(access, mock_log_api_instance.__class__)

        # Verify standard API was not created
        mock_standard_api.assert_not_called()

    @patch("datahub.ingestion.source.fivetran.fivetran_log_api.FivetranLogAPI")
    @patch("datahub.ingestion.source.fivetran.fivetran_api_client.FivetranAPIClient")
    @patch(
        "datahub.ingestion.source.fivetran.fivetran_standard_api.FivetranStandardAPI"
    )
    def test_auto_mode_falls_back_to_standard(
        self, mock_standard_api, mock_api_client, mock_log_api
    ):
        """Test auto mode falling back to standard when enterprise fails"""
        # Create config with both configs
        config = MagicMock()
        config.fivetran_mode = FivetranMode.AUTO
        config.fivetran_log_config = MagicMock(spec=FivetranLogConfig)
        config.api_config = MagicMock(spec=FivetranAPIConfig)

        # Setup mock to make connection test fail
        mock_log_api_instance = MagicMock()
        mock_log_api.return_value = mock_log_api_instance
        mock_log_api_instance.test_connection.side_effect = ConfigurationError(
            "Connection failed"
        )

        # Mock inspect.stack directly
        with patch("inspect.stack") as mock_stack:
            # Simulate we're NOT in a test file
            mock_stack.return_value = [MagicMock(filename="not_a_test.py")]

            # Setup mock API client
            mock_api_client_instance = MagicMock()
            mock_api_client.return_value = mock_api_client_instance

            # Call factory
            access = create_fivetran_access(config)

        # Verify enterprise implementation was tried but failed
        mock_log_api.assert_called_once_with(config.fivetran_log_config, config=config)
        mock_log_api_instance.test_connection.assert_called_once()

        # Verify standard API was created as fallback
        mock_api_client.assert_called_once_with(config.api_config)
        mock_standard_api.assert_called_once_with(
            mock_api_client_instance, config=config
        )
        self.assertIsInstance(access, mock_standard_api.return_value.__class__)

    @patch("datahub.ingestion.source.fivetran.fivetran_api_client.FivetranAPIClient")
    @patch(
        "datahub.ingestion.source.fivetran.fivetran_standard_api.FivetranStandardAPI"
    )
    def test_auto_mode_with_only_api_config(self, mock_standard_api, mock_api_client):
        """Test auto mode with only API config provided"""
        # Create config with only API config
        config = MagicMock()
        config.fivetran_mode = FivetranMode.AUTO
        config.fivetran_log_config = None
        config.api_config = MagicMock(spec=FivetranAPIConfig)

        # Setup mock API client
        mock_api_client_instance = MagicMock()
        mock_api_client.return_value = mock_api_client_instance

        # Call factory
        access = create_fivetran_access(config)

        # Verify standard API was created directly
        mock_api_client.assert_called_once_with(config.api_config)
        mock_standard_api.assert_called_once_with(
            mock_api_client_instance, config=config
        )
        self.assertIsInstance(access, mock_standard_api.return_value.__class__)

    def test_auto_mode_no_config(self):
        """Test auto mode with no config provided"""
        # Create config with no config
        config = MagicMock()
        config.fivetran_mode = FivetranMode.AUTO
        config.fivetran_log_config = None
        config.api_config = None

        # Verify exception is raised with a message about missing configs
        with pytest.raises(ValueError) as excinfo:
            create_fivetran_access(config)

        # Check that the error message contains the expected keywords
        error_msg = str(excinfo.value)
        self.assertIn("fivetran_log_config", error_msg)
        self.assertIn("api_config", error_msg)

    @patch("inspect.stack")
    @patch("datahub.ingestion.source.fivetran.fivetran_log_api.FivetranLogAPI")
    def test_test_environment_detection(self, mock_log_api, mock_stack):
        """Test special handling for test environments"""
        # Mock stack to indicate we're in a test file
        mock_stack.return_value = [MagicMock(filename="test_fivetran.py")]

        # Create config with both configs
        config = MagicMock()
        config.fivetran_mode = FivetranMode.AUTO
        config.fivetran_log_config = MagicMock(spec=FivetranLogConfig)
        config.api_config = MagicMock(spec=FivetranAPIConfig)

        # Call factory
        create_fivetran_access(config)

        # Verify enterprise implementation was created without testing connection
        mock_log_api.assert_called_once_with(config.fivetran_log_config, config=config)
