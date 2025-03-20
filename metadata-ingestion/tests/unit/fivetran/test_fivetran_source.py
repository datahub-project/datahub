import unittest
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
    FivetranSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.data_classes import (
    Connector,
)
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.ingestion.source.fivetran.fivetran_access import create_fivetran_access
from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient
from datahub.ingestion.source.fivetran.fivetran_constants import FivetranMode
from datahub.ingestion.source.fivetran.fivetran_standard_api import FivetranStandardAPI


class FivetranAPIClientTest(unittest.TestCase):
    """Tests for the Fivetran API client class"""

    @patch("requests.Session.request")
    def test_list_connectors(self, mock_request):
        """Test listing connectors with pagination handling"""
        # Setup mock responses
        mock_response1 = MagicMock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {
            "data": {
                "items": [{"id": "connector1", "name": "My Connector 1"}],
                "next_cursor": "cursor1",
            }
        }

        mock_response2 = MagicMock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {
            "data": {
                "items": [{"id": "connector2", "name": "My Connector 2"}],
                "next_cursor": None,
            }
        }

        # Configure mock to return different responses
        mock_request.side_effect = [mock_response1, mock_response2]

        # Create client and call method
        client = FivetranAPIClient(FivetranAPIConfig(api_key="test", api_secret="test"))
        result = client.list_connectors()

        # Verify results
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], "connector1")
        self.assertEqual(result[1]["id"], "connector2")

        # Verify the API was called with correct parameters
        self.assertEqual(mock_request.call_count, 2)

    @patch("requests.Session.request")
    def test_get_user_email(self, mock_request):
        """Test fetching user email from API"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "id": "user1",
                "email": "test@example.com",
                "given_name": "Test",
                "family_name": "User",
            }
        }

        mock_request.return_value = mock_response

        # Create client and call method
        client = FivetranAPIClient(FivetranAPIConfig(api_key="test", api_secret="test"))
        email = client.get_user("user1").get("email")

        # Verify result
        self.assertEqual(email, "test@example.com")

        # Verify the API was called with correct URL
        mock_request.assert_called_once()
        self.assertIn("/users/user1", mock_request.call_args[0][1])

    @patch("requests.Session.request")
    def test_extract_connector_metadata(self, mock_request):
        """Test extracting connector metadata from API response"""
        # Setup test data
        api_connector = {
            "id": "connector1",
            "name": "My Connector",
            "service": "postgres",
            "created_by": "user1",
            "paused": False,
            "schedule": {"sync_frequency": 1440},
            "group": {"id": "group1"},
        }

        sync_history = [
            {
                "id": "sync1",
                "started_at": "2023-09-20T06:37:32.606Z",
                "completed_at": "2023-09-20T06:38:05.056Z",
                "status": "COMPLETED",
            }
        ]

        # Create client
        client = FivetranAPIClient(FivetranAPIConfig(api_key="test", api_secret="test"))

        # Mock detect_destination_platform to avoid actual API calls
        with patch.object(
            client, "detect_destination_platform", return_value="snowflake"
        ):
            with patch.object(
                client, "get_destination_database", return_value="TEST_DB"
            ):
                # Call method under test
                connector = client.extract_connector_metadata(
                    api_connector, sync_history
                )

                # Verify results
                self.assertEqual(connector.connector_id, "connector1")
                self.assertEqual(connector.connector_name, "My Connector")
                self.assertEqual(connector.connector_type, "postgres")
                self.assertEqual(connector.destination_id, "group1")
                self.assertEqual(connector.user_id, "user1")
                self.assertEqual(len(connector.jobs), 1)
                self.assertEqual(
                    connector.additional_properties["destination_platform"], "snowflake"
                )
                self.assertEqual(
                    connector.additional_properties["destination_database"], "TEST_DB"
                )


class FivetranAccessTest(unittest.TestCase):
    """Tests for FivetranAccess and mode selection"""

    @patch("datahub.ingestion.source.fivetran.fivetran_log_api.FivetranLogAPI")
    @patch(
        "datahub.ingestion.source.fivetran.fivetran_standard_api.FivetranStandardAPI"
    )
    def test_create_fivetran_access_enterprise(self, mock_standard_api, mock_log_api):
        """Test creating a FivetranAccess instance in enterprise mode"""
        # Create a config with fivetran_log_config
        config = MagicMock()
        config.fivetran_mode = FivetranMode.ENTERPRISE
        config.fivetran_log_config = MagicMock()

        # Call the factory method
        access = create_fivetran_access(config)

        # Verify that FivetranLogAPI was created
        mock_log_api.assert_called_once()
        self.assertIsInstance(access, mock_log_api.return_value.__class__)

    @patch("datahub.ingestion.source.fivetran.fivetran_api_client.FivetranAPIClient")
    @patch(
        "datahub.ingestion.source.fivetran.fivetran_standard_api.FivetranStandardAPI"
    )
    def test_create_fivetran_access_standard(self, mock_standard_api, mock_api_client):
        """Test creating a FivetranAccess instance in standard mode"""
        # Create a config with api_config
        config = MagicMock()
        config.fivetran_mode = FivetranMode.STANDARD
        config.api_config = MagicMock()

        # Return mock API client from constructor
        mock_api_client.return_value = MagicMock()

        # Call the factory method
        access = create_fivetran_access(config)

        # Verify that FivetranStandardAPI was created
        mock_standard_api.assert_called_once()
        self.assertIsInstance(access, mock_standard_api.return_value.__class__)

    @patch("datahub.ingestion.source.fivetran.fivetran_log_api.FivetranLogAPI")
    @patch("datahub.ingestion.source.fivetran.fivetran_api_client.FivetranAPIClient")
    @patch(
        "datahub.ingestion.source.fivetran.fivetran_standard_api.FivetranStandardAPI"
    )
    def test_create_fivetran_access_auto_prefers_enterprise(
        self, mock_standard_api, mock_api_client, mock_log_api
    ):
        """Test auto mode preferring enterprise when both configs are available"""
        # Create a config with both configs
        config = MagicMock()
        config.fivetran_mode = FivetranMode.AUTO
        config.fivetran_log_config = MagicMock()
        config.api_config = MagicMock()

        # Setup mock to avoid actual connection test
        mock_log_api_instance = MagicMock()
        mock_log_api.return_value = mock_log_api_instance
        mock_log_api_instance.test_connection.return_value = True

        # Return mock API client from constructor
        mock_api_client.return_value = MagicMock()

        # Call the factory method
        access = create_fivetran_access(config)

        # Verify that FivetranLogAPI was created
        mock_log_api.assert_called_once()
        self.assertIsInstance(access, mock_log_api_instance.__class__)

    @patch("inspect.stack")
    @patch("datahub.ingestion.source.fivetran.fivetran_log_api.FivetranLogAPI")
    @patch(
        "datahub.ingestion.source.fivetran.fivetran_standard_api.FivetranStandardAPI"
    )
    def test_create_fivetran_access_auto_fallback_to_standard(
        self, mock_standard_api, mock_log_api, mock_stack
    ):
        """Test auto mode falling back to standard when enterprise fails"""
        # Import here to avoid circular imports
        from datahub.ingestion.source.fivetran.fivetran_access import (
            create_fivetran_access,
        )

        # Create a config with both configs
        config = MagicMock()
        config.fivetran_mode = FivetranMode.AUTO
        config.fivetran_log_config = MagicMock()
        config.api_config = MagicMock()

        # Set up mocks
        mock_log_api_instance = MagicMock()
        mock_log_api.return_value = mock_log_api_instance

        # Simulate we're not in a test environment
        mock_stack.return_value = [MagicMock(filename="production.py")]

        # Make enterprise mode fail
        mock_log_api_instance.test_connection.side_effect = Exception(
            "Connection failed"
        )

        # Call the factory method - should fallback to standard API
        create_fivetran_access(config)

        # Verify standard API was created
        mock_standard_api.assert_called_once()


class FivetranConfigTest(unittest.TestCase):
    """Tests for FivetranSourceConfig validation"""

    def test_config_validation_enterprise_mode(self):
        """Test config validation for enterprise mode"""
        with pytest.raises(
            ValueError, match="Enterprise mode requires 'fivetran_log_config'"
        ):
            FivetranSourceConfig.parse_obj({"fivetran_mode": "enterprise"})

    def test_config_validation_standard_mode(self):
        """Test config validation for standard mode"""
        with pytest.raises(ValueError, match="Standard mode requires 'api_config'"):
            FivetranSourceConfig.parse_obj({"fivetran_mode": "standard"})

    def test_config_validation_auto_mode(self):
        """Test config validation for auto mode"""
        with pytest.raises(
            ValueError, match="Either 'fivetran_log_config'.*or 'api_config'"
        ):
            FivetranSourceConfig.parse_obj({"fivetran_mode": "auto"})

    def test_sources_to_database_deprecated(self):
        """Test sources_to_database deprecated field is transformed correctly"""
        with pytest.warns(
            ConfigurationWarning, match="sources_to_database.*deprecated"
        ):
            config = FivetranSourceConfig.parse_obj(
                {
                    "fivetran_mode": "enterprise",
                    "fivetran_log_config": {
                        "destination_platform": "snowflake",
                        "snowflake_destination_config": {
                            "account_id": "test",
                            "warehouse": "test_wh",
                            "username": "test",
                            "password": "test",
                            "database": "test_db",
                            "role": "test_role",
                            "log_schema": "test_schema",
                        },
                    },
                    "sources_to_database": {
                        "connector1": "my_db",
                        "connector2": "my_db2",
                    },
                }
            )

        # Verify transformation happened
        self.assertEqual(
            config.sources_to_platform_instance["connector1"].database, "my_db"
        )
        self.assertEqual(
            config.sources_to_platform_instance["connector2"].database, "my_db2"
        )


class FivetranStandardAPITest(unittest.TestCase):
    """Tests for the Fivetran Standard API implementation"""

    @patch("datahub.ingestion.source.fivetran.fivetran_api_client.FivetranAPIClient")
    def test_get_destination_platform(self, mock_api_client):
        """Test destination platform detection logic"""
        # Setup test data
        connector = Connector(
            connector_id="connector1",
            connector_name="Test Connector",
            connector_type="postgres",
            paused=False,
            sync_frequency=1440,
            destination_id="dest1",
            user_id="user1",
            lineage=[],
            jobs=[],
        )

        # Create config with destination mapping
        config = MagicMock()
        config.destination_to_platform_instance = {
            "dest1": PlatformDetail(platform="custom_platform")
        }

        # Create standard API instance
        api = FivetranStandardAPI(mock_api_client, config)

        # Test with platform in destination_to_platform_instance
        platform = api._get_destination_platform(connector)
        self.assertEqual(platform, "custom_platform")

        # Test with platform in connector properties
        connector.additional_properties = {"destination_platform": "bigquery"}
        config.destination_to_platform_instance = {}
        platform = api._get_destination_platform(connector)
        self.assertEqual(platform, "bigquery")

        # Test fallback to fivetran_log_config
        connector.additional_properties = {}
        config.fivetran_log_config = MagicMock()
        config.fivetran_log_config.destination_platform = "snowflake"
        platform = api._get_destination_platform(connector)
        self.assertEqual(platform, "snowflake")


class FivetranSourceTest(unittest.TestCase):
    """Tests for the FivetranSource class"""

    @freeze_time("2022-06-07 17:00:00")
    @patch("datahub.ingestion.source.fivetran.fivetran.create_fivetran_access")
    def test_create_instance_enterprise(self, mock_create_access):
        """Test creating a FivetranSource instance with enterprise mode"""
        # Setup mock
        mock_create_access.return_value = MagicMock()

        # Create config
        config = {
            "fivetran_mode": "enterprise",
            "fivetran_log_config": {
                "destination_platform": "snowflake",
                "snowflake_destination_config": {
                    "account_id": "test",
                    "warehouse": "test_wh",
                    "username": "test",
                    "password": "test@123",
                    "database": "test_db",
                    "role": "test_role",
                    "log_schema": "test",
                },
            },
        }

        # Parse config and create source directly
        config_obj = FivetranSourceConfig.parse_obj(config)
        source = FivetranSource(config_obj, PipelineContext(run_id="test-run"))

        # Verify source was created properly
        self.assertIsInstance(source, FivetranSource)
        mock_create_access.assert_called_once()

    @freeze_time("2022-06-07 17:00:00")
    @patch("datahub.ingestion.source.fivetran.fivetran.create_fivetran_access")
    def test_create_instance_standard(self, mock_create_access):
        """Test creating a FivetranSource instance with standard mode"""
        # Setup mock
        mock_create_access.return_value = MagicMock()

        # Create config
        config = {
            "fivetran_mode": "standard",
            "api_config": {"api_key": "test_key", "api_secret": "test_secret"},
        }

        # Parse config and create source directly
        config_obj = FivetranSourceConfig.parse_obj(config)
        source = FivetranSource(config_obj, PipelineContext(run_id="test-run"))

        # Verify source was created properly
        self.assertIsInstance(source, FivetranSource)
        mock_create_access.assert_called_once()
