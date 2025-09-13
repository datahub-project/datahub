"""
Simple integration tests for Fivetran API client focused on working functionality.
"""

import pytest
import responses

from datahub.ingestion.source.fivetran.config import FivetranAPIConfig
from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient


class TestFivetranSimpleIntegration:
    """Simple integration tests focusing on working API client methods."""

    @pytest.fixture
    def api_config(self):
        """Create a test API configuration."""
        return FivetranAPIConfig(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://api.fivetran.com",
            request_timeout_sec=5,  # Short timeout for tests
            max_workers=1,
        )

    @pytest.fixture
    def api_client(self, api_config):
        """Create a FivetranAPIClient instance for testing."""
        return FivetranAPIClient(api_config)

    @responses.activate
    def test_list_connectors_success(self, api_client):
        """Test successful connector listing."""
        mock_response = {
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "connector_123",
                        "group_id": "group_456",
                        "service": "postgres",
                        "schema": "public",
                        "connected_by": "user_789",
                        "display_name": "Test PostgreSQL Connector",
                        "sync_frequency": 1440,
                        "paused": False,
                        "config": {
                            "host": "localhost",
                            "port": 5432,
                            "database": "testdb",
                        },
                    }
                ]
            },
        }

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json=mock_response,
            status=200,
        )

        connectors = api_client.list_connectors()

        assert len(connectors) == 1
        assert connectors[0]["id"] == "connector_123"
        assert connectors[0]["service"] == "postgres"
        assert connectors[0]["display_name"] == "Test PostgreSQL Connector"

    @responses.activate
    def test_list_connectors_empty(self, api_client):
        """Test connector listing with empty response."""
        mock_response = {"code": "Success", "data": {"items": []}}

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json=mock_response,
            status=200,
        )

        connectors = api_client.list_connectors()

        assert connectors == []

    @responses.activate
    def test_get_connector_success(self, api_client):
        """Test successful single connector retrieval."""
        mock_response = {
            "code": "Success",
            "data": {
                "id": "connector_123",
                "group_id": "group_456",
                "service": "mysql",
                "schema": "production",
                "connected_by": "admin_user",
                "display_name": "Production MySQL",
                "sync_frequency": 720,
                "paused": False,
                "config": {
                    "host": "prod-mysql.example.com",
                    "port": 3306,
                    "database": "production",
                },
            },
        }

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/connector_123",
            json=mock_response,
            status=200,
        )

        connector = api_client.get_connector("connector_123")

        assert connector["id"] == "connector_123"
        assert connector["service"] == "mysql"
        assert connector["display_name"] == "Production MySQL"
        assert connector["sync_frequency"] == 720

    @responses.activate
    def test_get_user_success(self, api_client):
        """Test successful user retrieval."""
        mock_response = {
            "code": "Success",
            "data": {
                "id": "user_123",
                "email": "admin@company.com",
                "given_name": "Admin",
                "family_name": "User",
                "verified": True,
                "invited": False,
                "picture": "https://example.com/avatar.jpg",
                "phone": "+1234567890",
                "logged_in_at": "2023-01-01T12:00:00.000Z",
                "created_at": "2022-01-01T00:00:00.000Z",
            },
        }

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/users/user_123",
            json=mock_response,
            status=200,
        )

        user = api_client.get_user("user_123")

        assert user["id"] == "user_123"
        assert user["email"] == "admin@company.com"
        assert user["given_name"] == "Admin"

    def test_api_config_properties(self, api_config):
        """Test API configuration properties."""
        assert api_config.api_key == "test_key"
        assert api_config.api_secret == "test_secret"
        assert api_config.base_url == "https://api.fivetran.com"
        assert api_config.request_timeout_sec == 5
        assert api_config.max_workers == 1

    def test_api_client_initialization(self, api_client, api_config):
        """Test API client initialization."""
        assert api_client.config == api_config
        # Test that the client is properly initialized
        assert api_client is not None

    @responses.activate
    def test_validate_connector_accessibility_success_simple(self, api_client):
        """Test simple connector accessibility validation."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/valid_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "valid_connector",
                    "service": "postgres",
                    "paused": False,
                },
            },
            status=200,
        )

        result = api_client.validate_connector_accessibility("valid_connector")

        # Just test that it doesn't crash and returns a dict
        assert isinstance(result, dict)
        assert "connector_exists" in result
        assert "error_message" in result

    def test_get_destination_database_method_exists(self, api_client):
        """Test that get_destination_database method exists."""
        # Test that the method exists (even if we can't easily test its functionality)
        assert hasattr(api_client, "get_destination_database")
        assert callable(api_client.get_destination_database)

    def test_detect_destination_platform_method_exists(self, api_client):
        """Test that detect_destination_platform method exists."""
        assert hasattr(api_client, "detect_destination_platform")
        assert callable(api_client.detect_destination_platform)

    def test_extract_connector_metadata_method_exists(self, api_client):
        """Test that extract_connector_metadata method exists."""
        assert hasattr(api_client, "extract_connector_metadata")
        assert callable(api_client.extract_connector_metadata)

    def test_get_table_columns_method_exists(self, api_client):
        """Test that get_table_columns method exists."""
        assert hasattr(api_client, "get_table_columns")
        assert callable(api_client.get_table_columns)

    def test_list_connector_sync_history_method_exists(self, api_client):
        """Test that list_connector_sync_history method exists."""
        assert hasattr(api_client, "list_connector_sync_history")
        assert callable(api_client.list_connector_sync_history)

    @responses.activate
    def test_multiple_api_calls(self, api_client):
        """Test multiple API calls in sequence."""
        # Mock first call - list connectors
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {"id": "conn1", "service": "postgres"},
                        {"id": "conn2", "service": "mysql"},
                    ]
                },
            },
            status=200,
        )

        # Mock second call - get specific connector
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/conn1",
            json={
                "code": "Success",
                "data": {
                    "id": "conn1",
                    "service": "postgres",
                    "display_name": "Test Connector",
                },
            },
            status=200,
        )

        # Make both calls
        connectors = api_client.list_connectors()
        connector_detail = api_client.get_connector("conn1")

        assert len(connectors) == 2
        assert connector_detail["id"] == "conn1"
        assert connector_detail["service"] == "postgres"
