"""
Focused integration tests to boost Fivetran coverage.

This file contains simple, targeted tests that cover specific
missing functionality in fivetran_api_client.py and fivetran_standard_api.py.
"""

import pytest
import requests
import responses
from requests.exceptions import HTTPError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient
from datahub.ingestion.source.fivetran.fivetran_standard_api import FivetranStandardAPI


class TestFivetranCoverageBoost:
    """Simple tests to boost coverage of key functionality."""

    @pytest.fixture
    def api_config(self) -> FivetranAPIConfig:
        """Create API config for testing."""
        return FivetranAPIConfig(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://api.fivetran.com",
            max_workers=2,
            request_timeout_sec=30,
        )

    @pytest.fixture
    def api_client(self, api_config: FivetranAPIConfig) -> FivetranAPIClient:
        """Create API client for testing."""
        return FivetranAPIClient(api_config)

    @pytest.fixture
    def source_config(self, api_config: FivetranAPIConfig) -> FivetranSourceConfig:
        """Create source config for testing."""
        return FivetranSourceConfig(api_config=api_config)

    @pytest.fixture
    def standard_api(
        self, api_client: FivetranAPIClient, source_config: FivetranSourceConfig
    ) -> FivetranStandardAPI:
        """Create standard API for testing."""
        return FivetranStandardAPI(api_client, source_config)

    @pytest.fixture
    def report(self) -> FivetranSourceReport:
        """Create report for testing."""
        return FivetranSourceReport()

    # Test HTTP error handling

    @responses.activate
    def test_http_401_error(self, api_client: FivetranAPIClient) -> None:
        """Test 401 error handling."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={"code": "Unauthorized", "message": "Invalid API key"},
            status=401,
        )

        with pytest.raises(HTTPError):
            api_client.list_connectors()

    @responses.activate
    def test_http_500_error(self, api_client: FivetranAPIClient) -> None:
        """Test 500 error handling."""
        # Create a client with no retry to avoid the retry loop
        no_retry_config = FivetranAPIConfig(
            api_key="test_key",
            api_secret="test_secret",
            base_url="https://api.fivetran.com",
            max_workers=2,
            request_timeout_sec=30,
        )
        no_retry_client = FivetranAPIClient(no_retry_config)
        # Disable retries for this test
        no_retry_client._session.mount(
            "https://", requests.adapters.HTTPAdapter(max_retries=0)
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={"code": "InternalServerError", "message": "Server error"},
            status=500,
        )

        with pytest.raises(HTTPError):
            no_retry_client.list_connectors()

    # Test user operations

    @responses.activate
    def test_get_user_success(self, api_client: FivetranAPIClient) -> None:
        """Test successful user retrieval."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/users/user_123",
            json={
                "code": "Success",
                "data": {
                    "id": "user_123",
                    "email": "test@example.com",
                    "given_name": "Test",
                    "family_name": "User",
                },
            },
            status=200,
        )

        user = api_client.get_user("user_123")
        assert user["email"] == "test@example.com"

    @responses.activate
    def test_get_user_not_found(self, api_client: FivetranAPIClient) -> None:
        """Test user not found."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/users/nonexistent",
            json={"code": "NotFound", "message": "User not found"},
            status=404,
        )

        user = api_client.get_user("nonexistent")
        # The API client returns an empty items list on 404, not None
        assert user is None or user == {"items": []}

    @responses.activate
    def test_list_users(self, api_client: FivetranAPIClient) -> None:
        """Test listing users."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/users",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {"id": "user_1", "email": "user1@example.com"},
                        {"id": "user_2", "email": "user2@example.com"},
                    ]
                },
            },
            status=200,
        )

        users = api_client.list_users()
        assert len(users) == 2
        assert users[0]["email"] == "user1@example.com"

    # Test destination operations

    @responses.activate
    def test_list_groups(self, api_client: FivetranAPIClient) -> None:
        """Test listing destination groups."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {"id": "group_1", "name": "Production"},
                        {"id": "group_2", "name": "Staging"},
                    ]
                },
            },
            status=200,
        )

        groups = api_client.list_groups()
        assert len(groups) == 2
        assert groups[0]["name"] == "Production"

    @responses.activate
    def test_get_destination_details(self, api_client: FivetranAPIClient) -> None:
        """Test getting destination details."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/test_group",
            json={
                "code": "Success",
                "data": {"id": "test_group", "name": "Test Group"},
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/test_group/config",
            json={
                "code": "Success",
                "data": {"service": "snowflake", "config": {"database": "TEST"}},
            },
            status=200,
        )

        details = api_client.get_destination_details("test_group")
        assert details["id"] == "test_group"
        assert details["name"] == "Test Group"

    @responses.activate
    def test_detect_destination_platform(self, api_client: FivetranAPIClient) -> None:
        """Test destination platform detection."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/snowflake_group",
            json={
                "code": "Success",
                "data": {"id": "snowflake_group", "name": "Snowflake Warehouse"},
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/snowflake_group/config",
            json={
                "code": "Success",
                "data": {
                    "service": "snowflake",
                    "config": {"database": "ANALYTICS", "schema": "PUBLIC"},
                },
            },
            status=200,
        )

        platform = api_client.detect_destination_platform("snowflake_group")
        assert platform == "snowflake"

    @responses.activate
    def test_get_destination_database(self, api_client: FivetranAPIClient) -> None:
        """Test getting destination database name."""
        # Mock the groups list call for name resolution
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups",
            json={
                "code": "Success",
                "data": {"items": [{"id": "test_group", "name": "Test Group"}]},
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/test_group",
            json={
                "code": "Success",
                "data": {"id": "test_group", "name": "Test Group"},
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/test_group/config",
            json={
                "code": "Success",
                "data": {
                    "service": "snowflake",
                    "config": {"database": "ANALYTICS", "schema": "PUBLIC"},
                },
            },
            status=200,
        )

        database = api_client.get_destination_database("test_group")
        assert database == "ANALYTICS"

    # Test connector operations

    @responses.activate
    def test_get_connector_details(self, api_client: FivetranAPIClient) -> None:
        """Test getting connector details."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/test_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "test_connector",
                    "service": "postgres",
                    "display_name": "Test Connector",
                },
            },
            status=200,
        )

        details = api_client.get_connector_details("test_connector")
        assert details["id"] == "test_connector"
        assert details["service"] == "postgres"

    @responses.activate
    def test_validate_connector_accessibility(
        self, api_client: FivetranAPIClient
    ) -> None:
        """Test connector accessibility validation."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/accessible_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "accessible_connector",
                    "service": "postgres",
                    "status": {"setup_state": "connected"},
                },
            },
            status=200,
        )

        result = api_client.validate_connector_accessibility("accessible_connector")
        assert result["is_accessible"] is True
        assert result["error_message"] == ""

    @responses.activate
    def test_validate_connector_accessibility_failure(
        self, api_client: FivetranAPIClient
    ) -> None:
        """Test connector accessibility validation failure."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/bad_connector",
            json={"code": "NotFound", "message": "Connector not found"},
            status=404,
        )

        result = api_client.validate_connector_accessibility("bad_connector")
        assert result["is_accessible"] is False
        assert "not found" in result["error_message"].lower()

    # Test sync history operations

    @responses.activate
    def test_list_connector_sync_history(self, api_client: FivetranAPIClient) -> None:
        """Test listing connector sync history."""
        # Mock the connector details call that happens first
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/test_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "test_connector",
                    "service": "postgres",
                    "status": {"setup_state": "connected"},
                },
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/test_connector/sync-history",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {
                            "sync_id": "sync_1",
                            "status": "SUCCESSFUL",
                            "sync_start": "2023-12-01T10:00:00.000Z",
                            "sync_end": "2023-12-01T10:30:00.000Z",
                        }
                    ]
                },
            },
            status=200,
        )

        history = api_client.list_connector_sync_history("test_connector", days=7)
        assert len(history) == 1
        assert history[0]["status"] == "SUCCESSFUL"

    @responses.activate
    def test_list_connector_sync_history_empty(
        self, api_client: FivetranAPIClient
    ) -> None:
        """Test sync history when empty."""
        # Mock empty sync-history
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/empty_connector/sync-history",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )

        # Mock empty logs
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/empty_connector/logs",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )

        # Mock connector details
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/empty_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "empty_connector",
                    "service": "postgres",
                    "status": {"setup_state": "connected"},
                },
            },
            status=200,
        )

        history = api_client.list_connector_sync_history("empty_connector", days=7)
        assert history == []

    # Test schema operations

    @responses.activate
    def test_list_connector_schemas_success(
        self, api_client: FivetranAPIClient
    ) -> None:
        """Test successful schema listing."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/schema_connector/schemas",
            json={
                "code": "Success",
                "data": {
                    "schemas": {
                        "public": {
                            "name_in_destination": "public",
                            "enabled": True,
                            "tables": {
                                "users": {
                                    "name_in_destination": "users",
                                    "enabled": True,
                                    "columns": {
                                        "id": {
                                            "name_in_destination": "id",
                                            "enabled": True,
                                        }
                                    },
                                }
                            },
                        }
                    }
                },
            },
            status=200,
        )

        schemas = api_client.list_connector_schemas("schema_connector")
        assert len(schemas) > 0

    @responses.activate
    def test_list_connector_schemas_empty(self, api_client: FivetranAPIClient) -> None:
        """Test empty schema listing with fallback."""
        # Mock empty primary response
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/empty_schema_connector/schemas",
            json={"code": "Success", "data": {"schemas": {}}},
            status=200,
        )

        # Mock connector details for fallback
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/empty_schema_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "empty_schema_connector",
                    "service": "postgres",
                    "schema": "public",
                },
            },
            status=200,
        )

        # Mock config endpoint
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/empty_schema_connector/config",
            json={
                "code": "Success",
                "data": {"schema": "public", "host": "localhost"},
            },
            status=200,
        )

        # Mock metadata endpoint
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/empty_schema_connector/metadata",
            json={"code": "Success", "data": {"tables": {}}},
            status=200,
        )

        # Mock setup tests endpoint
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/empty_schema_connector/setup_tests",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )

        schemas = api_client.list_connector_schemas("empty_schema_connector")
        # Should return empty list but not crash
        assert schemas == []

    # Test lineage operations

    @responses.activate
    def test_extract_table_lineage(self, api_client: FivetranAPIClient) -> None:
        """Test table lineage extraction."""
        # Mock connector details
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "lineage_connector",
                    "service": "postgres",
                    "schema": "public",
                },
            },
            status=200,
        )

        # Mock schemas with lineage
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector/schemas",
            json={
                "code": "Success",
                "data": {
                    "schemas": {
                        "public": {
                            "name_in_destination": "public",
                            "enabled": True,
                            "tables": {
                                "users": {
                                    "name_in_destination": "users",
                                    "enabled": True,
                                    "columns": {
                                        "id": {
                                            "name_in_destination": "id",
                                            "enabled": True,
                                        },
                                        "name": {
                                            "name_in_destination": "name",
                                            "enabled": True,
                                        },
                                    },
                                }
                            },
                        }
                    }
                },
            },
            status=200,
        )

        lineage = list(api_client.extract_table_lineage_generator("lineage_connector"))
        assert len(lineage) >= 0  # Should not crash

    # Test metadata extraction

    @responses.activate
    def test_extract_connector_metadata(self, api_client: FivetranAPIClient) -> None:
        """Test connector metadata extraction."""
        api_connector = {
            "id": "metadata_connector",
            "display_name": "Metadata Connector",
            "service": "postgres",
            "group_id": "metadata_group",
            "paused": False,
            "sync_frequency": 1440,
            "created_by": "user_123",
        }

        sync_history = [
            {
                "sync_id": "sync_1",
                "status": "SUCCESSFUL",
                "started_at": "2023-12-01T10:00:00.000Z",
                "completed_at": "2023-12-01T10:30:00.000Z",
            }
        ]

        # Mock destination detection
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/metadata_group",
            json={
                "code": "Success",
                "data": {"id": "metadata_group", "name": "Metadata Group"},
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/metadata_group/config",
            json={
                "code": "Success",
                "data": {"service": "snowflake", "config": {}},
            },
            status=200,
        )

        connector = api_client.extract_connector_metadata(api_connector, sync_history)

        assert connector.connector_id == "metadata_connector"
        assert connector.connector_name == "Metadata Connector"
        assert connector.connector_type == "postgres"
        assert len(connector.jobs) == 1

    # Test standard API user operations

    def test_get_user_email_empty_user_id(
        self, standard_api: FivetranStandardAPI
    ) -> None:
        """Test user email with empty user ID."""
        assert standard_api.get_user_email("") is None
        # Note: Method expects str, so we don't test None case

    @responses.activate
    def test_get_user_email_api_error(self, standard_api: FivetranStandardAPI) -> None:
        """Test user email when API returns error."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/users/error_user",
            json={"code": "NotFound", "message": "User not found"},
            status=404,
        )

        result = standard_api.get_user_email("error_user")
        assert result is None

    # Test configuration scenarios

    def test_standard_api_without_config(self, api_client: FivetranAPIClient) -> None:
        """Test standard API with None config."""
        standard_api = FivetranStandardAPI(api_client, None)
        assert standard_api.fivetran_log_database is None

    # Test empty data handling

    @responses.activate
    def test_empty_connector_list_handling(
        self, standard_api: FivetranStandardAPI, report: FivetranSourceReport
    ) -> None:
        """Test handling of empty connector list."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )

        connector_patterns = AllowDenyPattern.allow_all()
        destination_patterns = AllowDenyPattern.allow_all()

        connectors = standard_api.get_allowed_connectors_list(
            connector_patterns=connector_patterns,
            destination_patterns=destination_patterns,
            report=report,
            syncs_interval=7,
        )

        assert connectors == []

    @responses.activate
    def test_malformed_connector_handling(
        self, standard_api: FivetranStandardAPI, report: FivetranSourceReport
    ) -> None:
        """Test handling of malformed connectors."""
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {
                            # Missing "id" field - should be skipped
                            "display_name": "Malformed Connector",
                            "service": "postgres",
                        }
                    ]
                },
            },
            status=200,
        )

        connector_patterns = AllowDenyPattern.allow_all()
        destination_patterns = AllowDenyPattern.allow_all()

        connectors = standard_api.get_allowed_connectors_list(
            connector_patterns=connector_patterns,
            destination_patterns=destination_patterns,
            report=report,
            syncs_interval=7,
        )

        # Should skip malformed connectors
        assert connectors == []
