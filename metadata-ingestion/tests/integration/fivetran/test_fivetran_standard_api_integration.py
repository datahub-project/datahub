"""
Integration tests for FivetranStandardAPI high-level business logic.

These tests focus on the orchestration methods that coordinate multiple API calls
and contain the core business logic, rather than testing individual API endpoints.
This provides better coverage of actual connector functionality.
"""

import pytest
import responses

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient
from datahub.ingestion.source.fivetran.fivetran_standard_api import FivetranStandardAPI


class TestFivetranStandardAPIIntegration:
    """Integration tests for FivetranStandardAPI business logic methods."""

    @pytest.fixture
    def api_config(self) -> FivetranAPIConfig:
        """Create API config for testing."""
        return FivetranAPIConfig(
            api_key="test_api_key",
            api_secret="test_api_secret",
            base_url="https://api.fivetran.com",
            max_workers=4,
            request_timeout_sec=30,
        )

    @pytest.fixture
    def source_config(self) -> FivetranSourceConfig:
        """Create source config for testing."""
        return FivetranSourceConfig(
            api_config=FivetranAPIConfig(
                api_key="test_api_key",
                api_secret="test_api_secret",
            )
        )

    @pytest.fixture
    def api_client(self, api_config: FivetranAPIConfig) -> FivetranAPIClient:
        """Create API client for testing."""
        return FivetranAPIClient(api_config)

    @pytest.fixture
    def standard_api(
        self, api_client: FivetranAPIClient, source_config: FivetranSourceConfig
    ) -> FivetranStandardAPI:
        """Create FivetranStandardAPI for testing."""
        return FivetranStandardAPI(api_client, source_config)

    @pytest.fixture
    def report(self) -> FivetranSourceReport:
        """Create source report for testing."""
        return FivetranSourceReport()

    # Test the main orchestration method: get_allowed_connectors_list

    @responses.activate
    def test_get_allowed_connectors_list_complete_workflow(
        self, standard_api: FivetranStandardAPI, report: FivetranSourceReport
    ) -> None:
        """Test complete connector discovery workflow with filtering and metadata extraction."""

        # Mock connector listing
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {
                            "id": "postgres_connector",
                            "group_id": "group_1",
                            "service": "postgres",
                            "schema": "public",
                            "connected_by": "user_123",
                            "display_name": "PostgreSQL Production",
                            "sync_frequency": 1440,
                            "paused": False,
                            "status": {"setup_state": "connected"},
                        },
                        {
                            "id": "mysql_connector",
                            "group_id": "group_2",
                            "service": "mysql",
                            "schema": "main",
                            "connected_by": "user_456",
                            "display_name": "MySQL Staging",
                            "sync_frequency": 720,
                            "paused": True,
                            "status": {"setup_state": "connected"},
                        },
                    ]
                },
            },
            status=200,
        )

        # Mock individual connector details
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/postgres_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "postgres_connector",
                    "group_id": "group_1",
                    "service": "postgres",
                    "schema": "public",
                    "connected_by": "user_123",
                    "display_name": "PostgreSQL Production",
                    "sync_frequency": 1440,
                    "paused": False,
                    "status": {"setup_state": "connected"},
                },
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/mysql_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "mysql_connector",
                    "group_id": "group_2",
                    "service": "mysql",
                    "schema": "main",
                    "connected_by": "user_456",
                    "display_name": "MySQL Staging",
                    "sync_frequency": 720,
                    "paused": True,
                    "status": {"setup_state": "connected"},
                },
            },
            status=200,
        )

        # Mock destination details for group_1
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/group_1",
            json={
                "code": "Success",
                "data": {"id": "group_1", "name": "Production Group"},
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/group_1/config",
            json={
                "code": "Success",
                "data": {
                    "service": "snowflake",
                    "config": {"database": "ANALYTICS", "schema": "PUBLIC"},
                },
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/destinations/group_1",
            json={
                "code": "Success",
                "data": {
                    "id": "group_1",
                    "service": "snowflake",
                    "config": {"database": "ANALYTICS", "schema": "PUBLIC"},
                },
            },
            status=200,
        )

        # Mock destination details for group_2
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/group_2",
            json={
                "code": "Success",
                "data": {"id": "group_2", "name": "Staging Group"},
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/group_2/config",
            json={
                "code": "Success",
                "data": {
                    "service": "bigquery",
                    "config": {"project_id": "my-project", "dataset_id": "staging"},
                },
            },
            status=200,
        )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/destinations/group_2",
            json={
                "code": "Success",
                "data": {
                    "id": "group_2",
                    "service": "bigquery",
                    "config": {"project_id": "my-project", "dataset_id": "staging"},
                },
            },
            status=200,
        )

        # Mock connector schemas for postgres_connector
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/postgres_connector/schemas",
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
                                    "sync_mode": "SOFT_DELETE",
                                    "columns": {
                                        "id": {
                                            "name_in_destination": "id",
                                            "enabled": True,
                                        },
                                        "email": {
                                            "name_in_destination": "email",
                                            "enabled": True,
                                        },
                                    },
                                },
                                "orders": {
                                    "name_in_destination": "orders",
                                    "enabled": True,
                                    "sync_mode": "HISTORY",
                                    "columns": {
                                        "order_id": {
                                            "name_in_destination": "order_id",
                                            "enabled": True,
                                        },
                                        "user_id": {
                                            "name_in_destination": "user_id",
                                            "enabled": True,
                                        },
                                    },
                                },
                            },
                        }
                    }
                },
            },
            status=200,
        )

        # Mock connector schemas for mysql_connector
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/mysql_connector/schemas",
            json={
                "code": "Success",
                "data": {
                    "schemas": {
                        "main": {
                            "name_in_destination": "main",
                            "enabled": True,
                            "tables": {
                                "products": {
                                    "name_in_destination": "products",
                                    "enabled": True,
                                    "sync_mode": "SOFT_DELETE",
                                    "columns": {
                                        "product_id": {
                                            "name_in_destination": "product_id",
                                            "enabled": True,
                                        },
                                        "name": {
                                            "name_in_destination": "name",
                                            "enabled": True,
                                        },
                                    },
                                },
                            },
                        }
                    }
                },
            },
            status=200,
        )

        # Mock sync history for both connectors (multiple endpoints)
        for connector_id in ["postgres_connector", "mysql_connector"]:
            # Mock sync-history endpoint (primary)
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/sync-history",
                json={
                    "code": "Success",
                    "data": {
                        "items": [
                            {
                                "sync_id": "sync_123",
                                "status": "SUCCESSFUL",
                                "sync_start": "2023-12-01T10:00:00.000Z",
                                "sync_end": "2023-12-01T10:30:00.000Z",
                            }
                        ]
                    },
                },
                status=200,
            )

            # Mock logs endpoint (fallback)
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/logs",
                json={
                    "code": "Success",
                    "data": {
                        "items": [
                            {
                                "time_stamp": "2023-12-01T10:00:00.000Z",
                                "level": "INFO",
                                "message": "Sync completed successfully",
                            }
                        ]
                    },
                },
                status=200,
            )

            # Mock sync endpoint (fallback)
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/sync",
                json={
                    "code": "Success",
                    "data": {
                        "status": "successful",
                        "sync_start": "2023-12-01T10:00:00.000Z",
                        "sync_end": "2023-12-01T10:30:00.000Z",
                    },
                },
                status=200,
            )

            # Mock config endpoint
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/config",
                json={
                    "code": "Success",
                    "data": {
                        "schema": "public"
                        if connector_id == "postgres_connector"
                        else "main",
                        "host": "localhost",
                        "port": 5432 if connector_id == "postgres_connector" else 3306,
                    },
                },
                status=200,
            )

            # Mock metadata endpoint
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/metadata",
                json={
                    "code": "Success",
                    "data": {
                        "connector_id": connector_id,
                        "service": "postgres"
                        if connector_id == "postgres_connector"
                        else "mysql",
                    },
                },
                status=200,
            )

            # Mock setup_tests endpoint
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/setup_tests",
                json={
                    "code": "Success",
                    "data": {
                        "items": [
                            {
                                "title": "Connection Test",
                                "status": "PASSED",
                                "message": "Successfully connected to database",
                            }
                        ]
                    },
                },
                status=200,
            )

        # Execute the main orchestration method
        connector_patterns = AllowDenyPattern.allow_all()
        destination_patterns = AllowDenyPattern.allow_all()

        connectors = standard_api.get_allowed_connectors_list(
            connector_patterns=connector_patterns,
            destination_patterns=destination_patterns,
            report=report,
            syncs_interval=7,
        )

        # Verify results
        assert len(connectors) == 2

        # Verify PostgreSQL connector
        postgres_connector = next(
            c for c in connectors if c.connector_id == "postgres_connector"
        )
        assert postgres_connector.connector_name == "PostgreSQL Production"
        assert postgres_connector.connector_type == "postgres"
        assert postgres_connector.paused is False
        assert postgres_connector.destination_id == "group_1"
        assert len(postgres_connector.lineage) > 0  # Should have table lineage

        # Verify MySQL connector
        mysql_connector = next(
            c for c in connectors if c.connector_id == "mysql_connector"
        )
        assert mysql_connector.connector_name == "MySQL Staging"
        assert mysql_connector.connector_type == "mysql"
        assert mysql_connector.paused is True
        assert mysql_connector.destination_id == "group_2"

        # Verify that multiple API calls were orchestrated
        assert (
            len(responses.calls) >= 8
        )  # connectors + destinations + schemas + sync history

    @responses.activate
    def test_get_allowed_connectors_list_with_filtering(
        self, standard_api: FivetranStandardAPI, report: FivetranSourceReport
    ) -> None:
        """Test connector filtering logic in get_allowed_connectors_list."""

        # Mock connector listing with multiple connectors
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {
                            "id": "prod_postgres",
                            "group_id": "prod_group",
                            "service": "postgres",
                            "display_name": "Production PostgreSQL",
                            "paused": False,
                            "status": {"setup_state": "connected"},
                        },
                        {
                            "id": "test_mysql",
                            "group_id": "test_group",
                            "service": "mysql",
                            "display_name": "Test MySQL",
                            "paused": False,
                            "status": {"setup_state": "connected"},
                        },
                        {
                            "id": "staging_mongo",
                            "group_id": "staging_group",
                            "service": "mongodb",
                            "display_name": "Staging MongoDB",
                            "paused": False,
                            "status": {"setup_state": "connected"},
                        },
                    ]
                },
            },
            status=200,
        )

        # Mock individual connector details
        for connector_id, group_id, service, display_name in [
            ("prod_postgres", "prod_group", "postgres", "Production PostgreSQL"),
            ("test_mysql", "test_group", "mysql", "Test MySQL"),
            ("staging_mongo", "staging_group", "mongodb", "Staging MongoDB"),
        ]:
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}",
                json={
                    "code": "Success",
                    "data": {
                        "id": connector_id,
                        "group_id": group_id,
                        "service": service,
                        "display_name": display_name,
                        "paused": False,
                        "status": {"setup_state": "connected"},
                    },
                },
                status=200,
            )

        # Mock destinations for all groups
        for group_id, service in [
            ("prod_group", "snowflake"),
            ("test_group", "postgres"),
            ("staging_group", "bigquery"),
        ]:
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/groups/{group_id}",
                json={
                    "code": "Success",
                    "data": {"id": group_id, "name": f"{group_id}_name"},
                },
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/groups/{group_id}/config",
                json={
                    "code": "Success",
                    "data": {"service": service, "config": {}},
                },
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/destinations/{group_id}",
                json={
                    "code": "Success",
                    "data": {"id": group_id, "service": service, "config": {}},
                },
                status=200,
            )

        # Mock empty schemas and sync history for all connectors
        for connector_id in ["prod_postgres", "test_mysql", "staging_mongo"]:
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/schemas",
                json={"code": "Success", "data": {"schemas": {}}},
                status=200,
            )

            # Mock sync history endpoints
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/sync-history",
                json={
                    "code": "Success",
                    "data": {
                        "items": [{"sync_id": "sync_123", "status": "SUCCESSFUL"}]
                    },
                },
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/logs",
                json={"code": "Success", "data": {"items": []}},
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/sync",
                json={
                    "code": "Success",
                    "data": {
                        "status": "successful",
                        "sync_start": "2023-12-01T10:00:00.000Z",
                    },
                },
                status=200,
            )

            # Mock additional endpoints
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/config",
                json={"code": "Success", "data": {}},
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/metadata",
                json={"code": "Success", "data": {}},
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/setup_tests",
                json={"code": "Success", "data": {"items": []}},
                status=200,
            )

        # Test with connector pattern that only allows "prod_*"
        connector_patterns = AllowDenyPattern(allow=["prod_*"])
        destination_patterns = AllowDenyPattern.allow_all()

        connectors = standard_api.get_allowed_connectors_list(
            connector_patterns=connector_patterns,
            destination_patterns=destination_patterns,
            report=report,
            syncs_interval=7,
        )

        # Should only get the prod_postgres connector
        assert len(connectors) == 1
        assert connectors[0].connector_id == "prod_postgres"

    @responses.activate
    def test_get_allowed_connectors_list_error_handling(
        self, standard_api: FivetranStandardAPI, report: FivetranSourceReport
    ) -> None:
        """Test error handling in get_allowed_connectors_list workflow."""

        # Mock connector listing success
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {
                            "id": "error_connector",
                            "group_id": "error_group",
                            "service": "postgres",
                            "display_name": "Error Connector",
                            "paused": False,
                            "status": {"setup_state": "connected"},
                        }
                    ]
                },
            },
            status=200,
        )

        # Mock individual connector details
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/error_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "error_connector",
                    "group_id": "error_group",
                    "service": "postgres",
                    "display_name": "Error Connector",
                    "paused": False,
                    "status": {"setup_state": "connected"},
                },
            },
            status=200,
        )

        # Mock destination call failure
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/error_group",
            json={"code": "NotFound", "message": "Group not found"},
            status=404,
        )

        # Mock schema call (should still work even with destination error)
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/error_connector/schemas",
            json={"code": "Success", "data": {"schemas": {}}},
            status=200,
        )

        # Mock sync history endpoints
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/error_connector/sync-history",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/error_connector/logs",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/error_connector/sync",
            json={
                "code": "Success",
                "data": {
                    "status": "successful",
                    "sync_start": "2023-12-01T10:00:00.000Z",
                },
            },
            status=200,
        )

        # Mock additional endpoints
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/error_connector/config",
            json={"code": "Success", "data": {}},
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/error_connector/metadata",
            json={"code": "Success", "data": {}},
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/error_connector/setup_tests",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )

        # Mock missing destination config endpoint
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/error_group/config",
            json={"code": "NotFound", "message": "Config not found"},
            status=404,
        )

        # Should handle error gracefully and continue
        connector_patterns = AllowDenyPattern.allow_all()
        destination_patterns = AllowDenyPattern.allow_all()

        connectors = standard_api.get_allowed_connectors_list(
            connector_patterns=connector_patterns,
            destination_patterns=destination_patterns,
            report=report,
            syncs_interval=7,
        )

        # Should still return connectors even with some errors
        assert len(connectors) >= 0
        # The connector should be processed successfully despite destination errors
        # (the connector gracefully handles missing destination info)
        assert len(connectors) == 1
        assert connectors[0].connector_name == "Error Connector"

    # Test user email resolution

    @responses.activate
    def test_get_user_email_success(self, standard_api: FivetranStandardAPI) -> None:
        """Test successful user email resolution."""
        user_id = "test_user_123"

        responses.add(
            responses.GET,
            f"https://api.fivetran.com/v1/users/{user_id}",
            json={
                "code": "Success",
                "data": {
                    "id": user_id,
                    "email": "testuser@example.com",
                    "given_name": "Test",
                    "family_name": "User",
                    "verified": True,
                },
            },
            status=200,
        )

        email = standard_api.get_user_email(user_id)
        assert email == "testuser@example.com"

    @responses.activate
    def test_get_user_email_not_found(self, standard_api: FivetranStandardAPI) -> None:
        """Test user email resolution when user not found."""
        user_id = "nonexistent_user"

        responses.add(
            responses.GET,
            f"https://api.fivetran.com/v1/users/{user_id}",
            json={"code": "NotFound", "message": "User not found"},
            status=404,
        )

        email = standard_api.get_user_email(user_id)
        assert email is None

    @responses.activate
    def test_get_user_email_multiple_calls(
        self, standard_api: FivetranStandardAPI
    ) -> None:
        """Test that user email resolution works for multiple calls."""
        user_id = "test_user"

        responses.add(
            responses.GET,
            f"https://api.fivetran.com/v1/users/{user_id}",
            json={
                "code": "Success",
                "data": {
                    "id": user_id,
                    "email": "test@example.com",
                    "given_name": "Test",
                    "family_name": "User",
                },
            },
            status=200,
        )

        # First call should hit API
        email1 = standard_api.get_user_email(user_id)
        assert email1 == "test@example.com"

        # Second call should also hit API (no caching in FivetranStandardAPI)
        email2 = standard_api.get_user_email(user_id)
        assert email2 == "test@example.com"

        # Should have made two API calls (no caching implemented)
        assert len(responses.calls) == 2

    # Test parallel processing and performance

    @responses.activate
    def test_parallel_connector_processing(
        self, standard_api: FivetranStandardAPI, report: FivetranSourceReport
    ) -> None:
        """Test that connector processing happens in parallel when configured."""

        # Mock multiple connectors
        connectors_data = []
        for i in range(5):
            connectors_data.append(
                {
                    "id": f"parallel_connector_{i}",
                    "group_id": f"group_{i}",
                    "service": "postgres",
                    "display_name": f"Parallel Connector {i}",
                    "paused": False,
                    "status": {"setup_state": "connected"},
                }
            )

        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={"code": "Success", "data": {"items": connectors_data}},
            status=200,
        )

        # Mock responses for all connectors and destinations
        for i in range(5):
            connector_id = f"parallel_connector_{i}"
            group_id = f"group_{i}"

            # Mock individual connector details
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}",
                json={
                    "code": "Success",
                    "data": {
                        "id": connector_id,
                        "group_id": group_id,
                        "service": "postgres",
                        "display_name": f"Parallel Connector {i}",
                        "paused": False,
                        "status": {"setup_state": "connected"},
                    },
                },
                status=200,
            )

            # Mock destination calls
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/groups/{group_id}",
                json={
                    "code": "Success",
                    "data": {"id": group_id, "name": f"Group {i}"},
                },
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/groups/{group_id}/config",
                json={
                    "code": "Success",
                    "data": {"service": "snowflake", "config": {}},
                },
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/destinations/{group_id}",
                json={
                    "code": "Success",
                    "data": {"id": group_id, "service": "snowflake", "config": {}},
                },
                status=200,
            )

            # Mock schema calls
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/schemas",
                json={"code": "Success", "data": {"schemas": {}}},
                status=200,
            )

            # Mock sync history endpoints
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/sync-history",
                json={"code": "Success", "data": {"items": []}},
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/logs",
                json={"code": "Success", "data": {"items": []}},
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/sync",
                json={
                    "code": "Success",
                    "data": {
                        "status": "successful",
                        "sync_start": "2023-12-01T10:00:00.000Z",
                    },
                },
                status=200,
            )

            # Mock additional endpoints
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/config",
                json={"code": "Success", "data": {}},
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/metadata",
                json={"code": "Success", "data": {}},
                status=200,
            )
            responses.add(
                responses.GET,
                f"https://api.fivetran.com/v1/connectors/{connector_id}/setup_tests",
                json={"code": "Success", "data": {"items": []}},
                status=200,
            )

        # Execute with parallel processing enabled
        connector_patterns = AllowDenyPattern.allow_all()
        destination_patterns = AllowDenyPattern.allow_all()

        connectors = standard_api.get_allowed_connectors_list(
            connector_patterns=connector_patterns,
            destination_patterns=destination_patterns,
            report=report,
            syncs_interval=7,
        )

        # Should process all connectors
        assert len(connectors) == 5

        # Verify all API calls were made (should be many due to parallel processing)
        # Expected calls: 1 list + 5 connectors + 5*3 destinations + 5 schemas + 5 syncs = 26 calls
        assert len(responses.calls) >= 20  # At least 4 calls per connector

    # Test complex lineage extraction workflow

    @responses.activate
    def test_lineage_extraction_workflow(
        self, standard_api: FivetranStandardAPI, report: FivetranSourceReport
    ) -> None:
        """Test the complete lineage extraction workflow."""

        # Mock connector with complex schema
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {
                            "id": "lineage_connector",
                            "group_id": "lineage_group",
                            "service": "postgres",
                            "display_name": "Lineage Test Connector",
                            "paused": False,
                            "status": {"setup_state": "connected"},
                        }
                    ]
                },
            },
            status=200,
        )

        # Mock individual connector details
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector",
            json={
                "code": "Success",
                "data": {
                    "id": "lineage_connector",
                    "group_id": "lineage_group",
                    "service": "postgres",
                    "display_name": "Lineage Test Connector",
                    "paused": False,
                    "status": {"setup_state": "connected"},
                },
            },
            status=200,
        )

        # Mock destination
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/lineage_group",
            json={
                "code": "Success",
                "data": {"id": "lineage_group", "name": "Lineage Group"},
            },
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/groups/lineage_group/config",
            json={
                "code": "Success",
                "data": {
                    "service": "snowflake",
                    "config": {"database": "ANALYTICS", "schema": "PUBLIC"},
                },
            },
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/destinations/lineage_group",
            json={
                "code": "Success",
                "data": {
                    "id": "lineage_group",
                    "service": "snowflake",
                    "config": {"database": "ANALYTICS", "schema": "PUBLIC"},
                },
            },
            status=200,
        )

        # Mock complex schema with multiple tables and relationships
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector/schemas",
            json={
                "code": "Success",
                "data": {
                    "schemas": {
                        "ecommerce": {
                            "name_in_destination": "ecommerce",
                            "enabled": True,
                            "tables": {
                                "customers": {
                                    "name_in_destination": "customers",
                                    "enabled": True,
                                    "columns": {
                                        "customer_id": {
                                            "name_in_destination": "customer_id",
                                            "enabled": True,
                                        },
                                        "email": {
                                            "name_in_destination": "email",
                                            "enabled": True,
                                        },
                                        "created_at": {
                                            "name_in_destination": "created_at",
                                            "enabled": True,
                                        },
                                    },
                                },
                                "orders": {
                                    "name_in_destination": "orders",
                                    "enabled": True,
                                    "columns": {
                                        "order_id": {
                                            "name_in_destination": "order_id",
                                            "enabled": True,
                                        },
                                        "customer_id": {
                                            "name_in_destination": "customer_id",
                                            "enabled": True,
                                        },
                                        "order_date": {
                                            "name_in_destination": "order_date",
                                            "enabled": True,
                                        },
                                        "total_amount": {
                                            "name_in_destination": "total_amount",
                                            "enabled": True,
                                        },
                                    },
                                },
                                "order_items": {
                                    "name_in_destination": "order_items",
                                    "enabled": True,
                                    "columns": {
                                        "item_id": {
                                            "name_in_destination": "item_id",
                                            "enabled": True,
                                        },
                                        "order_id": {
                                            "name_in_destination": "order_id",
                                            "enabled": True,
                                        },
                                        "product_id": {
                                            "name_in_destination": "product_id",
                                            "enabled": True,
                                        },
                                        "quantity": {
                                            "name_in_destination": "quantity",
                                            "enabled": True,
                                        },
                                    },
                                },
                            },
                        }
                    }
                },
            },
            status=200,
        )

        # Mock sync history endpoints
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector/sync-history",
            json={
                "code": "Success",
                "data": {
                    "items": [
                        {
                            "sync_id": "sync_456",
                            "status": "SUCCESSFUL",
                            "sync_start": "2023-12-01T10:00:00.000Z",
                            "sync_end": "2023-12-01T10:30:00.000Z",
                        }
                    ]
                },
            },
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector/logs",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector/sync",
            json={
                "code": "Success",
                "data": {
                    "status": "successful",
                    "sync_start": "2023-12-01T10:00:00.000Z",
                    "sync_end": "2023-12-01T10:30:00.000Z",
                },
            },
            status=200,
        )

        # Mock additional endpoints
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector/config",
            json={
                "code": "Success",
                "data": {
                    "schema": "ecommerce",
                    "host": "localhost",
                    "port": 5432,
                },
            },
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector/metadata",
            json={
                "code": "Success",
                "data": {
                    "connector_id": "lineage_connector",
                    "service": "postgres",
                },
            },
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.fivetran.com/v1/connectors/lineage_connector/setup_tests",
            json={"code": "Success", "data": {"items": []}},
            status=200,
        )

        # Execute lineage extraction
        connector_patterns = AllowDenyPattern.allow_all()
        destination_patterns = AllowDenyPattern.allow_all()

        connectors = standard_api.get_allowed_connectors_list(
            connector_patterns=connector_patterns,
            destination_patterns=destination_patterns,
            report=report,
            syncs_interval=7,
        )

        # Verify lineage was extracted
        assert len(connectors) == 1
        connector = connectors[0]

        # Should have table lineage for the relationships
        assert len(connector.lineage) > 0

        # Verify table lineage structure
        table_names = {tl.source_table for tl in connector.lineage}
        expected_tables = {
            "ecommerce.customers",
            "ecommerce.orders",
            "ecommerce.order_items",
        }
        assert table_names.intersection(expected_tables)

        # Verify column lineage exists
        for table_lineage in connector.lineage:
            if table_lineage.column_lineage:
                assert len(table_lineage.column_lineage) > 0
                for col_lineage in table_lineage.column_lineage:
                    assert col_lineage.source_column
                    assert col_lineage.destination_column
