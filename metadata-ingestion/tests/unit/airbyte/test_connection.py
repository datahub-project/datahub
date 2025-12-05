"""Unit tests for Airbyte connection testing module."""

from unittest.mock import Mock, patch

from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
)
from datahub.ingestion.source.airbyte.connection import (
    test_connection as airbyte_test_connection,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDestinationPartial,
    AirbyteSourcePartial,
    AirbyteWorkspacePartial,
)


class TestAirbyteConnection:
    """Tests for test_connection function."""

    def test_connection_success_oss(self):
        """Test successful connection to OSS Airbyte."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = [
                AirbyteWorkspacePartial(workspace_id="test-workspace", name="Test")
            ]
            mock_client.list_connections.return_value = [
                AirbyteConnectionPartial(
                    connection_id="test-conn",
                    source_id="test-source",
                    destination_id="test-dest",
                )
            ]
            mock_client.get_source.return_value = AirbyteSourcePartial(
                source_id="test-source"
            )
            mock_client.get_destination.return_value = AirbyteDestinationPartial(
                destination_id="test-dest"
            )
            mock_client.list_jobs.return_value = []
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is None  # None means success

    def test_connection_no_workspaces_cloud(self):
        """Test connection failure when no workspaces found in Cloud."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="test-workspace",
            oauth2_client_id="test-client",
            oauth2_refresh_token="test-token",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = []
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is not None
            assert "No workspaces found with ID test-workspace" in result

    def test_connection_no_workspaces_oss(self):
        """Test connection failure when no workspaces found in OSS."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = []
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is not None
            assert "No workspaces found" in result

    def test_connection_invalid_workspace_response(self):
        """Test connection failure when workspace response is not a list."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = {"error": "Invalid"}
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is not None
            assert "expected a list response" in result

    def test_connection_exception_handling(self):
        """Test connection handles exceptions gracefully."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.side_effect = Exception("Connection failed")
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is not None
            assert "Connection failed" in result

    def test_connection_with_connections(self):
        """Test connection with existing connections."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = [
                AirbyteWorkspacePartial(workspace_id="test-workspace")
            ]
            mock_client.list_connections.return_value = [
                AirbyteConnectionPartial(
                    connection_id="test-conn",
                    source_id="test-source",
                    destination_id="test-dest",
                )
            ]
            mock_client.get_source.return_value = AirbyteSourcePartial(
                source_id="test-source"
            )
            mock_client.get_destination.return_value = AirbyteDestinationPartial(
                destination_id="test-dest"
            )
            mock_client.list_jobs.return_value = [{"id": "job1", "status": "succeeded"}]
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is None

    def test_connection_invalid_connections_response(self):
        """Test connection failure when connections response is invalid."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = [
                AirbyteWorkspacePartial(workspace_id="test-workspace")
            ]
            mock_client.list_connections.return_value = "invalid"
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is not None
            assert "expected a list response" in result

    def test_connection_source_failure(self):
        """Test connection handles source retrieval failure."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = [
                AirbyteWorkspacePartial(workspace_id="test-workspace")
            ]
            mock_client.list_connections.return_value = [
                AirbyteConnectionPartial(
                    connection_id="test-conn",
                    source_id="test-source",
                    destination_id="test-dest",
                )
            ]
            mock_client.get_source.side_effect = Exception("Source not found")
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is not None
            assert "Source not found" in result

    def test_connection_destination_failure(self):
        """Test connection handles destination retrieval failure."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = [
                AirbyteWorkspacePartial(workspace_id="test-workspace")
            ]
            mock_client.list_connections.return_value = [
                AirbyteConnectionPartial(
                    connection_id="test-conn",
                    source_id="test-source",
                    destination_id="test-dest",
                )
            ]
            mock_client.get_source.return_value = AirbyteSourcePartial(
                source_id="test-source"
            )
            mock_client.get_destination.side_effect = Exception("Dest not found")
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is not None
            assert "Dest not found" in result

    def test_connection_jobs_failure(self):
        """Test connection handles jobs retrieval failure."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        with patch(
            "datahub.ingestion.source.airbyte.connection.create_airbyte_client"
        ) as mock_create_client:
            mock_client = Mock()
            mock_client.list_workspaces.return_value = [
                AirbyteWorkspacePartial(workspace_id="test-workspace")
            ]
            mock_client.list_connections.return_value = [
                AirbyteConnectionPartial(
                    connection_id="test-conn",
                    source_id="test-source",
                    destination_id="test-dest",
                )
            ]
            mock_client.get_source.return_value = AirbyteSourcePartial(
                source_id="test-source"
            )
            mock_client.get_destination.return_value = AirbyteDestinationPartial(
                destination_id="test-dest"
            )
            mock_client.list_jobs.side_effect = Exception("Jobs failed")
            mock_create_client.return_value = mock_client

            result = airbyte_test_connection(config)

            assert result is not None
            assert "Jobs failed" in result
