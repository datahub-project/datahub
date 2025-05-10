import unittest
from unittest.mock import MagicMock, patch

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.airbyte.config import (
    AirbyteDeploymentType,
    AirbyteSourceConfig,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDestinationPartial,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteWorkspacePartial,
)
from datahub.ingestion.source.airbyte.source import AirbyteSource


class TestAirbyteSource(unittest.TestCase):
    """Test class for the AirbyteSource functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_ctx = MagicMock(spec=PipelineContext)
        # Add graph attribute to the mock context
        self.mock_ctx.graph = MagicMock()
        # Add pipeline_name attribute to the mock context
        self.mock_ctx.pipeline_name = "airbyte_test"

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_source_initialization(self, mock_create_client):
        """Test that the AirbyteSource class initializes correctly."""
        # Setup
        mock_create_client.return_value = self.mock_client
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            platform_instance="test-instance",
        )

        # Execute
        source = AirbyteSource(config, self.mock_ctx)

        # Assert
        self.assertEqual(source.platform, "airbyte")
        self.assertEqual(source.source_config, config)
        self.assertEqual(source.client, self.mock_client)
        mock_create_client.assert_called_once_with(config)

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_create_method(self, mock_create_client):
        """Test the static create method."""
        # Setup
        mock_create_client.return_value = self.mock_client
        config_dict = {
            "deployment_type": "oss",
            "host_port": "http://localhost:8000",
            "platform_instance": "test-instance",
        }

        # Execute
        source = AirbyteSource.create(config_dict, self.mock_ctx)

        # Assert
        self.assertIsInstance(source, AirbyteSource)
        self.assertEqual(source.platform, "airbyte")
        mock_create_client.assert_called_once()

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_pipelines(self, mock_create_client):
        """Test the _get_pipelines method."""
        # Setup
        mock_create_client.return_value = self.mock_client
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            platform_instance="test-instance",
        )
        source = AirbyteSource(config, self.mock_ctx)

        # Mock client responses
        workspace_dict = {
            "workspaceId": "workspace-1",
            "name": "Test Workspace",
        }
        connection_dict = {
            "connectionId": "connection-1",
            "name": "Test Connection",
            "sourceId": "source-1",
            "destinationId": "destination-1",
            "status": "active",
        }
        source_dict = {
            "sourceId": "source-1",
            "name": "Test Source",
            "sourceDefinitionId": "source-def-1",
            "workspaceId": "workspace-1",
            "connectionConfiguration": {"host": "localhost", "port": 5432},
        }
        destination_dict = {
            "destinationId": "destination-1",
            "name": "Test Destination",
            "destinationDefinitionId": "dest-def-1",
            "workspaceId": "workspace-1",
            "connectionConfiguration": {"host": "localhost", "port": 5432},
        }

        # Set up mock behaviors
        self.mock_client.list_workspaces.return_value = [workspace_dict]
        self.mock_client.list_connections.return_value = [connection_dict]
        self.mock_client.get_source.return_value = source_dict
        self.mock_client.get_destination.return_value = destination_dict

        # Execute
        pipelines = list(source._get_pipelines())

        # Assert
        self.assertEqual(len(pipelines), 1)
        self.assertIsInstance(pipelines[0], AirbytePipelineInfo)
        self.assertEqual(pipelines[0].workspace.workspace_id, "workspace-1")
        self.assertEqual(pipelines[0].connection.connection_id, "connection-1")
        self.assertEqual(pipelines[0].source.source_id, "source-1")
        self.assertEqual(pipelines[0].destination.destination_id, "destination-1")

        # Verify client method calls
        self.mock_client.list_workspaces.assert_called_once()
        self.mock_client.list_connections.assert_called_once_with(
            "workspace-1", pattern=AllowDenyPattern.allow_all()
        )
        self.mock_client.get_source.assert_called_once_with("source-1")
        self.mock_client.get_destination.assert_called_once_with("destination-1")

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_pipelines_with_filters(self, mock_create_client):
        """Test the _get_pipelines method with source and destination filters."""
        # Setup
        mock_create_client.return_value = self.mock_client
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            platform_instance="test-instance",
            source_pattern=AllowDenyPattern(allow=["Test Source"]),
            destination_pattern=AllowDenyPattern(allow=["Test Destination"]),
        )
        source = AirbyteSource(config, self.mock_ctx)

        # Mock client responses
        workspace_dict = {
            "workspaceId": "workspace-1",
            "name": "Test Workspace",
        }
        connection_dict = {
            "connectionId": "connection-1",
            "name": "Test Connection",
            "sourceId": "source-1",
            "destinationId": "destination-1",
            "status": "active",
        }
        source_dict = {
            "sourceId": "source-1",
            "name": "Test Source",
            "sourceDefinitionId": "source-def-1",
            "workspaceId": "workspace-1",
            "connectionConfiguration": {"host": "localhost", "port": 5432},
        }
        destination_dict = {
            "destinationId": "destination-1",
            "name": "Test Destination",
            "destinationDefinitionId": "dest-def-1",
            "workspaceId": "workspace-1",
            "connectionConfiguration": {"host": "localhost", "port": 5432},
        }

        # Set up mock behaviors
        self.mock_client.list_workspaces.return_value = [workspace_dict]
        self.mock_client.list_connections.return_value = [connection_dict]
        self.mock_client.get_source.return_value = source_dict
        self.mock_client.get_destination.return_value = destination_dict

        # Execute
        pipelines = list(source._get_pipelines())

        # Assert
        self.assertEqual(len(pipelines), 1)

        # Test with source filter that doesn't match
        config.source_pattern = AllowDenyPattern(allow=["Different Source"])
        source = AirbyteSource(config, self.mock_ctx)
        pipelines = list(source._get_pipelines())
        self.assertEqual(len(pipelines), 0)

        # Test with destination filter that doesn't match
        config.source_pattern = AllowDenyPattern(allow=["Test Source"])
        config.destination_pattern = AllowDenyPattern(allow=["Different Destination"])
        source = AirbyteSource(config, self.mock_ctx)
        pipelines = list(source._get_pipelines())
        self.assertEqual(len(pipelines), 0)

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    @patch(
        "datahub.ingestion.source.airbyte.source.AirbyteSource._create_dataflow_workunits"
    )
    @patch(
        "datahub.ingestion.source.airbyte.source.AirbyteSource._create_datajob_workunits"
    )
    @patch(
        "datahub.ingestion.source.airbyte.source.AirbyteSource._create_lineage_workunits"
    )
    def test_get_workunits(
        self,
        mock_create_lineage,
        mock_create_datajob,
        mock_create_dataflow,
        mock_create_client,
    ):
        """Test the get_workunits method."""
        # Setup
        mock_create_client.return_value = self.mock_client
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            platform_instance="test-instance",
        )
        source = AirbyteSource(config, self.mock_ctx)

        # Mock pipeline info
        workspace = AirbyteWorkspacePartial(
            workspace_id="workspace-1", name="Test Workspace"
        )
        connection = AirbyteConnectionPartial(
            connection_id="connection-1",
            name="Test Connection",
            source_id="source-1",
            destination_id="destination-1",
        )
        source_info = AirbyteSourcePartial(
            source_id="source-1",
            name="Test Source",
            source_definition_id="source-def-1",
            workspace_id="workspace-1",
        )
        destination = AirbyteDestinationPartial(
            destination_id="destination-1",
            name="Test Destination",
            destination_definition_id="dest-def-1",
            workspace_id="workspace-1",
        )

        pipeline_info = AirbytePipelineInfo(
            workspace=workspace,
            connection=connection,
            source=source_info,
            destination=destination,
        )

        # Mock source._get_pipelines to return our test data
        with patch.object(
            source, "_get_pipelines", return_value=[pipeline_info]
        ) as mock_get_pipelines:
            # Set up mocks for all the "create_*_workunits" methods
            mock_create_dataflow.return_value = ["dataflow_workunit"]
            mock_create_datajob.return_value = ["datajob_workunit"]
            mock_create_lineage.return_value = ["lineage_workunit"]

            # Execute
            workunits = list(source.get_workunits())

            # Assert
            mock_get_pipelines.assert_called_once()
            mock_create_dataflow.assert_called_once_with(pipeline_info)
            mock_create_datajob.assert_called_once_with(pipeline_info)
            mock_create_lineage.assert_called_once_with(pipeline_info)

            # Verify all the workunits are combined
            self.assertEqual(len(workunits), 3)
            self.assertEqual(
                workunits, ["dataflow_workunit", "datajob_workunit", "lineage_workunit"]
            )

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_error_handling_in_get_pipelines(self, mock_create_client):
        """Test error handling in the _get_pipelines method."""
        # Setup
        mock_create_client.return_value = self.mock_client
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            platform_instance="test-instance",
        )
        source = AirbyteSource(config, self.mock_ctx)

        # Mock client to raise exceptions
        self.mock_client.list_workspaces.return_value = [
            {"workspaceId": "workspace-1", "name": "Test Workspace"}
        ]
        self.mock_client.list_connections.side_effect = Exception("Connection error")

        # Execute
        pipelines = list(source._get_pipelines())

        # Assert
        self.assertEqual(len(pipelines), 0)  # No pipelines should be returned
        self.assertEqual(
            len(source.report.failures), 1
        )  # One failure should be reported

        # Test error with source retrieval - create a new source instance to avoid stale state
        source = AirbyteSource(config, self.mock_ctx)
        self.mock_client.list_connections.side_effect = None
        self.mock_client.list_connections.return_value = [
            {
                "connectionId": "connection-1",
                "name": "Test Connection",
                "sourceId": "source-1",
                "destinationId": "destination-1",
                "status": "active",
            }
        ]
        self.mock_client.get_source.side_effect = Exception("Source error")

        # Execute again with new source instance
        pipelines = list(source._get_pipelines())

        # Assert
        self.assertEqual(len(pipelines), 0)  # No pipelines should be returned
        self.assertEqual(
            len(source.report.failures), 1
        )  # One failure should be reported


if __name__ == "__main__":
    unittest.main()
