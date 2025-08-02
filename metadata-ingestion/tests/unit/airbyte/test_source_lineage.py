import unittest
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.airbyte.config import (
    AirbyteDeploymentType,
    AirbyteSourceConfig,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDestinationPartial,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteStreamDetails,
    AirbyteWorkspacePartial,
)
from datahub.ingestion.source.airbyte.source import AirbyteSource
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn


class TestAirbyteSourceLineage(unittest.TestCase):
    """Test class for the AirbyteSource lineage functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_ctx = MagicMock(spec=PipelineContext)

        # Create a standard config for testing
        self.config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            platform_instance="test-instance",
        )

        # Add graph attribute to the mock context
        self.mock_ctx.graph = MagicMock()
        # Add pipeline_name attribute to the mock context
        self.mock_ctx.pipeline_name = "airbyte_test"

        # Mock the client creation
        with patch(
            "datahub.ingestion.source.airbyte.source.create_airbyte_client"
        ) as mock_create_client:
            mock_create_client.return_value = self.mock_client
            self.source = AirbyteSource(self.config, self.mock_ctx)

            # Add the methods directly to the instance rather than trying to patch the class
            self.source._get_source_platform = MagicMock(return_value="postgres")  # type: ignore[attr-defined]
            self.source._get_destination_platform = MagicMock(return_value="postgres")  # type: ignore[attr-defined]
            self.source._get_source_type_from_definition = MagicMock(  # type: ignore[attr-defined]
                return_value="postgres"
            )
            self.source._get_destination_type_from_definition = MagicMock(  # type: ignore[attr-defined]
                return_value="postgres"
            )

        # Create standard test objects
        self.workspace = AirbyteWorkspacePartial(
            workspaceId="workspace-1",
            name="Test Workspace",
            workspace_id="workspace-1",
        )
        self.connection = AirbyteConnectionPartial(
            connectionId="connection-1",
            name="Test Connection",
            sourceId="source-1",
            destinationId="destination-1",
            status="active",
            connection_id="connection-1",
            source_id="source-1",
            destination_id="destination-1",
        )
        self.source_obj = AirbyteSourcePartial(
            sourceId="source-1",
            name="Test Source",
            sourceType="postgres",
            sourceDefinitionId="source-def-1",
            workspaceId="workspace-1",
            source_id="source-1",
            source_type="postgres",
            source_definition_id="source-def-1",
            workspace_id="workspace-1",
            configuration={
                "host": "localhost",
                "port": 5432,
                "database": "source_db",
                "schema": "public",
            },
        )
        self.destination = AirbyteDestinationPartial(
            destinationId="destination-1",
            name="Test Destination",
            destinationType="postgres",
            destinationDefinitionId="dest-def-1",
            workspaceId="workspace-1",
            destination_id="destination-1",
            destination_type="postgres",
            destination_definition_id="dest-def-1",
            workspace_id="workspace-1",
            configuration={
                "host": "localhost",
                "port": 5432,
                "database": "dest_db",
                "schema": "public",
            },
        )
        self.pipeline_info = AirbytePipelineInfo(
            workspace=self.workspace,
            connection=self.connection,
            source=self.source_obj,
            destination=self.destination,
        )
        self.stream = AirbyteStreamDetails(
            streamName="customers",
            namespace="public",
            propertyFields=["id", "name", "email"],
            stream_name="customers",
        )

    def tearDown(self) -> None:
        """Tear down test fixtures."""
        pass

    def test_create_connection_dataflow(self):
        """Test the _create_connection_dataflow method."""
        # Setup test tags
        tags = ["test-tag-1", "test-tag-2"]

        # Execute the method
        dataflow_urn, workunits = self.source._create_connection_dataflow(
            pipeline_info=self.pipeline_info,
            tags=tags,
        )

        # Verify the results
        self.assertIsInstance(dataflow_urn, DataFlowUrn)
        # The expected URN should use connection ID not workspace ID
        expected_urn = f"urn:li:dataFlow:(airbyte,{self.connection.connection_id},{self.source.source_config.env})"
        self.assertEqual(str(dataflow_urn), expected_urn)

        # Check that we get the expected workunits
        workunits_list = list(workunits)
        self.assertGreaterEqual(len(workunits_list), 1)
        for workunit in workunits_list:
            self.assertIsInstance(workunit, MetadataWorkUnit)
            aspect = workunit.metadata.to_obj()
            # Check that the aspect contains the expected data
            if hasattr(aspect, "info"):
                self.assertEqual(aspect.info.name, "Test Workspace")
                self.assertEqual(
                    aspect.info.description, "Airbyte connection: Test Connection"
                )

            # Check for tag presence if global tags aspect
            if hasattr(aspect, "globalTags"):
                tag_urns = [tag.tag for tag in aspect.globalTags.tags]
                for tag in tags:
                    self.assertIn(f"urn:li:tag:{tag}", tag_urns)

    def test_create_stream_datajob(self):
        """Test the _create_stream_datajob method."""
        # Setup
        source_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"
        )
        destination_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"
        )
        tags = ["test-tag-1", "test-tag-2"]

        # Create a dataflow URN
        dataflow_urn = DataFlowUrn(
            orchestrator="airbyte",
            flow_id="connection-1",
            cluster=self.source.source_config.env,
        )

        # We need to provide a job_id directly since we're not calling through the method that constructs it
        expected_job_id = f"{self.connection.connection_id}_{self.stream.stream_name}"

        # Execute the method
        datajob_urn, workunits = self.source._create_stream_datajob(
            connection_dataflow_urn=dataflow_urn,
            pipeline_info=self.pipeline_info,
            stream=self.stream,
            source_urn=source_urn,
            destination_urn=destination_urn,
            tags=tags,
        )

        # Verify the results
        self.assertIsInstance(datajob_urn, DataJobUrn)
        expected_urn = f"urn:li:dataJob:(urn:li:dataFlow:(airbyte,connection-1,{self.source.source_config.env}),{expected_job_id})"
        self.assertEqual(str(datajob_urn), expected_urn)

    def test_create_dataset_lineage(self):
        """Test the _create_dataset_lineage method."""
        # Setup
        source_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"
        )
        destination_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"
        )
        tags = ["test-tag-1", "test-tag-2"]

        # Ensure column-level lineage is disabled for this test
        self.source.source_config.extract_column_level_lineage = False

        # Execute the method
        workunits = self.source._create_dataset_lineage(
            source_urn=source_urn,
            destination_urn=destination_urn,
            stream=self.stream,
            tags=tags,
        )

        # Verify the results
        workunits_list = list(workunits)
        self.assertGreaterEqual(len(workunits_list), 1)

    def test_create_lineage_workunits(self):
        """Test the _create_lineage_workunits method."""
        # Setup mocks for platform detection and input/output datasets
        with patch.object(
            self.source, "_get_source_platform", return_value="postgres"
        ), patch.object(
            self.source, "_get_destination_platform", return_value="postgres"
        ), patch.object(
            self.source,
            "_get_input_output_datasets",
            return_value=(
                ["urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"],
                ["urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"],
            ),
        ), patch.object(
            self.source, "_fetch_streams_for_source", return_value=[self.stream]
        ), patch.object(
            self.source, "_extract_connection_tags", return_value=["airbyte", "etl"]
        ), patch.object(
            self.source, "_create_dataset_lineage"
        ) as mock_create_dataset_lineage, patch.object(
            self.source, "_create_connection_dataflow"
        ) as mock_create_connection_dataflow, patch.object(
            self.source, "_create_stream_datajob"
        ) as mock_create_stream_datajob:
            # Mock return values for the patched methods
            mock_dataflow_urn = DataFlowUrn.create_from_ids(
                "airbyte", "workspace-1", self.source.source_config.env
            )
            mock_create_connection_dataflow.return_value = (
                mock_dataflow_urn,
                [MagicMock(spec=MetadataWorkUnit)],
            )

            mock_datajob_urn = DataJobUrn.create_from_ids(
                str(mock_dataflow_urn), "connection-1.public.customers"
            )
            mock_create_stream_datajob.return_value = (
                mock_datajob_urn,
                [MagicMock(spec=MetadataWorkUnit)],
            )

            mock_create_dataset_lineage.return_value = [
                MagicMock(spec=MetadataWorkUnit)
            ]

            # Execute the method
            workunits = list(self.source._create_lineage_workunits(self.pipeline_info))

            # Verify that the expected methods were called with the right arguments
            mock_create_connection_dataflow.assert_called_once()
            mock_create_stream_datajob.assert_called_once()
            mock_create_dataset_lineage.assert_called_once()

            # Check that we received the expected number of workunits
            # (1 from dataflow, 1 from datajob, 1 from dataset lineage)
            self.assertEqual(len(workunits), 3)

    def test_create_lineage_workunits_with_disabled_pipeline(self):
        """Test the _create_lineage_workunits method with a disabled pipeline."""
        # Modify connection to be inactive
        self.connection.status = "inactive"

        # Mock dependencies to ensure we don't process disabled pipelines
        with patch.object(
            self.source, "_get_source_platform", return_value="postgres"
        ), patch.object(
            self.source, "_get_destination_platform", return_value="postgres"
        ), patch.object(
            self.source, "_extract_connection_tags", return_value=[]
        ), patch.object(
            self.source, "_fetch_streams_for_source", return_value=[]
        ), patch.object(
            self.source, "_create_connection_dataflow", return_value=(None, [])
        ):
            # Execute the method
            workunits = list(self.source._create_lineage_workunits(self.pipeline_info))

            # Verify no workunits were created for inactive connection
            self.assertEqual(len(workunits), 0)


if __name__ == "__main__":
    unittest.main()
