from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.airbyte.config import (
    AirbyteDeploymentType,
    AirbyteSourceConfig,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDatasetUrns,
    AirbyteDestinationPartial,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteStreamDetails,
    AirbyteWorkspacePartial,
    DataFlowResult,
    DataJobResult,
)
from datahub.ingestion.source.airbyte.source import AirbyteSource
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn


@pytest.fixture
def mock_client():
    """Create a mock Airbyte client."""
    return MagicMock()


@pytest.fixture
def mock_ctx():
    """Create a mock pipeline context."""
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = MagicMock()
    ctx.pipeline_name = "airbyte_test"
    return ctx


@pytest.fixture
def config():
    """Create a standard test config."""
    return AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        platform_instance="test-instance",
    )


@pytest.fixture
def source(config, mock_ctx, mock_client):
    """Create an AirbyteSource instance with mocked dependencies."""
    with patch(
        "datahub.ingestion.source.airbyte.source.create_airbyte_client"
    ) as mock_create_client:
        mock_create_client.return_value = mock_client
        source = AirbyteSource(config, mock_ctx)

        source._get_source_platform = MagicMock(return_value="postgres")  # type: ignore[attr-defined]
        source._get_destination_platform = MagicMock(return_value="postgres")  # type: ignore[attr-defined]
        source._get_source_type_from_definition = MagicMock(  # type: ignore[attr-defined]
            return_value="postgres"
        )
        source._get_destination_type_from_definition = MagicMock(  # type: ignore[attr-defined]
            return_value="postgres"
        )

        return source


@pytest.fixture
def workspace():
    """Create a test workspace."""
    return AirbyteWorkspacePartial(
        workspaceId="workspace-1",
        name="Test Workspace",
        workspace_id="workspace-1",
    )


@pytest.fixture
def connection():
    """Create a test connection."""
    return AirbyteConnectionPartial(
        connectionId="connection-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="destination-1",
        status="active",
        connection_id="connection-1",
        source_id="source-1",
        destination_id="destination-1",
    )


@pytest.fixture
def source_obj():
    """Create a test source object."""
    return AirbyteSourcePartial(
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


@pytest.fixture
def destination():
    """Create a test destination."""
    return AirbyteDestinationPartial(
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


@pytest.fixture
def pipeline_info(workspace, connection, source_obj, destination):
    """Create a test pipeline info."""
    return AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )


@pytest.fixture
def stream():
    """Create a test stream."""
    return AirbyteStreamDetails(
        streamName="customers",
        namespace="public",
        propertyFields=["id", "name", "email"],
        stream_name="customers",
    )


def test_create_connection_dataflow(source, pipeline_info):
    """Test the _create_connection_dataflow method."""
    tags = ["test-tag-1", "test-tag-2"]

    result = source._create_connection_dataflow(
        pipeline_info=pipeline_info,
        tags=tags,
    )

    assert isinstance(result, DataFlowResult)
    assert isinstance(result.dataflow_urn, DataFlowUrn)
    # The expected URN should use connection ID not workspace ID
    expected_urn = f"urn:li:dataFlow:(airbyte,{pipeline_info.connection.connection_id},{source.source_config.env})"
    assert str(result.dataflow_urn) == expected_urn

    # Check that we get the expected workunits
    workunits_list = list(result.work_units)
    assert len(workunits_list) >= 1
    for workunit in workunits_list:
        assert isinstance(workunit, MetadataWorkUnit)
        aspect = workunit.metadata.to_obj()
        # Check that the aspect contains the expected data
        if hasattr(aspect, "info"):
            assert aspect.info.name == "Test Workspace"
            assert aspect.info.description == "Airbyte connection: Test Connection"

        # Check for tag presence if global tags aspect
        if hasattr(aspect, "globalTags"):
            tag_urns = [tag.tag for tag in aspect.globalTags.tags]
            for tag in tags:
                assert f"urn:li:tag:{tag}" in tag_urns


def test_create_stream_datajob(source, pipeline_info, stream):
    """Test the _create_stream_datajob method."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"
    destination_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"
    )
    tags = ["test-tag-1", "test-tag-2"]

    dataflow_urn = DataFlowUrn(
        orchestrator="airbyte",
        flow_id="connection-1",
        cluster=source.source_config.env,
    )

    # We need to provide a job_id directly since we're not calling through the method that constructs it
    expected_job_id = f"{pipeline_info.connection.connection_id}_{stream.stream_name}"

    result = source._create_stream_datajob(
        connection_dataflow_urn=dataflow_urn,
        pipeline_info=pipeline_info,
        stream=stream,
        source_urn=source_urn,
        destination_urn=destination_urn,
        tags=tags,
    )

    assert isinstance(result, DataJobResult)
    assert isinstance(result.datajob_urn, DataJobUrn)
    expected_urn = f"urn:li:dataJob:(urn:li:dataFlow:(airbyte,connection-1,{source.source_config.env}),{expected_job_id})"
    assert str(result.datajob_urn) == expected_urn


def test_create_dataset_lineage(source, stream):
    """Test the _create_dataset_lineage method."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"
    destination_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)"
    )

    # Add destination to known_urns so lineage is emitted
    source.known_urns.add(destination_urn)

    # Ensure column-level lineage is disabled for this test
    source.source_config.extract_column_level_lineage = False

    workunits = source._create_dataset_lineage(
        source_urn=source_urn,
        destination_urn=destination_urn,
        stream=stream,
    )

    workunits_list = list(workunits)
    assert len(workunits_list) >= 1


def test_create_lineage_workunits(source, pipeline_info, stream):
    """Test the _create_lineage_workunits method."""
    with (
        patch.object(source, "_get_source_platform", return_value="postgres"),
        patch.object(source, "_get_destination_platform", return_value="postgres"),
        patch.object(source, "_fetch_streams_for_source", return_value=[stream]),
        patch.object(
            source, "_extract_connection_tags", return_value=["airbyte", "etl"]
        ),
        patch.object(source, "_create_dataset_lineage") as mock_create_dataset_lineage,
        patch.object(
            source, "_create_connection_dataflow"
        ) as mock_create_connection_dataflow,
        patch.object(source, "_create_stream_datajob") as mock_create_stream_datajob,
        patch.object(source, "_create_dataset_urns") as mock_create_dataset_urns,
    ):
        # Mock return values for the patched methods
        mock_dataflow_urn = DataFlowUrn.create_from_ids(
            "airbyte", "workspace-1", source.source_config.env
        )
        mock_create_connection_dataflow.return_value = DataFlowResult(
            dataflow_urn=mock_dataflow_urn,
            work_units=[MagicMock(spec=MetadataWorkUnit)],
        )

        mock_datajob_urn = DataJobUrn.create_from_ids(
            str(mock_dataflow_urn), "connection-1.public.customers"
        )
        mock_create_stream_datajob.return_value = DataJobResult(
            datajob_urn=mock_datajob_urn,
            work_units=[MagicMock(spec=MetadataWorkUnit)],
        )

        mock_create_dataset_urns.return_value = AirbyteDatasetUrns(
            source_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)",
            destination_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,TEST)",
        )

        mock_create_dataset_lineage.return_value = [MagicMock(spec=MetadataWorkUnit)]

        workunits = list(source._create_lineage_workunits(pipeline_info))

        # Verify that the expected methods were called with the right arguments
        mock_create_connection_dataflow.assert_called_once()
        mock_create_stream_datajob.assert_called_once()
        mock_create_dataset_lineage.assert_called_once()

        # Check that we received the expected number of workunits
        assert len(workunits) == 3


def test_create_lineage_workunits_with_disabled_pipeline(source, pipeline_info):
    """Test the _create_lineage_workunits method with a disabled pipeline."""

    pipeline_info.connection.status = "inactive"

    with (
        patch.object(source, "_get_source_platform", return_value="postgres"),
        patch.object(source, "_get_destination_platform", return_value="postgres"),
        patch.object(source, "_extract_connection_tags", return_value=[]),
        patch.object(source, "_fetch_streams_for_source", return_value=[]),
        patch.object(
            source,
            "_create_connection_dataflow",
            return_value=DataFlowResult(
                dataflow_urn=DataFlowUrn.create_from_ids(
                    "airbyte", "workspace-1", "PROD"
                ),
                work_units=[],
            ),
        ),
    ):
        workunits = list(source._create_lineage_workunits(pipeline_info))

        # Verify no workunits were created for inactive connection
        assert len(workunits) == 0
