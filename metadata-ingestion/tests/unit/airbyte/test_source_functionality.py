from unittest.mock import MagicMock, patch

import pytest

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
from datahub.metadata.schema_classes import DataProcessInstancePropertiesClass
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


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_source_initialization(mock_create_client, mock_ctx, mock_client):
    """Test that the AirbyteSource class initializes correctly."""
    mock_create_client.return_value = mock_client
    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        platform_instance="test-instance",
    )

    source = AirbyteSource(config, mock_ctx)

    assert source.platform == "airbyte"
    assert source.source_config == config
    assert source.client == mock_client
    mock_create_client.assert_called_once_with(config)


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_create_method(mock_create_client, mock_ctx, mock_client):
    """Test the static create method."""
    mock_create_client.return_value = mock_client
    config_dict = {
        "deployment_type": "oss",
        "host_port": "http://localhost:8000",
        "platform_instance": "test-instance",
    }

    source = AirbyteSource.create(config_dict, mock_ctx)

    assert isinstance(source, AirbyteSource)
    assert source.platform == "airbyte"
    mock_create_client.assert_called_once()


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_get_pipelines(mock_create_client, mock_ctx, mock_client):
    """Test the _get_pipelines method."""
    mock_create_client.return_value = mock_client
    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        platform_instance="test-instance",
    )
    source = AirbyteSource(config, mock_ctx)

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
        "sourceType": "postgres",
        "sourceDefinitionId": "source-def-1",
        "workspaceId": "workspace-1",
        "connectionConfiguration": {"host": "localhost", "port": 5432},
    }
    destination_dict = {
        "destinationId": "destination-1",
        "name": "Test Destination",
        "destinationType": "postgres",
        "destinationDefinitionId": "dest-def-1",
        "workspaceId": "workspace-1",
        "connectionConfiguration": {"host": "localhost", "port": 5432},
    }

    mock_client.list_workspaces.return_value = [workspace_dict]
    mock_client.list_connections.return_value = [connection_dict]
    mock_client.get_connection.return_value = connection_dict  # Full connection details
    mock_client.get_source.return_value = source_dict
    mock_client.get_destination.return_value = destination_dict

    pipelines = list(source._get_pipelines())

    assert len(pipelines) == 1
    assert isinstance(pipelines[0], AirbytePipelineInfo)
    assert pipelines[0].workspace.workspace_id == "workspace-1"
    assert pipelines[0].connection.connection_id == "connection-1"
    assert pipelines[0].source.source_id == "source-1"
    assert pipelines[0].destination.destination_id == "destination-1"

    mock_client.list_workspaces.assert_called_once()
    mock_client.list_connections.assert_called_once_with(
        "workspace-1", pattern=AllowDenyPattern.allow_all()
    )
    mock_client.get_connection.assert_called_once_with("connection-1")
    mock_client.get_source.assert_called_once_with("source-1")
    mock_client.get_destination.assert_called_once_with("destination-1")


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_get_pipelines_with_filters(mock_create_client, mock_ctx, mock_client):
    """Test the _get_pipelines method with source and destination filters."""
    mock_create_client.return_value = mock_client
    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        platform_instance="test-instance",
        source_pattern=AllowDenyPattern(allow=["Test Source"]),
        destination_pattern=AllowDenyPattern(allow=["Test Destination"]),
    )
    source = AirbyteSource(config, mock_ctx)

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
        "sourceType": "postgres",
        "sourceDefinitionId": "source-def-1",
        "workspaceId": "workspace-1",
        "connectionConfiguration": {"host": "localhost", "port": 5432},
    }
    destination_dict = {
        "destinationId": "destination-1",
        "name": "Test Destination",
        "destinationType": "postgres",
        "destinationDefinitionId": "dest-def-1",
        "workspaceId": "workspace-1",
        "connectionConfiguration": {"host": "localhost", "port": 5432},
    }

    mock_client.list_workspaces.return_value = [workspace_dict]
    mock_client.list_connections.return_value = [connection_dict]
    mock_client.get_connection.return_value = connection_dict  # Full connection details
    mock_client.get_source.return_value = source_dict
    mock_client.get_destination.return_value = destination_dict

    pipelines = list(source._get_pipelines())

    assert len(pipelines) == 1

    config.source_pattern = AllowDenyPattern(allow=["Different Source"])
    source = AirbyteSource(config, mock_ctx)
    pipelines = list(source._get_pipelines())
    assert len(pipelines) == 0

    config.source_pattern = AllowDenyPattern(allow=["Test Source"])
    config.destination_pattern = AllowDenyPattern(allow=["Different Destination"])
    source = AirbyteSource(config, mock_ctx)
    pipelines = list(source._get_pipelines())
    assert len(pipelines) == 0


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
    mock_create_lineage,
    mock_create_datajob,
    mock_create_dataflow,
    mock_create_client,
    mock_ctx,
    mock_client,
):
    """Test the get_workunits method."""
    mock_create_client.return_value = mock_client
    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        platform_instance="test-instance",
    )
    source = AirbyteSource(config, mock_ctx)

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
        mock_create_dataflow.return_value = ["dataflow_workunit"]
        mock_create_datajob.return_value = ["datajob_workunit"]
        mock_create_lineage.return_value = ["lineage_workunit"]

        workunits = list(source.get_workunits())

        mock_get_pipelines.assert_called_once()
        mock_create_dataflow.assert_called_once_with(pipeline_info)
        mock_create_datajob.assert_called_once_with(pipeline_info)
        mock_create_lineage.assert_called_once_with(pipeline_info)

        assert len(workunits) == 3
        assert workunits == [
            "dataflow_workunit",
            "datajob_workunit",
            "lineage_workunit",
        ]


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_error_handling_in_get_pipelines(mock_create_client, mock_ctx, mock_client):
    """Test error handling in the _get_pipelines method."""
    mock_create_client.return_value = mock_client
    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        platform_instance="test-instance",
    )
    source = AirbyteSource(config, mock_ctx)

    mock_client.list_workspaces.return_value = [
        {"workspaceId": "workspace-1", "name": "Test Workspace"}
    ]
    mock_client.list_connections.side_effect = Exception("Connection error")

    pipelines = list(source._get_pipelines())

    assert len(pipelines) == 0
    assert len(source.report.failures) == 1

    source = AirbyteSource(config, mock_ctx)
    mock_client.list_connections.side_effect = None
    mock_client.list_connections.return_value = [
        {
            "connectionId": "connection-1",
            "name": "Test Connection",
            "sourceId": "source-1",
            "destinationId": "destination-1",
            "status": "active",
        }
    ]
    mock_client.get_source.side_effect = Exception("Source error")

    pipelines = list(source._get_pipelines())

    assert len(pipelines) == 0
    assert len(source.report.failures) == 1


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_job_execution_enrichment_with_get_job(
    mock_create_client, mock_ctx, mock_client
):
    """Test that job executions are enriched with detailed sync statistics from get_job()."""
    mock_create_client.return_value = mock_client

    workspace = AirbyteWorkspacePartial(
        workspace_id="workspace-123", name="Test Workspace"
    )
    source = AirbyteSourcePartial(
        source_id="source-123",
        name="Postgres Source",
        source_type="postgres",
        workspace_id="workspace-123",
    )
    destination = AirbyteDestinationPartial(
        destination_id="dest-123",
        name="Snowflake Destination",
        destination_type="snowflake",
        workspace_id="workspace-123",
    )
    connection = AirbyteConnectionPartial(
        connection_id="conn-123",
        name="Test Connection",
        source_id="source-123",
        destination_id="dest-123",
        status="active",
        sync_catalog={
            "streams": [
                {
                    "stream": {
                        "name": "users",
                        "namespace": "public",
                        "jsonSchema": {
                            "type": "object",
                            "properties": {"id": {"type": "integer"}},
                        },
                    },
                    "config": {"selected": True, "destinationSyncMode": "append"},
                }
            ]
        },
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        source=source,
        destination=destination,
        connection=connection,
    )

    mock_client.list_jobs.return_value = [
        {
            "id": "job-456",
            "attempts": [
                {
                    "id": "attempt-1",
                    "status": "succeeded",
                    "createdAt": 1609459200000,
                    "endedAt": 1609462800000,
                }
            ],
        }
    ]

    mock_client.get_job.return_value = {
        "jobId": "job-456",
        "status": "succeeded",
        "bytesCommitted": 2048000,
        "recordsCommitted": 10000,
        "streamStatuses": [
            {
                "streamName": "public.users",
                "recordsCommitted": 10000,
                "bytesCommitted": 2048000,
            }
        ],
    }

    mock_client.list_streams.return_value = {
        "streams": [
            {
                "streamName": "users",
                "namespace": "public",
                "propertyFields": [["id"], ["name"], ["email"]],
            }
        ]
    }

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
    )

    source_obj = AirbyteSource(config, mock_ctx)
    source_obj.client = mock_client

    connection_dataflow_urn = DataFlowUrn(
        orchestrator="airbyte",
        flow_id="conn-123",
        cluster="PROD",
    )
    datajob_urn = DataJobUrn(
        flow=connection_dataflow_urn,
        job_id="conn-123_public.users",
    )

    workunits = list(
        source_obj._create_job_executions_workunits(
            pipeline_info, datajob_urn, "public.users"
        )
    )

    assert len(workunits) > 0

    dpi_props_found = False
    for wu in workunits:
        if hasattr(wu.metadata, "aspect") and isinstance(
            wu.metadata.aspect, DataProcessInstancePropertiesClass
        ):
            props = wu.metadata.aspect.customProperties
            if props.get("job_id") == "job-456":
                dpi_props_found = True
                assert props.get("bytes_committed") == "2048000"
                assert props.get("records_committed") == "10000"
                assert props.get("stream_records_committed") == "10000"
                assert props.get("stream_bytes_committed") == "2048000"
                break

    assert dpi_props_found, (
        "DataProcessInstanceProperties with enriched job stats not found"
    )
    mock_client.get_job.assert_called_once_with("job-456")


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_job_execution_without_get_job_details(
    mock_create_client, mock_ctx, mock_client
):
    """Test that job executions work even when get_job() fails."""
    mock_create_client.return_value = mock_client

    workspace = AirbyteWorkspacePartial(
        workspace_id="workspace-123", name="Test Workspace"
    )
    source = AirbyteSourcePartial(
        source_id="source-123",
        name="Postgres Source",
        source_type="postgres",
        workspace_id="workspace-123",
    )
    destination = AirbyteDestinationPartial(
        destination_id="dest-123",
        name="Snowflake Destination",
        destination_type="snowflake",
        workspace_id="workspace-123",
    )
    connection = AirbyteConnectionPartial(
        connection_id="conn-123",
        name="Test Connection",
        source_id="source-123",
        destination_id="dest-123",
        status="active",
        sync_catalog={
            "streams": [
                {
                    "stream": {
                        "name": "orders",
                        "namespace": "public",
                        "jsonSchema": {
                            "type": "object",
                            "properties": {"id": {"type": "integer"}},
                        },
                    },
                    "config": {"selected": True, "destinationSyncMode": "append"},
                }
            ]
        },
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        source=source,
        destination=destination,
        connection=connection,
    )

    mock_client.list_jobs.return_value = [
        {
            "id": "job-789",
            "attempts": [
                {
                    "id": "attempt-1",
                    "status": "succeeded",
                    "createdAt": 1609459200000,
                    "endedAt": 1609462800000,
                }
            ],
        }
    ]

    mock_client.get_job.side_effect = Exception("API Error")

    mock_client.list_streams.return_value = {
        "streams": [
            {
                "streamName": "orders",
                "namespace": "public",
                "propertyFields": [["id"]],
            }
        ]
    }

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
    )

    source_obj = AirbyteSource(config, mock_ctx)
    source_obj.client = mock_client

    connection_dataflow_urn = DataFlowUrn(
        orchestrator="airbyte",
        flow_id="conn-123",
        cluster="PROD",
    )
    datajob_urn = DataJobUrn(
        flow=connection_dataflow_urn,
        job_id="conn-123_public.orders",
    )

    workunits = list(
        source_obj._create_job_executions_workunits(
            pipeline_info, datajob_urn, "public.orders"
        )
    )

    assert len(workunits) > 0

    dpi_props_found = False
    for wu in workunits:
        if hasattr(wu.metadata, "aspect") and isinstance(
            wu.metadata.aspect, DataProcessInstancePropertiesClass
        ):
            props = wu.metadata.aspect.customProperties
            if props.get("job_id") == "job-789":
                dpi_props_found = True
                assert "bytes_committed" not in props
                assert "records_committed" not in props
                break

    assert dpi_props_found, (
        "DataProcessInstanceProperties not found even without get_job details"
    )
