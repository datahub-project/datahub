from unittest.mock import MagicMock, patch

import pytest

# mypy: disable-error-code="method-assign,attr-defined"
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
    AirbyteStream,
    AirbyteStreamConfig,
    AirbyteSyncCatalog,
    AirbyteWorkspacePartial,
)
from datahub.ingestion.source.airbyte.source import (
    AirbyteSource,
    _sanitize_platform_name,
)


@pytest.mark.parametrize(
    "input_name,expected_output",
    [
        ("test mysql source", "test-mysql-source"),
        ("test postgres source", "test-postgres-source"),
        ("Test Postgres Destination", "test-postgres-destination"),
        ("MySQL", "mysql"),
        ("PostgreSQL", "postgresql"),
        ("bigquery", "bigquery"),
        ("My Custom Platform", "my-custom-platform"),
        ("platform with  multiple  spaces", "platform-with--multiple--spaces"),
    ],
)
def test_sanitize_platform_name(input_name, expected_output):
    """Test platform name sanitization for URN compatibility."""
    assert _sanitize_platform_name(input_name) == expected_output


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
        return AirbyteSource(config, mock_ctx)


def test_fetch_streams_for_source(source):
    """Test the _fetch_streams_for_source method."""
    workspace = AirbyteWorkspacePartial(
        workspaceId="workspace-1",
        name="Test Workspace",
        workspace_id="workspace-1",
    )
    connection = AirbyteConnectionPartial(
        connectionId="connection-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="destination-1",
        status="active",
        connection_id="connection-1",
        source_id="source-1",
        destination_id="destination-1",
    )
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceDefinitionId="source-def-1",
        workspaceId="workspace-1",
        source_id="source-1",
        source_definition_id="source-def-1",
        workspace_id="workspace-1",
        configuration={},
    )
    destination = AirbyteDestinationPartial(
        destinationId="destination-1",
        name="Test Destination",
        destinationDefinitionId="dest-def-1",
        workspaceId="workspace-1",
        destination_id="destination-1",
        destination_definition_id="dest-def-1",
        workspace_id="workspace-1",
        configuration={},
    )

    customer_stream = AirbyteStream(
        name="customers",
        namespace="public",
        json_schema={"properties": {"id": {}, "name": {}, "email": {}}},
    )
    order_stream = AirbyteStream(
        name="orders",
        namespace="public",
        json_schema={
            "properties": {
                "id": {},
                "customer_id": {},
                "order_date": {},
                "total": {},
            }
        },
    )
    sync_catalog = AirbyteSyncCatalog(
        streams=[
            AirbyteStreamConfig(stream=customer_stream, config={"selected": True}),
            AirbyteStreamConfig(stream=order_stream, config={"selected": True}),
        ]
    )
    connection.sync_catalog = sync_catalog

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )

    streams = source._fetch_streams_for_source(pipeline_info)

    assert len(streams) == 2
    assert streams[0].details.stream_name == "customers"
    assert streams[0].details.namespace == "public"
    assert sorted(streams[0].details.get_column_names()) == ["email", "id", "name"]
    assert streams[1].details.stream_name == "orders"
    assert streams[1].details.namespace == "public"
    assert sorted(streams[1].details.get_column_names()) == [
        "customer_id",
        "id",
        "order_date",
        "total",
    ]
