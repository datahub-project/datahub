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
    assert _sanitize_platform_name(input_name) == expected_output


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def mock_ctx():
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = MagicMock()
    ctx.pipeline_name = "airbyte_test"
    return ctx


@pytest.fixture
def config():
    return AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        platform_instance="test-instance",
    )


@pytest.fixture
def source(config, mock_ctx, mock_client):
    with patch(
        "datahub.ingestion.source.airbyte.source.create_airbyte_client"
    ) as mock_create_client:
        mock_create_client.return_value = mock_client
        return AirbyteSource(config, mock_ctx)


def test_fetch_streams_for_source(source):
    workspace = AirbyteWorkspacePartial(
        workspace_id="workspace-1",
        name="Test Workspace",
    )
    connection = AirbyteConnectionPartial(
        connection_id="connection-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="destination-1",
        status="active",
    )
    source_obj = AirbyteSourcePartial(
        source_id="source-1",
        name="Test Source",
        source_definition_id="source-def-1",
        workspace_id="workspace-1",
        configuration={},
    )
    destination = AirbyteDestinationPartial(
        destination_id="destination-1",
        name="Test Destination",
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


def test_fetch_streams_reports_ambiguous_stream_namespaces(source):
    connection = AirbyteConnectionPartial(
        connection_id="connection-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="destination-1",
        status="active",
        ambiguous_stream_namespaces={"users": ["public", "analytics"]},
        sync_catalog=AirbyteSyncCatalog(
            streams=[
                AirbyteStreamConfig(
                    stream=AirbyteStream(name="users", namespace=None),
                    config={"selected": True},
                )
            ]
        ),
    )
    pipeline_info = AirbytePipelineInfo(
        workspace=AirbyteWorkspacePartial(
            workspace_id="workspace-1", name="Test Workspace"
        ),
        connection=connection,
        source=AirbyteSourcePartial(
            source_id="source-1",
            name="Test Source",
            source_definition_id="source-def-1",
            workspace_id="workspace-1",
            configuration={"schema": "public"},
        ),
        destination=AirbyteDestinationPartial(
            destination_id="destination-1",
            name="Test Destination",
            destination_definition_id="dest-def-1",
            workspace_id="workspace-1",
            configuration={},
        ),
    )

    streams = source._fetch_streams_for_source(pipeline_info)

    assert len(streams) == 1
    assert streams[0].details.namespace == "public"
    assert any(
        "Ambiguous Stream Namespace" in str(warning)
        for warning in source.report.warnings
    )


def _pipeline_with_connection(
    connection: AirbyteConnectionPartial,
) -> AirbytePipelineInfo:
    return AirbytePipelineInfo(
        workspace=AirbyteWorkspacePartial(
            workspace_id="workspace-1", name="Test Workspace"
        ),
        connection=connection,
        source=AirbyteSourcePartial(
            source_id="source-1",
            name="Test Source",
            source_definition_id="source-def-1",
            workspace_id="workspace-1",
            configuration={},
        ),
        destination=AirbyteDestinationPartial(
            destination_id="destination-1",
            name="Test Destination",
            destination_definition_id="dest-def-1",
            workspace_id="workspace-1",
            configuration={},
        ),
    )


def _connection_with_one_stream(**overrides: object) -> AirbyteConnectionPartial:
    return AirbyteConnectionPartial(
        connection_id="connection-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="destination-1",
        status="active",
        sync_catalog=AirbyteSyncCatalog(
            streams=[
                AirbyteStreamConfig(
                    stream=AirbyteStream(name="users", namespace="public"),
                    config={"selected": True},
                )
            ]
        ),
        **overrides,
    )


def test_fetch_streams_warns_once_per_source_when_streams_api_missing(source):
    """An unavailable /streams response costs namespaces and column-level
    lineage, so it has to reach the report — but only once per source, not
    once per connection. Must not raise a report.failure: that would disable
    stale-entity removal for the whole run."""
    pipeline_info = _pipeline_with_connection(
        _connection_with_one_stream(
            streams_api_unavailable=True,
            streams_api_unavailable_status_code=500,
            streams_api_unavailable_message="500 Internal Server Error",
        )
    )

    source._fetch_streams_for_source(pipeline_info)
    source._fetch_streams_for_source(pipeline_info)

    matching = [
        warning
        for warning in source.report.warnings
        if "Stream Metadata Unavailable" in str(warning)
    ]
    assert len(matching) == 1
    assert "HTTP 500" in matching[0].context[0]
    assert "detail=500 Internal Server Error" in matching[0].context[0]
    assert "default_schema" in matching[0].message
    assert not source.report.failures


def test_fetch_streams_warns_with_no_status_for_connection_error(source):
    pipeline_info = _pipeline_with_connection(
        _connection_with_one_stream(
            streams_api_unavailable=True,
            streams_api_unavailable_message="Error connecting to Airbyte API",
        )
    )

    source._fetch_streams_for_source(pipeline_info)

    matching = [
        warning
        for warning in source.report.warnings
        if "Stream Metadata Unavailable" in str(warning)
    ]
    assert len(matching) == 1
    assert "no HTTP status (network or connection error)" in matching[0].context[0]
    assert "detail=Error connecting to Airbyte API" in matching[0].context[0]
    assert not source.report.failures


def test_fetch_streams_reports_skipped_stream_payloads(source):
    pipeline_info = _pipeline_with_connection(
        _connection_with_one_stream(
            skipped_stream_payloads=[
                "configurations.streams[1] (orders): 1 invalid field(s)"
            ]
        )
    )

    source._fetch_streams_for_source(pipeline_info)

    assert any(
        "Unreadable Stream Payload" in str(warning)
        for warning in source.report.warnings
    )
