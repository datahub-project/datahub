"""Tests for Airbyte source improvements: lowercase URNs, known URNs, auto-detection."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.airbyte.config import (
    AirbyteDeploymentType,
    AirbyteSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDestinationPartial,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteStream,
    AirbyteStreamConfig,
    AirbyteStreamDetails,
    AirbyteWorkspacePartial,
    PlatformInfo,
    PropertyFieldPath,
)
from datahub.ingestion.source.airbyte.source import AirbyteSource


@pytest.fixture
def mock_ctx():
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = MagicMock()
    ctx.pipeline_name = "airbyte_test"
    return ctx


@pytest.fixture
def mock_client():
    return MagicMock()


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_convert_urns_to_lowercase_enabled(mock_create_client, mock_ctx):
    """Test that URNs are lowercased when convert_urns_to_lowercase is True."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={
            "source-1": PlatformDetail(
                platform="postgres",
                convert_urns_to_lowercase=True,
            )
        },
    )
    source = AirbyteSource(config, mock_ctx)

    workspace = AirbyteWorkspacePartial(
        workspaceId="ws-1", name="Test Workspace", workspace_id="ws-1"
    )
    connection = AirbyteConnectionPartial(
        connectionId="conn-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="dest-1",
        status="active",
        connection_id="conn-1",
        source_id="source-1",
        destination_id="dest-1",
    )
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceType="PostgreSQL",
        source_id="source-1",
        source_definition_id="def-1",
        workspace_id="ws-1",
    )
    destination = AirbyteDestinationPartial(
        destinationId="dest-1",
        name="Test Dest",
        destination_id="dest-1",
        destination_definition_id="def-2",
        workspace_id="ws-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )

    # Create stream with uppercase name
    stream = AirbyteStreamDetails(
        streamName="CUSTOMERS",
        namespace="PUBLIC",
        propertyFields=[],
        stream_name="CUSTOMERS",
    )

    # Create mock stream config
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="CUSTOMERS", namespace="PUBLIC"), config={}
    )

    # Get the dataset URNs
    dataset_urns = source._create_dataset_urns(
        pipeline_info, stream_config, stream, "test-instance"
    )

    # Source URN should have lowercased dataset name
    assert "public.customers" in dataset_urns.source_urn
    assert "PUBLIC" not in dataset_urns.source_urn
    assert "CUSTOMERS" not in dataset_urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_convert_urns_to_lowercase_disabled(mock_create_client, mock_ctx):
    """Test that URNs preserve case when convert_urns_to_lowercase is False."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={
            "source-1": PlatformDetail(
                platform="postgres",
                convert_urns_to_lowercase=False,
            )
        },
        destinations_to_platform_instance={
            "dest-1": PlatformDetail(
                platform="snowflake",
                convert_urns_to_lowercase=False,
            )
        },
    )
    source = AirbyteSource(config, mock_ctx)

    workspace = AirbyteWorkspacePartial(
        workspaceId="ws-1", name="Test Workspace", workspace_id="ws-1"
    )
    connection = AirbyteConnectionPartial(
        connectionId="conn-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="dest-1",
        status="active",
        connection_id="conn-1",
        source_id="source-1",
        destination_id="dest-1",
    )
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceType="PostgreSQL",
        source_id="source-1",
        source_definition_id="def-1",
        workspace_id="ws-1",
    )
    destination = AirbyteDestinationPartial(
        destinationId="dest-1",
        name="Test Dest",
        destination_id="dest-1",
        destination_definition_id="def-2",
        workspace_id="ws-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )

    # Create stream with uppercase name
    stream = AirbyteStreamDetails(
        streamName="CUSTOMERS",
        namespace="PUBLIC",
        propertyFields=[],
        stream_name="CUSTOMERS",
    )

    # Create mock stream config
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="CUSTOMERS", namespace="PUBLIC"), config={}
    )

    # Get the dataset URNs
    dataset_urns = source._create_dataset_urns(
        pipeline_info, stream_config, stream, "test-instance"
    )

    # Source URN should preserve case
    assert "PUBLIC.CUSTOMERS" in dataset_urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_auto_detect_two_tier_platform(mock_create_client, mock_ctx):
    """Test automatic 2-tier platform detection when schema equals database."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={
            "source-1": PlatformDetail(
                platform="mysql",
            )
        },
    )
    source = AirbyteSource(config, mock_ctx)

    workspace = AirbyteWorkspacePartial(
        workspaceId="ws-1", name="Test Workspace", workspace_id="ws-1"
    )
    connection = AirbyteConnectionPartial(
        connectionId="conn-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="dest-1",
        status="active",
        connection_id="conn-1",
        source_id="source-1",
        destination_id="dest-1",
    )
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceType="MySQL",
        source_id="source-1",
        source_definition_id="def-1",
        workspace_id="ws-1",
        configuration={"database": "mydb", "schema": "mydb"},  # Schema == Database
    )
    destination = AirbyteDestinationPartial(
        destinationId="dest-1",
        name="Test Dest",
        destination_id="dest-1",
        destination_definition_id="def-2",
        workspace_id="ws-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )

    stream_details = AirbyteStreamDetails(
        stream_name="customers", namespace="mydb", property_fields=[]
    )

    # Create mock stream config
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="customers", namespace="mydb"), config={}
    )

    # Create URNs - should auto-detect 2-tier and exclude duplicate schema
    urns = source._create_dataset_urns(
        pipeline_info, stream_config, stream_details, None
    )

    # Should be 2-tier: mydb.customers (not mydb.mydb.customers)
    assert "mydb.customers" in urns.source_urn
    assert "mydb.mydb.customers" not in urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_three_tier_platform_preserved(mock_create_client, mock_ctx):
    """Test that 3-tier platforms (Snowflake) preserve schema when different from database."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={
            "source-1": PlatformDetail(
                platform="snowflake",
            )
        },
    )
    source = AirbyteSource(config, mock_ctx)

    workspace = AirbyteWorkspacePartial(
        workspaceId="ws-1", name="Test Workspace", workspace_id="ws-1"
    )
    connection = AirbyteConnectionPartial(
        connectionId="conn-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="dest-1",
        status="active",
        connection_id="conn-1",
        source_id="source-1",
        destination_id="dest-1",
    )
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceType="Snowflake",
        source_id="source-1",
        source_definition_id="def-1",
        workspace_id="ws-1",
        configuration={"database": "DW_ANALYTICS", "schema": "PUBLIC"},
    )
    destination = AirbyteDestinationPartial(
        destinationId="dest-1",
        name="Test Dest",
        destination_id="dest-1",
        destination_definition_id="def-2",
        workspace_id="ws-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )

    stream_details = AirbyteStreamDetails(
        stream_name="customers", namespace="PUBLIC", property_fields=[]
    )

    # Create mock stream config
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="customers", namespace="PUBLIC"), config={}
    )

    urns = source._create_dataset_urns(
        pipeline_info, stream_config, stream_details, None
    )

    # Should be 3-tier: dw_analytics.public.customers
    assert "dw_analytics.public.customers" in urns.source_urn
    assert "dw_analytics.customers" not in urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_fully_qualified_table_name_parsing(mock_create_client, mock_ctx):
    """Test that fully qualified table names are parsed to extract just the table name."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={"source-1": PlatformDetail(platform="postgres")},
    )
    source = AirbyteSource(config, mock_ctx)

    workspace = AirbyteWorkspacePartial(
        workspaceId="ws-1", name="Test Workspace", workspace_id="ws-1"
    )
    connection = AirbyteConnectionPartial(
        connectionId="conn-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="dest-1",
        status="active",
        connection_id="conn-1",
        source_id="source-1",
        destination_id="dest-1",
    )
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceType="PostgreSQL",
        source_id="source-1",
        source_definition_id="def-1",
        workspace_id="ws-1",
        configuration={"database": "mydb"},
    )
    destination = AirbyteDestinationPartial(
        destinationId="dest-1",
        name="Test Dest",
        destination_id="dest-1",
        destination_definition_id="def-2",
        workspace_id="ws-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )

    # Stream name contains fully qualified path
    stream_details = AirbyteStreamDetails(
        stream_name="public.customers", namespace="public", property_fields=[]
    )

    # Create mock stream config
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="public.customers", namespace="public"), config={}
    )

    urns = source._create_dataset_urns(
        pipeline_info, stream_config, stream_details, None
    )

    # Should extract just 'customers' from 'public.customers'
    assert "mydb.public.customers" in urns.source_urn
    # Should NOT have doubled table name
    assert "public.customers.public.customers" not in urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_known_urns_prevents_phantom_destinations(mock_create_client, mock_ctx):
    """Test that destinations not in known_urns don't get lineage emitted."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
    )
    source = AirbyteSource(config, mock_ctx)

    stream_details = AirbyteStreamDetails(
        stream_name="customers",
        namespace="public",
        property_fields=[
            PropertyFieldPath(path=["id"]),
            PropertyFieldPath(path=["name"]),
        ],
    )

    # Create a minimal pipeline_info for the test
    workspace = AirbyteWorkspacePartial(
        workspaceId="test", name="Test", workspace_id="test"
    )
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceType="postgres",
        source_id="source-1",
        source_type="postgres",
    )
    dest = AirbyteDestinationPartial(
        destinationId="dest-1",
        name="Test Dest",
        destinationType="snowflake",
        destination_id="dest-1",
        destination_type="snowflake",
    )
    connection = AirbyteConnectionPartial(
        connectionId="conn-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="dest-1",
        status="active",
        connection_id="conn-1",
        source_id="source-1",
        destination_id="dest-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace, connection=connection, source=source_obj, destination=dest
    )

    # Destination is NOT in known_urns
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    dest_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers,PROD)"
    source.known_urns = {source_urn}

    work_units = list(
        source._create_dataset_lineage(
            pipeline_info,
            source_urn,
            dest_urn,
            stream_details,
        )
    )

    # Should only emit lineage if destination is in known_urns
    # Since destination is NOT in known_urns, no lineage MCPs should be emitted
    lineage_mcps = [
        wu
        for wu in work_units
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and "UpstreamLineage" in str(wu.metadata.aspect)
        and hasattr(wu.metadata, "entityUrn")
        and dest_urn in str(wu.metadata.entityUrn)
    ]
    assert len(lineage_mcps) == 0


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_known_urns_allows_airbyte_to_airbyte_lineage(mock_create_client, mock_ctx):
    """Test that lineage IS emitted when destination is also a source (in known_urns)."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
    )
    source = AirbyteSource(config, mock_ctx)

    stream_details = AirbyteStreamDetails(
        stream_name="customers",
        namespace="public",
        property_fields=[
            PropertyFieldPath(path=["id"]),
            PropertyFieldPath(path=["name"]),
        ],
    )

    # Create a minimal pipeline_info for the test
    workspace = AirbyteWorkspacePartial(
        workspaceId="test", name="Test", workspace_id="test"
    )
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceType="postgres",
        source_id="source-1",
        source_type="postgres",
    )
    dest = AirbyteDestinationPartial(
        destinationId="dest-1",
        name="Test Dest",
        destinationType="snowflake",
        destination_id="dest-1",
        destination_type="snowflake",
    )
    connection = AirbyteConnectionPartial(
        connectionId="conn-1",
        name="Test Connection",
        sourceId="source-1",
        destinationId="dest-1",
        status="active",
        connection_id="conn-1",
        source_id="source-1",
        destination_id="dest-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace, connection=connection, source=source_obj, destination=dest
    )

    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    dest_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers,PROD)"

    # Both source AND destination are in known_urns (destination is also a source elsewhere)
    source.known_urns = {source_urn, dest_urn}

    work_units = list(
        source._create_dataset_lineage(
            pipeline_info,
            source_urn,
            dest_urn,
            stream_details,
        )
    )

    # Should emit lineage because destination IS in known_urns
    lineage_mcps = [
        wu
        for wu in work_units
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and "UpstreamLineage" in str(wu.metadata.aspect)
        and hasattr(wu.metadata, "entityUrn")
        and dest_urn in str(wu.metadata.entityUrn)
    ]
    assert len(lineage_mcps) == 1


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_platform_caching(mock_create_client, mock_ctx):
    """Test that platform detection results are cached."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={
            "source-1": PlatformDetail(platform="postgres", platform_instance="prod")
        },
    )
    source = AirbyteSource(config, mock_ctx)

    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test Source",
        sourceType="PostgreSQL",
        source_id="source-1",
        source_definition_id="def-1",
        workspace_id="ws-1",
    )

    # First call - should populate cache
    result1 = source._get_platform_for_source(source_obj)
    assert result1 == PlatformInfo(
        platform="postgres", platform_instance="prod", env=None
    )
    assert "source-1" in source._source_platform_cache

    # Second call - should use cache
    result2 = source._get_platform_for_source(source_obj)
    assert result2 == result1
    assert result2 is source._source_platform_cache["source-1"]


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_warning_deduplication(mock_create_client, mock_ctx):
    """Test that platform detection fallback warnings only appear once per source."""
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
    )
    source = AirbyteSource(config, mock_ctx)

    # Source without source_type (triggers fallback warning)
    source_obj = AirbyteSourcePartial(
        sourceId="source-1",
        name="Test MySQL Source",
        source_type=None,  # Missing!
        source_id="source-1",
        source_definition_id="def-1",
        workspace_id="ws-1",
    )

    # First call - should warn
    source._get_platform_for_source(source_obj)
    assert "source-1" in source._warned_source_ids

    # Second call - should NOT warn again
    initial_warning_count = len(source.report.warnings)
    source._get_platform_for_source(source_obj)
    final_warning_count = len(source.report.warnings)

    # No new warnings should be added
    assert final_warning_count == initial_warning_count
