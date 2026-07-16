from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    mock_create_client.return_value = MagicMock()

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

    workspace = AirbyteWorkspacePartial(workspace_id="ws-1", name="Test Workspace")
    connection = AirbyteConnectionPartial(
        connection_id="conn-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="dest-1",
        status="active",
    )
    source_obj = AirbyteSourcePartial(
        source_id="source-1",
        name="Test Source",
        source_type="PostgreSQL",
        source_definition_id="def-1",
        workspace_id="ws-1",
    )
    destination = AirbyteDestinationPartial(
        destination_id="dest-1",
        name="Test Dest",
        destination_definition_id="def-2",
        workspace_id="ws-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )

    stream = AirbyteStreamDetails(
        stream_name="CUSTOMERS",
        namespace="PUBLIC",
        property_fields=[],
    )
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="CUSTOMERS", namespace="PUBLIC"), config={}
    )

    dataset_urns = source._create_dataset_urns(pipeline_info, stream_config, stream)

    assert "public.customers" in dataset_urns.source_urn
    assert "PUBLIC" not in dataset_urns.source_urn
    assert "CUSTOMERS" not in dataset_urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_convert_urns_to_lowercase_disabled(mock_create_client, mock_ctx):
    mock_create_client.return_value = MagicMock()

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

    workspace = AirbyteWorkspacePartial(workspace_id="ws-1", name="Test Workspace")
    connection = AirbyteConnectionPartial(
        connection_id="conn-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="dest-1",
        status="active",
    )
    source_obj = AirbyteSourcePartial(
        source_id="source-1",
        name="Test Source",
        source_type="PostgreSQL",
        source_definition_id="def-1",
        workspace_id="ws-1",
    )
    destination = AirbyteDestinationPartial(
        destination_id="dest-1",
        name="Test Dest",
        destination_definition_id="def-2",
        workspace_id="ws-1",
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )

    stream = AirbyteStreamDetails(
        stream_name="CUSTOMERS",
        namespace="PUBLIC",
        property_fields=[],
    )
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="CUSTOMERS", namespace="PUBLIC"), config={}
    )

    dataset_urns = source._create_dataset_urns(pipeline_info, stream_config, stream)

    assert "PUBLIC.CUSTOMERS" in dataset_urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_auto_detect_two_tier_platform(mock_create_client, mock_ctx):
    # When the source's `schema` equals its `database` we treat the
    # platform as 2-tier and emit `<database>.<table>` instead of
    # `<database>.<schema>.<table>` (the latter would duplicate the tier).
    mock_create_client.return_value = MagicMock()

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

    workspace = AirbyteWorkspacePartial(workspace_id="ws-1", name="Test Workspace")
    connection = AirbyteConnectionPartial(
        connection_id="conn-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="dest-1",
        status="active",
    )
    source_obj = AirbyteSourcePartial(
        source_id="source-1",
        name="Test Source",
        source_type="MySQL",
        source_definition_id="def-1",
        workspace_id="ws-1",
        configuration={"database": "mydb", "schema": "mydb"},  # Schema == Database
    )
    destination = AirbyteDestinationPartial(
        destination_id="dest-1",
        name="Test Dest",
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
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="customers", namespace="mydb"), config={}
    )

    urns = source._create_dataset_urns(pipeline_info, stream_config, stream_details)

    assert "mydb.customers" in urns.source_urn
    assert "mydb.mydb.customers" not in urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_three_tier_platform_preserved(mock_create_client, mock_ctx):
    mock_create_client.return_value = MagicMock()

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

    workspace = AirbyteWorkspacePartial(workspace_id="ws-1", name="Test Workspace")
    connection = AirbyteConnectionPartial(
        connection_id="conn-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="dest-1",
        status="active",
    )
    source_obj = AirbyteSourcePartial(
        source_id="source-1",
        name="Test Source",
        source_type="Snowflake",
        source_definition_id="def-1",
        workspace_id="ws-1",
        configuration={"database": "DW_ANALYTICS", "schema": "PUBLIC"},
    )
    destination = AirbyteDestinationPartial(
        destination_id="dest-1",
        name="Test Dest",
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
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="customers", namespace="PUBLIC"), config={}
    )

    urns = source._create_dataset_urns(pipeline_info, stream_config, stream_details)

    assert "dw_analytics.public.customers" in urns.source_urn
    assert "dw_analytics.customers" not in urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_fully_qualified_table_name_parsing(mock_create_client, mock_ctx):
    # Some connectors emit `<schema>.<table>` as the stream name; we only
    # want the leaf for URN composition so we don't end up with
    # `mydb.public.public.customers`.
    mock_client = MagicMock()
    mock_create_client.return_value = mock_client

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={"source-1": PlatformDetail(platform="postgres")},
    )
    source = AirbyteSource(config, mock_ctx)

    workspace = AirbyteWorkspacePartial(workspace_id="ws-1", name="Test Workspace")
    connection = AirbyteConnectionPartial(
        connection_id="conn-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="dest-1",
        status="active",
    )
    source_obj = AirbyteSourcePartial(
        source_id="source-1",
        name="Test Source",
        source_type="PostgreSQL",
        source_definition_id="def-1",
        workspace_id="ws-1",
        configuration={"database": "mydb"},
    )
    destination = AirbyteDestinationPartial(
        destination_id="dest-1",
        name="Test Dest",
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
        stream_name="public.customers", namespace="public", property_fields=[]
    )
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="public.customers", namespace="public"), config={}
    )

    urns = source._create_dataset_urns(pipeline_info, stream_config, stream_details)

    assert "mydb.public.customers" in urns.source_urn
    assert "public.customers.public.customers" not in urns.source_urn


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_upstream_lineage_emitted_for_cross_platform_destination(
    mock_create_client, mock_ctx
):
    # Regression guard: UpstreamLineage must be emitted on the destination
    # whenever source and destination URNs differ. An earlier version gated
    # this on a `known_urns` set that was never populated.
    mock_create_client.return_value = MagicMock()

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
    pipeline_info = AirbytePipelineInfo(
        workspace=AirbyteWorkspacePartial(workspace_id="test", name="Test"),
        connection=AirbyteConnectionPartial(
            connection_id="conn-1",
            name="Test Connection",
            source_id="source-1",
            destination_id="dest-1",
            status="active",
        ),
        source=AirbyteSourcePartial(
            source_id="source-1", name="Test Source", source_type="postgres"
        ),
        destination=AirbyteDestinationPartial(
            destination_id="dest-1", name="Test Dest", destination_type="snowflake"
        ),
    )

    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    dest_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers,PROD)"

    work_units = list(
        source._emit_destination_upstream_lineage(
            pipeline_info=pipeline_info,
            source_urn=source_urn,
            destination_urn=dest_urn,
            stream=stream_details,
        )
    )

    lineage_mcps = [
        wu
        for wu in work_units
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and "UpstreamLineage" in type(wu.metadata.aspect).__name__
        and wu.metadata.entityUrn == dest_urn
    ]
    assert len(lineage_mcps) == 1


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_upstream_lineage_skipped_for_self_lineage(mock_create_client, mock_ctx):
    # Self-lineage would produce a meaningless `dataset -> itself` edge.
    mock_create_client.return_value = MagicMock()

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
    )
    source = AirbyteSource(config, mock_ctx)

    stream_details = AirbyteStreamDetails(
        stream_name="customers", namespace="public", property_fields=[]
    )
    pipeline_info = AirbytePipelineInfo(
        workspace=AirbyteWorkspacePartial(workspace_id="test", name="Test"),
        connection=AirbyteConnectionPartial(
            connection_id="conn-1",
            name="Test Connection",
            source_id="source-1",
            destination_id="dest-1",
            status="active",
        ),
        source=AirbyteSourcePartial(
            source_id="source-1", name="Test Source", source_type="postgres"
        ),
        destination=AirbyteDestinationPartial(
            destination_id="dest-1", name="Test Dest", destination_type="postgres"
        ),
    )

    same_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"

    work_units = list(
        source._emit_destination_upstream_lineage(
            pipeline_info=pipeline_info,
            source_urn=same_urn,
            destination_urn=same_urn,
            stream=stream_details,
        )
    )
    assert work_units == []


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_platform_caching(mock_create_client, mock_ctx):
    mock_create_client.return_value = MagicMock()

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={
            "source-1": PlatformDetail(platform="postgres", platform_instance="prod")
        },
    )
    source = AirbyteSource(config, mock_ctx)

    source_obj = AirbyteSourcePartial(
        source_id="source-1",
        name="Test Source",
        source_type="PostgreSQL",
        source_definition_id="def-1",
        workspace_id="ws-1",
    )

    result1 = source._get_platform_for_source(source_obj)
    assert result1 == PlatformInfo(
        platform="postgres", platform_instance="prod", env=None
    )
    assert "source-1" in source._source_platform_cache

    result2 = source._get_platform_for_source(source_obj)
    assert result2 is source._source_platform_cache["source-1"]


@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_warning_deduplication(mock_create_client, mock_ctx):
    # Platform-detection fallback warnings dedupe per source_id to avoid
    # flooding the report when many connections share the same broken source.
    mock_create_client.return_value = MagicMock()

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
    )
    source = AirbyteSource(config, mock_ctx)

    # Missing source_type forces a name-based fallback warning.
    source_obj = AirbyteSourcePartial(
        source_id="source-1",
        name="Test MySQL Source",
        source_type=None,
        source_definition_id="def-1",
        workspace_id="ws-1",
    )

    source._get_platform_for_source(source_obj)
    assert "source-1" in source._warned_source_ids

    initial_warning_count = len(source.report.warnings)
    source._get_platform_for_source(source_obj)
    assert len(source.report.warnings) == initial_warning_count
