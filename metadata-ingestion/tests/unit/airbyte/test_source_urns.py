from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.source.airbyte.client import AirbyteOSSClient
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
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
    AirbyteSyncCatalog,
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
def test_include_schema_in_urn_forces_three_tier_postgres(mock_create_client, mock_ctx):
    mock_create_client.return_value = MagicMock()

    config = AirbyteSourceConfig(
        deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
        host_port="http://localhost:8000",
        sources_to_platform_instance={
            "source-1": PlatformDetail(
                platform="postgres",
                platform_instance="my_instance",
                include_schema_in_urn=True,
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
        configuration={"database": "my_db"},
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
        stream_name="events",
        namespace="my_schema",
        property_fields=[],
    )
    stream_config = AirbyteStreamConfig(
        stream=AirbyteStream(name="events", namespace="my_schema"),
        config={},
    )

    urns = source._create_dataset_urns(pipeline_info, stream_config, stream_details)

    assert "my_instance.my_db.my_schema.events" in urns.source_urn
    assert "my_db.events," not in urns.source_urn


@patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
@patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
@patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
def test_streams_namespace_backfill_to_dataset_urn(
    mock_create_client, mock_make_request, mock_list_streams, mock_ctx
):
    mock_make_request.return_value = {
        "connectionId": "conn-1",
        "sourceId": "source-1",
        "destinationId": "dest-1",
        "name": "Test Connection",
        "status": "active",
        "configurations": {
            "streams": [{"name": "events", "syncMode": "full_refresh_overwrite"}]
        },
    }
    # Field names match the Public API's StreamProperties schema exactly, so this
    # seam breaks if real-Airbyte support regresses.
    mock_list_streams.return_value = [
        {
            "streamName": "events",
            "streamnamespace": "my_schema",
            "propertyFields": [["id"]],
        }
    ]

    client = AirbyteOSSClient(
        AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
    )
    connection = client.get_connection("conn-1")
    assert connection.sync_catalog is not None
    assert connection.sync_catalog.streams[0].stream.namespace == "my_schema"

    mock_create_client.return_value = client
    source = AirbyteSource(
        AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            sources_to_platform_instance={
                "source-1": PlatformDetail(
                    platform="postgres",
                    platform_instance="my_instance",
                    include_schema_in_urn=True,
                    convert_urns_to_lowercase=True,
                )
            },
            destinations_to_platform_instance={
                "dest-1": PlatformDetail(
                    platform="snowflake",
                    platform_instance="my_instance",
                    convert_urns_to_lowercase=True,
                )
            },
        ),
        mock_ctx,
    )

    pipeline_info = AirbytePipelineInfo(
        workspace=AirbyteWorkspacePartial(workspace_id="ws-1", name="Test Workspace"),
        connection=connection,
        source=AirbyteSourcePartial(
            source_id="source-1",
            name="Test Source",
            source_type="PostgreSQL",
            source_definition_id="def-1",
            workspace_id="ws-1",
            configuration={"database": "my_db"},
        ),
        destination=AirbyteDestinationPartial(
            destination_id="dest-1",
            name="Test Dest",
            destination_type="Snowflake",
            destination_definition_id="def-2",
            workspace_id="ws-1",
            configuration={"database": "raw"},
        ),
    )

    streams = source._fetch_streams_for_source(pipeline_info)
    assert len(streams) == 1
    assert streams[0].details.namespace == "my_schema"

    urns = source._create_dataset_urns(
        pipeline_info, streams[0].config, streams[0].details
    )
    assert "my_instance.my_db.my_schema.events" in urns.source_urn


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


def _source_schema_pipeline(
    source_configuration: Dict[str, Any],
    stream_namespace: Optional[str] = None,
) -> AirbytePipelineInfo:
    return AirbytePipelineInfo(
        workspace=AirbyteWorkspacePartial(workspace_id="ws-1", name="Test Workspace"),
        connection=AirbyteConnectionPartial(
            connection_id="conn-1",
            name="Test Connection",
            source_id="source-1",
            destination_id="dest-1",
            status="active",
            sync_catalog=AirbyteSyncCatalog(
                streams=[
                    AirbyteStreamConfig(
                        stream=AirbyteStream(
                            name="transfers", namespace=stream_namespace
                        ),
                        config={"selected": True},
                    )
                ]
            ),
        ),
        source=AirbyteSourcePartial(
            source_id="source-1",
            name="Test Source",
            source_type="mssql",
            source_definition_id="def-1",
            workspace_id="ws-1",
            configuration=source_configuration,
        ),
        destination=AirbyteDestinationPartial(
            destination_id="dest-1",
            name="Test Dest",
            destination_type="Snowflake",
            destination_definition_id="def-2",
            workspace_id="ws-1",
            configuration={"database": "raw"},
        ),
    )


def _source_with_details(mock_ctx: MagicMock, details: PlatformDetail) -> AirbyteSource:
    with patch(
        "datahub.ingestion.source.airbyte.source.create_airbyte_client"
    ) as mock_create_client:
        mock_create_client.return_value = MagicMock()
        return AirbyteSource(
            AirbyteSourceConfig(
                deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
                host_port="http://localhost:8000",
                sources_to_platform_instance={"source-1": details},
            ),
            mock_ctx,
        )


def test_per_table_schema_outranks_connector_wide_schema(mock_ctx):
    # The connector-wide schema key holds the database name here, which used to
    # shadow the per-table schema and collapse the URN to two tiers.
    source = _source_with_details(
        mock_ctx, PlatformDetail(platform="mssql", convert_urns_to_lowercase=True)
    )
    pipeline_info = _source_schema_pipeline(
        {
            "database": "wallet_db",
            "schemas": ["wallet_db"],
            "tables": [{"name": "transfers", "schema": "dbo"}],
        }
    )

    streams = source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == "dbo"
    urns = source._create_dataset_urns(
        pipeline_info, streams[0].config, streams[0].details
    )
    assert "wallet_db.dbo.transfers" in urns.source_urn


def test_default_schema_fills_in_when_no_schema_is_discoverable(mock_ctx):
    source = _source_with_details(
        mock_ctx,
        PlatformDetail(
            platform="mssql", default_schema="dbo", convert_urns_to_lowercase=True
        ),
    )
    pipeline_info = _source_schema_pipeline({"database": "wallet_db"})

    streams = source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == "dbo"
    urns = source._create_dataset_urns(
        pipeline_info, streams[0].config, streams[0].details
    )
    assert "wallet_db.dbo.transfers" in urns.source_urn


def test_default_schema_yields_to_schemas_airbyte_and_the_connector_report(mock_ctx):
    source = _source_with_details(
        mock_ctx,
        PlatformDetail(
            platform="mssql", default_schema="dbo", convert_urns_to_lowercase=True
        ),
    )

    from_airbyte = source._fetch_streams_for_source(
        _source_schema_pipeline({"database": "wallet_db"}, stream_namespace="reporting")
    )
    assert from_airbyte[0].details.namespace == "reporting"

    from_per_table = source._fetch_streams_for_source(
        _source_schema_pipeline(
            {
                "database": "wallet_db",
                "tables": [{"name": "transfers", "schema": "audit"}],
            }
        )
    )
    assert from_per_table[0].details.namespace == "audit"


def _namespace_warnings(source: AirbyteSource) -> List[StructuredLogEntry]:
    return [
        warning
        for warning in source.report.warnings
        if "Stream Namespaces Not Reported" in str(warning)
    ]


def _guessed_schema_warnings(source: AirbyteSource) -> List[StructuredLogEntry]:
    return [
        warning
        for warning in source.report.warnings
        if "Stream Schema Guessed" in str(warning)
    ]


def _missing_namespace_warnings(source: AirbyteSource) -> List[StructuredLogEntry]:
    return [
        warning
        for warning in source.report.warnings
        if "Stream Namespace Missing" in str(warning)
    ]


def test_warns_when_a_stream_alone_has_no_namespace(mock_ctx):
    # Airbyte answered with namespaces for this source, so a stream left
    # without one is its own gap and the version cannot be blamed for it.
    source = _source_with_details(mock_ctx, PlatformDetail(platform="postgres"))
    pipeline_info = _source_schema_pipeline({"database": "wallet_db"})

    streams = source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == ""
    warnings = _missing_namespace_warnings(source)
    assert len(warnings) == 1
    assert "transfers" in str(warnings[0])
    assert _namespace_warnings(source) == []


@pytest.mark.parametrize(
    "source_configuration",
    [
        {"database": "wallet_db"},
        # A schemas entry naming nothing we recognise has to leave the tier
        # empty; reading the wrong key would point the URN at a made-up schema.
        {"database": "wallet_db", "schemas": [{"dataset": "wallet_db"}]},
    ],
)
def test_warns_once_per_source_when_streams_api_reports_no_namespaces(
    mock_ctx, source_configuration
):
    source = _source_with_details(mock_ctx, PlatformDetail(platform="postgres"))
    pipeline_info = _source_schema_pipeline(source_configuration)
    pipeline_info.connection.streams_api_namespaces_absent = True

    streams = source._fetch_streams_for_source(pipeline_info)
    source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == ""
    assert len(_namespace_warnings(source)) == 1
    assert "transfers" in str(_namespace_warnings(source)[0])


@pytest.mark.parametrize(
    "details,source_configuration",
    [
        (
            PlatformDetail(platform="mssql", default_schema="dbo"),
            {"database": "wallet_db"},
        ),
        (
            PlatformDetail(platform="mssql"),
            {
                "database": "wallet_db",
                "tables": [{"name": "transfers", "schema": "dbo"}],
            },
        ),
    ],
)
def test_no_namespace_warning_when_another_tier_supplies_the_schema(
    mock_ctx, details, source_configuration
):
    # The warning has to reflect what resolution produced, not what Airbyte
    # sent, or it points operators at a setting they already applied.
    source = _source_with_details(mock_ctx, details)
    pipeline_info = _source_schema_pipeline(source_configuration)
    pipeline_info.connection.streams_api_namespaces_absent = True

    streams = source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == "dbo"
    urns = source._create_dataset_urns(
        pipeline_info, streams[0].config, streams[0].details
    )
    assert "wallet_db.dbo.transfers" in urns.source_urn
    assert _namespace_warnings(source) == []
    assert _guessed_schema_warnings(source) == []


@pytest.mark.parametrize(
    "schemas",
    [
        ["source_schema", "audit_schema"],
        [{"name": "source_schema"}, {"schema": "audit_schema"}],
    ],
)
def test_warns_when_a_multi_schema_list_forces_a_guessed_schema(mock_ctx, schemas):
    # Reproduced against Airbyte 1.6.9: a Postgres source over two schemas
    # reports no namespace on either surface, so every stream takes schemas[0]
    # and the ones living elsewhere claim another table's URN.
    source = _source_with_details(mock_ctx, PlatformDetail(platform="postgres"))
    pipeline_info = _source_schema_pipeline({"database": "test", "schemas": schemas})
    pipeline_info.connection.streams_api_namespaces_absent = True

    streams = source._fetch_streams_for_source(pipeline_info)
    source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == "source_schema"
    warnings = _guessed_schema_warnings(source)
    assert len(warnings) == 1
    assert "transfers" in str(warnings[0])
    assert "audit_schema" in str(warnings[0])
    # The schema tier is populated, just not trustworthy.
    assert _namespace_warnings(source) == []


def test_default_schema_on_a_multi_schema_source_still_warns(mock_ctx):
    # default_schema is one name for the whole source, so on a source that
    # replicates several schemas it is right for at most one of them.
    source = _source_with_details(
        mock_ctx, PlatformDetail(platform="postgres", default_schema="audit_schema")
    )
    pipeline_info = _source_schema_pipeline(
        {"database": "test", "schemas": ["source_schema", "audit_schema"]}
    )
    pipeline_info.connection.streams_api_namespaces_absent = True

    streams = source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == "audit_schema"
    warnings = _guessed_schema_warnings(source)
    assert len(warnings) == 1
    assert "schema=audit_schema" in str(warnings[0])


def test_per_table_schema_is_trusted_on_a_multi_schema_source(mock_ctx):
    # A per-table schema is per-stream, so it stays authoritative even when the
    # connector replicates several schemas.
    source = _source_with_details(mock_ctx, PlatformDetail(platform="postgres"))
    pipeline_info = _source_schema_pipeline(
        {
            "database": "test",
            "schemas": ["source_schema", "audit_schema"],
            "tables": [{"name": "transfers", "schema": "audit_schema"}],
        }
    )
    pipeline_info.connection.streams_api_namespaces_absent = True

    streams = source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == "audit_schema"
    assert _guessed_schema_warnings(source) == []


@pytest.mark.parametrize(
    "source_configuration",
    [
        {"database": "test", "schemas": ["source_schema"]},
        {"database": "test", "schema": "source_schema"},
        # Snowflake / BigQuery send objects rather than bare names, under
        # either key.
        {"database": "test", "schemas": [{"name": "source_schema"}]},
        {"database": "test", "schemas": [{"schema": "source_schema"}]},
    ],
)
def test_no_guess_warning_when_the_configuration_names_one_schema(
    mock_ctx, source_configuration
):
    source = _source_with_details(mock_ctx, PlatformDetail(platform="postgres"))
    pipeline_info = _source_schema_pipeline(source_configuration)
    pipeline_info.connection.streams_api_namespaces_absent = True

    streams = source._fetch_streams_for_source(pipeline_info)

    assert streams[0].details.namespace == "source_schema"
    assert _guessed_schema_warnings(source) == []
    assert _namespace_warnings(source) == []
