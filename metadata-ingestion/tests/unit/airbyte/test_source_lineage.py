from typing import Any, Dict, Optional
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
    AirbyteDatasetUrns,
    AirbyteDestinationPartial,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteStreamConfig,
    AirbyteStreamDetails,
    AirbyteStreamInfo,
    AirbyteWorkspacePartial,
    PropertyFieldPath,
)
from datahub.ingestion.source.airbyte.source import AirbyteSource
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    GlobalTagsClass,
    UpstreamLineageClass,
)
from datahub.sdk.dataflow import DataFlow


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


@pytest.fixture
def workspace():
    return AirbyteWorkspacePartial(workspace_id="workspace-1", name="Test Workspace")


@pytest.fixture
def connection():
    return AirbyteConnectionPartial(
        connection_id="connection-1",
        name="Test Connection",
        source_id="source-1",
        destination_id="destination-1",
        status="active",
    )


@pytest.fixture
def source_obj():
    return AirbyteSourcePartial(
        source_id="source-1",
        name="Test Source",
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
    return AirbyteDestinationPartial(
        destination_id="destination-1",
        name="Test Destination",
        destination_type="snowflake",
        destination_definition_id="dest-def-1",
        workspace_id="workspace-1",
        configuration={
            "host": "localhost",
            "port": 443,
            "database": "dest_db",
            "schema": "public",
        },
    )


@pytest.fixture
def pipeline_info(workspace, connection, source_obj, destination):
    return AirbytePipelineInfo(
        workspace=workspace,
        connection=connection,
        source=source_obj,
        destination=destination,
    )


@pytest.fixture
def stream():
    return AirbyteStreamDetails(
        stream_name="customers",
        namespace="public",
        property_fields=[
            PropertyFieldPath(path=["id"]),
            PropertyFieldPath(path=["name"]),
            PropertyFieldPath(path=["email"]),
        ],
    )


def _aspect_of_type(workunits, aspect_cls):
    for wu in workunits:
        aspect = wu.metadata.aspect
        if isinstance(aspect, aspect_cls):
            return aspect
    return None


def test_build_connection_dataflow_returns_sdk_v2_dataflow(source, pipeline_info):
    # Regression guard: the connection DataFlow URN must include
    # platform_instance. An earlier implementation emitted the workspace
    # DataFlow URN with a platform_instance prefix but the connection URN
    # without, which broke any cross-aspect URN lookup.
    tags = ["urn:li:tag:test-tag-1", "urn:li:tag:test-tag-2"]

    result = source._build_connection_dataflow(pipeline_info, tags)

    assert isinstance(result, DataFlow)
    expected_urn = (
        "urn:li:dataFlow:(airbyte,test-instance.connection-1,"
        f"{source.source_config.env})"
    )
    assert str(result.urn) == expected_urn

    workunits = result.as_workunits()
    flow_info = _aspect_of_type(workunits, DataFlowInfoClass)
    assert flow_info is not None
    assert flow_info.name == "Test Connection"
    # `set_description` routes to `DataFlowInfoClass` only under an active
    # ingestion-attribution context; check the SDK property which merges
    # both the ingestion- and editable-side aspects.
    assert result.description == (
        "Airbyte connection from Test Source to Test Destination"
    )
    # Regression guard for W3 (Cloud externalUrl bug): the URL must use the
    # configured host_port for OSS, not the literal string "None/...".
    assert flow_info.externalUrl is not None
    assert flow_info.externalUrl.startswith("http://localhost:8000/workspaces/")

    if tags:
        global_tags = _aspect_of_type(workunits, GlobalTagsClass)
        assert global_tags is not None
        emitted = {tag.tag for tag in global_tags.tags}
        assert set(tags).issubset(emitted)


def test_build_stream_datajob_uses_connection_flow_urn(source, pipeline_info, stream):
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    destination_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,dest_db.public.customers,PROD)"
    )

    connection_dataflow = source._build_connection_dataflow(pipeline_info, tags=[])
    result = source._build_stream_datajob(
        connection_dataflow=connection_dataflow,
        pipeline_info=pipeline_info,
        stream=stream,
        source_urn=source_urn,
        destination_urn=destination_urn,
        tags=[],
    )

    expected_job_id = f"{pipeline_info.connection.connection_id}_{stream.stream_name}"
    assert str(result.urn).endswith(f"{expected_job_id})")
    assert str(connection_dataflow.urn) in str(result.urn)

    workunits = result.as_workunits()
    io_aspect = _aspect_of_type(workunits, DataJobInputOutputClass)
    assert io_aspect is not None
    assert io_aspect.inputDatasets == [source_urn]
    assert io_aspect.outputDatasets == [destination_urn]

    info = _aspect_of_type(workunits, DataJobInfoClass)
    assert info is not None
    assert info.name == "customers"


def test_emit_destination_upstream_lineage_skips_self_lineage(
    source, pipeline_info, stream
):
    # Self-lineage (source URN == destination URN) would produce a
    # meaningless `dataset -> itself` edge.
    same_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"

    workunits = list(
        source._emit_destination_upstream_lineage(
            pipeline_info=pipeline_info,
            source_urn=same_urn,
            destination_urn=same_urn,
            stream=stream,
        )
    )
    assert workunits == []


def test_emit_destination_upstream_lineage_emits_when_cross_platform(
    source, pipeline_info, stream
):
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    destination_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,dest_db.public.customers,PROD)"
    )

    source.source_config.extract_column_level_lineage = False
    workunits = list(
        source._emit_destination_upstream_lineage(
            pipeline_info=pipeline_info,
            source_urn=source_urn,
            destination_urn=destination_urn,
            stream=stream,
        )
    )

    assert len(workunits) == 1
    lineage = _aspect_of_type(workunits, UpstreamLineageClass)
    assert lineage is not None
    assert lineage.upstreams[0].dataset == source_urn
    assert workunits[0].metadata.entityUrn == destination_urn


def test_create_lineage_workunits_end_to_end(source, pipeline_info, stream):
    mock_stream_config = MagicMock(spec=AirbyteStreamConfig)
    stream_info = AirbyteStreamInfo(config=mock_stream_config, details=stream)

    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    destination_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,dest_db.public.customers,PROD)"
    )

    with (
        patch.object(source, "_fetch_streams_for_source", return_value=[stream_info]),
        patch.object(
            source,
            "_create_dataset_urns",
            return_value=AirbyteDatasetUrns(
                source_urn=source_urn,
                destination_urn=destination_urn,
            ),
        ),
    ):
        workunits = list(source._create_lineage_workunits(pipeline_info))

    aspect_types = {type(wu.metadata.aspect).__name__ for wu in workunits}
    assert "DataFlowInfoClass" in aspect_types
    assert "DataJobInfoClass" in aspect_types
    assert "DataJobInputOutputClass" in aspect_types
    # Regression guard: cross-platform pairs must emit UpstreamLineage on
    # the destination, not just the DataJob's inlets/outlets.
    assert "UpstreamLineageClass" in aspect_types


def test_snowflake_destination_lowercases_columns(source, pipeline_info, stream):
    # Snowflake folds identifiers to lowercase when `convert_urns_to_lowercase`
    # is set; non-Snowflake platforms must preserve column case regardless.
    source.source_config.destinations_to_platform_instance = {
        "destination-1": PlatformDetail(
            platform="snowflake", convert_urns_to_lowercase=True
        ),
    }
    source.source_config.sources_to_platform_instance = {
        "source-1": PlatformDetail(platform="postgres", convert_urns_to_lowercase=True),
    }
    upper_stream = AirbyteStreamDetails(
        stream_name="customers",
        namespace="public",
        property_fields=[
            PropertyFieldPath(path=["CustomerID"]),
            PropertyFieldPath(path=["Email"]),
        ],
    )

    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    destination_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,public.customers,PROD)"
    )

    lineages = source._build_fine_grained_lineages(
        pipeline_info=pipeline_info,
        stream=upper_stream,
        source_urn=source_urn,
        destination_urn=destination_urn,
    )

    assert len(lineages) == 2
    assert any("CustomerID" in u for u in lineages[0].upstreams)
    assert all("customerid" in d for d in lineages[0].downstreams)


def test_postgres_to_mysql_preserves_column_case(source, pipeline_info, stream):
    source.source_config.destinations_to_platform_instance = {
        "destination-1": PlatformDetail(
            platform="mysql", convert_urns_to_lowercase=True
        ),
    }
    source.source_config.sources_to_platform_instance = {
        "source-1": PlatformDetail(platform="postgres", convert_urns_to_lowercase=True),
    }

    cased_stream = AirbyteStreamDetails(
        stream_name="customers",
        namespace="public",
        property_fields=[
            PropertyFieldPath(path=["CustomerID"]),
        ],
    )

    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    destination_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,public.customers,PROD)"

    lineages = source._build_fine_grained_lineages(
        pipeline_info=pipeline_info,
        stream=cased_stream,
        source_urn=source_urn,
        destination_urn=destination_urn,
    )

    assert len(lineages) == 1
    assert any("CustomerID" in u for u in lineages[0].upstreams)
    assert any("CustomerID" in d for d in lineages[0].downstreams)


def test_snowflake_source_lowercases_columns(source, pipeline_info, stream):
    source.source_config.sources_to_platform_instance = {
        "source-1": PlatformDetail(
            platform="snowflake", convert_urns_to_lowercase=True
        ),
    }
    source.source_config.destinations_to_platform_instance = {
        "destination-1": PlatformDetail(
            platform="postgres", convert_urns_to_lowercase=True
        ),
    }

    cased_stream = AirbyteStreamDetails(
        stream_name="customers",
        namespace="public",
        property_fields=[
            PropertyFieldPath(path=["CustomerID"]),
        ],
    )

    source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,public.customers,PROD)"
    destination_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"
    )

    lineages = source._build_fine_grained_lineages(
        pipeline_info=pipeline_info,
        stream=cased_stream,
        source_urn=source_urn,
        destination_urn=destination_urn,
    )

    assert len(lineages) == 1
    # Source is Snowflake → upstream column lowercased; Postgres destination preserves case.
    assert all("customerid" in u for u in lineages[0].upstreams)
    assert any("CustomerID" in d for d in lineages[0].downstreams)


# ---------------------------------------------------------------------------
# W5: _resolve_destination_schema — Airbyte 4-tier namespace precedence
#
# Branches (in order):
#   1. Per-stream `destinationNamespace` override     (highest priority)
#   2. Connection-level `namespace_definition` (source | destination | customformat)
#   3. Destination's configuration schema             (fallback)
#   4. Source schema                                  (last resort)
#
# A regression in any branch silently produces wrong destination URNs.
# ---------------------------------------------------------------------------


def _make_stream_config(
    destination_namespace: Optional[str] = None,
) -> AirbyteStreamConfig:
    return AirbyteStreamConfig(
        stream={"name": "customers", "namespace": "public", "jsonSchema": {}},
        config=(
            {"destinationNamespace": destination_namespace}
            if destination_namespace is not None
            else {}
        ),
    )


def _make_connection(
    namespace_definition: Optional[str] = None,
    namespace_format: Optional[str] = None,
) -> AirbyteConnectionPartial:
    return AirbyteConnectionPartial(
        connection_id="conn-1",
        source_id="source-1",
        destination_id="destination-1",
        namespace_definition=namespace_definition,
        namespace_format=namespace_format,
    )


def _make_destination(schema: Optional[str] = None) -> AirbyteDestinationPartial:
    config: Dict[str, Any] = {"host": "h", "port": 1, "database": "d"}
    if schema is not None:
        config["schema"] = schema
    return AirbyteDestinationPartial(
        destination_id="destination-1",
        destination_type="postgres",
        configuration=config,
    )


def test_resolve_destination_schema_per_stream_override_wins(source):
    stream_config = _make_stream_config(destination_namespace="stream_override")
    connection = _make_connection(namespace_definition="destination")
    destination = _make_destination(schema="dest_default")

    result = source._resolve_destination_schema(
        stream_config=stream_config,
        connection=connection,
        source_schema="source_schema",
        destination=destination,
        stream_name="customers",
    )

    assert result == "stream_override"


def test_resolve_destination_schema_namespace_source(source):
    stream_config = _make_stream_config()
    connection = _make_connection(namespace_definition="source")
    destination = _make_destination(schema="dest_default")

    result = source._resolve_destination_schema(
        stream_config=stream_config,
        connection=connection,
        source_schema="source_schema",
        destination=destination,
        stream_name="customers",
    )

    assert result == "source_schema"


def test_resolve_destination_schema_namespace_destination(source):
    stream_config = _make_stream_config()
    connection = _make_connection(namespace_definition="destination")
    destination = _make_destination(schema="dest_default")

    result = source._resolve_destination_schema(
        stream_config=stream_config,
        connection=connection,
        source_schema="source_schema",
        destination=destination,
        stream_name="customers",
    )

    assert result == "dest_default"


def test_resolve_destination_schema_customformat_interpolates_source_namespace(source):
    stream_config = _make_stream_config()
    connection = _make_connection(
        namespace_definition="customformat",
        namespace_format="prefix_${SOURCE_NAMESPACE}_suffix",
    )
    destination = _make_destination()

    result = source._resolve_destination_schema(
        stream_config=stream_config,
        connection=connection,
        source_schema="public",
        destination=destination,
        stream_name="customers",
    )

    assert result == "prefix_public_suffix"


def test_resolve_destination_schema_customformat_underscore_variant(source):
    # `custom_format` (underscore) is accepted as an alias for `customformat`.
    stream_config = _make_stream_config()
    connection = _make_connection(
        namespace_definition="custom_format",
        namespace_format="namespace_${SOURCE_NAMESPACE}",
    )
    destination = _make_destination()

    result = source._resolve_destination_schema(
        stream_config=stream_config,
        connection=connection,
        source_schema="public",
        destination=destination,
        stream_name="customers",
    )

    assert result == "namespace_public"


def test_resolve_destination_schema_falls_back_to_destination_default(source):
    stream_config = _make_stream_config()
    connection = _make_connection()
    destination = _make_destination(schema="dest_default")

    result = source._resolve_destination_schema(
        stream_config=stream_config,
        connection=connection,
        source_schema="source_schema",
        destination=destination,
        stream_name="customers",
    )

    assert result == "dest_default"


def test_resolve_destination_schema_falls_back_to_source_schema(source):
    stream_config = _make_stream_config()
    connection = _make_connection()
    destination = _make_destination()

    result = source._resolve_destination_schema(
        stream_config=stream_config,
        connection=connection,
        source_schema="source_schema",
        destination=destination,
        stream_name="customers",
    )

    assert result == "source_schema"
