from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceConfig,
)
from datahub.ingestion.source.hightouch.hightouch import (
    HightouchSource as HightouchIngestionSource,
)
from datahub.ingestion.source.hightouch.hightouch_utils import normalize_column_name
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SiblingsClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)


@pytest.fixture
def pipeline_context():
    return PipelineContext(run_id="test_run")


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_column_lineage_emission(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_client.extract_field_mappings.return_value = [
        Mock(source_field="user_id", destination_field="UserId", is_primary_key=True),
        Mock(source_field="email", destination_field="Email", is_primary_key=False),
        Mock(source_field="name", destination_field="Name", is_primary_key=False),
    ]

    source_instance = HightouchIngestionSource(config, pipeline_context)

    model = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "fieldMappings": [
                {"sourceField": "user_id", "destinationField": "UserId"},
            ]
        },
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Salesforce",
        slug="salesforce",
        type="salesforce",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={},
    )

    mock_client.get_model_by_id.return_value = model
    mock_client.get_source_by_id.return_value = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )
    mock_client.get_destination_by_id.return_value = destination

    datajob = source_instance._sync_handler.generate_datajob_from_sync(sync)

    assert datajob.fine_grained_lineages is not None
    assert len(datajob.fine_grained_lineages) == 3
    assert source_instance.report.column_lineage_emitted == 3


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_tags_emission_for_model(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
    )

    source_instance = HightouchIngestionSource(config, pipeline_context)

    model = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        tags={"team": "data", "priority": "high"},
    )

    source = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance._sources_cache["source_1"] = source

    workunits = list(source_instance._get_model_workunits(model))

    tag_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, GlobalTagsClass)
    ]

    assert len(tag_workunits) == 1
    tag_wu = tag_workunits[0]
    assert hasattr(tag_wu, "metadata") and isinstance(
        tag_wu.metadata, MetadataChangeProposalWrapper
    )
    tag_aspect = tag_wu.metadata.aspect
    assert isinstance(tag_aspect, GlobalTagsClass)
    assert len(tag_aspect.tags) == 2
    assert any("ht_team_data" in str(tag.tag) for tag in tag_aspect.tags)
    assert any("ht_priority_high" in str(tag.tag) for tag in tag_aspect.tags)


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_view_emission_for_raw_sql_model(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
    )

    source_instance = HightouchIngestionSource(config, pipeline_context)

    model = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        raw_sql="SELECT * FROM customers WHERE active = true",
    )

    source = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance._sources_cache["source_1"] = source

    workunits = list(source_instance._get_model_workunits(model))

    subtype_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, SubTypesClass)
        and hasattr(wu.metadata, "entityType")
        and wu.metadata.entityType == "dataset"
    ]

    view_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, ViewPropertiesClass)
    ]

    assert len(subtype_workunits) == 1
    subtype_wu = subtype_workunits[0]
    assert hasattr(subtype_wu, "metadata") and isinstance(
        subtype_wu.metadata, MetadataChangeProposalWrapper
    )
    subtype_aspect = subtype_wu.metadata.aspect
    assert isinstance(subtype_aspect, SubTypesClass)
    assert "Hightouch Model" in subtype_aspect.typeNames
    assert "View" in subtype_aspect.typeNames

    assert len(view_workunits) == 1
    view_wu = view_workunits[0]
    assert hasattr(view_wu, "metadata") and isinstance(
        view_wu.metadata, MetadataChangeProposalWrapper
    )
    view_aspect = view_wu.metadata.aspect
    assert isinstance(view_aspect, ViewPropertiesClass)
    assert view_aspect.viewLogic == model.raw_sql
    assert view_aspect.viewLanguage == "SQL"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_table_subtype_for_table_model(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
    )

    source_instance = HightouchIngestionSource(config, pipeline_context)

    model = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="table",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance._sources_cache["source_1"] = source

    workunits = list(source_instance._get_model_workunits(model))

    subtype_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, SubTypesClass)
        and hasattr(wu.metadata, "entityType")
        and wu.metadata.entityType == "dataset"
    ]

    assert len(subtype_workunits) == 1
    subtype_wu = subtype_workunits[0]
    assert hasattr(subtype_wu, "metadata") and isinstance(
        subtype_wu.metadata, MetadataChangeProposalWrapper
    )
    subtype_aspect = subtype_wu.metadata.aspect
    assert isinstance(subtype_aspect, SubTypesClass)
    assert "Hightouch Model" in subtype_aspect.typeNames
    assert "Table" in subtype_aspect.typeNames


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_subtypes_always_emitted_for_all_models(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
    )

    source_instance = HightouchIngestionSource(config, pipeline_context)

    model_without_sql = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="custom",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance._sources_cache["source_1"] = source

    workunits = list(source_instance._get_model_workunits(model_without_sql))

    subtype_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, SubTypesClass)
        and hasattr(wu.metadata, "entityType")
        and wu.metadata.entityType == "dataset"
    ]

    view_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, ViewPropertiesClass)
    ]

    assert len(subtype_workunits) == 1
    subtype_wu = subtype_workunits[0]
    assert hasattr(subtype_wu, "metadata") and isinstance(
        subtype_wu.metadata, MetadataChangeProposalWrapper
    )
    subtype_aspect = subtype_wu.metadata.aspect
    assert isinstance(subtype_aspect, SubTypesClass)
    assert "Hightouch Model" in subtype_aspect.typeNames
    assert "View" in subtype_aspect.typeNames

    assert len(view_workunits) == 0


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_tags_in_sync_custom_properties(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    source_instance = HightouchIngestionSource(config, pipeline_context)

    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        tags={"team": "data", "priority": "high"},
    )

    model = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Salesforce",
        slug="salesforce",
        type="salesforce",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_client.get_model_by_id.return_value = model
    mock_client.get_source_by_id.return_value = source
    mock_client.get_destination_by_id.return_value = destination
    mock_client.extract_field_mappings.return_value = []

    datajob = source_instance._sync_handler.generate_datajob_from_sync(sync)

    assert "hightouch_tags" in datajob.custom_properties
    tags_str = datajob.custom_properties["hightouch_tags"]
    assert "team:data" in tags_str
    assert "priority:high" in tags_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_schedule_in_custom_properties(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    source_instance = HightouchIngestionSource(config, pipeline_context)

    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        schedule={"type": "cron", "expression": "0 */6 * * *"},
    )

    model = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Salesforce",
        slug="salesforce",
        type="salesforce",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_client.get_model_by_id.return_value = model
    mock_client.get_source_by_id.return_value = source
    mock_client.get_destination_by_id.return_value = destination
    mock_client.extract_field_mappings.return_value = []

    datajob = source_instance._sync_handler.generate_datajob_from_sync(sync)

    assert "schedule" in datajob.custom_properties
    assert "cron" in datajob.custom_properties["schedule"]


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_column_lineage_not_emitted_when_no_mappings(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_client.extract_field_mappings.return_value = []

    source_instance = HightouchIngestionSource(config, pipeline_context)

    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={},
    )

    model = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Salesforce",
        slug="salesforce",
        type="salesforce",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_client.get_model_by_id.return_value = model
    mock_client.get_source_by_id.return_value = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )
    mock_client.get_destination_by_id.return_value = destination

    datajob = source_instance._sync_handler.generate_datajob_from_sync(sync)

    assert (
        datajob.fine_grained_lineages is None or len(datajob.fine_grained_lineages) == 0
    )
    assert source_instance.report.column_lineage_emitted == 0


@pytest.mark.parametrize(
    "input_name,expected_output",
    [
        ("user_id", "userid"),
        ("UserId", "userid"),
        ("USER_ID", "userid"),
        ("user-id", "userid"),
        ("userId", "userid"),
        ("UserID", "userid"),
    ],
)
def test_normalize_column_name(input_name, expected_output):
    assert normalize_column_name(input_name) == expected_output


@pytest.mark.parametrize(
    "source_input,dest_input,model_schema,dest_schema,expected_source,expected_dest",
    [
        (
            "userId",
            "USERID",
            ["user_id", "email_address", "created_at"],
            ["UserId", "EmailAddress", "CreatedAt"],
            "user_id",
            "UserId",
        ),
        (
            "emailaddress",
            "EmailAddress",
            ["email_address", "first_name", "last_name"],
            ["EmailAddress", "FirstName", "LastName"],
            "email_address",
            "EmailAddress",
        ),
        (
            "not_in_schema",
            "also_not_in_schema",
            ["user_id", "email"],
            ["UserId", "Email"],
            "not_in_schema",
            "also_not_in_schema",
        ),
    ],
)
@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_column_name_fuzzy_matching(
    mock_api_client_class,
    pipeline_context,
    source_input,
    dest_input,
    model_schema,
    dest_schema,
    expected_source,
    expected_dest,
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    source_instance = HightouchIngestionSource(config, pipeline_context)

    column_pair = source_instance._lineage_handler.normalize_and_match_column(
        source_input,
        dest_input,
        model_schema,
        dest_schema,
    )

    assert column_pair.source_field == expected_source
    assert column_pair.destination_field == expected_dest


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_column_lineage_with_fuzzy_matching_integration(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    source_instance = HightouchIngestionSource(config, pipeline_context)

    mock_graph = Mock()
    source_instance.graph = mock_graph
    source_instance._lineage_handler.graph = mock_graph

    model_schema_meta = Mock()
    model_schema_meta.fields = [
        Mock(fieldPath="user_id"),
        Mock(fieldPath="email_address"),
    ]

    dest_schema_meta = Mock()
    dest_schema_meta.fields = [
        Mock(fieldPath="UserId"),
        Mock(fieldPath="EmailAddress"),
    ]

    mock_graph.get_schema_metadata.side_effect = [model_schema_meta, dest_schema_meta]

    mock_client.extract_field_mappings.return_value = [
        Mock(source_field="userid", destination_field="USERID", is_primary_key=True),
        Mock(
            source_field="emailaddress",
            destination_field="EmailAddress",
            is_primary_key=False,
        ),
    ]

    model = HightouchModel(
        id="model_1",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    sync = HightouchSync(
        id="sync_1",
        slug="test-sync",
        workspace_id="workspace_1",
        model_id="model_1",
        destination_id="dest_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={},
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Salesforce",
        slug="salesforce",
        type="salesforce",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_client.get_model_by_id.return_value = model
    mock_client.get_source_by_id.return_value = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )
    mock_client.get_destination_by_id.return_value = destination

    datajob = source_instance._sync_handler.generate_datajob_from_sync(sync)

    assert datajob.fine_grained_lineages is not None
    assert len(datajob.fine_grained_lineages) == 2

    if datajob.fine_grained_lineages:
        upstream_fields = [
            lineage.upstreams[0].split(",")[-1].rstrip(")")
            for lineage in datajob.fine_grained_lineages
            if lineage.upstreams
        ]
        assert "user_id" in upstream_fields
        assert "email_address" in upstream_fields


def _schema_field(path: str) -> SchemaFieldClass:
    return SchemaFieldClass(
        fieldPath=path,
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="STRING",
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_table_model_column_lineage_matches_case_insensitively(
    mock_api_client_class, pipeline_context
):
    """generate_table_model_column_lineage maps each model column to its upstream
    column, matching across casing differences (e.g. Snowflake uppercasing)."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    mock_graph = Mock()
    source_instance._lineage_handler.graph = mock_graph
    upstream_schema = Mock()
    upstream_schema.fields = [
        Mock(fieldPath="CUSTOMER_ID"),
        Mock(fieldPath="EMAIL"),
        Mock(fieldPath="UNMAPPED_COL"),
    ]
    mock_graph.get_schema_metadata.return_value = upstream_schema

    model = HightouchModel(
        id="model_1",
        name="customers",
        slug="customers-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="table",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )
    upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.customers,PROD)"
    model_urn = "urn:li:dataset:(urn:li:dataPlatform:hightouch,customers-model,PROD)"

    lineages = source_instance._lineage_handler.generate_table_model_column_lineage(
        model,
        model_urn,
        upstream_urn,
        [_schema_field("customer_id"), _schema_field("email")],
    )

    assert len(lineages) == 2
    upstream_field_urns = [
        lineage.upstreams[0] for lineage in lineages if lineage.upstreams
    ]
    assert any("CUSTOMER_ID" in urn for urn in upstream_field_urns)
    assert any("EMAIL" in urn for urn in upstream_field_urns)


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_table_model_column_lineage_empty_without_graph(
    mock_api_client_class, pipeline_context
):
    """Without a graph the CLL resolver returns nothing rather than guessing."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)
    source_instance._lineage_handler.graph = None

    model = HightouchModel(
        id="model_1",
        name="customers",
        slug="customers-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="table",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    lineages = source_instance._lineage_handler.generate_table_model_column_lineage(
        model,
        "urn:li:dataset:(urn:li:dataPlatform:hightouch,customers-model,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.customers,PROD)",
        [_schema_field("customer_id")],
    )

    assert lineages == []


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_table_model_column_lineage_emitted_end_to_end(
    mock_api_client_class, pipeline_context
):
    """The full model-emission path emits an UpstreamLineage with fineGrainedLineages
    for a table model when a graph resolves the upstream schema. The integration
    harness has no graph, so this is the only place the e2e table CLL path runs."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
        include_sibling_relationships=False,
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    source_instance = HightouchIngestionSource(config, pipeline_context)

    mock_graph = Mock()
    upstream_schema = Mock()
    upstream_schema.fields = [
        Mock(fieldPath="CUSTOMER_ID"),
        Mock(fieldPath="EMAIL"),
        Mock(fieldPath="UNMAPPED_COL"),
    ]
    mock_graph.get_schema_metadata.return_value = upstream_schema
    source_instance.graph = mock_graph
    source_instance._schema_handler.graph = mock_graph
    source_instance._lineage_handler.graph = mock_graph

    model = HightouchModel(
        id="model_1",
        name="customers",
        slug="customers-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="table",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema=[
            {"name": "customer_id", "type": "INTEGER"},
            {"name": "email", "type": "STRING"},
        ],
    )

    source_instance._sources_cache["source_1"] = HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={"database": "analytics"},
    )

    workunits = list(source_instance._get_model_workunits(model))

    upstream_lineages = [
        wu.metadata.aspect
        for wu in workunits
        if hasattr(wu, "metadata")
        and isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]

    assert len(upstream_lineages) == 1
    fine_grained = upstream_lineages[0].fineGrainedLineages
    assert fine_grained is not None
    assert len(fine_grained) == 2
    assert source_instance.report.column_lineage_emitted == 2


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_siblings_emission_for_single_table_raw_sql_models(
    mock_api_client_class, pipeline_context
):
    """
    Test that siblings ARE emitted for raw_sql models with a single upstream table
    when include_sibling_relationships=True.
    """
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
        include_sibling_relationships=True,
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    source_instance = HightouchIngestionSource(config, pipeline_context)

    model = HightouchModel(
        id="model_1",
        name="Customer Aggregation",
        slug="customer-agg",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        raw_sql="SELECT * FROM customers WHERE status = 'active'",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Production",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={"database": "production"},
    )
    mock_client.get_source_by_id.return_value = source_connection

    workunits = list(source_instance._get_model_workunits(model))

    siblings_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, SiblingsClass)
    ]

    # Should emit sibling aspect on the Hightouch model (primary)
    assert len(siblings_workunits) == 1
    sibling_wu = siblings_workunits[0]
    assert isinstance(sibling_wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(sibling_wu.metadata.aspect, SiblingsClass)
    assert sibling_wu.metadata.aspect.primary is True
    assert len(sibling_wu.metadata.aspect.siblings) == 1
    # Should reference the upstream snowflake table
    assert "snowflake" in sibling_wu.metadata.aspect.siblings[0]
    assert "customers" in sibling_wu.metadata.aspect.siblings[0]


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_no_siblings_for_multi_table_raw_sql_models(
    mock_api_client_class, pipeline_context
):
    """
    Test that siblings are NOT emitted for raw_sql models that reference multiple tables,
    even with include_sibling_relationships=True.
    """
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
        include_sibling_relationships=True,
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    source_instance = HightouchIngestionSource(config, pipeline_context)

    model = HightouchModel(
        id="model_1",
        name="Customer Orders",
        slug="customer-orders",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        raw_sql="SELECT c.*, o.* FROM customers c JOIN orders o ON c.id = o.customer_id",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Production",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={"database": "production"},
    )
    mock_client.get_source_by_id.return_value = source_connection

    workunits = list(source_instance._get_model_workunits(model))

    siblings_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, SiblingsClass)
    ]

    # Should NOT emit siblings for multi-table queries
    assert len(siblings_workunits) == 0


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_no_siblings_when_include_sibling_relationships_false(
    mock_api_client_class, pipeline_context
):
    """
    Test that siblings are NOT emitted when include_sibling_relationships=False.
    """
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        emit_models_as_datasets=True,
        include_sibling_relationships=False,
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    source_instance = HightouchIngestionSource(config, pipeline_context)

    model = HightouchModel(
        id="model_1",
        name="customers",
        slug="customers-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="table",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Production",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={"database": "production"},
    )
    mock_client.get_source_by_id.return_value = source_connection

    workunits = list(source_instance._get_model_workunits(model))

    siblings_workunits = [
        wu
        for wu in workunits
        if hasattr(wu, "metadata")
        and isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, SiblingsClass)
    ]

    assert len(siblings_workunits) == 0
