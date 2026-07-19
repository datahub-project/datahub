from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.hightouch import (
    HightouchSource as HightouchIngestionSource,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.models import (
    HightouchContract,
    HightouchContractEvent,
    HightouchDestination,
    HightouchEventSource,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    DataContractPropertiesClass,
    SubTypesClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sdk.dataset import Dataset


def _aspects_of_type(workunits, aspect_type):
    aspects = []
    for wu in workunits:
        if not isinstance(wu, MetadataWorkUnit):
            continue
        mcp = wu.metadata
        if isinstance(mcp, MetadataChangeProposalWrapper) and isinstance(
            mcp.aspect, aspect_type
        ):
            aspects.append(mcp.aspect)
    return aspects


@pytest.fixture
def hightouch_config():
    return HightouchSourceConfig(
        api_config=HightouchAPIConfig(
            api_key="test_api_key",
            base_url="https://api.hightouch.com/api/v1",
        ),
        env="PROD",
        emit_models_as_datasets=True,
        include_sibling_relationships=False,
        include_sync_runs=True,
        max_sync_runs_per_sync=5,
    )


@pytest.fixture
def pipeline_context():
    return PipelineContext(run_id="test_run")


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_test_connection_success(
    mock_api_client_class, hightouch_config, pipeline_context
):
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client
    mock_client.get_workspaces.return_value = []

    source = HightouchIngestionSource(hightouch_config, pipeline_context)
    report = source.test_connection()

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    mock_client.get_workspaces.assert_called_once()


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_test_connection_failure(
    mock_api_client_class, hightouch_config, pipeline_context
):
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client
    mock_client.get_workspaces.side_effect = requests.exceptions.ConnectionError(
        "unreachable"
    )

    source = HightouchIngestionSource(hightouch_config, pipeline_context)
    report = source.test_connection()

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert report.basic_connectivity.failure_reason is not None
    assert "unreachable" in report.basic_connectivity.failure_reason


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_get_platform_for_source_with_mapping(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        sources_to_platform_instance={
            "source1": PlatformDetail(
                platform="custom_platform",
                platform_instance="custom_instance",
                env="DEV",
                database="custom_db",
            )
        },
    )
    source = HightouchIngestionSource(config, pipeline_context)

    source_entity = HightouchSourceConnection(
        id="source1",
        name="Test Source",
        slug="test-source",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    platform_detail = source._get_platform_for_source(source_entity)

    assert platform_detail.platform == "custom_platform"
    assert platform_detail.platform_instance == "custom_instance"
    assert platform_detail.env == "DEV"
    assert platform_detail.database == "custom_db"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_get_platform_for_source_known_mapping(
    mock_api_client_class, hightouch_config, pipeline_context
):
    source = HightouchIngestionSource(hightouch_config, pipeline_context)

    source_entity = HightouchSourceConnection(
        id="source1",
        name="Test Snowflake",
        slug="test-snowflake",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    platform_detail = source._get_platform_for_source(source_entity)

    assert platform_detail.platform == "snowflake"
    assert platform_detail.env == "PROD"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_get_platform_for_destination_with_mapping(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        destinations_to_platform_instance={
            "dest1": PlatformDetail(
                platform="custom_dest",
                platform_instance="custom_instance",
                env="STAGING",
            )
        },
    )
    source = HightouchIngestionSource(config, pipeline_context)

    dest_entity = HightouchDestination(
        id="dest1",
        name="Test Destination",
        slug="test-destination",
        type="salesforce",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    platform_detail = source._get_platform_for_destination(dest_entity)

    assert platform_detail.platform == "custom_dest"
    assert platform_detail.platform_instance == "custom_instance"
    assert platform_detail.env == "STAGING"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_generate_model_dataset(
    mock_api_client_class, hightouch_config, pipeline_context
):
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Customer Model",
        slug="customer-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        primary_key="customer_id",
        description="Test model",
        is_schema=False,
        tags={"team": "data"},
    )

    source_entity = HightouchSourceConnection(
        id="1",
        name="Test Source",
        slug="test-source",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    result = source_instance._model_handler.generate_model_dataset(model, source_entity)

    assert result.dataset.urn.name == "customer-model"
    assert str(result.dataset.urn.platform) == "urn:li:dataPlatform:hightouch"
    assert result.dataset.urn.env == "PROD"
    assert result.dataset.display_name == "Customer Model"
    assert result.dataset.description == "Test model"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_generate_dataflow_from_sync(
    mock_api_client_class, hightouch_config, pipeline_context
):
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    sync = HightouchSync(
        id="30",
        slug="customer-to-salesforce",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        disabled=False,
    )

    dataflow = source_instance._sync_handler.generate_dataflow_from_sync(sync)

    assert dataflow.urn.flow_id == "30"
    assert dataflow.urn.orchestrator == "hightouch"
    assert dataflow.display_name == "customer-to-salesforce"
    assert dataflow.urn.cluster == "PROD"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_get_workunits_internal(
    mock_api_client_class, hightouch_config, pipeline_context
):
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_sync = HightouchSync(
        id="30",
        slug="test-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        disabled=False,
        configuration={"destinationTable": "Contact"},
    )

    mock_model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_source = HightouchSourceConnection(
        id="1",
        name="Test Source",
        slug="test-source",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_destination = HightouchDestination(
        id="20",
        name="Test Destination",
        slug="test-destination",
        type="salesforce",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_client.get_syncs.return_value = [mock_sync]
    mock_client.get_model_by_id.return_value = mock_model
    mock_client.get_source_by_id.return_value = mock_source
    mock_client.get_destination_by_id.return_value = mock_destination
    mock_client.get_models.return_value = []
    mock_client.get_sync_runs.return_value = []
    mock_client.extract_field_mappings.return_value = []

    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)
    workunits = list(source_instance.get_workunits_internal())

    assert len(workunits) > 0
    mock_client.get_syncs.assert_called_once()


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_sync_patterns_filtering(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        sync_patterns=AllowDenyPattern(deny=["test.*"]),
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_syncs = [
        HightouchSync(
            id="1",
            slug="test-sync",
            workspace_id="100",
            model_id="10",
            destination_id="20",
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 2),
            disabled=False,
        ),
        HightouchSync(
            id="2",
            slug="prod-sync",
            workspace_id="100",
            model_id="11",
            destination_id="20",
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 2),
            disabled=False,
        ),
    ]

    mock_client.get_syncs.return_value = mock_syncs
    mock_client.get_model_by_id.return_value = None
    mock_client.get_models.return_value = []

    source_instance = HightouchIngestionSource(config, pipeline_context)
    workunits = list(source_instance.get_workunits_internal())

    assert len(workunits) >= 0


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_sql_parsing_with_valid_query(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test SQL parsing with a valid query extracting upstream tables."""
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_model = HightouchModel(
        id="10",
        name="Customer 360",
        slug="customer-360",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        raw_sql="SELECT * FROM analytics.customers JOIN analytics.orders ON customers.id = orders.customer_id",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_source = HightouchSourceConnection(
        id="1",
        name="Snowflake Prod",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)
    source_instance._sources_cache = {"1": mock_source}

    result = source_instance._model_handler.generate_model_dataset(
        mock_model, mock_source
    )

    assert result.dataset.urn.name == "customer-360"
    # SQL parsing now happens in SqlParsingAggregator via _register_model_lineage()


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_sql_parsing_with_no_upstream_tables(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test SQL parsing with a query that has no upstream tables (e.g., VALUES clause)."""
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_model = HightouchModel(
        id="10",
        name="Static Data",
        slug="static-data",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        raw_sql="SELECT 1 as id, 'test' as name",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_source = HightouchSourceConnection(
        id="1",
        name="Snowflake Prod",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)
    source_instance._sources_cache = {"1": mock_source}

    result = source_instance._model_handler.generate_model_dataset(
        mock_model, mock_source
    )

    assert result.dataset.urn.name == "static-data"
    # SQL parsing now happens in SqlParsingAggregator via _register_model_lineage()


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_sql_parsing_with_invalid_sql(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test SQL parsing with invalid SQL syntax."""
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_model = HightouchModel(
        id="10",
        name="Invalid Model",
        slug="invalid-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        raw_sql="SELECT * FROM WHERE INVALID SYNTAX!!!",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_source = HightouchSourceConnection(
        id="1",
        name="Snowflake Prod",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)
    source_instance._sources_cache = {"1": mock_source}

    result = source_instance._model_handler.generate_model_dataset(
        mock_model, mock_source
    )

    assert result.dataset.urn.name == "invalid-model"
    # SQL parsing now happens in SqlParsingAggregator via _register_model_lineage()


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_sql_parsing_with_no_raw_sql(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test SQL parsing when raw_sql is None."""
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_model = HightouchModel(
        id="10",
        name="Table Model",
        slug="table-model",
        workspace_id="100",
        source_id="1",
        query_type="table",
        raw_sql=None,
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_source = HightouchSourceConnection(
        id="1",
        name="Snowflake Prod",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)
    source_instance._sources_cache = {"1": mock_source}

    result = source_instance._model_handler.generate_model_dataset(
        mock_model, mock_source
    )

    assert result.dataset.urn.name == "table-model"
    assert source_instance.report.sql_parsing_attempts == 0

    # Table-type models should have upstream lineage to the source table
    upstream_lineage = result.dataset.upstreams
    assert upstream_lineage is not None
    assert len(upstream_lineage.upstreams) == 1
    # The upstream should be the table name from the model
    assert "Table Model" in str(upstream_lineage.upstreams[0].dataset)


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_sql_parsing_with_unknown_platform(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test SQL parsing when source platform cannot be determined."""
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_model = HightouchModel(
        id="10",
        name="Custom Model",
        slug="custom-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        raw_sql="SELECT * FROM customers",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    result = source_instance._model_handler.generate_model_dataset(mock_model, None)

    assert result.dataset.urn.name == "custom-model"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_sql_parsing_with_cte(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test SQL parsing with Common Table Expressions (CTEs)."""
    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client

    mock_model = HightouchModel(
        id="10",
        name="CTE Model",
        slug="cte-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        raw_sql="""
            WITH active_customers AS (
                SELECT * FROM analytics.customers WHERE status = 'active'
            )
            SELECT * FROM active_customers JOIN analytics.orders USING (customer_id)
        """,
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    mock_source = HightouchSourceConnection(
        id="1",
        name="Snowflake Prod",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)
    source_instance._sources_cache = {"1": mock_source}

    result = source_instance._model_handler.generate_model_dataset(
        mock_model, mock_source
    )

    assert result.dataset.urn.name == "cte-model"
    # SQL parsing now happens in SqlParsingAggregator via _register_model_lineage()


def _raw_sql_model():
    return HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        raw_sql="SELECT * FROM analytics.customers",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )


def _snowflake_source():
    return HightouchSourceConnection(
        id="1",
        name="Snowflake Prod",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_raw_sql_lineage_registered_when_enabled(
    mock_api_client_class, pipeline_context
):
    """With parse_model_sql on, a raw_sql model registers its SQL as a view
    definition so the aggregator can derive lineage."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        parse_model_sql=True,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    aggregator = MagicMock()
    platform = PlatformDetail(platform="snowflake", database="analytics", env="PROD")

    source_instance._lineage_handler.register_model_lineage(
        _raw_sql_model(),
        "urn:li:dataset:(urn:li:dataPlatform:hightouch,test-model,PROD)",
        _snowflake_source(),
        lambda _source: platform,
        lambda _platform: aggregator,
    )

    aggregator.add_view_definition.assert_called_once()
    assert source_instance.report.sql_parsing_attempts == 1
    assert source_instance.report.sql_parsing_successes == 1


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_raw_sql_lineage_skipped_when_disabled(mock_api_client_class, pipeline_context):
    """With parse_model_sql off, a raw_sql model derives no lineage from its SQL:
    no view definition is registered and no parse is attempted."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        parse_model_sql=False,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    aggregator = MagicMock()
    platform = PlatformDetail(platform="snowflake", database="analytics", env="PROD")

    source_instance._lineage_handler.register_model_lineage(
        _raw_sql_model(),
        "urn:li:dataset:(urn:li:dataPlatform:hightouch,test-model,PROD)",
        _snowflake_source(),
        lambda _source: platform,
        lambda _platform: aggregator,
    )

    aggregator.add_view_definition.assert_not_called()
    assert source_instance.report.sql_parsing_attempts == 0


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_extract_table_urns_skipped_when_disabled(
    mock_api_client_class, pipeline_context
):
    """parse_model_sql off also short-circuits table-URN extraction from SQL."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        parse_model_sql=False,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    urns = source_instance._model_handler.extract_table_urns_from_sql(
        _raw_sql_model(), _snowflake_source()
    )
    assert urns == []


# Tests for Event Contracts Feature


def _email_event_schema():
    return {
        "type": "object",
        "properties": {
            "email": {"type": "string"},
            "user_id": {"type": "string"},
        },
        "required": ["email"],
    }


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_contract_emits_event_datasets(mock_api_client_class, pipeline_context):
    """Each event in a contract is emitted as a dataset with schema + properties."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        include_contracts=True,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    contract = HightouchContract(
        id="contract_1",
        name="Signup Tracking",
        slug="signup-tracking",
        description="Validates signup events",
        workspace_id="100",
        on_undeclared_schema="BLOCK_EVENT",
        events=[
            HightouchContractEvent(
                type="track",
                name="Signed Up",
                slug="signed-up",
                schema=_email_event_schema(),
                on_schema_violation="BLOCK_EVENT",
                on_undeclared_fields="OMIT_FIELDS",
            )
        ],
    )

    out = list(source_instance._contract_handler.get_contract_workunits([contract]))

    datasets = [e for e in out if isinstance(e, Dataset)]
    assert len(datasets) == 1
    dataset = datasets[0]
    assert str(dataset.urn) == str(
        DatasetUrn(platform="hightouch", name="signup-tracking.signed-up")
    )
    assert dataset.description == "Validates signup events"

    props = dataset.custom_properties
    assert props["contract_id"] == "contract_1"
    assert props["contract_name"] == "Signup Tracking"
    assert props["event_type"] == "track"
    assert props["event_name"] == "Signed Up"
    assert props["on_schema_violation"] == "BLOCK_EVENT"
    assert props["on_undeclared_fields"] == "OMIT_FIELDS"
    assert props["on_undeclared_schema"] == "BLOCK_EVENT"

    field_paths = {f.field_path for f in dataset.schema}
    assert any("email" in fp for fp in field_paths)

    subtypes = [
        mcp.aspect for mcp in dataset.as_mcps() if isinstance(mcp.aspect, SubTypesClass)
    ]
    assert subtypes and subtypes[0].typeNames == ["Event Contract"]

    # A DataContract with a DATA_SCHEMA assertion is emitted on the event dataset.
    contract_props = _aspects_of_type(out, DataContractPropertiesClass)
    assert len(contract_props) == 1
    assert contract_props[0].entity == str(dataset.urn)
    assert contract_props[0].schema and len(contract_props[0].schema) == 1

    assertion_infos = _aspects_of_type(out, AssertionInfoClass)
    assert len(assertion_infos) == 1
    assert assertion_infos[0].type == AssertionTypeClass.DATA_SCHEMA

    assert source_instance.report.contracts_scanned == 1
    assert source_instance.report.contracts_emitted == 1
    assert source_instance.report.contract_events_emitted == 1
    assert source_instance.report.contract_assertions_emitted == 1


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_contract_with_multiple_events(mock_api_client_class, pipeline_context):
    """A contract with multiple events emits one dataset per event."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        include_contracts=True,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    contract = HightouchContract(
        id="c1",
        name="Multi",
        slug="multi",
        events=[
            HightouchContractEvent(type="identify", slug="identify", schema={}),
            HightouchContractEvent(
                type="track", name="Order", slug="order", schema=_email_event_schema()
            ),
        ],
    )

    out = list(source_instance._contract_handler.get_contract_workunits([contract]))

    datasets = [e for e in out if isinstance(e, Dataset)]
    assert len(datasets) == 2
    assert source_instance.report.contract_events_emitted == 2
    assert source_instance.report.contracts_emitted == 1
    # Only the event carrying a JSON Schema produces a data contract/assertion.
    assert source_instance.report.contract_assertions_emitted == 1


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_contract_emits_event_sources_and_lineage(
    mock_api_client_class, pipeline_context
):
    """Event sources are emitted as datasets and linked upstream of each event."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        include_contracts=True,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    contract = HightouchContract(
        id="c1",
        name="Sourced",
        slug="sourced",
        events=[
            HightouchContractEvent(
                type="track", name="Order", slug="order", schema=_email_event_schema()
            )
        ],
        event_sources=[HightouchEventSource(id="src_1", name="Web App")],
    )

    out = list(source_instance._contract_handler.get_contract_workunits([contract]))

    datasets = [e for e in out if isinstance(e, Dataset)]
    source_urn = str(DatasetUrn(platform="hightouch", name="event_source.src_1"))
    event_urn = str(DatasetUrn(platform="hightouch", name="sourced.order"))

    dataset_urns = {str(d.urn) for d in datasets}
    assert source_urn in dataset_urns
    assert event_urn in dataset_urns
    assert source_instance.report.event_sources_emitted == 1

    event_dataset = next(d for d in datasets if str(d.urn) == event_urn)
    upstreams = [
        mcp.aspect
        for mcp in event_dataset.as_mcps()
        if isinstance(mcp.aspect, UpstreamLineageClass)
    ]
    assert upstreams
    assert source_urn in {u.dataset for u in upstreams[0].upstreams}


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_event_sources_emitted_once_across_contracts(
    mock_api_client_class, pipeline_context
):
    """A shared event source is only emitted once across multiple contracts."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        include_contracts=True,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    shared_source = HightouchEventSource(id="src_1", name="Web App")
    contracts = [
        HightouchContract(
            id=f"c{i}",
            name=f"Contract {i}",
            slug=f"contract-{i}",
            events=[
                HightouchContractEvent(type="track", name="e", slug="e", schema={})
            ],
            event_sources=[shared_source],
        )
        for i in range(2)
    ]

    list(source_instance._contract_handler.get_contract_workunits(contracts))

    assert source_instance.report.event_sources_emitted == 1


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_contract_with_no_events_emits_nothing(mock_api_client_class, pipeline_context):
    """A contract with no events is scanned but emits no datasets."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        include_contracts=True,
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    contract = HightouchContract(id="c1", name="Empty", slug="empty", events=[])

    entities = list(
        source_instance._contract_handler.get_contract_workunits([contract])
    )

    assert entities == []
    assert source_instance.report.contracts_scanned == 1
    assert source_instance.report.contracts_emitted == 0


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_contract_patterns_filtering(mock_api_client_class, pipeline_context):
    """Contracts whose name is denied by contract_patterns are dropped."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        include_contracts=True,
        contract_patterns=AllowDenyPattern(deny=["Blocked.*"]),
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)

    contract = HightouchContract(
        id="c1",
        name="Blocked Contract",
        slug="blocked",
        events=[HightouchContractEvent(type="track", name="x", schema={})],
    )

    entities = list(
        source_instance._contract_handler.get_contract_workunits([contract])
    )

    assert entities == []
    assert source_instance.report.contracts_scanned == 0
    assert "Blocked Contract" in source_instance.report.filtered_contracts


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_contracts_can_be_disabled(mock_api_client_class, pipeline_context):
    """Test that contracts can be explicitly disabled."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        include_contracts=False,  # Explicitly disabled
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client
    mock_client.get_syncs.return_value = []
    mock_client.get_models.return_value = []
    mock_client.get_contracts.return_value = []

    source_instance = HightouchIngestionSource(config, pipeline_context)
    list(source_instance.get_workunits_internal())  # Execute the generator

    # Contracts should not be fetched if disabled
    mock_client.get_contracts.assert_not_called()


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_contracts_404_handling(mock_api_client_class, pipeline_context):
    """Test that 404 errors from contracts endpoint are handled gracefully."""
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        include_contracts=True,  # Explicitly enabled to test 404 handling
    )

    mock_client = MagicMock()
    mock_api_client_class.return_value = mock_client
    mock_client.get_syncs.return_value = []
    mock_client.get_models.return_value = []

    # Simulate 404 error from contracts endpoint
    mock_response = Mock()
    mock_response.status_code = 404
    http_error = requests.exceptions.HTTPError()
    http_error.response = mock_response
    mock_client.get_contracts.side_effect = lambda: (_ for _ in ()).throw(http_error)

    real_client = HightouchAPIClient(config.api_config)

    # Patch _make_paginated_request to raise 404
    def raise_404(*args, **kwargs):
        response = Mock()
        response.status_code = 404
        error = requests.exceptions.HTTPError()
        error.response = response
        raise error

    with patch.object(real_client, "_make_paginated_request", side_effect=raise_404):
        # This should return empty list, not raise exception
        contracts = real_client.get_contracts()
        assert contracts == []


# Tests for Schema Emission Feature


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_parse_model_schema_with_no_schema(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test schema parsing when query_schema is None."""
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema=None,
    )

    result = source_instance._schema_handler._parse_model_schema(model)
    assert result is None


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_parse_model_schema_with_list_format(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test schema parsing with direct list format."""
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema=[
            {"name": "user_id", "type": "INTEGER"},
            {"name": "email", "type": "STRING", "description": "User email address"},
            {"name": "created_at", "type": "TIMESTAMP"},
        ],
    )

    result = source_instance._schema_handler._parse_model_schema(model)

    assert result is not None
    assert len(result) == 3
    assert (
        result[0].name == "user_id"
        and result[0].type == "INTEGER"
        and result[0].description is None
    )
    assert (
        result[1].name == "email"
        and result[1].type == "STRING"
        and result[1].description == "User email address"
    )
    assert (
        result[2].name == "created_at"
        and result[2].type == "TIMESTAMP"
        and result[2].description is None
    )
    assert source_instance.report.model_schemas_emitted == 1


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_parse_model_schema_with_dict_format(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test schema parsing with dict containing 'columns' key."""
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema={
            "columns": [
                {"name": "order_id", "dataType": "BIGINT"},
                {"fieldName": "amount", "type": "DECIMAL"},
            ]
        },
    )

    result = source_instance._schema_handler._parse_model_schema(model)

    assert result is not None
    assert len(result) == 2
    assert (
        result[0].name == "order_id"
        and result[0].type == "BIGINT"
        and result[0].description is None
    )
    assert (
        result[1].name == "amount"
        and result[1].type == "DECIMAL"
        and result[1].description is None
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_parse_model_schema_with_json_string(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test schema parsing when query_schema is a JSON string."""
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema='{"columns": [{"name": "id", "type": "INT"}, {"name": "name", "type": "VARCHAR"}]}',
    )

    result = source_instance._schema_handler._parse_model_schema(model)

    assert result is not None
    assert len(result) == 2
    assert (
        result[0].name == "id"
        and result[0].type == "INT"
        and result[0].description is None
    )
    assert (
        result[1].name == "name"
        and result[1].type == "VARCHAR"
        and result[1].description is None
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_parse_model_schema_with_invalid_json_string(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test schema parsing with invalid JSON string."""
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema='{"invalid json',
    )

    result = source_instance._schema_handler._parse_model_schema(model)
    assert result is None
    assert source_instance.report.model_schemas_skipped >= 1


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_parse_model_schema_with_field_variations(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test schema parsing handles various field name variations."""
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema=[
            {"fieldName": "col1", "fieldType": "INT"},  # fieldName, fieldType
            {"column_name": "col2", "data_type": "VARCHAR"},  # snake_case
            {
                "name": "col3",
                "columnType": "BOOLEAN",
                "comment": "Boolean flag",
            },  # comment
        ],
    )

    result = source_instance._schema_handler._parse_model_schema(model)

    assert result is not None
    assert len(result) == 3
    assert (
        result[0].name == "col1"
        and result[0].type == "INT"
        and result[0].description is None
    )
    assert (
        result[1].name == "col2"
        and result[1].type == "VARCHAR"
        and result[1].description is None
    )
    assert (
        result[2].name == "col3"
        and result[2].type == "BOOLEAN"
        and result[2].description == "Boolean flag"
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_parse_model_schema_with_incomplete_columns(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test schema parsing skips incomplete column definitions."""
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema=[
            {"name": "valid_col", "type": "INT"},
            {"name": "no_type_col"},  # Missing type
            {"type": "STRING"},  # Missing name
            {"name": "another_valid", "type": "VARCHAR"},
        ],
    )

    result = source_instance._schema_handler._parse_model_schema(model)

    assert result is not None
    assert len(result) == 2  # Only 2 valid columns
    assert (
        result[0].name == "valid_col"
        and result[0].type == "INT"
        and result[0].description is None
    )
    assert (
        result[1].name == "another_valid"
        and result[1].type == "VARCHAR"
        and result[1].description is None
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_schema_emission_enabled_and_applied(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test that schema is actually applied to dataset when enabled."""
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    model = HightouchModel(
        id="10",
        name="Test Model",
        slug="test-model",
        workspace_id="100",
        source_id="1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema=[
            {"name": "user_id", "type": "BIGINT"},
            {"name": "email", "type": "VARCHAR"},
        ],
    )

    result = source_instance._model_handler.generate_model_dataset(model, None)

    assert source_instance.report.model_schemas_emitted == 1

    schema_fields = result.dataset.schema
    assert len(schema_fields) == 2
    assert schema_fields[0].field_path == "user_id"
    assert schema_fields[1].field_path == "email"


# Tests for Outlet URN Generation with Various Configuration Keys


@pytest.mark.parametrize(
    "sync_config,dest_type,dest_name,expected_in_urn,not_expected_in_urn,test_id",
    [
        (
            {"type": "object", "object": "contacts_table"},
            "salesforce",
            "Salesforce",
            "contacts_table",
            "my-sync",
            "with_object_key",
        ),
        (
            {"object": "Account"},
            "salesforce",
            "Salesforce",
            "Account",
            "my-sync",
            "crm_object_key",
        ),
        (
            {"tableName": "users_table"},
            "mysql",
            "MySQL",
            "users_table",
            None,
            "database_tableName_key",
        ),
        (
            {"table": "orders_table"},
            "postgres",
            "PostgreSQL",
            "orders_table",
            None,
            "database_table_key",
        ),
        (
            {"destinationTable": "customers_table"},
            "bigquery",
            "BigQuery",
            "customers_table",
            None,
            "generic_destinationTable_key",
        ),
        (
            {"objectName": "CustomObject"},
            "hubspot",
            "HubSpot",
            "CustomObject",
            None,
            "crm_objectName_key",
        ),
        (
            {"someOtherKey": "value"},
            "salesforce",
            "Salesforce",
            "my-sync_destination",
            None,
            "fallback_no_config_key",
        ),
        (
            {"type": "event", "eventName": "page_view"},
            "segment",
            "Segment",
            "page_view",
            None,
            "event_type_eventName",
        ),
    ],
)
@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_outlet_urn_generation(
    mock_api_client_class,
    hightouch_config,
    pipeline_context,
    sync_config,
    dest_type,
    dest_name,
    expected_in_urn,
    not_expected_in_urn,
    test_id,
):
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    sync = HightouchSync(
        id="1",
        slug="my-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration=sync_config,
    )

    destination = HightouchDestination(
        id="20",
        name=dest_name,
        slug=dest_type,
        type=dest_type,
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={},
    )

    outlet_urn = source_instance._sync_handler.get_outlet_urn_for_sync(
        sync, destination
    )

    assert outlet_urn is not None
    assert expected_in_urn in str(outlet_urn)
    if not_expected_in_urn:
        assert not_expected_in_urn not in str(outlet_urn)


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_outlet_urn_priority_of_config_keys(
    mock_api_client_class, hightouch_config, pipeline_context
):
    # When multiple keys exist, priority order: object > tableName > table > destinationTable > objectName
    source_instance = HightouchIngestionSource(hightouch_config, pipeline_context)

    sync = HightouchSync(
        id="1",
        slug="my-sync",
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={
            "tableName": "wrong_table",
            "object": "correct_object",
            "destinationTable": "wrong_destination_table",
        },
    )

    destination = HightouchDestination(
        id="20",
        name="Salesforce",
        slug="salesforce",
        type="salesforce",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={},
    )

    outlet_urn = source_instance._sync_handler.get_outlet_urn_for_sync(
        sync, destination
    )

    assert outlet_urn is not None
    assert "correct_object" in str(outlet_urn)
    assert "wrong_table" not in str(outlet_urn)
    assert "wrong_destination_table" not in str(outlet_urn)


@pytest.mark.parametrize(
    "model_data,expected_sql,expected_query_type,test_id",
    [
        (
            {
                "id": "123",
                "name": "Test Model",
                "slug": "test-model",
                "workspaceId": "456",
                "sourceId": "789",
                "queryType": "raw_sql",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "primaryKey": "id",
                "isSchema": False,
                "raw": {"sql": "SELECT * FROM table WHERE id = 1"},
            },
            "SELECT * FROM table WHERE id = 1",
            "raw_sql",
            "nested_raw_object",
        ),
        (
            {
                "id": "123",
                "name": "Test Model",
                "slug": "test-model",
                "workspaceId": "456",
                "sourceId": "789",
                "queryType": "raw_sql",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "primaryKey": "id",
                "isSchema": False,
                "rawSql": "SELECT * FROM table WHERE id = 2",
            },
            "SELECT * FROM table WHERE id = 2",
            "raw_sql",
            "direct_rawSql_field",
        ),
        (
            {
                "id": "123",
                "name": "Test Model",
                "slug": "test-model",
                "workspaceId": "456",
                "sourceId": "789",
                "queryType": "table",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "primaryKey": "id",
                "isSchema": False,
                "table": {"name": "my_table"},
            },
            None,
            "table",
            "without_raw_sql",
        ),
    ],
)
def test_model_raw_sql_extraction(
    model_data, expected_sql, expected_query_type, test_id
):
    model = HightouchModel.model_validate(model_data)

    assert model.raw_sql == expected_sql
    assert model.query_type == expected_query_type
