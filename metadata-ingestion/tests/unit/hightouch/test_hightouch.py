from datetime import datetime
from typing import List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests
import requests_mock

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
    HightouchSyncRun,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    DataContractPropertiesClass,
    DataProcessInstanceRunEventClass,
    MetadataChangeProposalClass,
    SubTypesClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sdk.datajob import DataJob
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


_SOURCES_URL = "https://api.hightouch.com/api/v1/sources"


def test_test_connection_success() -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(_SOURCES_URL, json={"data": []})
        report = HightouchIngestionSource.test_connection(
            {"api_config": {"api_key": "test_api_key"}}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert mocker.last_request.qs == {"limit": ["1"]}


def test_test_connection_reports_a_rejected_key() -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(_SOURCES_URL, status_code=401)
        report = HightouchIngestionSource.test_connection(
            {"api_config": {"api_key": "bad_key"}}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert "rejected the API key" in (report.basic_connectivity.failure_reason or "")


def test_test_connection_reports_an_unreachable_api() -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(_SOURCES_URL, exc=requests.exceptions.ConnectionError)
        report = HightouchIngestionSource.test_connection(
            {"api_config": {"api_key": "test_api_key"}}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert "Could not reach" in (report.basic_connectivity.failure_reason or "")


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
        destination_to_platform_instance={
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
    list(source_instance.get_workunits_internal())

    # The denied 'test-sync' (model_id="10") must be filtered out before any
    # per-sync processing, while the allowed 'prod-sync' (model_id="11") is
    # processed and therefore resolves its model.
    looked_up_model_ids = {
        call.args[0] for call in mock_client.get_model_by_id.call_args_list
    }
    assert "11" in looked_up_model_ids
    assert "10" not in looked_up_model_ids


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
        raw={
            "sql": "SELECT * FROM analytics.customers JOIN analytics.orders ON customers.id = orders.customer_id"
        },
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
        raw={"sql": "SELECT 1 as id, 'test' as name"},
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
        raw={"sql": "SELECT * FROM WHERE INVALID SYNTAX!!!"},
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
        table={"name": "CUSTOMERS"},
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

    # The table model's upstream (emitted with its column lineage in
    # emit_model_aspects) is its table, lowercased for Snowflake, not its
    # display name.
    assert result.dataset.custom_properties["source_table_urn"].endswith(
        ",customers,PROD)"
    )


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
        raw={"sql": "SELECT * FROM customers"},
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
        raw={
            "sql": """
            WITH active_customers AS (
                SELECT * FROM analytics.customers WHERE status = 'active'
            )
            SELECT * FROM active_customers JOIN analytics.orders USING (customer_id)
        """
        },
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
        raw={"sql": "SELECT * FROM analytics.customers"},
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


def _sync_for_model(slug: str, referenced_columns: List[str]) -> HightouchSync:
    return HightouchSync(
        id=f"{slug}-id",
        slug=slug,
        workspace_id="100",
        model_id="10",
        destination_id="20",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={"object": "Contact"},
        referenced_columns=referenced_columns,
    )


def _salesforce_destination() -> HightouchDestination:
    return HightouchDestination(
        id="20",
        name="Salesforce",
        slug="salesforce",
        type="salesforce",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )


def _run_source(mock_client: MagicMock, **config: object) -> HightouchIngestionSource:
    mock_client.get_source_by_id.return_value = _snowflake_source()
    mock_client.get_destination_by_id.return_value = _salesforce_destination()
    mock_client.get_contracts.return_value = []
    mock_client.extract_field_mappings.return_value = []
    with patch(
        "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient",
        return_value=mock_client,
    ):
        return HightouchIngestionSource(
            HightouchSourceConfig(
                api_config=HightouchAPIConfig(api_key="test"), env="PROD", **config
            ),
            PipelineContext(run_id="test"),
        )


def test_model_shared_by_syncs_is_emitted_once_with_all_their_columns() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [
        _sync_for_model("sync-a", ["user_id", "email"]),
        _sync_for_model("sync-b", ["email", "plan"]),
    ]
    mock_client.get_models.return_value = [_raw_sql_model()]
    source = _run_source(mock_client, include_sync_runs=False)

    workunits = list(source.get_workunits_internal())

    model_datasets = [
        wu
        for wu in workunits
        if isinstance(wu, Dataset)
        and wu.urn.platform == "urn:li:dataPlatform:hightouch"
    ]
    assert len(model_datasets) == 1
    assert [f.field_path for f in model_datasets[0].schema] == [
        "user_id",
        "email",
        "plan",
    ]
    # The model list already had the model, so no per-sync fetch was needed.
    mock_client.get_model_by_id.assert_not_called()


def test_models_not_emitted_link_syncs_to_their_source_tables() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [_sync_for_model("sync-a", ["user_id"])]
    mock_client.get_model_by_id.return_value = _raw_sql_model()
    source = _run_source(
        mock_client, emit_models_as_datasets=False, include_sync_runs=False
    )

    workunits = list(source.get_workunits_internal())

    assert not any(
        isinstance(wu, Dataset) and wu.urn.platform == "urn:li:dataPlatform:hightouch"
        for wu in workunits
    )
    datajob = next(wu for wu in workunits if isinstance(wu, DataJob))
    assert [str(urn) for urn in datajob.inlets] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers,PROD)"
    ]
    mock_client.get_models.assert_not_called()


def test_denied_model_inlet_is_source_table_not_model() -> None:
    mock_client = MagicMock()
    table_model = HightouchModel(
        id="10",
        name="Customers",
        slug="customers",
        workspace_id="100",
        source_id="1",
        query_type="table",
        table={"name": "CUSTOMERS"},
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )
    source = _run_source(
        mock_client,
        emit_models_as_datasets=True,
        model_patterns=AllowDenyPattern(deny=["Customers"]),
    )

    inlets = source._sync_handler.get_inlet_urns_for_model(
        table_model, _sync_for_model("sync-a", [])
    )

    assert [str(urn) for urn in inlets] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)"
    ]


def test_pattern_filtered_syncs_and_models_are_reported() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [
        _sync_for_model("kept-sync", []),
        _sync_for_model("denied-sync", []),
    ]
    mock_client.get_models.return_value = [_raw_sql_model()]
    source = _run_source(
        mock_client,
        include_sync_runs=False,
        sync_patterns=AllowDenyPattern(deny=["denied-sync"]),
        model_patterns=AllowDenyPattern(deny=["Test Model"]),
    )

    list(source.get_workunits_internal())

    assert list(source.report.filtered_syncs) == ["denied-sync"]
    assert list(source.report.filtered_models) == ["Test Model"]


@pytest.mark.parametrize(
    "status, expected_events, expected_result",
    [
        ("success", ["STARTED", "COMPLETE"], "SUCCESS"),
        ("warning", ["STARTED", "COMPLETE"], "SUCCESS"),
        ("failed", ["STARTED", "COMPLETE"], "FAILURE"),
        ("cancelled", ["STARTED", "COMPLETE"], "SKIPPED"),
        ("interrupted", ["STARTED", "COMPLETE"], "SKIPPED"),
        ("queued", ["STARTED"], None),
        ("querying", ["STARTED"], None),
        ("processing", ["STARTED"], None),
        ("reporting", ["STARTED"], None),
    ],
)
def test_sync_run_status_maps_to_run_events(
    status: str, expected_events: List[str], expected_result: Optional[str]
) -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [_sync_for_model("sync-a", [])]
    mock_client.get_model_by_id.return_value = _raw_sql_model()
    mock_client.get_sync_runs.return_value = [
        HightouchSyncRun.model_validate(
            {
                "id": 99,
                "status": status,
                "startedAt": "2023-01-01T00:00:00Z",
                "finishedAt": None
                if expected_result is None
                else "2023-01-01T00:05:00Z",
                "createdAt": "2023-01-01T00:00:00Z",
            }
        )
    ]
    source = _run_source(mock_client, emit_models_as_datasets=False)

    run_events = _aspects_of_type(
        source.get_workunits_internal(), DataProcessInstanceRunEventClass
    )

    assert [event.status for event in run_events] == expected_events
    results = [e.result.type for e in run_events if e.result is not None]
    assert results == ([expected_result] if expected_result else [])
    # Every status in the API's SyncRunStatus enum is known to the connector.
    assert not source.report.unknown_sync_run_statuses


def test_sync_runs_are_skipped_when_the_limit_is_zero() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [_sync_for_model("sync-a", [])]
    mock_client.get_model_by_id.return_value = _raw_sql_model()
    source = _run_source(
        mock_client, emit_models_as_datasets=False, max_sync_runs_per_sync=0
    )

    list(source.get_workunits_internal())

    mock_client.get_sync_runs.assert_not_called()


def test_failed_sync_run_fetch_keeps_the_sync() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [_sync_for_model("sync-a", [])]
    mock_client.get_model_by_id.return_value = _raw_sql_model()
    mock_client.get_sync_runs.side_effect = requests.exceptions.HTTPError("503")
    source = _run_source(mock_client, emit_models_as_datasets=False)

    workunits = list(source.get_workunits_internal())

    assert any(isinstance(wu, DataJob) for wu in workunits)
    assert [w.title for w in source.report.warnings] == ["Failed to fetch sync runs"]


def _customers_table_model() -> HightouchModel:
    return HightouchModel(
        id="10",
        name="Customers",
        slug="customers",
        workspace_id="100",
        source_id="1",
        query_type="table",
        table={"name": "CUSTOMERS"},
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )


def test_table_model_is_a_sibling_of_its_table() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = []
    mock_client.get_models.return_value = [_customers_table_model()]
    source = _run_source(mock_client, include_sibling_relationships=True)

    workunits = list(source.get_workunits_internal())

    siblings = [
        wu.metadata
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit)
        and isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "siblings"
    ]
    assert [mcp.entityUrn for mcp in siblings] == [
        "urn:li:dataset:(urn:li:dataPlatform:hightouch,customers,PROD)"
    ]
    assert siblings[0].aspect is not None
    assert "urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)" in str(
        siblings[0].aspect.value
    )


def test_preload_error_skips_only_that_sync() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [_sync_for_model("sync-a", [])]
    mock_client.get_model_by_id.return_value = _raw_sql_model()
    source = _run_source(
        mock_client, emit_models_as_datasets=False, include_sync_runs=False
    )
    # The first source lookup (during preload) fails; the sync still ingests.
    mock_client.get_source_by_id.side_effect = [
        requests.exceptions.HTTPError("503"),
        _snowflake_source(),
    ]
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    source.graph = graph
    source._schema_handler.graph = graph

    workunits = list(source.get_workunits_internal())

    assert any(isinstance(wu, DataJob) for wu in workunits)
    assert [w.title for w in source.report.warnings] == [
        "Could not preload schemas for SQL parsing"
    ]
    assert not source.report.failures


def test_platform_resolution_does_not_change_the_recipe_mapping() -> None:
    mapping = PlatformDetail(platform="snowflake")
    mock_client = MagicMock()
    source = _run_source(mock_client, sources_to_platform_instance={"1": mapping})
    warehouse = HightouchSourceConnection(
        id="1",
        name="Snowflake Prod",
        slug="snowflake-prod",
        type="snowflake",
        workspace_id="100",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={"database": "ANALYTICS"},
    )

    resolved = source._get_platform_for_source(warehouse)

    assert resolved.database == "ANALYTICS"
    assert resolved.env == "PROD"
    configured = source.config.sources_to_platform_instance["1"]
    assert configured.database is None
    assert configured.env is None


def test_unknown_sync_run_status_is_not_recorded_as_success() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [_sync_for_model("sync-a", [])]
    mock_client.get_model_by_id.return_value = _raw_sql_model()
    mock_client.get_sync_runs.return_value = [
        HightouchSyncRun.model_validate(
            {
                "id": 99,
                "status": "bogus",
                "startedAt": "2023-01-01T00:00:00Z",
                "finishedAt": "2023-01-01T00:05:00Z",
                "createdAt": "2023-01-01T00:00:00Z",
            }
        )
    ]
    source = _run_source(mock_client, emit_models_as_datasets=False)

    run_events = _aspects_of_type(
        source.get_workunits_internal(), DataProcessInstanceRunEventClass
    )

    assert [e.result.type for e in run_events if e.result] == ["SKIPPED"]
    assert dict(source.report.unknown_sync_run_statuses) == {"bogus": 1}
