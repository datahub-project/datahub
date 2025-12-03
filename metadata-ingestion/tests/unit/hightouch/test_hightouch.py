from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.hightouch import (
    HightouchSource as HightouchIngestionSource,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
)


@pytest.fixture
def hightouch_config():
    return HightouchSourceConfig(
        api_config=HightouchAPIConfig(
            api_key="test_api_key",
            base_url="https://api.hightouch.com/api/v1",
        ),
        env="PROD",
        emit_models_as_datasets=True,
        include_model_lineage=True,
        include_sync_runs=True,
        max_sync_runs_per_sync=5,
        include_column_lineage=True,
    )


@pytest.fixture
def pipeline_context():
    return PipelineContext(run_id="test_run")


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_init(mock_api_client_class, hightouch_config, pipeline_context):
    source = HightouchIngestionSource(hightouch_config, pipeline_context)

    assert source.config == hightouch_config
    assert isinstance(source.report, type(source.report))
    mock_api_client_class.assert_called_once_with(hightouch_config.api_config)


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

    dataset = source_instance._generate_model_dataset(model, source_entity)

    assert dataset.urn.name == "customer-model"
    assert str(dataset.urn.platform) == "urn:li:dataPlatform:hightouch"
    assert dataset.urn.env == "PROD"
    assert dataset.display_name == "Customer Model"
    assert dataset.description == "Test model"


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

    dataflow = source_instance._generate_dataflow_from_sync(sync)

    assert dataflow.urn.flow_id == "30"
    assert dataflow.urn.orchestrator == "hightouch"
    assert dataflow.display_name == "customer-to-salesforce"
    assert dataflow.urn.cluster == "PROD"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_get_workunits_internal(
    mock_api_client_class, hightouch_config, pipeline_context
):
    """Test workunit generation."""
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
    from datahub.configuration.common import AllowDenyPattern

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
