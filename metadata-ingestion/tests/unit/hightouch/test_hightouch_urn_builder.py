from datetime import datetime
from unittest.mock import patch

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
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.urns import DatasetUrn


@pytest.fixture
def hightouch_config():
    return HightouchSourceConfig(
        api_config=HightouchAPIConfig(
            api_key="test_api_key",
            base_url="https://api.hightouch.com/api/v1",
        ),
        env="PROD",
        platform_instance="prod-instance",
        include_sibling_relationships=True,
    )


@pytest.fixture
def pipeline_context():
    return PipelineContext(run_id="test_run")


@pytest.fixture
def mock_source():
    return HightouchIngestionSource(
        HightouchSourceConfig(
            api_config=HightouchAPIConfig(api_key="test"),
            env="PROD",
        ),
        PipelineContext(run_id="test"),
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_model_urn_on_hightouch_platform(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        platform_instance="prod-instance",
        include_sibling_relationships=False,
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    model = HightouchModel(
        id="model_123",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_model_urn(model, source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "hightouch" in urn_str
    assert "customer-model" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_model_urn_on_source_platform(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        platform_instance="prod-instance",
        include_sibling_relationships=True,
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="analytics",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    model = HightouchModel(
        id="model_123",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_model_urn(model, source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "hightouch" in urn_str
    assert "customer-model" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_upstream_table_urn(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="raw_data",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_upstream_table_urn("customers", source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "snowflake" in urn_str
    assert "raw_data.customers" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_upstream_table_urn_with_qualified_name(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="raw_data",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_upstream_table_urn("schema.customers", source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "snowflake" in urn_str
    assert "schema.customers" in urn_str
    assert "raw_data" not in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_destination_urn(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        destinations_to_platform_instance={
            "dest_1": PlatformDetail(
                platform="salesforce",
                platform_instance="salesforce-prod",
                env="PROD",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Salesforce Destination",
        slug="salesforce-dest",
        type="salesforce",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_destination_urn("Contact", destination)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "salesforce" in urn_str
    assert "Contact" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_destination_urn_with_database(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        destinations_to_platform_instance={
            "dest_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="analytics",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Snowflake Destination",
        slug="snowflake-dest",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_destination_urn("customers_enriched", destination)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "snowflake" in urn_str
    assert "analytics.customers_enriched" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_caching_source_details(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="raw_data",
            )
        },
    )

    mock_source_instance = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=mock_source_instance.config,
        get_platform_for_source=mock_source_instance._get_platform_for_source,
        get_platform_for_destination=mock_source_instance._get_platform_for_destination,
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    _ = urn_builder.make_upstream_table_urn("table1", source_connection)
    _ = urn_builder.make_upstream_table_urn("table2", source_connection)

    assert "source_1" in urn_builder._platform_detail_cache
    assert len(urn_builder._platform_detail_cache) == 1


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_fallback_to_source_type(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={},
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    source_connection = HightouchSourceConnection(
        id="source_unknown",
        name="Unknown Source",
        slug="unknown-source",
        type="bigquery",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_upstream_table_urn("table1", source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "bigquery" in urn_str
