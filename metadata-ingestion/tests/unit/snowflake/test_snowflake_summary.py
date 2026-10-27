from datetime import datetime, timezone
from unittest import mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeIdentifierConfig,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_summary import (
    SnowflakeSummaryConfig,
    SnowflakeSummarySource,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)


@mock.patch("snowflake.connector.connect")
def test_snowflake_summary_source_initialization(mock_sf_connect):
    # Mock the connection object and its query method
    mock_conn = mock.MagicMock()
    mock_conn.query.return_value = []
    mock_sf_connect.return_value = mock_conn

    # Create a basic config
    config = SnowflakeSummaryConfig(
        account_id="test_account",
        username="test_user",
        password="test_password",
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    # Create a mock context
    ctx = PipelineContext(run_id="test")

    # Create the source
    source = SnowflakeSummarySource(ctx, config)

    # Get workunits to trigger initialization
    list(source.get_workunits_internal())

    # Verify that SnowflakeSchemaGenerator was initialized with all required parameters
    report = source.get_report()
    assert isinstance(report, source.report.__class__)
    assert hasattr(report, "schema_counters")
    assert hasattr(report, "object_counters")
    assert hasattr(report, "num_snowflake_queries")
    assert hasattr(report, "num_snowflake_mutations")


@mock.patch("snowflake.connector.connect")
def test_snowflake_summary_source_missing_filters(mock_sf_connect):
    # Mock the connection object and its query method
    mock_conn = mock.MagicMock()
    mock_conn.query.return_value = []
    mock_sf_connect.return_value = mock_conn

    # Create a basic config
    config = SnowflakeSummaryConfig(
        account_id="test_account",
        username="test_user",
        password="test_password",
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    # Create a mock context
    ctx = PipelineContext(run_id="test")

    # Create the source
    source = SnowflakeSummarySource(ctx, config)

    # Get workunits to trigger initialization
    list(source.get_workunits_internal())

    # Verify that SnowflakeSchemaGenerator was initialized with filters
    report = source.get_report()
    assert isinstance(report, source.report.__class__)
    assert hasattr(report, "filtered")  # This is added by the SnowflakeFilter


def test_schema_generator_disables_semantic_model_entities_for_summary_config():
    """SnowflakeSchemaGenerator is constructed with SnowflakeSummaryConfig (which has
    no `semantic_views` field) as well as SnowflakeV2Config. The isinstance guard on
    `config` (replacing a previous nested-getattr guard) must resolve
    emit_semantic_model_entities to False for a SnowflakeSummaryConfig without
    raising AttributeError."""
    config = SnowflakeSummaryConfig(
        account_id="test_account",
        username="test_user",
        password="test_password",
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    report = SnowflakeV2Report()

    gen = SnowflakeSchemaGenerator(
        config=config,  # type: ignore
        report=report,
        connection=mock.MagicMock(),
        filters=SnowflakeFilter(filter_config=config, structured_reporter=report),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=SnowflakeIdentifierConfig(), structured_reporter=report
        ),
        domain_registry=None,
        profiler=None,
        aggregator=None,
        snowsight_url_builder=None,
    )

    assert gen.data_dictionary._emit_semantic_model_entities is False
