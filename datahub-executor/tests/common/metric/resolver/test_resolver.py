import time
from unittest.mock import MagicMock

import pytest
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
)

from datahub_executor.common.assertion.types import AssertionDatabaseParams
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
    InsufficientDataException,
    SourceConnectionErrorException,
)
from datahub_executor.common.metric.resolver.resolver import MetricResolver
from datahub_executor.common.metric.types import (
    InvalidMetricResolverSourceTypeException,
    Metric,
    MetricResolverStrategy,
    MetricSourceType,
    UnsupportedMetricException,
)
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.types import (
    DatasetFilter,
    DatasetFilterType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    FieldMetricType,
    SchemaFieldSpec,
)


@pytest.fixture
def mock_connection_provider() -> MagicMock:
    """Create a mock connection provider."""
    mock = MagicMock(spec=DataHubIngestionSourceConnectionProvider)
    mock.graph = MagicMock()
    return mock


@pytest.fixture
def mock_source_provider() -> MagicMock:
    """Create a mock source provider."""
    return MagicMock(spec=SourceProvider)


@pytest.fixture
def mock_source() -> MagicMock:
    """Create a mock source."""
    mock = MagicMock()
    mock.get_row_count.return_value = 100
    mock.get_field_metric_value.return_value = 50.0
    return mock


@pytest.fixture
def resolver(
    mock_connection_provider: MagicMock, mock_source_provider: MagicMock
) -> MetricResolver:
    """Create a MetricResolver instance with mock dependencies."""
    return MetricResolver(
        connection_provider=mock_connection_provider,
        source_provider=mock_source_provider,
    )


@pytest.fixture
def entity_urn() -> str:
    """Return a sample entity URN."""
    return "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"


@pytest.fixture
def database_params() -> AssertionDatabaseParams:
    """Return sample database parameters."""
    return AssertionDatabaseParams(
        qualified_name="my_schema.my_database.my_table",
        table_name="my_table",
    )


@pytest.fixture
def filter_params() -> DatasetFilter:
    """Return sample filter parameters."""
    return DatasetFilter(
        type=DatasetFilterType.SQL,
        sql="WHERE created_at > '2023-01-01'",
    )


class TestMetricResolver:
    def test_get_metric_unsupported_metric(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
    ) -> None:
        """Test that an unsupported metric raises an UnsupportedMetricException."""
        with pytest.raises(UnsupportedMetricException) as excinfo:
            resolver.get_metric(
                entity_urn=entity_urn,
                metric_name="unsupported_metric",
                database_params=database_params,
                filter_params=None,
            )

        assert "Unsupported metric_name=unsupported_metric provided" in str(
            excinfo.value
        )

    def test_get_metric_row_count_from_datahub(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test fetching row count from DataHub profile."""
        # Setup mock dataset profile
        dataset_profile = DatasetProfileClass(
            timestampMillis=int(time.time() * 1000),
            rowCount=1000,
            columnCount=10,
            fieldProfiles=[],
        )
        mock_connection_provider.graph.get_latest_timeseries_value.return_value = (
            dataset_profile
        )

        # Call with strategy to use DataHub profile
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )
        metric = resolver.get_metric(
            entity_urn=entity_urn,
            metric_name="row_count",
            database_params=database_params,
            filter_params=None,
            strategy=strategy,
        )

        # Assert the result
        assert isinstance(metric, Metric)
        assert metric.value == 1000
        mock_connection_provider.graph.get_latest_timeseries_value.assert_called_once_with(
            entity_urn=entity_urn,
            aspect_type=DatasetProfileClass,
            filter_criteria_map={},
        )

    def test_get_metric_row_count_from_datahub_missing_profile(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test that missing profile raises InsufficientDataException."""
        # Setup mock to return None
        mock_connection_provider.graph.get_latest_timeseries_value.return_value = None

        # Call with strategy to use DataHub profile
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )

        with pytest.raises(InsufficientDataException) as excinfo:
            resolver.get_metric(
                entity_urn=entity_urn,
                metric_name="row_count",
                database_params=database_params,
                filter_params=None,
                strategy=strategy,
            )

        assert f"Unable to find a dataset profile or rowCount for {entity_urn}" in str(
            excinfo.value
        )

    def test_get_metric_row_count_from_dathaub_missing_row_count(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test that profile with missing rowCount raises InsufficientDataException."""
        # Setup mock to return profile without rowCount
        dataset_profile = DatasetProfileClass(
            timestampMillis=int(time.time() * 1000), columnCount=10, fieldProfiles=[]
        )
        mock_connection_provider.graph.get_latest_timeseries_value.return_value = (
            dataset_profile
        )

        # Call with strategy to use DataHub profile
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )

        with pytest.raises(InsufficientDataException) as excinfo:
            resolver.get_metric(
                entity_urn=entity_urn,
                metric_name="row_count",
                database_params=database_params,
                filter_params=None,
                strategy=strategy,
            )

        assert f"Unable to find a dataset profile or rowCount for {entity_urn}" in str(
            excinfo.value
        )

    def test_get_metric_row_count_from_information_schema(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        filter_params: DatasetFilter,
        mock_connection_provider: MagicMock,
        mock_source_provider: MagicMock,
        mock_source: MagicMock,
    ) -> None:
        """Test fetching row count from INFORMATION_SCHEMA."""
        # Setup mocks
        mock_connection_provider.get_connection.return_value = MagicMock()
        mock_source_provider.create_source_from_connection.return_value = mock_source

        # Call with strategy to use INFORMATION_SCHEMA
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.INFORMATION_SCHEMA
        )
        metric = resolver.get_metric(
            entity_urn=entity_urn,
            metric_name="row_count",
            database_params=database_params,
            filter_params=filter_params,
            strategy=strategy,
        )

        # Assert the result
        assert isinstance(metric, Metric)
        assert metric.value == 100
        mock_connection_provider.get_connection.assert_called_once_with(entity_urn)
        mock_source_provider.create_source_from_connection.assert_called_once()
        mock_source.get_row_count.assert_called_once_with(
            entity_urn,
            database_params,
            DatasetVolumeAssertionParameters(
                source_type=DatasetVolumeSourceType.INFORMATION_SCHEMA
            ),
            filter_params,
        )

    def test_get_metric_row_count_from_query(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        filter_params: DatasetFilter,
        mock_connection_provider: MagicMock,
        mock_source_provider: MagicMock,
        mock_source: MagicMock,
    ) -> None:
        """Test fetching row count using QUERY source type."""
        # Setup mocks
        mock_connection_provider.get_connection.return_value = MagicMock()
        mock_source_provider.create_source_from_connection.return_value = mock_source

        # Call with strategy to use QUERY
        strategy = MetricResolverStrategy(source_type=MetricSourceType.QUERY)
        metric = resolver.get_metric(
            entity_urn=entity_urn,
            metric_name="row_count",
            database_params=database_params,
            filter_params=filter_params,
            strategy=strategy,
        )

        # Assert the result
        assert isinstance(metric, Metric)
        assert metric.value == 100
        mock_connection_provider.get_connection.assert_called_once_with(entity_urn)
        mock_source_provider.create_source_from_connection.assert_called_once()
        mock_source.get_row_count.assert_called_once_with(
            entity_urn,
            database_params,
            DatasetVolumeAssertionParameters(source_type=DatasetVolumeSourceType.QUERY),
            filter_params,
        )

    def test_get_metric_row_count_invalid_source_type(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
    ) -> None:
        """Test that an invalid source type raises InvalidMetricResolverSourceTypeException."""
        # Use a source type not supported for row_count
        strategy = MetricResolverStrategy(source_type=MetricSourceType.AUDIT_LOG)

        with pytest.raises(InvalidMetricResolverSourceTypeException) as excinfo:
            resolver.get_metric(
                entity_urn=entity_urn,
                metric_name="row_count",
                database_params=database_params,
                filter_params=None,
                strategy=strategy,
            )

        assert (
            f"Unsupported metrics resolver source type {strategy.source_type}"
            in str(excinfo.value)
        )

    def test_get_metric_row_count_connection_error(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test that missing connection raises SourceConnectionErrorException."""
        # Setup mock to return None for connection
        mock_connection_provider.get_connection.return_value = None

        # Call with strategy to use INFORMATION_SCHEMA
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.INFORMATION_SCHEMA
        )

        with pytest.raises(SourceConnectionErrorException) as excinfo:
            resolver.get_metric(
                entity_urn=entity_urn,
                metric_name="row_count",
                database_params=database_params,
                filter_params=None,
                strategy=strategy,
            )

        assert (
            f"Unable to retrieve valid connection for Data Platform with urn={entity_urn}"
            in str(excinfo.value)
        )

    def test_get_field_metric_from_datahub_profile(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test fetching field metric from DataHub profile."""
        # Create a field
        field = SchemaFieldSpec(
            path="my_field",
            type="NUMBER",
            native_type="int",
            kind=None,
        )

        # Setup mock dataset profile with field profile
        field_profile = DatasetFieldProfileClass(
            fieldPath="my_field",
            uniqueCount=50,
            uniqueProportion=0.5,
            nullCount=10,
            nullProportion=0.1,
            min="0",
            max="100",
            mean="50.0",
            median="45.0",
            stdev="10.0",
        )

        dataset_profile = DatasetProfileClass(
            timestampMillis=int(time.time() * 1000),
            rowCount=1000,
            columnCount=10,
            fieldProfiles=[field_profile],
        )

        mock_connection_provider.graph.get_latest_timeseries_value.return_value = (
            dataset_profile
        )

        # Test different metrics
        metrics_to_test = [
            (FieldMetricType.UNIQUE_COUNT, 50.0),
            (FieldMetricType.UNIQUE_PERCENTAGE, 50.0),  # 0.5 * 100
            (FieldMetricType.NULL_COUNT, 10.0),
            (FieldMetricType.NULL_PERCENTAGE, 10.0),  # 0.1 * 100
            (FieldMetricType.MIN, 0.0),
            (FieldMetricType.MAX, 100.0),
            (FieldMetricType.MEAN, 50.0),
            (FieldMetricType.MEDIAN, 45.0),
            (FieldMetricType.STDDEV, 10.0),
        ]

        for metric_type, expected_value in metrics_to_test:
            strategy = MetricResolverStrategy(
                source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
            )
            metric = resolver.get_field_metric(
                entity_urn=entity_urn,
                field=field,
                metric=metric_type,
                database_params=AssertionDatabaseParams(
                    qualified_name="my_schema.my_database.my_table",
                    table_name="my_table",
                ),
                filter_params=None,
                high_watermark_field=None,
                previous_high_watermark=None,
                strategy=strategy,
            )

            # Assert the result
            assert isinstance(metric, Metric)
            assert metric.value == expected_value

    def test_get_field_metric_missing_dataset_profile(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test that missing dataset profile raises InsufficientDataException."""
        # Setup mock to return None
        mock_connection_provider.graph.get_latest_timeseries_value.return_value = None

        field = SchemaFieldSpec(
            path="my_field", type="NUMBER", native_type="INTEGER", kind=None
        )

        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )

        with pytest.raises(InsufficientDataException) as excinfo:
            resolver.get_field_metric(
                entity_urn=entity_urn,
                field=field,
                metric=FieldMetricType.UNIQUE_COUNT,
                database_params=AssertionDatabaseParams(
                    qualified_name="my_schema.my_database.my_table",
                    table_name="my_table",
                ),
                filter_params=None,
                high_watermark_field=None,
                previous_high_watermark=None,
                strategy=strategy,
            )

        assert f"Unable to find latest dataset profile for {entity_urn}" in str(
            excinfo.value
        )

    def test_get_field_metric_missing_field_profile(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test that missing field profile raises InsufficientDataException."""
        field = SchemaFieldSpec(
            path="non_existent_field", type="NUMBER", native_type="INTEGER", kind=None
        )

        # Setup dataset profile without the requested field
        field_profile = DatasetFieldProfileClass(
            fieldPath="other_field",  # Different field
            uniqueCount=50,
        )

        dataset_profile = DatasetProfileClass(
            timestampMillis=int(time.time() * 1000),
            rowCount=1000,
            fieldProfiles=[field_profile],
        )

        mock_connection_provider.graph.get_latest_timeseries_value.return_value = (
            dataset_profile
        )

        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )

        with pytest.raises(InsufficientDataException) as excinfo:
            resolver.get_field_metric(
                entity_urn=entity_urn,
                field=field,
                metric=FieldMetricType.UNIQUE_COUNT,
                database_params=AssertionDatabaseParams(
                    qualified_name="my_schema.my_database.my_table",
                    table_name="my_table",
                ),
                filter_params=None,
                high_watermark_field=None,
                previous_high_watermark=None,
                strategy=strategy,
            )

        assert (
            f"Unable to find dataset field profile for {entity_urn} {field.path}"
            in str(excinfo.value)
        )

    def test_get_field_metric_missing_metric_value(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test that missing metric value raises InsufficientDataException."""
        field = SchemaFieldSpec(
            path="my_field", type="NUMBER", native_type="INTEGER", kind=None
        )

        # Setup dataset profile with field profile but missing the requested metric
        field_profile = DatasetFieldProfileClass(
            fieldPath="my_field",
            # No uniqueCount value
            nullCount=10,
        )

        dataset_profile = DatasetProfileClass(
            timestampMillis=int(time.time() * 1000),
            rowCount=1000,
            fieldProfiles=[field_profile],
        )

        mock_connection_provider.graph.get_latest_timeseries_value.return_value = (
            dataset_profile
        )

        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )

        with pytest.raises(InsufficientDataException) as excinfo:
            resolver.get_field_metric(
                entity_urn=entity_urn,
                field=field,
                metric=FieldMetricType.UNIQUE_COUNT,  # Not in the profile
                database_params=AssertionDatabaseParams(
                    qualified_name="my_schema.my_database.my_table",
                    table_name="my_table",
                ),
                filter_params=None,
                high_watermark_field=None,
                previous_high_watermark=None,
                strategy=strategy,
            )

        assert (
            f"Unable to find dataset field profile data for for {entity_urn} {FieldMetricType.UNIQUE_COUNT.name}"
            in str(excinfo.value)
        )

    def test_get_field_metric_from_source(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        filter_params: DatasetFilter,
        mock_connection_provider: MagicMock,
        mock_source_provider: MagicMock,
        mock_source: MagicMock,
    ) -> None:
        """Test fetching field metric from source."""
        # Setup mocks
        mock_connection_provider.get_connection.return_value = MagicMock()
        mock_source_provider.create_source_from_connection.return_value = mock_source

        field = SchemaFieldSpec(
            path="my_field", type="NUMBER", native_type="INTEGER", kind=None
        )

        high_watermark_field = SchemaFieldSpec(
            path="updated_at", type="TIMESTAMP", native_type="TIMESTAMP", kind=None
        )

        strategy = MetricResolverStrategy(source_type=MetricSourceType.QUERY)
        metric = resolver.get_field_metric(
            entity_urn=entity_urn,
            field=field,
            metric=FieldMetricType.UNIQUE_COUNT,
            database_params=database_params,
            filter_params=filter_params,
            high_watermark_field=high_watermark_field,
            previous_high_watermark="2023-01-01T00:00:00Z",
            strategy=strategy,
        )

        # Assert the result
        assert isinstance(metric, Metric)
        assert metric.value == 50.0
        mock_connection_provider.get_connection.assert_called_once_with(entity_urn)
        mock_source_provider.create_source_from_connection.assert_called_once()
        mock_source.get_field_metric_value.assert_called_once_with(
            entity_urn,
            field,
            FieldMetricType.UNIQUE_COUNT,
            database_params,
            filter_params,
            "2023-01-01T00:00:00Z",
            high_watermark_field,
        )

    def test_get_field_metric_unsupported_metric(
        self, resolver: MetricResolver, entity_urn: str
    ) -> None:
        """Test that unsupported metric raises UnsupportedMetricException."""
        field = SchemaFieldSpec(
            path="my_field", type="NUMBER", native_type="INTEGER", kind=None
        )

        # Use NEGATIVE_COUNT which is not supported in DATAHUB_DATASET_PROFILE
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )

        with pytest.raises(UnsupportedMetricException) as excinfo:
            resolver.get_field_metric(
                entity_urn=entity_urn,
                field=field,
                metric=FieldMetricType.NEGATIVE_COUNT,
                database_params=AssertionDatabaseParams(
                    qualified_name="my_schema.my_database.my_table",
                    table_name="my_table",
                ),
                filter_params=None,
                high_watermark_field=None,
                previous_high_watermark=None,
                strategy=strategy,
            )

        assert f"Unsupported metric_name={FieldMetricType.NEGATIVE_COUNT.name}" in str(
            excinfo.value
        )

    def test_get_field_metric_invalid_source_type(
        self, resolver: MetricResolver, entity_urn: str
    ) -> None:
        """Test that invalid source type raises InvalidMetricResolverSourceTypeException."""
        field = SchemaFieldSpec(
            path="my_field", type="NUMBER", native_type="INTEGER", kind=None
        )

        # Use a source type not supported for field metrics
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.INFORMATION_SCHEMA
        )

        with pytest.raises(InvalidMetricResolverSourceTypeException) as excinfo:
            resolver.get_field_metric(
                entity_urn=entity_urn,
                field=field,
                metric=FieldMetricType.UNIQUE_COUNT,
                database_params=AssertionDatabaseParams(
                    qualified_name="my_schema.my_database.my_table",
                    table_name="my_table",
                ),
                filter_params=None,
                high_watermark_field=None,
                previous_high_watermark=None,
                strategy=strategy,
            )

        assert (
            f"Unsupported metrics resolver source type {strategy.source_type}"
            in str(excinfo.value)
        )

    def test_get_field_metric_connection_error(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test that missing connection raises SourceConnectionErrorException."""
        # Setup mock to return None for connection
        mock_connection_provider.get_connection.return_value = None

        field = SchemaFieldSpec(
            path="my_field", type="NUMBER", native_type="INTEGER", kind=None
        )

        strategy = MetricResolverStrategy(source_type=MetricSourceType.QUERY)

        with pytest.raises(SourceConnectionErrorException) as excinfo:
            resolver.get_field_metric(
                entity_urn=entity_urn,
                field=field,
                metric=FieldMetricType.UNIQUE_COUNT,
                database_params=AssertionDatabaseParams(
                    qualified_name="my_schema.my_database.my_table",
                    table_name="my_table",
                ),
                filter_params=None,
                high_watermark_field=None,
                previous_high_watermark=None,
                strategy=strategy,
            )

        assert (
            f"Unable to retrieve valid connection for Data Platform with urn={entity_urn}"
            in str(excinfo.value)
        )

    def test_dataset_profile_staleness_threshold(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        mock_connection_provider: MagicMock,
    ) -> None:
        """Test that stale dataset profiles are rejected based on the configured threshold."""
        # Import the config value for the staleness threshold
        # Calculate a timestamp that is just beyond the staleness threshold
        import time

        from datahub_executor.config import (
            METRIC_RESOLVER_DATAHUB_DATASET_PROFILE_MAX_AGE_HOURS,
        )

        now_ms = int(time.time() * 1000)
        hours_to_ms = 1000 * 60 * 60
        max_age_ms = hours_to_ms * METRIC_RESOLVER_DATAHUB_DATASET_PROFILE_MAX_AGE_HOURS
        stale_timestamp = now_ms - max_age_ms - 1000  # 1 second beyond the threshold

        # Create a stale dataset profile
        dataset_profile = DatasetProfileClass(
            timestampMillis=stale_timestamp,
            rowCount=1000,
            columnCount=10,
            fieldProfiles=[],
        )

        # Setup mock to return the stale profile
        mock_connection_provider.graph.get_latest_timeseries_value.return_value = (
            dataset_profile
        )

        # Call with strategy to use DataHub profile
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )

        # Test that stale profile raises InsufficientDataException
        with pytest.raises(InsufficientDataException) as excinfo:
            resolver.get_metric(
                entity_urn=entity_urn,
                metric_name="row_count",
                database_params=AssertionDatabaseParams(
                    qualified_name="my_schema.my_database.my_table",
                    table_name="my_table",
                ),
                filter_params=None,
                strategy=strategy,
            )

        # Verify the error message contains information about the staleness
        assert "too stale to use" in str(excinfo.value)
        assert (
            f"maximum age of {METRIC_RESOLVER_DATAHUB_DATASET_PROFILE_MAX_AGE_HOURS} hours"
            in str(excinfo.value)
        )

        # Now test with a fresh profile (just within the threshold)
        fresh_timestamp = now_ms - max_age_ms + 1000  # 1 second within the threshold
        fresh_profile = DatasetProfileClass(
            timestampMillis=fresh_timestamp,
            rowCount=1000,
            columnCount=10,
            fieldProfiles=[],
        )

        # Setup mock to return the fresh profile
        mock_connection_provider.graph.get_latest_timeseries_value.return_value = (
            fresh_profile
        )

        # This should not raise an exception
        metric = resolver.get_metric(
            entity_urn=entity_urn,
            metric_name="row_count",
            database_params=AssertionDatabaseParams(
                qualified_name="my_schema.my_database.my_table",
                table_name="my_table",
            ),
            filter_params=None,
            strategy=strategy,
        )

        # Verify the metric was returned correctly
        assert isinstance(metric, Metric)
        assert metric.value == 1000

    def test_default_strategy_behavior(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        filter_params: DatasetFilter,
        mock_connection_provider: MagicMock,
        mock_source_provider: MagicMock,
        mock_source: MagicMock,
    ) -> None:
        """Test default strategies when not provided."""
        # Setup mocks
        mock_connection_provider.get_connection.return_value = MagicMock()
        mock_source_provider.create_source_from_connection.return_value = mock_source

        # Test row_count default (INFORMATION_SCHEMA)
        metric = resolver.get_metric(
            entity_urn=entity_urn,
            metric_name="row_count",
            database_params=database_params,
            filter_params=filter_params,
            strategy=None,  # No strategy provided
        )

        assert isinstance(metric, Metric)
        assert metric.value == 100
        mock_source.get_row_count.assert_called_with(
            entity_urn,
            database_params,
            DatasetVolumeAssertionParameters(
                source_type=DatasetVolumeSourceType.INFORMATION_SCHEMA
            ),
            filter_params,
        )

        # Reset mocks
        mock_connection_provider.reset_mock()
        mock_source_provider.reset_mock()
        mock_source.reset_mock()

        # Setup mocks again
        mock_connection_provider.get_connection.return_value = MagicMock()
        mock_source_provider.create_source_from_connection.return_value = mock_source

        # Test field metric default (QUERY)
        field = SchemaFieldSpec(
            path="my_field",
            type="NUMBER",
            native_type="INTEGER",
            kind=None,
        )

        metric = resolver.get_field_metric(
            entity_urn=entity_urn,
            field=field,
            metric=FieldMetricType.UNIQUE_COUNT,
            database_params=database_params,
            filter_params=filter_params,
            high_watermark_field=None,
            previous_high_watermark=None,
            strategy=None,  # No strategy provided
        )

        assert isinstance(metric, Metric)
        assert metric.value == 50.0
        mock_source.get_field_metric_value.assert_called_once()

    def test_filter_params_dict_conversion(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        filter_params: DatasetFilter,
        mock_connection_provider: MagicMock,
        mock_source_provider: MagicMock,
        mock_source: MagicMock,
    ) -> None:
        """Test that filter_params are correctly converted to __dict__ when passed to source methods."""
        # Setup mocks
        mock_connection_provider.get_connection.return_value = MagicMock()
        mock_source_provider.create_source_from_connection.return_value = mock_source

        # Call with strategy to use INFORMATION_SCHEMA
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.INFORMATION_SCHEMA
        )
        metric = resolver.get_metric(
            entity_urn=entity_urn,
            metric_name="row_count",
            database_params=database_params,
            filter_params=filter_params,
            strategy=strategy,
        )

        # Assert the result
        assert isinstance(metric, Metric)
        assert metric.value == 100

        # Verify that filter_params.__dict__ was passed to get_row_count, not the raw filter_params object
        mock_source.get_row_count.assert_called_once()
        call_args = mock_source.get_row_count.call_args[0]

        # The filter_params should be the 4th argument (after entity_urn, database_params, DatasetVolumeAssertionParameters)
        actual_filter_params = call_args[3]
        expected_filter_dict = filter_params.__dict__

        # Verify that the actual filter_params passed is the dictionary representation, not the model object
        assert actual_filter_params == expected_filter_dict
        assert isinstance(actual_filter_params, dict)
        assert actual_filter_params["type"] == filter_params.type
        assert actual_filter_params["sql"] == filter_params.sql

    def test_filter_params_none_handling(
        self,
        resolver: MetricResolver,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        mock_connection_provider: MagicMock,
        mock_source_provider: MagicMock,
        mock_source: MagicMock,
    ) -> None:
        """Test that None filter_params are handled correctly."""
        # Setup mocks
        mock_connection_provider.get_connection.return_value = MagicMock()
        mock_source_provider.create_source_from_connection.return_value = mock_source

        # Call with strategy to use INFORMATION_SCHEMA and None filter_params
        strategy = MetricResolverStrategy(
            source_type=MetricSourceType.INFORMATION_SCHEMA
        )
        metric = resolver.get_metric(
            entity_urn=entity_urn,
            metric_name="row_count",
            database_params=database_params,
            filter_params=None,  # Explicitly pass None
            strategy=strategy,
        )

        # Assert the result
        assert isinstance(metric, Metric)
        assert metric.value == 100

        # Verify that None was passed to get_row_count for filter_params
        mock_source.get_row_count.assert_called_once()
        call_args = mock_source.get_row_count.call_args[0]

        # The filter_params should be the 4th argument
        actual_filter_params = call_args[3]

        # Verify that None was passed correctly
        assert actual_filter_params is None
