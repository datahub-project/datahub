import logging
import time
from typing import List, Optional, Union

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
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    FieldMetricType,
    SchemaFieldSpec,
)
from datahub_executor.config import (
    METRIC_RESOLVER_DATAHUB_DATASET_PROFILE_MAX_AGE_HOURS,
)

logger = logging.getLogger(__name__)

HOURS_TO_MS = 1000 * 60 * 60
DATASET_PROFILE_MAX_AGE_MS = (
    HOURS_TO_MS * METRIC_RESOLVER_DATAHUB_DATASET_PROFILE_MAX_AGE_HOURS
)


class MetricResolver:
    """
    A Metric Resolver resolves specific metrics for datasets and fields (today).
    It can do so using a MetricResolverStrategy, which dictates where the metric is fetched from.

    Today this supports resolving row counts and field aggregation metrics from a source
    system or from DataHub itself (e.g. using DatasetProfile aspects.)
    """

    def __init__(
        self,
        connection_provider: DataHubIngestionSourceConnectionProvider,
        source_provider: SourceProvider,
    ) -> None:
        self.connection_provider = connection_provider
        self.source_provider = source_provider

    def get_metric(
        self,
        entity_urn: str,
        metric_name: str,
        database_params: AssertionDatabaseParams,
        filter_params: Optional[DatasetFilter],
        strategy: Optional[MetricResolverStrategy] = None,
    ) -> Metric:
        """
        Resolve a specific metric.
        """
        logger.debug(
            "MetricResolver: get_metric called for entity_urn=%s, metric_name=%s, "
            "database_params=%s, filter_params=%s, strategy=%s",
            entity_urn,
            metric_name,
            database_params,
            filter_params,
            strategy,
        )

        if metric_name == "row_count":
            return self._fetch_row_count_metric(
                entity_urn=entity_urn,
                database_params=database_params,
                filter_params=filter_params,
                strategy=strategy,
            )

        raise UnsupportedMetricException(
            f"Unsupported metric_name={metric_name} provided"
        )

    def get_field_metric(
        self,
        entity_urn: str,
        field: SchemaFieldSpec,
        metric: FieldMetricType,
        database_params: AssertionDatabaseParams,
        filter_params: Optional[DatasetFilter],
        high_watermark_field: Optional[SchemaFieldSpec],
        previous_high_watermark: Optional[str],
        strategy: Optional[MetricResolverStrategy] = None,
    ) -> Metric:
        """
        Resolve an aggregation metric for a specific field
        """
        # If no strategy, default to QUERY, since these are generally aggreagtions.
        source_type = strategy.source_type if strategy else MetricSourceType.QUERY

        logger.debug(
            "MetricResolver: resolve_row_count_metric with source_type=%s for entity_urn=%s",
            source_type,
            entity_urn,
        )

        # first, check whether we should be fetching this metric from datahub's backend.
        # if so, don't try to create a source.
        if source_type == MetricSourceType.DATAHUB_DATASET_PROFILE:
            return self._fetch_field_aggregation_metric_from_datahub_profile(
                entity_urn, field, metric
            )

        elif source_type == MetricSourceType.QUERY:
            # Use a custom query approach
            return self._fetch_field_aggregation_metric_from_source(
                entity_urn,
                field,
                metric,
                database_params,
                filter_params,
                high_watermark_field,
                previous_high_watermark,
            )

        # Add more handling if you support other MetricSourceType values
        else:
            msg = (
                f"Unsupported metrics resolver source type {source_type} "
                f"provided when fetching row count for entity {entity_urn}"
            )
            logger.error(msg)
            raise InvalidMetricResolverSourceTypeException(msg)

    def _fetch_field_aggregation_metric_from_datahub_profile(
        self, entity_urn: str, field: SchemaFieldSpec, metric: FieldMetricType
    ) -> Metric:
        if metric not in [
            FieldMetricType.UNIQUE_COUNT,
            FieldMetricType.UNIQUE_PERCENTAGE,
            FieldMetricType.NULL_COUNT,
            FieldMetricType.NULL_PERCENTAGE,
            FieldMetricType.MIN,
            FieldMetricType.MAX,
            FieldMetricType.MEAN,
            FieldMetricType.MEDIAN,
            FieldMetricType.STDDEV,
        ]:
            raise UnsupportedMetricException(
                f"Unsupported metric_name={metric.name} provided when fetching from DataHub dataset profiles"
            )

        dataset_profile: Optional[DatasetProfileClass] = (
            self.connection_provider.graph.get_latest_timeseries_value(
                entity_urn=entity_urn,
                aspect_type=DatasetProfileClass,
                filter_criteria_map={},
            )
        )

        if dataset_profile is None or dataset_profile.fieldProfiles is None:
            raise InsufficientDataException(
                message=f"Unable to find latest dataset profile for {entity_urn}"
            )

        dataset_field_profile: Optional[DatasetFieldProfileClass] = (
            self._get_dataset_field_profile(
                dataset_profile.fieldProfiles,
                field.path,
            )
        )
        if dataset_field_profile is None:
            raise InsufficientDataException(
                message=f"Unable to find dataset field profile for {entity_urn} {field.path}"
            )

        now_ms = int(time.time() * 1000)
        oldest_valid_timestamp = now_ms - DATASET_PROFILE_MAX_AGE_MS

        if dataset_profile.timestampMillis < oldest_valid_timestamp:
            msg = f"Dataset profile for {entity_urn} {field.path} is too stale to use: timestamp={dataset_profile.timestampMillis} is older than the maximum age of {METRIC_RESOLVER_DATAHUB_DATASET_PROFILE_MAX_AGE_HOURS} hours."
            logger.warning(msg)
            raise InsufficientDataException(message=msg)

        metric_value = self._get_field_metric_value_from_dataset_field_profile(
            dataset_field_profile,
            metric,
        )

        if metric_value is None:
            raise InsufficientDataException(
                message=f"Unable to find dataset field profile data for for {entity_urn} {metric.name}"
            )

        return Metric(value=metric_value, timestamp_ms=now_ms)

    def _fetch_field_aggregation_metric_from_source(
        self,
        entity_urn: str,
        field: SchemaFieldSpec,
        metric: FieldMetricType,
        database_params: AssertionDatabaseParams,
        filter_params: Optional[DatasetFilter],
        high_watermark_field: Optional[SchemaFieldSpec],
        previous_high_watermark: Optional[str],
    ) -> Metric:
        # Step 1: Retrieve Connection
        connection = self.connection_provider.get_connection(entity_urn)
        if connection is None:
            msg = f"Unable to retrieve valid connection for Data Platform with urn={entity_urn}"
            logger.error(msg)
            raise SourceConnectionErrorException(message=msg, connection_urn=entity_urn)

        # Step 2: Create a source from the connection
        source = self.source_provider.create_source_from_connection(connection)
        logger.debug("MetricResolver: Created source for entity_urn=%s", entity_urn)

        metric_value = source.get_field_metric_value(
            entity_urn,
            field,
            metric,
            database_params,
            filter_params,
            previous_high_watermark,
            high_watermark_field,
        )

        logger.debug(
            "Fetched aggregated field metric=%d from source for entity_urn=%s",
            metric_value,
            entity_urn,
        )
        now_time = int(time.time() * 1000)
        return Metric(value=metric_value, timestamp_ms=now_time)

    def _fetch_row_count_metric(
        self,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        filter_params: Optional[DatasetFilter],
        strategy: Optional[MetricResolverStrategy],
    ) -> Metric:
        """
        Resolve a row_count metric for the given entity_urn, based on the provided
        strategy's source type. Defaults to INFORMATION_SCHEMA if no strategy given.
        """
        # If no strategy, default to INFORMATION_SCHEMA
        source_type = (
            strategy.source_type if strategy else MetricSourceType.INFORMATION_SCHEMA
        )

        logger.debug(
            "MetricResolver: resolve_row_count_metric with source_type=%s for entity_urn=%s",
            source_type,
            entity_urn,
        )

        # first, check whether we should be fetching this metric from datahub's backend.
        # if so, don't try to create a source.
        if source_type == MetricSourceType.DATAHUB_DATASET_PROFILE:
            return self._fetch_row_count_metric_from_datahub(entity_urn)

        elif source_type == MetricSourceType.INFORMATION_SCHEMA:
            # Use the "INFORMATION_SCHEMA" approach
            return self._fetch_row_count_from_source(
                entity_urn,
                database_params,
                filter_params,
                DatasetVolumeSourceType.INFORMATION_SCHEMA,
            )

        elif source_type == MetricSourceType.QUERY:
            # Use a custom query approach
            return self._fetch_row_count_from_source(
                entity_urn,
                database_params,
                filter_params,
                DatasetVolumeSourceType.QUERY,
            )

        # Add more handling if you support other MetricSourceType values
        else:
            msg = (
                f"Unsupported metrics resolver source type {source_type} "
                f"provided when fetching row count for entity {entity_urn}"
            )
            logger.error(msg)
            raise InvalidMetricResolverSourceTypeException(msg)

    def _fetch_row_count_from_source(
        self,
        entity_urn: str,
        database_params: AssertionDatabaseParams,
        filter_params: Optional[DatasetFilter],
        volume_source_type: DatasetVolumeSourceType,  # TODO: Change this to be something else once the source is improved.
    ) -> Metric:
        """
        Helper method that uses source.get_row_count(...) to retrieve a row count metric.
        """
        logger.debug(
            "_fetch_row_count_from_source called with volume_source_type=%s, entity_urn=%s, "
            "database_params=%s, filter_params=%s",
            volume_source_type,
            entity_urn,
            database_params,
            filter_params,
        )

        # Step 1: Retrieve Connection
        connection = self.connection_provider.get_connection(entity_urn)
        if connection is None:
            msg = f"Unable to retrieve valid connection for Data Platform with urn={entity_urn}"
            logger.error(msg)
            raise SourceConnectionErrorException(message=msg, connection_urn=entity_urn)

        # Step 2: Create a source from the connection
        source = self.source_provider.create_source_from_connection(connection)
        logger.debug("MetricResolver: Created source for entity_urn=%s", entity_urn)

        row_count = source.get_row_count(
            entity_urn,
            database_params,  # type: ignore
            DatasetVolumeAssertionParameters(source_type=volume_source_type),
            filter_params,  # type: ignore
        )
        logger.info(
            "Fetched row_count=%d from source (volume_source_type=%s) for entity_urn=%s",
            row_count,
            volume_source_type,
            entity_urn,
        )
        now_time = int(time.time() * 1000)
        return Metric(value=row_count, timestamp_ms=now_time)

    def _fetch_row_count_metric_from_datahub(self, entity_urn: str) -> Metric:
        """
        Fetch the row_count from the DataHub dataset profile. Raises an exception if
        the dataset profile or rowCount is not available.
        """
        logger.debug(
            "resolve_datahub_row_count_metric called for entity_urn=%s",
            entity_urn,
        )

        if not isinstance(
            self.connection_provider, DataHubIngestionSourceConnectionProvider
        ):
            msg = (
                "connection_provider is not a DataHubIngestionSourceConnectionProvider; "
                "cannot retrieve DataHub dataset profile."
            )
            logger.error(msg)
            raise SourceConnectionErrorException(message=msg, connection_urn=entity_urn)

        dataset_profile: Optional[DatasetProfileClass] = (
            self.connection_provider.graph.get_latest_timeseries_value(
                entity_urn=entity_urn,
                aspect_type=DatasetProfileClass,
                filter_criteria_map={},
            )
        )

        if dataset_profile is None or dataset_profile.rowCount is None:
            msg = f"Unable to find a dataset profile or rowCount for {entity_urn}"
            logger.warning(msg)
            raise InsufficientDataException(message=msg)

        now_ms = int(time.time() * 1000)
        oldest_valid_timestamp = now_ms - DATASET_PROFILE_MAX_AGE_MS

        if dataset_profile.timestampMillis < oldest_valid_timestamp:
            msg = f"Dataset profile for {entity_urn} is too stale to use: timestamp={dataset_profile.timestampMillis} is older than the maximum age of {METRIC_RESOLVER_DATAHUB_DATASET_PROFILE_MAX_AGE_HOURS} hours."
            logger.warning(msg)
            raise InsufficientDataException(message=msg)

        row_count = dataset_profile.rowCount
        logger.info(
            "Retrieved rowCount=%d from dataset profile for entity_urn=%s",
            row_count,
            entity_urn,
        )

        return Metric(value=row_count, timestamp_ms=now_ms)

    def _get_dataset_field_profile(
        self,
        field_profiles: List[DatasetFieldProfileClass],
        field_path: str,
    ) -> Optional[DatasetFieldProfileClass]:
        for field_profile in field_profiles:
            if field_path == field_profile.fieldPath:
                return field_profile
        return None

    def _get_field_metric_value_from_dataset_field_profile(
        self,
        dataset_field_profile: DatasetFieldProfileClass,
        metric: FieldMetricType,
    ) -> Union[None, float]:
        metric_value: Union[None, str, int, float] = None

        if metric == FieldMetricType.UNIQUE_COUNT:
            metric_value = dataset_field_profile.uniqueCount
        if metric == FieldMetricType.UNIQUE_PERCENTAGE:
            metric_value = (
                dataset_field_profile.uniqueProportion * 100.0
                if dataset_field_profile.uniqueProportion is not None
                else None
            )
        if metric == FieldMetricType.NULL_COUNT:
            metric_value = dataset_field_profile.nullCount
        if metric == FieldMetricType.NULL_PERCENTAGE:
            metric_value = (
                dataset_field_profile.nullProportion * 100.0
                if dataset_field_profile.nullProportion is not None
                else None
            )
        if metric == FieldMetricType.MIN:
            metric_value = dataset_field_profile.min
        if metric == FieldMetricType.MAX:
            metric_value = dataset_field_profile.max
        if metric == FieldMetricType.MEAN:
            metric_value = dataset_field_profile.mean
        if metric == FieldMetricType.MEDIAN:
            metric_value = dataset_field_profile.median
        if metric == FieldMetricType.STDDEV:
            metric_value = dataset_field_profile.stdev

        if metric_value is not None:
            return float(metric_value)
        return None
