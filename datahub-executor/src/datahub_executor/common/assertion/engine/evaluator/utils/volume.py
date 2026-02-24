from typing import Dict, Optional, assert_never

from datahub_executor.common.metric.types import (
    InvalidMetricResolverSourceTypeException,
    MetricResolverStrategy,
    MetricSourceType,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationParameters,
    DatasetVolumeSourceType,
)


def get_filter_parameters(assertion: Assertion) -> Optional[Dict]:
    """
    Extracts filter information from Volume Assertion and returns it as a dictionary
    """
    volume_assertion = assertion.volume_assertion
    if volume_assertion is not None and volume_assertion.filter is not None:
        return volume_assertion.filter.model_dump()
    return None


def convert_volume_parameters_to_metric_resolver_strategy(
    parameters: AssertionEvaluationParameters,
) -> MetricResolverStrategy:
    dataset_volume_parameters = parameters.dataset_volume_parameters

    if dataset_volume_parameters:
        metric_source_type = _to_metric_source_type(
            dataset_volume_parameters.source_type
        )

        time_bucketing_strategy = dataset_volume_parameters.time_bucketing_strategy
        bucketing_timestamp_field_path = None
        bucketing_interval_unit = None
        bucketing_interval_multiple = None
        bucketing_timezone = None
        late_arrival_grace_period_unit = None
        late_arrival_grace_period_multiple = None
        if time_bucketing_strategy is not None:
            if dataset_volume_parameters.source_type != DatasetVolumeSourceType.QUERY:
                raise InvalidMetricResolverSourceTypeException(
                    message=(
                        "Time bucketing strategy is only supported for volume "
                        "sourceType=QUERY."
                    ),
                    source_type=str(dataset_volume_parameters.source_type),
                )
            bucketing_timestamp_field_path = (
                time_bucketing_strategy.timestamp_field_path
            )
            bucketing_interval_unit = time_bucketing_strategy.bucket_interval.unit.value
            bucketing_interval_multiple = (
                time_bucketing_strategy.bucket_interval.multiple
            )
            bucketing_timezone = time_bucketing_strategy.timezone
            late_arrival_grace_period_unit = (
                time_bucketing_strategy.late_arrival_grace_period.unit.value
                if time_bucketing_strategy.late_arrival_grace_period
                else None
            )
            late_arrival_grace_period_multiple = (
                time_bucketing_strategy.late_arrival_grace_period.multiple
                if time_bucketing_strategy.late_arrival_grace_period
                else None
            )
        return MetricResolverStrategy(
            source_type=metric_source_type,
            bucketing_timestamp_field_path=bucketing_timestamp_field_path,
            bucketing_interval_unit=bucketing_interval_unit,
            bucketing_interval_multiple=bucketing_interval_multiple,
            bucketing_timezone=bucketing_timezone,
            late_arrival_grace_period_unit=late_arrival_grace_period_unit,
            late_arrival_grace_period_multiple=late_arrival_grace_period_multiple,
        )

    raise InvalidMetricResolverSourceTypeException(
        message=(
            "Failed to bind from DatasetVolumeMetricParameters. Invalid parameters "
            f"{dataset_volume_parameters} provided"
        ),
        source_type=(
            str(dataset_volume_parameters.source_type)
            if dataset_volume_parameters
            else "None"
        ),
    )


def _to_metric_source_type(source_type: DatasetVolumeSourceType) -> MetricSourceType:
    if source_type == DatasetVolumeSourceType.INFORMATION_SCHEMA:
        return MetricSourceType.INFORMATION_SCHEMA
    if source_type == DatasetVolumeSourceType.QUERY:
        return MetricSourceType.QUERY
    if source_type == DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE:
        return MetricSourceType.DATAHUB_DATASET_PROFILE
    # Keep this exhaustive: adding a new DatasetVolumeSourceType should require
    # updating this mapper and be caught by static type checking.
    assert_never(source_type)
