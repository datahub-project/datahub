import pytest

from datahub_executor.common.assertion.engine.evaluator.utils.volume import (
    convert_volume_parameters_to_metric_resolver_strategy,
)
from datahub_executor.common.metric.types import (
    InvalidMetricResolverSourceTypeException,
    MetricSourceType,
)
from datahub_executor.common.types import (
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionTimeBucketingStrategy,
    CalendarInterval,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    TimeWindowSize,
)


def test_bucketing_requires_query_source_type() -> None:
    parameters = AssertionEvaluationParameters(
        type=AssertionEvaluationParametersType.DATASET_VOLUME,
        dataset_volume_parameters=DatasetVolumeAssertionParameters(
            source_type=DatasetVolumeSourceType.INFORMATION_SCHEMA,
            time_bucketing_strategy=AssertionTimeBucketingStrategy(
                timestamp_field_path="event_ts",
                bucket_interval=TimeWindowSize(unit=CalendarInterval.DAY, multiple=1),
                timezone="UTC",
            ),
        ),
    )

    with pytest.raises(InvalidMetricResolverSourceTypeException) as exc:
        convert_volume_parameters_to_metric_resolver_strategy(parameters)

    assert "sourceType=QUERY" in str(exc.value)


def test_query_source_allows_bucketing_strategy() -> None:
    parameters = AssertionEvaluationParameters(
        type=AssertionEvaluationParametersType.DATASET_VOLUME,
        dataset_volume_parameters=DatasetVolumeAssertionParameters(
            source_type=DatasetVolumeSourceType.QUERY,
            time_bucketing_strategy=AssertionTimeBucketingStrategy(
                timestamp_field_path="event_ts",
                bucket_interval=TimeWindowSize(unit=CalendarInterval.DAY, multiple=1),
                timezone="UTC",
            ),
        ),
    )

    strategy = convert_volume_parameters_to_metric_resolver_strategy(parameters)
    assert strategy.source_type == MetricSourceType.QUERY
    assert strategy.bucketing_timestamp_field_path == "event_ts"
    assert strategy.bucketing_interval_unit == "DAY"
