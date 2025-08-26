from typing import Dict, Optional

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
        return volume_assertion.filter.__dict__
    return None


def convert_volume_parameters_to_metric_resolver_strategy(
    parameters: AssertionEvaluationParameters,
) -> MetricResolverStrategy:
    dataset_volume_parameters = parameters.dataset_volume_parameters

    if dataset_volume_parameters:
        if (
            dataset_volume_parameters.source_type
            == DatasetVolumeSourceType.INFORMATION_SCHEMA
        ):
            return MetricResolverStrategy(
                source_type=MetricSourceType.INFORMATION_SCHEMA
            )
        elif dataset_volume_parameters.source_type == DatasetVolumeSourceType.QUERY:
            return MetricResolverStrategy(source_type=MetricSourceType.QUERY)
        elif (
            dataset_volume_parameters.source_type
            == DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE
        ):
            return MetricResolverStrategy(
                source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
            )

    raise InvalidMetricResolverSourceTypeException(
        f"Failed to bind from DatasetVolumeMetricParameters. Invalid parameters {dataset_volume_parameters} provided"
    )
