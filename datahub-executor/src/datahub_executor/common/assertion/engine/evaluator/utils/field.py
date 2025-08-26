from datahub_executor.common.metric.types import (
    MetricResolverStrategy,
    MetricSourceType,
)
from datahub_executor.common.types import (
    DatasetFieldAssertionParameters,
    DatasetFieldSourceType,
)


def convert_field_parameters_to_metric_resolver_strategy(
    parameters: DatasetFieldAssertionParameters,
) -> MetricResolverStrategy:
    if parameters.source_type == DatasetFieldSourceType.ALL_ROWS_QUERY:
        return MetricResolverStrategy(source_type=MetricSourceType.QUERY)
    elif parameters.source_type == DatasetFieldSourceType.CHANGED_ROWS_QUERY:
        return MetricResolverStrategy(source_type=MetricSourceType.QUERY)
    elif parameters.source_type == DatasetFieldSourceType.DATAHUB_DATASET_PROFILE:
        return MetricResolverStrategy(
            source_type=MetricSourceType.DATAHUB_DATASET_PROFILE
        )
    else:
        raise Exception(
            f"Unsupported field source type {parameters.source_type} found - No matching Metric Resolver Strategy!"
        )
