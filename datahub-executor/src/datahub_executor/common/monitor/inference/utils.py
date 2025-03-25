import logging

from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    EmbeddedAssertionClass,
    TimeWindowClass,
    TimeWindowSizeClass,
)

from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
)

logger = logging.getLogger(__name__)


def build_std_parameters(boundary: MetricBoundary) -> AssertionStdParametersClass:
    """
    Build standard parameters for a boundary.
    """
    return AssertionStdParametersClass(
        minValue=AssertionStdParameterClass(
            type="NUMBER", value=str(boundary.lower_bound.value)
        ),
        maxValue=AssertionStdParameterClass(
            type="NUMBER", value=str(boundary.upper_bound.value)
        ),
    )


def create_embedded_assertion(
    assertion_info: AssertionInfoClass,
    boundary: MetricBoundary,
    window_size_seconds: int,
) -> EmbeddedAssertionClass:
    """
    Create an embedded assertion for a time window.
    """
    return EmbeddedAssertionClass(
        assertion=assertion_info,
        evaluationTimeWindow=TimeWindowClass(
            startTimeMillis=boundary.start_time_ms,
            length=TimeWindowSizeClass(
                unit="SECOND",
                multiple=window_size_seconds,
            ),
        ),
    )


def create_inference_source() -> AssertionSourceClass:
    """
    Create an assertion source indicating it was inferred.
    """
    return AssertionSourceClass(type=AssertionSourceTypeClass.INFERRED)
