import logging
from typing import List

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

from datahub_executor.common.metric.types import Metric, Operation
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
)
from datahub_executor.common.types import Anomaly

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


def is_metric_anomaly(metric: Metric, anomalies: List[Anomaly]) -> bool:
    """
    Simply checks whether the metric is in the set of anomalies reported.
    """
    for anomaly in anomalies:
        if anomaly.metric:
            anomaly_metric = anomaly.metric
            # If the anomaly is the same as the metric, filter it out!
            if (
                anomaly_metric.timestamp_ms == metric.timestamp_ms
                and anomaly_metric.value == metric.value
            ):
                return True
    return False


def annotate_operations_with_anomalies(
    operations: List[Operation], anomalies: List[Anomaly]
) -> List[Operation]:
    """
    This method annotates operations that come directly AFTER an anomaly event was recorded.

    When an anomaly is recorded, it means that the table did NOT updated when it should have -
    it was missing an operation.

    Thus, we need to mark the "closing operation" or the next subsequent operation after
    an anomaly as anomalous, indicating that it was "too late". Later on, we used this
    to filter out anomalies delta's of time between subsequent updates so that they are
    not included as "normal" when predicting the normal update interval.

    Example:
        If O is operation, A is anomaly:

        O1 A1 O2 -> O2 will be marked as an anomaly.
        O1 O2 A1 O3 -> O3 will be marked as an anomaly.
        Q1 A1 Q2 A2 O3 -> O2 and O3 will be marked as anomalies.
    """

    # First, make sure both operations and anomalies are sorted in ASC order (default)
    operations.sort(key=lambda op: op.timestamp_ms)
    anomalies.sort(key=lambda an: an.timestamp_ms)

    operation_idx = 0
    anomaly_idx = 0

    while operation_idx < len(operations) and anomaly_idx < len(anomalies):
        operation = operations[operation_idx]
        anomaly = anomalies[anomaly_idx]

        if anomaly.timestamp_ms <= operation.timestamp_ms:
            # We've found an operation that needs to be marked as an anomaly.
            # Mutate the operation in place.
            operation.is_anomaly = True
            anomaly_idx = anomaly_idx + 1
        else:
            # Current anomaly timestamp is higher, let's increment the operations.
            operation_idx = operation_idx + 1

    # That't it, we've mutated the operations in place.
    return operations
