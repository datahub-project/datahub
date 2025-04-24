import logging
from typing import List, Optional

from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    CalendarIntervalClass,
    EmbeddedAssertionClass,
    FixedIntervalScheduleClass,
    TimeWindowClass,
    TimeWindowSizeClass,
)

from datahub_executor.common.metric.types import Metric, Operation
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
)
from datahub_executor.common.monitor.inference.types import Event
from datahub_executor.common.types import (
    Anomaly,
    AssertionMonitorMetricsCubeBootstrapState,
    AssertionMonitorMetricsCubeBootstrapStatus,
    CronSchedule,
    Monitor,
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


def get_event_timespan_seconds(events: List[Event]) -> int:
    """
    Returns the difference (in seconds) between the earliest and latest event timestamps.
    """
    if not events:
        return 0

    sorted_events = sorted(
        events, key=lambda x: x.timestamp_ms
    )  # Preserve original order
    return (
        sorted_events[-1].timestamp_ms - sorted_events[0].timestamp_ms
    ) // 1000  # Convert to seconds


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


def convert_interval_to_minutes(
    unit: CalendarIntervalClass | str, multiple: int
) -> float:
    """Helper method to convert a time unit and multiple to minutes"""
    # Convert the unit and multiple to minutes
    if unit == CalendarIntervalClass.SECOND:
        return multiple / 60
    elif unit == CalendarIntervalClass.MINUTE:
        return multiple
    elif unit == CalendarIntervalClass.HOUR:
        return multiple * 60
    elif unit == CalendarIntervalClass.DAY:
        return multiple * 24 * 60
    elif unit == CalendarIntervalClass.WEEK:
        return multiple * 7 * 24 * 60
    elif unit == CalendarIntervalClass.MONTH:
        # Approximation: 30 days in a month
        return multiple * 30 * 24 * 60
    elif unit == CalendarIntervalClass.QUARTER:
        # Approximation: 91 days in a quarter (365/4)
        return multiple * 91 * 24 * 60
    elif unit == CalendarIntervalClass.YEAR:
        # Approximation: 365 days in a year
        return multiple * 365 * 24 * 60
    else:
        raise ValueError(f"Unsupported calendar interval unit: {unit}")


def build_evaluation_schedule_from_fixed_interval(
    fixed_interval: FixedIntervalScheduleClass,
) -> CronSchedule:
    """
    Builds a cron schedule for a freshness interval.
    This always schedules at a frequency lower than the interval.
    The max run interval is every hour.
    The min run interval is once every 5 minutes.

    If the interval is 2 days, the check will run hourly.
    If the interval is 1 day, the check will run hourly.
    If the interval is 6 hours, the check will run hourly.
    If the interval is 1 hour, the check will run every 30 minutes.
    If the interval is 30 minutes, the check will run once every 10 minutes.
    If the interval is 10 minutes, the check will run once every 5 minutes.
    Any lower will also go to 5 minutes.
    """
    unit = fixed_interval.unit
    multiple = fixed_interval.multiple

    # Convert the interval to minutes for easier comparison
    total_minutes = convert_interval_to_minutes(unit, multiple)

    # Determine cron schedule based on the interval
    if total_minutes > 60:  # More than 1 hour
        return CronSchedule(cron="0 * * * *", timezone="UTC")  # Hourly
    elif total_minutes > 30:  # More than 30 minutes but less than or equal to 1 hour
        return CronSchedule(cron="0,30 * * * *", timezone="UTC")  # Every 30 minutes
    elif (
        total_minutes > 10
    ):  # More than 10 minutes but less than or equal to 30 minutes
        return CronSchedule(
            cron="0,10,20,30,40,50 * * * *", timezone="UTC"
        )  # Every 10 minutes
    else:  # 10 minutes or less
        return CronSchedule(
            cron="0,5,10,15,20,25,30,35,40,45,50,55 * * * *", timezone="UTC"
        )  # Every 5 minutes


def check_is_metrics_cube_bootstrapped(monitor: Monitor) -> bool:
    """
    Check if the metrics cube has already been bootstrapped.
    """
    bootstrap_status = try_extract_metrics_cube_bootstrap_status(monitor)
    return bool(
        bootstrap_status
        and bootstrap_status.state
        == AssertionMonitorMetricsCubeBootstrapState.COMPLETED
    )


def try_extract_metrics_cube_bootstrap_status(
    monitor: Monitor,
) -> Optional[AssertionMonitorMetricsCubeBootstrapStatus]:
    """
    Get the metrics cube bootstrap status for a monitor.
    Returns None if any of the intermediate fields are None.
    """
    if not monitor.assertion_monitor:
        return None

    if not monitor.assertion_monitor.bootstrap_status:
        return None

    return monitor.assertion_monitor.bootstrap_status.metrics_cube_bootstrap_status
