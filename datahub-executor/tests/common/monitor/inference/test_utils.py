from typing import cast
from unittest.mock import Mock

import pytest
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionStdParametersClass,
    CalendarIntervalClass,
    EmbeddedAssertionClass,
    FixedIntervalScheduleClass,
)

from datahub_executor.common.metric.types import Metric, Operation
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
)
from datahub_executor.common.monitor.inference.utils import (
    annotate_operations_with_anomalies,
    build_evaluation_schedule_from_fixed_interval,
    build_std_parameters,
    convert_interval_to_minutes,
    create_embedded_assertion,
    create_inference_source,
    get_event_timespan_seconds,
    is_metric_anomaly,
)
from datahub_executor.common.types import Anomaly, CronSchedule


@pytest.fixture
def mock_boundary() -> MetricBoundary:
    """Create a mock MetricBoundary for testing."""
    lower_bound = Mock(spec=MetricBoundary)
    lower_bound.value = 100.0

    upper_bound = Mock(spec=MetricBoundary)
    upper_bound.value = 200.0

    boundary = Mock(spec=MetricBoundary)
    boundary.lower_bound = lower_bound
    boundary.upper_bound = upper_bound
    boundary.start_time_ms = 1677600000000  # Example timestamp (March 1, 2023)

    return cast(MetricBoundary, boundary)


@pytest.fixture
def mock_assertion_info() -> AssertionInfoClass:
    """Create a mock AssertionInfoClass for testing."""
    return AssertionInfoClass(
        type="VOLUME",
        source=AssertionSourceClass(type=AssertionSourceTypeClass.INFERRED),
    )


def test_build_std_parameters(mock_boundary: MetricBoundary) -> None:
    """Test building standard parameters from a boundary."""
    # Act
    result = build_std_parameters(mock_boundary)

    # Assert
    assert isinstance(result, AssertionStdParametersClass)

    assert result.minValue is not None
    assert result.minValue.type == "NUMBER"
    assert result.minValue.value == "100.0"

    assert result.maxValue is not None
    assert result.maxValue.type == "NUMBER"
    assert result.maxValue.value == "200.0"


def test_create_embedded_assertion(
    mock_assertion_info: AssertionInfoClass,
    mock_boundary: MetricBoundary,
) -> None:
    """Test creating an embedded assertion with time window."""
    # Arrange
    window_size_seconds = 3600  # 1 hour

    # Act
    result = create_embedded_assertion(
        mock_assertion_info, mock_boundary, window_size_seconds
    )

    # Assert
    assert isinstance(result, EmbeddedAssertionClass)
    assert result.assertion == mock_assertion_info

    assert result.evaluationTimeWindow is not None
    assert result.evaluationTimeWindow.startTimeMillis == mock_boundary.start_time_ms

    assert result.evaluationTimeWindow.length is not None
    assert result.evaluationTimeWindow.length.unit == "SECOND"
    assert result.evaluationTimeWindow.length.multiple == window_size_seconds


def test_create_inference_source() -> None:
    """Test creating an inference source."""
    # Act
    result = create_inference_source()

    # Assert
    assert isinstance(result, AssertionSourceClass)
    assert result.type == AssertionSourceTypeClass.INFERRED


def test_get_event_timespan_empty_list() -> None:
    """Test with an empty event list."""
    assert get_event_timespan_seconds([]) == 0


def test_get_event_timespan_single_event() -> None:
    """Test with a single event (should return 0)."""
    events = [
        Operation(timestamp_ms=1640995200000, type="INSERT")
    ]  # Jan 1, 2022, 00:00:00 UTC
    assert get_event_timespan_seconds(events) == 0


def test_get_event_timespan_two_events() -> None:
    """Test with two events."""
    events = [
        Operation(
            timestamp_ms=1640995200000, type="INSERT"
        ),  # Jan 1, 2022, 00:00:00 UTC
        Operation(
            timestamp_ms=1641002400000, type="INSERT"
        ),  # Jan 1, 2022, 02:00:00 UTC (2 hours later)
    ]
    assert get_event_timespan_seconds(events) == 2 * 60 * 60  # 7200 seconds


def test_get_event_timespan_multiple_unsorted_events() -> None:
    """Test with multiple events in random order."""
    events = [
        Operation(timestamp_ms=1641024000000, type="INSERT"),  # 08:00 UTC
        Operation(timestamp_ms=1640995200000, type="INSERT"),  # 00:00 UTC (Earliest)
        Operation(timestamp_ms=1641016800000, type="INSERT"),  # 03:00 UTC
    ]
    assert get_event_timespan_seconds(events) == 8 * 60 * 60  # 28800 seconds


def test_get_event_timespan_large_timespan() -> None:
    """Test with events spanning several days."""
    events = [
        Operation(timestamp_ms=1640995200000, type="INSERT"),  # Jan 1, 2022
        Operation(
            timestamp_ms=1641600000000, type="INSERT"
        ),  # Jan 8, 2022 (7 days later)
    ]
    assert get_event_timespan_seconds(events) == 7 * 24 * 60 * 60  # 604800 seconds


def test_is_metric_anomaly() -> None:
    """Test detecting whether a metric is an anomaly"""
    # Case 1: Metric is an anomaly.
    anomalies = [
        Anomaly(timestamp_ms=0, metric=Metric(value=123, timestamp_ms=1)),
        Anomaly(timestamp_ms=1, metric=Metric(value=124, timestamp_ms=2)),
    ]

    metric = Metric(value=123, timestamp_ms=1)

    assert is_metric_anomaly(metric, anomalies) is True

    # Case 2: Metric is not an anomaly, does not match value.
    anomalies = [Anomaly(timestamp_ms=0, metric=Metric(value=123, timestamp_ms=1))]

    metric = Metric(value=124, timestamp_ms=1)

    assert is_metric_anomaly(metric, anomalies) is False

    # Case 2: Metric is not an anomaly, does not match timestamp.
    anomalies = [Anomaly(timestamp_ms=0, metric=Metric(value=123, timestamp_ms=1))]

    metric = Metric(value=123, timestamp_ms=2)

    assert is_metric_anomaly(metric, anomalies) is False

    # Case 3: Empty anomalies.
    anomalies = []
    metric = Metric(value=123, timestamp_ms=1)

    assert is_metric_anomaly(metric, anomalies) is False


def test_annotate_operations_with_anomalies() -> None:
    # Case 1: There are no anomalies.
    # Operations should remain unchanged.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=1, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=2, is_anomaly=False),
    ]

    anomalies: list[Anomaly] = []

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)

    assert operations == annotated_operations

    # Case 2: There are anomalies at the beginning
    # First operation should be marked as an anomaly.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=1, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=2, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=-1, metric=None),
        Anomaly(timestamp_ms=-2, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)

    # Only the first should be marked an anomaly
    assert annotated_operations[0].is_anomaly is True
    for operation in annotated_operations[1:]:
        assert operation.is_anomaly is False

    # Case 3: There are anomalies at the end
    # Operations should remain unchanged.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=1, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=2, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=4, metric=None),
        Anomaly(timestamp_ms=5, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)

    assert operations == annotated_operations

    # Case 4: There is single anomalies in the middle - none equal to the exact times.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=4, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=6, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=1, metric=None),
        Anomaly(timestamp_ms=3, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)
    assert annotated_operations[0].is_anomaly is False
    assert annotated_operations[1].is_anomaly is True
    assert annotated_operations[2].is_anomaly is False

    # Case 5: There are multiple anomalies in the middle.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=4, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=6, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=1, metric=None),
        Anomaly(timestamp_ms=3, metric=None),
        Anomaly(timestamp_ms=5, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)
    assert annotated_operations[0].is_anomaly is False
    assert annotated_operations[1].is_anomaly is True
    assert annotated_operations[2].is_anomaly is True

    # Case 6: If the anomaly happened at the exact same time as the operation,
    # it was probably a timing fluke. Either way to be safe, we do mark as an anomaly.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=4, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=6, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=0, metric=None),
        Anomaly(timestamp_ms=4, metric=None),
        Anomaly(timestamp_ms=6, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)
    assert annotated_operations[0].is_anomaly is True
    assert annotated_operations[1].is_anomaly is True
    assert annotated_operations[2].is_anomaly is True


def test_convert_interval_to_minutes() -> None:
    # Test seconds conversion
    assert convert_interval_to_minutes(CalendarIntervalClass.SECOND, 30) == 0.5

    # Test minutes conversion
    assert convert_interval_to_minutes(CalendarIntervalClass.MINUTE, 45) == 45

    # Test hours conversion
    assert convert_interval_to_minutes(CalendarIntervalClass.HOUR, 2) == 120

    # Test days conversion
    assert convert_interval_to_minutes(CalendarIntervalClass.DAY, 1) == 24 * 60

    # Test weeks conversion
    assert convert_interval_to_minutes(CalendarIntervalClass.WEEK, 1) == 7 * 24 * 60

    # Test months conversion
    assert convert_interval_to_minutes(CalendarIntervalClass.MONTH, 1) == 30 * 24 * 60

    # Test quarters conversion
    assert convert_interval_to_minutes(CalendarIntervalClass.QUARTER, 1) == 91 * 24 * 60

    # Test years conversion
    assert convert_interval_to_minutes(CalendarIntervalClass.YEAR, 1) == 365 * 24 * 60

    # Test invalid unit
    try:
        # This would need to be adapted based on how you want to test for exceptions
        # in your actual codebase. Here we're simulating an invalid unit.
        invalid_unit = "INVALID_UNIT"  # type: ignore
        convert_interval_to_minutes(invalid_unit, 1)  # type: ignore
    except ValueError:
        pass


def test_build_evaluation_schedule_from_fixed_interval() -> None:
    # Test schedule for 2 days interval (should be hourly)
    two_days_interval = FixedIntervalScheduleClass(CalendarIntervalClass.DAY, 2)
    assert build_evaluation_schedule_from_fixed_interval(
        two_days_interval
    ) == CronSchedule(cron="0 * * * *", timezone="UTC")

    # Test schedule for 1 day interval (should be hourly)
    one_day_interval = FixedIntervalScheduleClass(CalendarIntervalClass.DAY, 1)
    assert build_evaluation_schedule_from_fixed_interval(
        one_day_interval
    ) == CronSchedule(cron="0 * * * *", timezone="UTC")

    # Test schedule for 6 hours interval (should be hourly)
    six_hours_interval = FixedIntervalScheduleClass(CalendarIntervalClass.HOUR, 6)
    assert build_evaluation_schedule_from_fixed_interval(
        six_hours_interval
    ) == CronSchedule(cron="0 * * * *", timezone="UTC")

    # Test schedule for 1 hour interval (should be every 30 minutes)
    one_hour_interval = FixedIntervalScheduleClass(CalendarIntervalClass.HOUR, 1)
    assert build_evaluation_schedule_from_fixed_interval(
        one_hour_interval
    ) == CronSchedule(cron="0,30 * * * *", timezone="UTC")

    # Test schedule for 45 minutes interval (should be every 30 minutes)
    forty_five_min_interval = FixedIntervalScheduleClass(
        CalendarIntervalClass.MINUTE, 45
    )
    assert build_evaluation_schedule_from_fixed_interval(
        forty_five_min_interval
    ) == CronSchedule(cron="0,30 * * * *", timezone="UTC")

    # Test schedule for 30 minutes interval (should be every 10 minutes)
    thirty_min_interval = FixedIntervalScheduleClass(CalendarIntervalClass.MINUTE, 30)
    assert build_evaluation_schedule_from_fixed_interval(
        thirty_min_interval
    ) == CronSchedule(cron="0,10,20,30,40,50 * * * *", timezone="UTC")

    # Test schedule for 15 minutes interval (should be every 10 minutes)
    fifteen_min_interval = FixedIntervalScheduleClass(CalendarIntervalClass.MINUTE, 15)
    assert build_evaluation_schedule_from_fixed_interval(
        fifteen_min_interval
    ) == CronSchedule(cron="0,10,20,30,40,50 * * * *", timezone="UTC")

    # Test schedule for 10 minutes interval (should be every 5 minutes)
    ten_min_interval = FixedIntervalScheduleClass(CalendarIntervalClass.MINUTE, 10)
    assert build_evaluation_schedule_from_fixed_interval(
        ten_min_interval
    ) == CronSchedule(cron="0,5,10,15,20,25,30,35,40,45,50,55 * * * *", timezone="UTC")

    # Test schedule for 5 minutes interval (should be every 5 minutes)
    five_min_interval = FixedIntervalScheduleClass(CalendarIntervalClass.MINUTE, 5)
    assert build_evaluation_schedule_from_fixed_interval(
        five_min_interval
    ) == CronSchedule(cron="0,5,10,15,20,25,30,35,40,45,50,55 * * * *", timezone="UTC")

    # Test schedule for 1 minute interval (should be every 5 minutes)
    one_min_interval = FixedIntervalScheduleClass(CalendarIntervalClass.MINUTE, 1)
    assert build_evaluation_schedule_from_fixed_interval(
        one_min_interval
    ) == CronSchedule(cron="0,5,10,15,20,25,30,35,40,45,50,55 * * * *", timezone="UTC")

    # Test schedule for 30 seconds interval (should be every 5 minutes)
    thirty_seconds_interval = FixedIntervalScheduleClass(
        CalendarIntervalClass.SECOND, 30
    )
    assert build_evaluation_schedule_from_fixed_interval(
        thirty_seconds_interval
    ) == CronSchedule(cron="0,5,10,15,20,25,30,35,40,45,50,55 * * * *", timezone="UTC")
