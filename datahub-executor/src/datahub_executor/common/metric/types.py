from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel

from datahub_executor.common.exceptions import AssertionResultException


class MetricSourceType(str, Enum):
    """
    The source of a given metric.
    """

    DATAHUB_DATASET_PROFILE = "DATAHUB_DATASET_PROFILE"
    DATAHUB_OPERATION = "DATAHUB_OPERATION"
    AUDIT_LOG = "AUDIT_LOG"
    INFORMATION_SCHEMA = "INFORMATION_SCHEMA"
    FILE_METADATA = "FILE_METADATA"
    QUERY = "QUERY"


class MetricResolverStrategy(BaseModel):
    """
    Specific details about where to pull a metric from.
    """

    source_type: MetricSourceType
    bucketing_timestamp_field_path: str | None = None
    bucketing_interval_unit: str | None = None
    bucketing_interval_multiple: int | None = None
    bucketing_timezone: str | None = None
    late_arrival_grace_period_unit: str | None = None
    late_arrival_grace_period_multiple: int | None = None
    bucket_start_time_ms: int | None = None
    bucket_end_time_ms: int | None = None
    metric_timestamp_ms: int | None = None

    def __repr__(self) -> str:
        return (
            "MetricResolverStrategy("
            f"source_type={self.source_type}, "
            f"bucketing_interval_unit={self.bucketing_interval_unit}, "
            f"bucket_start_time_ms={self.bucket_start_time_ms}, "
            f"bucket_end_time_ms={self.bucket_end_time_ms}"
            ")"
        )


class Metric(BaseModel):
    """A simple shell Metric class to hold a metric name and integer value."""

    timestamp_ms: int
    value: float

    def timestamp(self) -> datetime:
        """Convert timestamp_ms to a datetime object."""
        return datetime.fromtimestamp(self.timestamp_ms / 1000, timezone.utc)

    def __repr__(self) -> str:
        return f"Metric(timestamp_ms={self.timestamp_ms}, value={self.value})"


class Operation(BaseModel):
    """A simple shell class to hold operations for a table."""

    timestamp_ms: int
    type: str

    # This flag is used to indicate whether this update
    # follows a window of time (interval) that would be
    # considered anomalous for freshness purposes.
    #
    # Meaning, there should have been an update BEFORE this operation,
    # but there was not. Only used for smart assertions.
    is_anomaly: bool = False

    def __repr__(self) -> str:
        return f"Operation(timestamp_ms={self.timestamp_ms}, type={self.type})"


class InvalidMetricResolverSourceTypeException(AssertionResultException):
    """Invalid metric resolver source type for assertion evaluation."""

    def __init__(self, message: str, source_type: str) -> None:
        super().__init__(message)
        self.source_type = source_type


class UnsupportedMetricException(AssertionResultException):
    """Unsupported metric name for assertion evaluation."""

    def __init__(self, message: str, metric_name: str) -> None:
        super().__init__(message)
        self.metric_name = metric_name
