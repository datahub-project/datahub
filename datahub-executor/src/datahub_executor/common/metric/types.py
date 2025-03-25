from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel


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

    def __repr__(self) -> str:
        return f"MetricResolverStrategy(source_type={self.source_type})"


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

    def __repr__(self) -> str:
        return f"Operation(timestamp_ms={self.timestamp_ms}, type={self.type})"


class InvalidMetricResolverSourceTypeException(Exception):
    """Base class for invalid metric resolver exceptions."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class UnsupportedMetricException(Exception):
    """Base class for unsupported metric types exceptions."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
