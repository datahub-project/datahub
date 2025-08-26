from typing import TypeVar

from datahub_executor.common.metric.types import Metric, Operation

# Define a generic type for events (Metric or Operation)
Event = TypeVar("Event", Metric, Operation)
