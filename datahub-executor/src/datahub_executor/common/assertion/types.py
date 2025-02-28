from enum import Enum
from typing import Optional


class AssertionStateType(Enum):
    """Enumeration of assertion state types."""

    MONITOR_TIMESERIES_STATE = "MONITOR_TIMESERIES_STATE"


class AssertionState:
    type: AssertionStateType
    timestamp: Optional[int]
    properties: dict

    def __init__(
        self, type: AssertionStateType, properties: dict, timestamp: Optional[int]
    ):
        self.type = type
        self.timestamp = timestamp
        self.properties = properties


class AssertionDatabaseParams:
    qualified_name: Optional[str]
    table_name: Optional[str]

    def __init__(self, qualified_name: Optional[str], table_name: Optional[str]):
        self.qualified_name = qualified_name
        self.table_name = table_name
