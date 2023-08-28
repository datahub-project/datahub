import time
from dataclasses import dataclass
from datetime import datetime, timezone


def get_current_time_in_seconds() -> int:
    return int(time.time())


def ts_millis_to_datetime(ts_millis: int) -> datetime:
    """Converts input timestamp in milliseconds to a datetime object with UTC timezone"""
    return datetime.fromtimestamp(ts_millis / 1000, tz=timezone.utc)


def datetime_to_ts_millis(dt: datetime) -> int:
    """Converts a datetime object to timestamp in milliseconds"""
    return int(round(dt.timestamp() * 1000))


@dataclass
class TimeWindow:
    start_time: datetime
    end_time: datetime

    def contains(self, other: "TimeWindow") -> bool:
        """Whether current window contains other window completely"""
        return self.start_time <= other.start_time <= other.end_time <= self.end_time

    def left_intersects(self, other: "TimeWindow") -> bool:
        """Whether only left part of current window overlaps other window."""
        return other.start_time <= self.start_time < other.end_time < self.end_time

    def right_intersects(self, other: "TimeWindow") -> bool:
        """Whether only right part of current window overlaps other window."""
        return self.start_time < other.start_time < self.end_time <= other.end_time

    def starts_after(self, other: "TimeWindow") -> bool:
        """Whether current window starts after other window ends"""
        return other.start_time <= other.end_time < self.start_time

    def ends_after(self, other: "TimeWindow") -> bool:
        """Whether current window ends after other window ends."""
        return self.end_time > other.end_time
