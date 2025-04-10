import time
from dataclasses import dataclass
from datetime import datetime

from datahub.emitter.mce_builder import make_ts_millis, parse_ts_millis


def get_current_time_in_seconds() -> int:
    return int(time.time())


def ts_millis_to_datetime(ts_millis: int) -> datetime:
    """Converts input timestamp in milliseconds to a datetime object with UTC timezone"""
    return parse_ts_millis(ts_millis)


def datetime_to_ts_millis(dt: datetime) -> int:
    """Converts a datetime object to timestamp in milliseconds"""
    # TODO: Deprecate these helpers in favor of make_ts_millis and parse_ts_millis.
    # The other ones support None with a typing overload.
    # Also possibly move those helpers to this file.
    return make_ts_millis(dt)


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
