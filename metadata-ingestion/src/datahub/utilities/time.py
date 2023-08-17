import time
from datetime import datetime, timezone


def get_current_time_in_seconds() -> int:
    return int(time.time())


def ts_millis_to_datetime(ts_millis: int) -> datetime:
    """Converts input timestamp in milliseconds to a datetime object with UTC timezone"""
    return datetime.fromtimestamp(ts_millis / 1000, tz=timezone.utc)


def datetime_to_ts_millis(dt: datetime) -> int:
    """Converts a datetime object to timestamp in milliseconds"""
    return int(round(dt.timestamp() * 1000))
