import time
from datetime import datetime, timezone


def get_current_time_in_seconds() -> int:
    return int(time.time())


def get_datetime_from_ts_millis_in_utc(ts_millis: int) -> datetime:
    return datetime.fromtimestamp(ts_millis / 1000, tz=timezone.utc)


def datetime_to_ts_millis(dt: datetime) -> int:
    return int(
        round(dt.timestamp() * 1000),
    )
