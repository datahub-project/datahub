from datetime import datetime, timedelta, timezone

import humanfriendly


def format_datetime_relative(some_val: datetime) -> str:
    """Formats a datetime as a human-readable string relative to now."""

    # check if we have a tz_aware object or not (https://stackoverflow.com/questions/5802108/how-to-check-if-a-datetime-object-is-localized-with-pytz)
    tz_aware = (
        some_val.tzinfo is not None and some_val.tzinfo.utcoffset(some_val) is not None
    )
    now = datetime.now(timezone.utc) if tz_aware else datetime.now()
    diff = now - some_val
    if abs(diff) < timedelta(seconds=1):
        # the timestamps are close enough that printing a duration isn't useful
        return f"{some_val} (now)"
    elif diff > timedelta(seconds=0):
        # timestamp is in the past
        return f"{some_val} ({humanfriendly.format_timespan(diff)} ago)"
    else:
        # timestamp is in the future
        return f"{some_val} (in {humanfriendly.format_timespan(some_val - now)})"
