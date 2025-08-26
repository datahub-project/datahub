from datetime import datetime, timezone
from typing import Optional


def show_last_updated(last_ingested_time: Optional[datetime]) -> bool:
    now = datetime.now(tz=timezone.utc)
    return bool(last_ingested_time and (last_ingested_time - now).days > 30)


def get_last_updated_copy(last_updated_time: datetime) -> str:
    """
    Converts a UTC time to a relative time string from now.
    '2 hours ago', '2 days ago', etc.
    """
    now = datetime.now(tz=timezone.utc)
    delta = now - last_updated_time
    if delta.days > 0:
        return f"{delta.days} days ago"
    if delta.seconds < 60:
        return "just now"
    if delta.seconds < 3600:
        return f"{delta.seconds // 60} minutes ago"
    return f"{delta.seconds // 3600} hours ago"


def slack_ts_to_datetime(ts: str) -> datetime:
    """Converts a Slack timestamp to a datetime object.

    Args:
        ts: The Slack timestamp to convert.

    Returns:
        The datetime object.

    Examples:
        >>> import datetime
        >>> slack_ts_to_datetime("1743560027.534299").isoformat()
        '2025-04-02T02:13:47.534299+00:00'
    """

    return datetime.fromtimestamp(float(ts), tz=timezone.utc)
