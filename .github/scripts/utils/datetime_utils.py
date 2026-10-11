from datetime import datetime


def parse_dt(value: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp from the GitHub API (handles trailing Z)."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def duration_seconds(start: datetime | None, end: datetime | None) -> int | None:
    """Compute duration in whole seconds. Returns None if either timestamp is None."""
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds()))
