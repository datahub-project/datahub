"""Timestamp parsing shared by the profile and anomaly mappers.

Its own module because both mappers need it and neither owns it.
"""

from datetime import datetime, timezone


def parse_timestamp_millis(value: str | None) -> int | None:
    """Parse a Qualytics ISO-8601 timestamp to epoch millis, or None if unparseable.

    ``datetime.fromisoformat`` only learned to accept a trailing ``Z`` in 3.11, and we
    support 3.10, so it is normalised first. A naive timestamp is treated as UTC --
    Qualytics serialises UTC, and guessing local time would shift every profile and
    assertion result by the ingesting machine's offset.

    Returns None rather than raising or defaulting to "now": callers place these on a
    timeline, and a fabricated timestamp corrupts the history it is meant to describe.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)
