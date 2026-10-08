"""Timestamp parsing shared by the profile and anomaly mappers.

Its own module because both mappers need it and neither owns it.
"""

import re
from datetime import datetime, timezone

# Fractional seconds of any length, followed by the offset or the end of the string.
_FRACTION = re.compile(r"\.(\d+)(?=[+-]|$)")


def _six_digit_fraction(match: re.Match[str]) -> str:
    return "." + match.group(1)[:6].ljust(6, "0")


def parse_timestamp_millis(value: str | None) -> int | None:
    """Parse a Qualytics ISO-8601 timestamp to epoch millis, or None if unparseable.

    ``datetime.fromisoformat`` only learned to accept a trailing ``Z`` in 3.11, and we
    support 3.10, so it is normalised first. So is the fraction: 3.10 accepts only
    three or six digits, so seven-digit precision would parse on 3.11 and be dropped
    as undated on 3.10. It is padded or truncated to microseconds, which is what 3.11
    does itself. A naive timestamp is treated as UTC --
    Qualytics serialises UTC, and guessing local time would shift every profile and
    assertion result by the ingesting machine's offset.

    Returns None rather than raising or defaulting to "now": callers place these on a
    timeline, and a fabricated timestamp corrupts the history it is meant to describe.
    """
    if not value:
        return None
    try:
        normalised = _FRACTION.sub(_six_digit_fraction, value.replace("Z", "+00:00"))
        parsed = datetime.fromisoformat(normalised)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)
