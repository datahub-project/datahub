"""Last-modified dates for tool results.

Tools report when an entity last changed as a UTC calendar date, ``YYYY-MM-DD``.
Freshness questions are about days to years, a date reads the same on every call,
and no time zone is implied. The date is shown for the agent to weigh; no tool
filters or ranks by it.

Every date passes through ``date_from_millis``, so ``LAST_MODIFIED_DATES_ENABLED=false``
turns them all off at once: tools then return no ``lastModified`` at all, and the
Cloud SQL context tools skip the table read that only serves dates.
"""

from datetime import datetime, timezone
from typing import Any, Optional

from datahub.cli.env_utils import get_boolean_env_variable

LAST_MODIFIED_KEY = "lastModified"
ENABLED_ENV_VAR = "LAST_MODIFIED_DATES_ENABLED"


def last_modified_dates_enabled() -> bool:
    """Read per call so a changed setting applies without a restart in tests."""
    return get_boolean_env_variable(ENABLED_ENV_VAR, default=True)


def date_from_millis(millis: object) -> Optional[str]:
    """``YYYY-MM-DD`` (UTC) for an epoch-millisecond timestamp, or None when unknown
    or when dates are switched off.

    GMS reports an unknown dataset modification time as 0, so non-positive values
    count as missing.
    """
    if isinstance(millis, bool) or not isinstance(millis, (int, float)):
        return None
    if millis <= 0 or not last_modified_dates_enabled():
        return None
    try:
        return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def with_last_modified_dates(value: Any) -> Any:
    """Replace every ``lastModified`` audit stamp (``{"time": ..., ...}``) with its date.

    A stamp with an unknown time is removed rather than shown as 1970, and so is every
    stamp when dates are switched off. Works in place on a cleaned GraphQL response and
    returns it.
    """
    if isinstance(value, dict):
        stamp = value.get(LAST_MODIFIED_KEY)
        if isinstance(stamp, dict) and "time" in stamp:
            date = date_from_millis(stamp.get("time"))
            if date is None:
                del value[LAST_MODIFIED_KEY]
            else:
                value[LAST_MODIFIED_KEY] = date
        for child in value.values():
            with_last_modified_dates(child)
    elif isinstance(value, list):
        for item in value:
            with_last_modified_dates(item)
    return value
