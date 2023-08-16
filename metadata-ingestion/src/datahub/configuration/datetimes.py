import contextlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import click
import dateutil.parser
import humanfriendly

logger = logging.getLogger(__name__)


def parse_user_datetime(input: str) -> datetime:
    """Parse absolute and relative time strings into datetime objects.

    This parses strings like "2022-01-01 01:02:03" and "-7 days"
    and timestamps like "1630440123".

    Args:
        input: A string representing a datetime or relative time.

    Returns:
        A timezone-aware datetime object in UTC. If the input specifies a different
        timezone, it will be converted to UTC.
    """

    # Special cases.
    if input == "now":
        return datetime.now(tz=timezone.utc)
    elif input == "min":
        return datetime.min.replace(tzinfo=timezone.utc)
    elif input == "max":
        return datetime.max.replace(tzinfo=timezone.utc)

    # First try parsing as a timestamp.
    with contextlib.suppress(ValueError):
        ts = float(input)
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, ValueError):
            # This is likely a timestamp in milliseconds.
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

    # Then try parsing as a relative time.
    with contextlib.suppress(humanfriendly.InvalidTimespan):
        delta = parse_relative_timespan(input)
        return datetime.now(tz=timezone.utc) + delta

    # Finally, try parsing as an absolute time.
    with contextlib.suppress(dateutil.parser.ParserError):
        return parse_absolute_time(input)

    raise ValueError(f"Could not parse {input} as a datetime or relative time.")


def parse_absolute_time(input: str) -> datetime:
    dt = dateutil.parser.parse(input)
    if dt.tzinfo is None:
        # Assume that the user meant to specify a time in UTC.
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        # Convert to UTC.
        dt = dt.astimezone(timezone.utc)
    return dt


def parse_relative_timespan(input: str) -> timedelta:
    neg = False
    input = input.strip()

    if input.startswith("+"):
        input = input[1:]
    elif input.startswith("-"):
        input = input[1:]
        neg = True

    seconds = humanfriendly.parse_timespan(input)
    delta = timedelta(seconds=seconds)
    if neg:
        delta = -delta

    logger.debug(f'Parsed "{input}" as {delta}.')
    return delta


class ClickDatetime(click.ParamType):
    name = "datetime"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> datetime:
        if isinstance(value, datetime):
            return value

        try:
            return parse_user_datetime(value)
        except ValueError as e:
            self.fail(str(e), param, ctx)
