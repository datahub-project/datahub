import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

import click
import dateutil.parser
import humanfriendly


def parse_user_datetime(input: str) -> datetime:
    """Parse absolute and relative time strings into datetime objects.

    This parses strings like "2022-01-01 01:02:03" and "+10m"
    and timestamps like "1630440123".

    It will always return a datetime aware object in UTC.
    """

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
        delta = _parse_relative_timespan(input)
        return datetime.now(tz=timezone.utc) + delta

    # Finally, try parsing as an absolute time.
    with contextlib.suppress(dateutil.parser.ParserError):
        dt = dateutil.parser.parse(input)
        if dt.tzinfo is None:
            # Assume that the user meant to specify a time in UTC.
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC.
            dt = dt.astimezone(timezone.utc)
        return dt

    raise ValueError(f"Could not parse {input} as a datetime or relative time.")


def _parse_relative_timespan(input: str) -> timedelta:
    neg = False
    input = input.strip()

    if input.startswith("+"):
        input = input[1:]
    elif input.startswith("-"):
        input = input[1:]
        neg = True

    seconds = humanfriendly.parse_timespan(input)

    if neg:
        return -timedelta(seconds=seconds)
    return +timedelta(seconds=seconds)


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
