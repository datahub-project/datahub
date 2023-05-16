import contextlib
from datetime import datetime, timedelta

import dateutil.parser
import humanfriendly


def parse_user_datetime(input: str) -> datetime:
    """Parse absolute and relative time strings into datetime objects.

    This parses strings like "2022-01-01 01:02:03" and "+10m"
    and timestamps like "1630440123".
    """

    # First try parsing as a timestamp.
    with contextlib.suppress(ValueError):
        ts = float(input)
        try:
            return datetime.fromtimestamp(ts)
        except (OverflowError, ValueError):
            # This is likely a timestamp in milliseconds.
            return datetime.fromtimestamp(ts / 1000)

    # Then try parsing as a relative time.
    with contextlib.suppress(humanfriendly.InvalidTimespan):
        delta = _parse_relative_timespan(input)
        return datetime.now() + delta

    # Finally, try parsing as an absolute time.
    with contextlib.suppress(dateutil.parser.ParserError):
        return dateutil.parser.parse(input)

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
