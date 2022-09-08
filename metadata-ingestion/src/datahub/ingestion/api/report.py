import json
import pprint
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict

import humanfriendly

# The sort_dicts option was added in Python 3.8.
if sys.version_info >= (3, 8):
    PPRINT_OPTIONS = {"sort_dicts": False}
else:
    PPRINT_OPTIONS: Dict = {}


@dataclass
class Report:
    @staticmethod
    def to_str(some_val: Any) -> str:
        if isinstance(some_val, Enum):
            return some_val.name
        elif isinstance(some_val, timedelta):
            return humanfriendly.format_timespan(some_val)
        elif isinstance(some_val, datetime):
            try:
                # check if we have a tz_aware object or not (https://stackoverflow.com/questions/5802108/how-to-check-if-a-datetime-object-is-localized-with-pytz)
                tz_aware = (
                    some_val.tzinfo is not None
                    and some_val.tzinfo.utcoffset(some_val) is not None
                )
                now = datetime.now(timezone.utc) if tz_aware else datetime.now()
                diff = now - some_val
                if abs(diff) < timedelta(seconds=1):
                    # the timestamps are close enough that printing a duration isn't useful
                    return f"{some_val} (now)."
                elif diff > timedelta(seconds=0):
                    # timestamp is in the past
                    return f"{some_val} ({humanfriendly.format_timespan(diff)} ago)."
                else:
                    # timestamp is in the future
                    return f"{some_val} (in {humanfriendly.format_timespan(some_val - now)})."
            except Exception:
                # we don't want to fail reporting because we were unable to pretty print a timestamp
                return str(datetime)
        else:
            return str(some_val)

    @staticmethod
    def to_dict(some_val: Any) -> Any:
        """A cheap way to generate a dictionary."""
        if hasattr(some_val, "as_obj"):
            return some_val.as_obj()
        if hasattr(some_val, "dict"):  # pydantic models
            return some_val.dict()
        if hasattr(some_val, "asdict"):  # dataclasses
            return some_val.asdict()
        if isinstance(some_val, list):
            return [Report.to_dict(v) for v in some_val if v is not None]
        if isinstance(some_val, dict):
            return {
                Report.to_str(k): Report.to_dict(v)
                for k, v in some_val.items()
                if v is not None
            }

        # fall through option
        return Report.to_str(some_val)

    def compute_stats(self) -> None:
        """A hook to compute derived stats"""
        pass

    def as_obj(self) -> dict:
        self.compute_stats()
        return {
            str(key): Report.to_dict(value)
            for (key, value) in self.__dict__.items()
            if value is not None
            and not str(key).startswith("_")  # ignore nulls and fields starting with _
        }

    def as_string(self) -> str:
        return pprint.pformat(self.as_obj(), width=150, **PPRINT_OPTIONS)

    def as_json(self) -> str:
        return json.dumps(self.as_obj())
