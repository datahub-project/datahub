import dataclasses
import json
import logging
import pprint
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional

import humanfriendly
import pydantic
from pydantic import BaseModel
from typing_extensions import Literal, Protocol, runtime_checkable

from datahub.ingestion.api.report_helpers import format_datetime_relative
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)
LogLevel = Literal["ERROR", "WARNING", "INFO", "DEBUG"]

# The sort_dicts option was added in Python 3.8.
if sys.version_info >= (3, 8):
    PPRINT_OPTIONS = {"sort_dicts": False}
else:
    PPRINT_OPTIONS: Dict = {}


@runtime_checkable
class SupportsAsObj(Protocol):
    def as_obj(self) -> dict:
        ...


def _stacklevel_if_supported(level: int) -> dict:
    # The logging module added support for stacklevel in Python 3.8.
    if sys.version_info >= (3, 8):
        return {"stacklevel": level}
    else:
        return {}


@dataclass
class Report(SupportsAsObj):
    @staticmethod
    def to_str(some_val: Any) -> str:
        if isinstance(some_val, Enum):
            return some_val.name
        else:
            return str(some_val)

    @staticmethod
    def to_pure_python_obj(some_val: Any) -> Any:
        """A cheap way to generate a dictionary."""

        if isinstance(some_val, SupportsAsObj):
            return some_val.as_obj()
        elif isinstance(some_val, pydantic.BaseModel):
            return some_val.dict()
        elif dataclasses.is_dataclass(some_val):
            return dataclasses.asdict(some_val)
        elif isinstance(some_val, list):
            return [Report.to_pure_python_obj(v) for v in some_val if v is not None]
        elif isinstance(some_val, timedelta):
            return humanfriendly.format_timespan(some_val)
        elif isinstance(some_val, datetime):
            try:
                return format_datetime_relative(some_val)
            except Exception:
                # we don't want to fail reporting because we were unable to pretty print a timestamp
                return str(datetime)
        elif isinstance(some_val, dict):
            return {
                Report.to_str(k): Report.to_pure_python_obj(v)
                for k, v in some_val.items()
                if v is not None
            }
        elif isinstance(some_val, (int, float, bool)):
            return some_val
        else:
            # fall through option
            return Report.to_str(some_val)

    def compute_stats(self) -> None:
        """A hook to compute derived stats"""
        pass

    def as_obj(self) -> dict:
        self.compute_stats()
        return {
            str(key): Report.to_pure_python_obj(value)
            for (key, value) in self.__dict__.items()
            # ignore nulls and fields starting with _
            if value is not None and not str(key).startswith("_")
        }

    def as_string(self) -> str:
        return pprint.pformat(self.as_obj(), width=150, **PPRINT_OPTIONS)

    def as_json(self) -> str:
        return json.dumps(self.as_obj())

    # TODO add helper method for warning / failure status + counts?


class ReportAttribute(BaseModel):
    severity: LogLevel = "DEBUG"
    help: Optional[str] = None

    @property
    def logger_sev(self) -> int:
        log_levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }
        return log_levels[self.severity]

    def log(self, msg: str) -> None:
        logger.log(level=self.logger_sev, msg=msg, **_stacklevel_if_supported(3))


class EntityFilterReport(ReportAttribute):
    type: str

    processed_entities: LossyList[str] = pydantic.Field(default_factory=LossyList)
    dropped_entities: LossyList[str] = pydantic.Field(default_factory=LossyList)

    def processed(self, entity: str, type: Optional[str] = None) -> None:
        self.log(f"Processed {type or self.type} {entity}")
        self.processed_entities.append(entity)

    def dropped(self, entity: str, type: Optional[str] = None) -> None:
        self.log(f"Filtered {type or self.type} {entity}")
        self.dropped_entities.append(entity)

    def as_obj(self) -> dict:
        return {
            "filtered": self.dropped_entities.as_obj(),
            "processed": self.processed_entities.as_obj(),
        }

    @staticmethod
    def field(type: str, severity: LogLevel = "DEBUG") -> "EntityFilterReport":
        """A helper to create a dataclass field."""

        return dataclasses.field(
            default_factory=lambda: EntityFilterReport(type=type, severity=severity)
        )
