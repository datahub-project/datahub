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
from typing_extensions import Literal

from datahub.ingestion.api.report_helpers import format_datetime_relative
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)
LogLevel = Literal["ERROR", "WARNING", "INFO", "DEBUG"]

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
                return format_datetime_relative(some_val)
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
        elif isinstance(some_val, pydantic.BaseModel):
            return some_val.dict()
        elif dataclasses.is_dataclass(some_val):
            return dataclasses.asdict(some_val)
        elif isinstance(some_val, list):
            return [Report.to_dict(v) for v in some_val if v is not None]
        elif isinstance(some_val, dict):
            return {
                Report.to_str(k): Report.to_dict(v)
                for k, v in some_val.items()
                if v is not None
            }
        else:
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


class EntityFilterReport(ReportAttribute):
    type: str

    processed: LossyList[str] = LossyList()
    filtered: LossyList[str] = LossyList()

    def __call__(self, entity: str) -> Any:
        logger.log(
            level=self.logger_sev, msg=f"Processed {self.type} {entity}", stacklevel=2
        )
        self.processed.append(entity)

    def dropped(self, entity: str) -> None:
        logger.log(
            level=self.logger_sev, msg=f"Filtered {self.type} {entity}", stacklevel=2
        )
        self.filtered.append(entity)

    def as_obj(self) -> dict:
        return {
            "filtered": self.filtered,
            "processed": self.processed,
        }

    @staticmethod
    def field(type: str, severity: LogLevel = "DEBUG") -> "EntityFilterReport":
        return dataclasses.field(
            default_factory=lambda: EntityFilterReport(type=type, severity=severity)
        )
