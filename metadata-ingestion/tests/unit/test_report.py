import dataclasses
import logging
from typing import Any, List, Literal, Optional

from pydantic import BaseModel

from datahub.ingestion.api.report import Report

logger = logging.getLogger(__name__)

LogLevel = Literal["ERROR", "WARNING", "INFO", "DEBUG"]


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

    processed: List[str] = []
    filtered: List[str] = []

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


def make_entity_filter_report(
    type: str, severity: LogLevel = "DEBUG"
) -> EntityFilterReport:
    return dataclasses.field(
        default_factory=lambda: EntityFilterReport(type=type, severity=severity)
    )


@dataclasses.dataclass
class MyReport(Report):
    views: EntityFilterReport = make_entity_filter_report(type="view")


def test_report_types():

    report = MyReport()

    report.views(entity="foo")
    report.views.dropped(entity="bar")

    assert report.as_obj() == {
        "views": {"filtered": ["bar"], "processed": ["foo"]},
    }
