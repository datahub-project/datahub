import dataclasses
import logging
from typing import Any, ClassVar, Dict

from datahub.ingestion.api.report import Report

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class EntityFilterReport(Report):
    _ALIASES: ClassVar[Dict[str, str]] = {
        "_dropped": "dropped",
    }

    processed: list[str] = dataclasses.field(default_factory=list)
    _dropped: list[str] = dataclasses.field(default_factory=list)

    def __call__(self, entity: str) -> Any:
        print(f"call call {self} {entity}")
        self.processed.append(entity)

    def dropped(self, entity: str) -> None:
        print(f"call dropped {self} {entity}")
        self._dropped.append(entity)


def make_entity_filter_report():
    return dataclasses.field(default_factory=EntityFilterReport)
    # return ListReport()


@dataclasses.dataclass
class MyReport(Report):
    views: EntityFilterReport = make_entity_filter_report()


def test_report_types():

    report = MyReport()

    report.views(entity="foo")
    report.views.dropped(entity="bar")

    assert report.as_obj() == {
        "views": {"dropped": ["bar"], "processed": ["foo"]},
    }
