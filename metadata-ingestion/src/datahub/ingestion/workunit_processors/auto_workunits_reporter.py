from typing import TYPE_CHECKING, Iterable, TypeVar

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)

if TYPE_CHECKING:
    from datahub.ingestion.api.source import SourceReport

T = TypeVar("T", bound=MetadataWorkUnit)


def auto_workunit_reporter(report: "SourceReport", stream: Iterable[T]) -> Iterable[T]:
    """
    Calls report.report_workunit() on each workunit.
    """
    for wu in stream:
        report.report_workunit(wu)
        yield wu

    if report.event_not_produced_warn and report.events_produced == 0:
        report.warning(
            title="No metadata was produced by the source",
            message="Please check the source configuration, filters, and permissions.",
        )


class AutoWorkunitsReporterProcessor(WorkunitProcessor):
    """Report each workunit to the source report for metrics tracking."""

    NAME = "auto_workunit_reporter"

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self._source_report = ctx.source_report

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        return auto_workunit_reporter(self._source_report, stream)
