from dataclasses import dataclass
from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)


@dataclass
class AutoWorkunitsReporterProcessorReport(WorkunitProcessorReport):
    """Report for AutoWorkunitsReporterProcessor metrics."""

    pass


# TODO: move _source_report.report_workunit here and also move report metrics here to AutoWorkunitsReporterProcessorReport


class AutoWorkunitsReporterProcessor(
    WorkunitProcessor[AutoWorkunitsReporterProcessorReport]
):
    """Report each workunit to the source report for metrics tracking."""

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self._source_report = ctx.source_report

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """
        Calls report.report_workunit() on each workunit.
        """
        for wu in stream:
            self._source_report.report_workunit(wu)
            yield wu

        if (
            self._source_report.event_not_produced_warn
            and self._source_report.events_produced == 0
        ):
            self._source_report.warning(
                title="No metadata was produced by the source",
                message="Please check the source configuration, filters, and permissions.",
            )
