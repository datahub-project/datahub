from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)


class AutoWorkunitsReporterProcessor(WorkunitProcessor):
    """Report each workunit to the source report for metrics tracking."""

    NAME = "auto_workunit_reporter"

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self._source_report = ctx.source_report

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.source_helpers import auto_workunit_reporter

        return auto_workunit_reporter(self._source_report, stream)
