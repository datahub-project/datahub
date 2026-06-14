import logging
from dataclasses import dataclass
from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.utilities.urns.urn_iter import lowercase_dataset_urns

logger = logging.getLogger(__name__)


@dataclass
class AutoLowercaseUrnsProcessorReport(WorkunitProcessorReport):
    """Report for AutoLowercaseUrnsProcessor metrics."""

    num_exceptions: int = 0  # Failed to lowercase URNs


class AutoLowercaseUrnsProcessor(WorkunitProcessor[AutoLowercaseUrnsProcessorReport]):
    """Lowercase all dataset URNs in the stream."""

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return bool(getattr(ctx.source_config, "convert_urns_to_lowercase", False))

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """Lowercase all dataset urns"""
        for wu in stream:
            try:
                old_urn = wu.get_urn()
                lowercase_dataset_urns(wu.metadata)
                wu.id = wu.id.replace(old_urn, wu.get_urn())

                yield wu
            except Exception as e:
                self.report.num_exceptions += 1
                logger.warning(f"Failed to lowercase urns for {wu}: {e}", exc_info=True)
                yield wu
