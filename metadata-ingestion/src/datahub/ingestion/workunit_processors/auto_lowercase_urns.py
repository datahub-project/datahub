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
        # Gate on the value explicitly set in the recipe rather than the parsed
        # config's effective (default-applied) value. Enabling this from a
        # connector default (e.g. Snowflake defaults convert_urns_to_lowercase
        # to True) lowercases the platform_instance segment of dataset URNs,
        # which changes asset identity on upgrade and orphans previously
        # attached metadata (assertions, incidents, manual edits) for recipes
        # that never explicitly opted in. Restores the pre-#17852 behavior.
        pipeline_config = getattr(ctx.pipeline_context, "pipeline_config", None)
        if not (
            pipeline_config and pipeline_config.source and pipeline_config.source.config
        ):
            return False
        raw_config = pipeline_config.source.config
        return bool(
            (
                hasattr(raw_config, "convert_urns_to_lowercase")
                and raw_config.convert_urns_to_lowercase
            )
            or (
                hasattr(raw_config, "get")
                and raw_config.get("convert_urns_to_lowercase")
            )
        )

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
