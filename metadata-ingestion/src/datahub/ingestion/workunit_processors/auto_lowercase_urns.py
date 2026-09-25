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
        # convert_urns_to_lowercase affects dataset identity, so any change in
        # how the flag is evaluated changes URNs and breaks previously-emitted
        # entities. We deliberately restore the previous (buggy) behavior — gate
        # on the value explicitly present in the recipe rather than the parsed
        # config's default-applied value — to avoid that. The correct,
        # default-honoring check (to restore once it can be done identity-safely)
        # would be:
        #     return bool(getattr(ctx.source_config, "convert_urns_to_lowercase", False))
        pipeline_config = getattr(ctx.pipeline_context, "pipeline_config", None)
        raw_config = getattr(getattr(pipeline_config, "source", None), "config", None)
        # Only what the recipe explicitly set; None means the key is absent.
        recipe_value = (
            raw_config.get("convert_urns_to_lowercase")
            if isinstance(raw_config, dict)
            else getattr(raw_config, "convert_urns_to_lowercase", None)
        )

        if recipe_value is None and getattr(
            ctx.source_config, "convert_urns_to_lowercase", False
        ):
            ctx.source_report.warning(
                title="URN lowercasing not applied",
                message="convert_urns_to_lowercase defaults to true for this source "
                "but is not set in the recipe; leaving it disabled to preserve "
                "existing dataset URNs. Set it explicitly in the recipe to enable.",
            )

        return bool(recipe_value)

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
