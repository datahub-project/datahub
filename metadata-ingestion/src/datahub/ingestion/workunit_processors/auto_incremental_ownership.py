from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)


class AutoIncrementalOwnershipProcessor(WorkunitProcessor):
    """Convert ownership aspects to incremental patches when incremental_ownership is enabled."""

    NAME = "auto_incremental_ownership"

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return bool(getattr(ctx.source_config, "incremental_ownership", False))

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.incremental_ownership_helper import (
            auto_incremental_ownership,
        )

        return auto_incremental_ownership(incremental_ownership=True, stream=stream)
