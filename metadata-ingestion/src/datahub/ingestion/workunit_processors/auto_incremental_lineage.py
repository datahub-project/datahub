from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)


class AutoIncrementalLineageProcessor(WorkunitProcessor):
    """Convert lineage aspects to incremental patches when incremental_lineage is enabled."""

    NAME = "auto_incremental_lineage"

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return bool(getattr(ctx.source_config, "incremental_lineage", False))

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.incremental_lineage_helper import (
            auto_incremental_lineage,
        )

        return auto_incremental_lineage(incremental_lineage=True, stream=stream)
