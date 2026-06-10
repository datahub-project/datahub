from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)


class AutoIncrementalPropertiesProcessor(WorkunitProcessor):
    """Convert dataset properties to incremental patches when incremental_properties is enabled."""

    NAME = "auto_incremental_properties"

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return bool(getattr(ctx.source_config, "incremental_properties", False))

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.incremental_properties_helper import (
            auto_incremental_properties,
        )

        return auto_incremental_properties(incremental_properties=True, stream=stream)
