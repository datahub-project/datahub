from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)


class AutoLowercaseUrnsProcessor(WorkunitProcessor):
    """Lowercase all dataset URNs in the stream."""

    NAME = "auto_lowercase_urns"

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return bool(getattr(ctx.source_config, "convert_urns_to_lowercase", False))

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.source_helpers import auto_lowercase_urns

        return auto_lowercase_urns(stream)
