from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)


class AutoFixDuplicateSchemaFieldPathsProcessor(WorkunitProcessor):
    """Remove duplicate field paths from schema metadata aspects."""

    NAME = "auto_fix_duplicate_schema_field_paths"

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self._platform = ctx.infer_platform()

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.source_helpers import (
            auto_fix_duplicate_schema_field_paths,
        )

        return auto_fix_duplicate_schema_field_paths(stream, platform=self._platform)
