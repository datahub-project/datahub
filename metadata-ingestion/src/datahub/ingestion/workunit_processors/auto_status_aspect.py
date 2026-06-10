from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessor


class AutoStatusAspectProcessor(WorkunitProcessor):
    """Add status aspect (removed=False) to entities that don't have one."""

    NAME = "auto_status_aspect"

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.source_helpers import auto_status_aspect

        return auto_status_aspect(stream)
