from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessor


class AutoPatchLastModifiedProcessor(WorkunitProcessor):
    """Patch datasetProperties.lastModified from operation aspects when not set."""

    NAME = "auto_patch_last_modified"

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.auto_work_units.auto_dataset_properties_aspect import (
            auto_patch_last_modified,
        )

        return auto_patch_last_modified(stream)
