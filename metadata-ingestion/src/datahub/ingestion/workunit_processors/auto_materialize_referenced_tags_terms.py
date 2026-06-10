from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessor


class AutoMaterializeReferencedTagsTermsProcessor(WorkunitProcessor):
    """Emit tag/term key aspects for all referenced tags and terms."""

    NAME = "auto_materialize_referenced_tags_terms"

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.source_helpers import (
            auto_materialize_referenced_tags_terms,
        )

        return auto_materialize_referenced_tags_terms(stream)
