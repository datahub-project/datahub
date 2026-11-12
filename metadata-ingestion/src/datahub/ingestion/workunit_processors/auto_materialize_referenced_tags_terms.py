import logging
from dataclasses import dataclass
from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorReport,
)
from datahub.metadata.urns import GlossaryTermUrn, TagUrn, Urn
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns

logger = logging.getLogger(__name__)


@dataclass
class AutoMaterializeReferencedTagsTermsProcessorReport(WorkunitProcessorReport):
    """Report for AutoMaterializeReferencedTagsTermsProcessor metrics."""

    num_exceptions: int = 0  # Invalid URNs that couldn't be materialized


class AutoMaterializeReferencedTagsTermsProcessor(
    WorkunitProcessor[AutoMaterializeReferencedTagsTermsProcessorReport]
):
    """Emit tag/term key aspects for all referenced tags and terms."""

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """For all references to tags/terms, emit a tag/term key aspect to ensure that the tag exists in our backend."""

        urn_entity_types = [TagUrn.ENTITY_TYPE, GlossaryTermUrn.ENTITY_TYPE]

        # Note: this code says "tags", but it applies to both tags and terms.

        referenced_tags = set()
        tags_with_aspects = set()

        for wu in stream:
            for urn in list_urns(wu.metadata):
                if guess_entity_type(urn) in urn_entity_types:
                    referenced_tags.add(urn)

            urn = wu.get_urn()
            if guess_entity_type(urn) in urn_entity_types:
                tags_with_aspects.add(urn)

            yield wu

        for urn in sorted(referenced_tags - tags_with_aspects):
            try:
                urn_tp = Urn.from_string(urn)
                assert isinstance(urn_tp, (TagUrn, GlossaryTermUrn))

                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=urn_tp.to_key_aspect(),
                ).as_workunit()
            except InvalidUrnError:
                self.report.num_exceptions += 1
                logger.warning(
                    f"Source produced an invalid urn, so no key aspect will be generated: {urn}"
                )
