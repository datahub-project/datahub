import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional, Set

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import GlossaryTermKeyClass, TagKeyClass
from datahub.metadata.urns import GlossaryTermUrn, TagUrn, Urn
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


def find_existing_tags_terms(
    graph: Optional["DataHubGraph"], urns: Set[str]
) -> Optional[Set[str]]:
    """Return the subset of tag/term urns that already exist in DataHub.

    Re-emitting a key aspect for an existing tag/term is an UPSERT on that entity,
    which needs edit rights on it. Skipping existing ones lets a token scoped to its
    own assets attach existing tags/terms. Without a graph nothing is treated as
    existing; if the lookup fails, returns None so callers can count the failure and
    materialize everything as before.
    """
    if graph is None or not urns:
        return set()
    existing: Set[str] = set()
    try:
        for entity_type, key_aspect in (
            (TagUrn.ENTITY_TYPE, TagKeyClass.ASPECT_NAME),
            (GlossaryTermUrn.ENTITY_TYPE, GlossaryTermKeyClass.ASPECT_NAME),
        ):
            batch: List[str] = sorted(
                u for u in urns if guess_entity_type(u) == entity_type
            )
            if batch:
                # One unchunked request per entity type; a failure (e.g. a
                # request-size limit) falls back to materializing everything.
                existing.update(
                    graph.get_entities(entity_type, batch, aspects=[key_aspect])
                )
    except Exception as e:
        logger.warning(
            f"Failed to check which tags/terms already exist; "
            f"materializing all of them: {e}"
        )
        return None
    return existing & urns


@dataclass
class AutoMaterializeReferencedTagsTermsProcessorReport(WorkunitProcessorReport):
    """Report for AutoMaterializeReferencedTagsTermsProcessor metrics."""

    num_exceptions: int = 0  # Invalid URNs that couldn't be materialized
    # Referenced tags/terms not re-emitted because they already exist in DataHub.
    num_skipped_existing: int = 0
    # Existence lookups that failed, so every referenced tag/term was emitted.
    num_existence_check_failures: int = 0


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

        missing = referenced_tags - tags_with_aspects
        existing = find_existing_tags_terms(self.ctx.pipeline_context.graph, missing)
        if existing is None:
            self.report.num_existence_check_failures += 1
            existing = set()
        self.report.num_skipped_existing += len(existing)

        for urn in sorted(missing - existing):
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
