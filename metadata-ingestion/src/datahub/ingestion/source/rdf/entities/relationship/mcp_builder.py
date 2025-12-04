"""
Relationship MCP Builder

Creates DataHub MCPs for glossary term relationships.
Only creates isRelatedTerms (inherits) - not hasRelatedTerms (contains).
"""

import logging
from typing import Any, Dict, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RelationshipType,
)
from datahub.metadata.schema_classes import GlossaryRelatedTermsClass

logger = logging.getLogger(__name__)


class RelationshipMCPBuilder(EntityMCPBuilder[DataHubRelationship]):
    """
    Creates MCPs for glossary term relationships.

    Creates only isRelatedTerms MCPs for broader relationships.
    Per specification, hasRelatedTerms (contains) is NOT created for broader.
    """

    @property
    def entity_type(self) -> str:
        return "relationship"

    def build_mcps(
        self, relationship: DataHubRelationship, context: Dict[str, Any] | None = None
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for a single relationship.
        Relationships are typically built in bulk via build_all_mcps.
        """
        return []  # Individual relationships are aggregated

    def build_all_mcps(
        self,
        relationships: List[DataHubRelationship],
        context: Dict[str, Any] | None = None,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for all relationships.

        Aggregates relationships by source term and creates one MCP per term
        with all its broader relationships.

        Only creates isRelatedTerms (inherits) - not hasRelatedTerms (contains).
        """
        mcps = []

        # Aggregate broader relationships by child term
        broader_terms_map: Dict[str, List[str]] = {}  # child_urn -> [broader_term_urns]

        for rel in relationships:
            if rel.relationship_type == RelationshipType.BROADER:
                source = str(rel.source_urn)
                target = str(rel.target_urn)

                if source not in broader_terms_map:
                    broader_terms_map[source] = []
                broader_terms_map[source].append(target)

        # Create isRelatedTerms MCPs
        for child_urn, broader_urns in broader_terms_map.items():
            try:
                unique_broader = list(set(broader_urns))  # Deduplicate

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=child_urn,
                    aspect=GlossaryRelatedTermsClass(isRelatedTerms=unique_broader),
                )
                mcps.append(mcp)

                logger.debug(
                    f"Created isRelatedTerms MCP for {child_urn} with {len(unique_broader)} broader terms"
                )

            except Exception as e:
                logger.error(f"Failed to create MCP for {child_urn}: {e}")

        logger.info(f"Built {len(mcps)} relationship MCPs")
        return mcps
