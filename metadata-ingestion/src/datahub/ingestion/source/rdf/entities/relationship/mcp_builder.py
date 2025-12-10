"""
Relationship MCP Builder

Creates DataHub MCPs for glossary term relationships.
Creates only inheritance relationships (isRelatedTerms).
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

    Handles both skos:broader and skos:narrower relationships.
    Creates only inheritance relationships (isRelatedTerms):
    - Both broader and narrower normalize to child → parent inheritance
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

        Handles both broader and narrower relationships, creating only inheritance
        relationships (isRelatedTerms) in DataHub.

        Both broader and narrower are normalized to child → parent inheritance:
        - broader: child → parent → child inherits from parent
        - narrower: parent → child → normalize to child → parent (child inherits from parent)
        """
        mcps = []

        # Normalize relationships: both broader and narrower create child → parent inheritance
        # broader: child → parent means child inherits from parent
        # narrower: parent → child means normalize to child → parent (child inherits from parent)
        # Map: child_urn -> [parent_urns] (for isRelatedTerms only)
        is_related_map: Dict[str, List[str]] = {}

        for rel in relationships:
            if rel.relationship_type == RelationshipType.BROADER:
                # broader: child → parent
                child_urn = str(rel.source_urn)
                parent_urn = str(rel.target_urn)

                # Child inherits from parent
                if child_urn not in is_related_map:
                    is_related_map[child_urn] = []
                is_related_map[child_urn].append(parent_urn)

            elif rel.relationship_type == RelationshipType.NARROWER:
                # narrower: parent → child (normalize to child → parent)
                parent_urn = str(rel.source_urn)
                child_urn = str(rel.target_urn)

                # Child inherits from parent (normalized direction)
                if child_urn not in is_related_map:
                    is_related_map[child_urn] = []
                is_related_map[child_urn].append(parent_urn)

        # Create isRelatedTerms MCPs (child → parent, inheritance only)
        for child_urn, parent_urns in is_related_map.items():
            try:
                unique_parents = list(set(parent_urns))  # Deduplicate

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=child_urn,
                    aspect=GlossaryRelatedTermsClass(isRelatedTerms=unique_parents),
                )
                mcps.append(mcp)

                logger.debug(
                    f"Created isRelatedTerms MCP for {child_urn} with {len(unique_parents)} parent terms"
                )

            except Exception as e:
                logger.error(
                    f"Failed to create isRelatedTerms MCP for {child_urn}: {e}"
                )

        logger.info(f"Built {len(mcps)} relationship MCPs (isRelatedTerms only)")
        return mcps
