"""
Relationship MCP Builder

Creates DataHub MCPs for ontology-routed glossary term relationships.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubNativeRelationship,
    DataHubRelationship,
)
from datahub.metadata.schema_classes import GlossaryRelatedTermsClass

logger = logging.getLogger(__name__)


class RelationshipMCPBuilder(EntityMCPBuilder[DataHubNativeRelationship]):
    """Creates MCPs for aligned native glossary relationships."""

    @property
    def entity_type(self) -> str:
        return "relationship"

    def build_mcps(
        self,
        relationship: DataHubNativeRelationship,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        return []

    def build_all_mcps(
        self,
        relationships: List[DataHubNativeRelationship],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        if not relationships:
            return self._build_legacy_mcps(context)

        grouped: Dict[tuple[str, str], List[str]] = defaultdict(list)
        provenance: Dict[tuple[str, str, str], Dict[str, str]] = {}

        for rel in relationships:
            key = (rel.source_urn, rel.field)
            grouped[key].append(rel.target_urn)
            prov_key = (rel.source_urn, rel.target_urn, rel.field)
            provenance[prov_key] = {
                "originalPredicateIri": rel.original_predicate_iri,
                "mappingClass": rel.mapping_class.value,
                "mapsTo": rel.maps_to,
            }

        mcps: List[MetadataChangeProposalWrapper] = []
        for (source_urn, field), targets in grouped.items():
            unique_targets = sorted(set(targets))
            aspect_kwargs: Dict[str, List[str]] = {field: unique_targets}
            try:
                mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=source_urn,
                        aspect=GlossaryRelatedTermsClass(**aspect_kwargs),
                    )
                )
            except (ValueError, RuntimeError, AttributeError) as exc:
                logger.error(
                    "Failed to create glossaryRelatedTerms MCP for %s: %s",
                    source_urn,
                    exc,
                )

        if context is not None:
            context["relationship_provenance"] = provenance

        logger.info("Built %d native relationship MCPs", len(mcps))
        return mcps

    def _build_legacy_mcps(
        self, context: Optional[Dict[str, Any]]
    ) -> List[MetadataChangeProposalWrapper]:
        """Support legacy DataHubRelationship objects during transition."""
        if context is None:
            return []
        datahub_graph = context.get("datahub_graph")
        if datahub_graph is None:
            return []
        legacy = [
            DataHubRelationship.from_native(rel)
            for rel in getattr(datahub_graph, "native_relationships", [])
        ]
        legacy = [rel for rel in legacy if rel is not None]
        if not legacy:
            return []

        is_related_map: Dict[str, List[str]] = defaultdict(list)
        for rel in legacy:
            is_related_map[str(rel.source_urn)].append(str(rel.target_urn))

        mcps = []
        for source_urn, targets in is_related_map.items():
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=source_urn,
                    aspect=GlossaryRelatedTermsClass(
                        isRelatedTerms=sorted(set(targets))
                    ),
                )
            )
        return mcps
