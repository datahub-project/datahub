"""
Relationship Converter

Converts RDF relationships to DataHub format.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.rdf.entities.base import EntityConverter
from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,
)
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RDFRelationship,
)

logger = logging.getLogger(__name__)


class RelationshipConverter(EntityConverter[RDFRelationship, DataHubRelationship]):
    """
    Converts RDF relationships to DataHub relationships.

    Handles URN generation for source and target terms.
    """

    def __init__(self, urn_generator: GlossaryTermUrnGenerator | None = None):
        """
        Initialize the converter.

        Args:
            urn_generator: URN generator for creating DataHub URNs (uses GlossaryTermUrnGenerator for term URNs)
        """
        self.urn_generator = urn_generator or GlossaryTermUrnGenerator()

    @property
    def entity_type(self) -> str:
        return "relationship"

    def convert(
        self, rdf_rel: RDFRelationship, context: Dict[str, Any] | None = None
    ) -> Optional[DataHubRelationship]:
        """Convert a single RDF relationship to DataHub format."""
        try:
            source_urn = self.urn_generator.generate_glossary_term_urn(
                rdf_rel.source_uri
            )
            target_urn = self.urn_generator.generate_glossary_term_urn(
                rdf_rel.target_uri
            )

            return DataHubRelationship(
                source_urn=source_urn,
                target_urn=target_urn,
                relationship_type=rdf_rel.relationship_type,
                properties=rdf_rel.properties or {},
            )

        except Exception as e:
            logger.warning(f"Error converting relationship: {e}")
            return None

    def convert_all(
        self,
        rdf_relationships: List[RDFRelationship],
        context: Dict[str, Any] | None = None,
    ) -> List[DataHubRelationship]:
        """Convert all RDF relationships to DataHub format."""
        datahub_relationships = []
        seen = set()

        for rdf_rel in rdf_relationships:
            datahub_rel = self.convert(rdf_rel, context)
            if datahub_rel:
                # Deduplicate
                rel_key = (
                    datahub_rel.source_urn,
                    datahub_rel.target_urn,
                    datahub_rel.relationship_type,
                )
                if rel_key not in seen:
                    datahub_relationships.append(datahub_rel)
                    seen.add(rel_key)

        logger.info(f"Converted {len(datahub_relationships)} relationships")
        return datahub_relationships
