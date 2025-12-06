"""
Glossary Term Converter

Converts RDF AST glossary terms to DataHub AST format.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.rdf.entities.base import EntityConverter
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
    RDFGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,
)

# Lazy import to avoid circular dependency with relationship module
# Import relationship types only when needed

logger = logging.getLogger(__name__)


class GlossaryTermConverter(EntityConverter[RDFGlossaryTerm, DataHubGlossaryTerm]):
    """
    Converts RDF glossary terms to DataHub glossary terms.

    Handles:
    - URN generation from IRIs
    - Path segment extraction for domain hierarchy
    - Custom property mapping (SKOS metadata)
    - Relationship conversion
    """

    def __init__(self, urn_generator: GlossaryTermUrnGenerator | None = None):
        """
        Initialize the converter.

        Args:
            urn_generator: URN generator for creating DataHub URNs
        """
        self.urn_generator = urn_generator or GlossaryTermUrnGenerator()

    @property
    def entity_type(self) -> str:
        return "glossary_term"

    def convert(
        self, rdf_term: RDFGlossaryTerm, context: Dict[str, Any] | None = None
    ) -> Optional[DataHubGlossaryTerm]:
        """
        Convert an RDF glossary term to DataHub format.

        Per specification Section 3.7.2, custom properties include:
        - skos:notation → customProperties
        - skos:scopeNote → customProperties
        - skos:altLabel → customProperties (array)
        - skos:hiddenLabel → customProperties (array)
        """
        try:
            # Generate DataHub URN
            term_urn = self.urn_generator.generate_glossary_term_urn(rdf_term.uri)

            # Convert relationships to dictionary format
            relationships = self._convert_relationships(rdf_term.relationships)

            # Parse IRI path into segments for domain hierarchy (as tuple for consistency)
            path_segments = list(
                self.urn_generator.derive_path_from_iri(rdf_term.uri, include_last=True)
            )

            # Build custom properties including SKOS-specific properties
            custom_props = dict(rdf_term.custom_properties)

            # Ensure original IRI is preserved
            if "rdf:originalIRI" not in custom_props:
                custom_props["rdf:originalIRI"] = rdf_term.uri

            # Add SKOS properties per spec Section 3.7.2
            if rdf_term.notation:
                custom_props["skos:notation"] = rdf_term.notation

            if rdf_term.scope_note:
                custom_props["skos:scopeNote"] = rdf_term.scope_note

            if rdf_term.alternative_labels:
                custom_props["skos:altLabel"] = ",".join(rdf_term.alternative_labels)

            if rdf_term.hidden_labels:
                custom_props["skos:hiddenLabel"] = ",".join(rdf_term.hidden_labels)

            return DataHubGlossaryTerm(
                urn=term_urn,
                name=rdf_term.name,
                definition=rdf_term.definition,
                source=rdf_term.uri,  # Use original IRI as source reference
                relationships=relationships,
                custom_properties=custom_props,
                path_segments=path_segments,
            )

        except Exception as e:
            logger.warning(f"Error converting glossary term {rdf_term.name}: {e}")
            return None

    def convert_all(
        self, rdf_terms: List[RDFGlossaryTerm], context: Dict[str, Any] | None = None
    ) -> List[DataHubGlossaryTerm]:
        """Convert all RDF glossary terms to DataHub format."""
        datahub_terms = []

        for rdf_term in rdf_terms:
            datahub_term = self.convert(rdf_term, context)
            if datahub_term:
                datahub_terms.append(datahub_term)
                logger.debug(f"Converted glossary term: {datahub_term.name}")

        logger.info(f"Converted {len(datahub_terms)} glossary terms")
        return datahub_terms

    def collect_relationships(
        self, rdf_terms: List[RDFGlossaryTerm], context: Dict[str, Any] | None = None
    ) -> Dict[str, List[str]]:
        # Lazy import to avoid circular dependency
        from datahub.ingestion.source.rdf.entities.relationship.ast import (
            DataHubRelationship,
        )

        """
        Collect all relationships from glossary terms as DataHubRelationship objects.

        This is used to populate the global relationships list in the DataHub AST.
        """
        all_relationships = []
        seen = set()

        for rdf_term in rdf_terms:
            for rdf_rel in rdf_term.relationships:
                try:
                    source_urn = self.urn_generator.generate_glossary_term_urn(
                        rdf_rel.source_uri
                    )
                    target_urn = self.urn_generator.generate_glossary_term_urn(
                        rdf_rel.target_uri
                    )

                    # Deduplicate
                    rel_key = (source_urn, target_urn, rdf_rel.relationship_type)
                    if rel_key in seen:
                        continue
                    seen.add(rel_key)

                    datahub_rel = DataHubRelationship(
                        source_urn=source_urn,
                        target_urn=target_urn,
                        relationship_type=rdf_rel.relationship_type,
                        properties=rdf_rel.properties,
                    )
                    all_relationships.append(datahub_rel)

                except Exception as e:
                    logger.warning(
                        f"Failed to convert relationship from term {rdf_term.uri}: {e}"
                    )

        if all_relationships:
            logger.info(
                f"Collected {len(all_relationships)} relationships from glossary terms"
            )

        return all_relationships

    def _convert_relationships(
        self, rdf_relationships: List[Any]
    ) -> Dict[str, List[str]]:
        """
        Convert RDF relationships to DataHub dictionary format.

        Only supports broader and narrower.
        """
        # Lazy import to avoid circular dependency
        from datahub.ingestion.source.rdf.entities.relationship.ast import (
            RelationshipType,
        )

        relationships = {"broader": [], "narrower": []}

        for rel in rdf_relationships:
            target_urn = self.urn_generator.generate_glossary_term_urn(rel.target_uri)

            if rel.relationship_type == RelationshipType.BROADER:
                relationships["broader"].append(target_urn)
            elif rel.relationship_type == RelationshipType.NARROWER:
                relationships["narrower"].append(target_urn)

        return relationships
