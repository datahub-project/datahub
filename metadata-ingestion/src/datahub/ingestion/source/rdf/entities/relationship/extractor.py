"""
Relationship Extractor

Extracts glossary term relationships from RDF graphs.
Only extracts skos:broader and skos:narrower (per spec).
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import Graph, Namespace, URIRef

from datahub.ingestion.source.rdf.entities.base import EntityExtractor
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    RDFRelationship,
    RelationshipType,
)

logger = logging.getLogger(__name__)

SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")


class RelationshipExtractor(EntityExtractor[RDFRelationship]):
    """
    Extracts term-to-term relationships from RDF graphs.

    Only extracts:
    - skos:broader (child → parent inheritance)
    - skos:narrower (parent → child inheritance)

    Does NOT extract (per specification):
    - skos:related
    - skos:exactMatch (only for field-to-term)
    - skos:closeMatch
    """

    @property
    def entity_type(self) -> str:
        return "relationship"

    def can_extract(self, graph: Graph, uri: URIRef) -> bool:
        """Check if this URI has extractable relationships."""
        for _ in graph.objects(uri, SKOS.broader):
            return True
        for _ in graph.objects(uri, SKOS.narrower):
            return True
        return False

    def extract(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] | None = None
    ) -> Optional[RDFRelationship]:
        """
        Extract a single relationship. Not typically used directly.
        Use extract_all or extract_for_term instead.
        """
        return None  # Relationships are extracted in bulk

    def extract_all(
        self, graph: Graph, context: Dict[str, Any] | None = None
    ) -> List[RDFRelationship]:
        """Extract all relationships from the RDF graph."""
        relationships = []
        seen = set()

        # Extract broader relationships
        for subject, _, obj in graph.triples((None, SKOS.broader, None)):
            if isinstance(subject, URIRef) and isinstance(obj, URIRef):
                rel_key = (str(subject), str(obj), "broader")
                if rel_key not in seen:
                    relationships.append(
                        RDFRelationship(
                            source_uri=str(subject),
                            target_uri=str(obj),
                            relationship_type=RelationshipType.BROADER,
                        )
                    )
                    seen.add(rel_key)

        # Extract narrower relationships
        for subject, _, obj in graph.triples((None, SKOS.narrower, None)):
            if isinstance(subject, URIRef) and isinstance(obj, URIRef):
                rel_key = (str(subject), str(obj), "narrower")
                if rel_key not in seen:
                    relationships.append(
                        RDFRelationship(
                            source_uri=str(subject),
                            target_uri=str(obj),
                            relationship_type=RelationshipType.NARROWER,
                        )
                    )
                    seen.add(rel_key)

        logger.info(f"Extracted {len(relationships)} relationships")
        return relationships

    def extract_for_term(self, graph: Graph, term_uri: URIRef) -> List[RDFRelationship]:
        """
        Extract relationships for a specific glossary term.

        Args:
            graph: The RDF graph
            term_uri: The URI of the term

        Returns:
            List of relationships where this term is the source
        """
        relationships = []

        # Only broader and narrower are supported
        relationship_mappings = {
            SKOS.broader: RelationshipType.BROADER,
            SKOS.narrower: RelationshipType.NARROWER,
        }

        for predicate, rel_type in relationship_mappings.items():
            for obj in graph.objects(term_uri, predicate):
                if isinstance(obj, URIRef):
                    relationships.append(
                        RDFRelationship(
                            source_uri=str(term_uri),
                            target_uri=str(obj),
                            relationship_type=rel_type,
                        )
                    )

        return relationships
