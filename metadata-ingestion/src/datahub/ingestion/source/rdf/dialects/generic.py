#!/usr/bin/env python3
"""
Generic RDF Dialect implementation.

This dialect handles mixed or unknown RDF modeling approaches.
"""

from typing import Optional

from rdflib import RDF, RDFS, Graph, URIRef
from rdflib.namespace import OWL, SKOS

from datahub.ingestion.source.rdf.dialects.base import RDFDialect, RDFDialectInterface


class GenericDialect(RDFDialectInterface):
    """Generic dialect for mixed or unknown RDF modeling approaches."""

    @property
    def dialect_type(self) -> RDFDialect:
        """Return the dialect type."""
        return RDFDialect.GENERIC

    def detect(self, graph: Graph) -> bool:
        """
        Generic dialect is the fallback - always returns True.

        Args:
            graph: RDFLib Graph to analyze

        Returns:
            Always True (fallback dialect)
        """
        return True

    def matches_subject(self, graph: Graph, subject: URIRef) -> bool:
        """
        Check if a specific subject matches generic dialect.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to check

        Returns:
            True if the subject matches generic dialect (fallback)
        """
        # Generic: matches any subject that looks like glossary term or structured property
        return self.looks_like_glossary_term(
            graph, subject
        ) or self.looks_like_structured_property(graph, subject)

    def classify_entity_type(self, graph: Graph, subject: URIRef) -> Optional[str]:
        """
        Classify the entity type using generic rules (try both patterns).

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to classify

        Returns:
            Entity type string or None if not applicable
        """
        # Generic: try both patterns
        if self.looks_like_glossary_term(graph, subject):
            return "glossary_term"
        elif self.looks_like_structured_property(graph, subject):
            return "structured_property"

        return None

    def looks_like_glossary_term(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like a glossary term (generic approach).

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to check

        Returns:
            True if the URI looks like a glossary term
        """
        # Must have a label
        has_label = self._has_label(graph, uri)
        if not has_label:
            return False

        # Check for SKOS Concept
        is_skos_concept = (uri, RDF.type, SKOS.Concept) in graph
        if is_skos_concept:
            # Exclude if it has any ontology construct types
            ontology_types = [
                OWL.Ontology,
                RDF.Property,
                OWL.ObjectProperty,
                OWL.DatatypeProperty,
                OWL.FunctionalProperty,
                RDFS.Class,
                OWL.Class,
            ]

            has_ontology_type = any(
                (uri, RDF.type, ontology_type) in graph
                for ontology_type in ontology_types
            )
            if has_ontology_type:
                return False

            return True

        # Check for OWL Class with label
        is_owl_class = (uri, RDF.type, OWL.Class) in graph
        if is_owl_class:
            # Exclude ontology construct types
            ontology_types = [
                OWL.Ontology,
                RDF.Property,
                OWL.ObjectProperty,
                OWL.DatatypeProperty,
                OWL.FunctionalProperty,
                RDFS.Class,
            ]

            has_ontology_type = any(
                (uri, RDF.type, ontology_type) in graph
                for ontology_type in ontology_types
            )
            if has_ontology_type:
                return False

            return True

        return False

    def looks_like_structured_property(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like a structured property (generic approach).

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to check

        Returns:
            True if the URI looks like a structured property
        """
        # Prioritize owl:ObjectProperty as the primary identifier for structured properties
        property_indicators = [OWL.ObjectProperty, OWL.DatatypeProperty, RDF.Property]

        for indicator in property_indicators:
            if (uri, RDF.type, indicator) in graph:
                return True

        return False

    def _has_label(self, graph: Graph, uri: URIRef) -> bool:
        """Check if a URI has a label."""
        # Check for RDFS labels
        if (uri, RDFS.label, None) in graph:
            return True

        # Check for SKOS labels
        skos_labels = [SKOS.prefLabel, SKOS.altLabel, SKOS.hiddenLabel]
        for label_predicate in skos_labels:
            if (uri, label_predicate, None) in graph:
                return True

        return False
