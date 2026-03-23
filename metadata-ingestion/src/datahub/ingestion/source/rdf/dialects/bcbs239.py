#!/usr/bin/env python3
"""
Default RDF Dialect implementation.

This dialect handles SKOS-based business glossaries used in regulatory reporting.
"""

from typing import Optional

from rdflib import RDF, RDFS, Graph, URIRef
from rdflib.namespace import OWL, SKOS

from datahub.ingestion.source.rdf.dialects.base import RDFDialect, RDFDialectInterface


class DefaultDialect(RDFDialectInterface):
    """Default dialect for SKOS-based business glossaries."""

    @property
    def dialect_type(self) -> RDFDialect:
        """Return the dialect type."""
        return RDFDialect.DEFAULT

    def detect(self, graph: Graph) -> bool:
        """
        Detect if this is a default-style graph (SKOS-heavy).

        Args:
            graph: RDFLib Graph to analyze

        Returns:
            True if this dialect matches the graph
        """
        # Count different patterns
        skos_concepts = len(list(graph.subjects(RDF.type, SKOS.Concept)))
        owl_classes = len(list(graph.subjects(RDF.type, OWL.Class)))

        # Default: SKOS-heavy (more SKOS Concepts than OWL Classes)
        return skos_concepts > 0 and skos_concepts > owl_classes

    def classify_entity_type(self, graph: Graph, subject: URIRef) -> Optional[str]:
        """
        Classify the entity type using default rules.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to classify

        Returns:
            Entity type string or None if not applicable
        """
        # Default: SKOS Concepts are glossary terms
        if self.looks_like_glossary_term(graph, subject):
            return "glossary_term"

        return None

    def looks_like_glossary_term(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like a SKOS glossary term (default style).

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

        # Must be a SKOS Concept
        is_skos_concept = (uri, RDF.type, SKOS.Concept) in graph
        if not is_skos_concept:
            return False

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
            (uri, RDF.type, ontology_type) in graph for ontology_type in ontology_types
        )
        if has_ontology_type:
            return False

        return True

    def matches_subject(self, graph: Graph, subject: URIRef) -> bool:
        """
        Check if a specific subject matches default dialect.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to check

        Returns:
            True if the subject matches default dialect
        """
        # Default: SKOS Concept with label
        return self.looks_like_glossary_term(graph, subject)

    def looks_like_structured_property(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like a structured property (BCBS239 style).

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
        # Check for SKOS labels
        skos_labels = [SKOS.prefLabel, SKOS.altLabel, SKOS.hiddenLabel]
        for label_predicate in skos_labels:
            if (uri, label_predicate, None) in graph:
                return True

        # Check for RDFS labels
        if (uri, RDFS.label, None) in graph:
            return True

        return False
