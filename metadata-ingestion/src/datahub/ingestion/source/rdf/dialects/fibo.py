#!/usr/bin/env python3
"""
FIBO RDF Dialect implementation.

This dialect handles OWL-based formal ontologies used in financial domain modeling.
"""

from typing import Optional

from rdflib import RDF, RDFS, Graph, URIRef
from rdflib.namespace import OWL, SKOS

from datahub.ingestion.source.rdf.dialects.base import RDFDialect, RDFDialectInterface


class FIBODialect(RDFDialectInterface):
    """FIBO dialect for OWL-based formal ontologies."""

    @property
    def dialect_type(self) -> RDFDialect:
        """Return the dialect type."""
        return RDFDialect.FIBO

    def detect(self, graph: Graph) -> bool:
        """
        Detect if this is a FIBO-style graph (OWL-heavy).

        Args:
            graph: RDFLib Graph to analyze

        Returns:
            True if this dialect matches the graph
        """
        # Count different patterns
        owl_classes = len(list(graph.subjects(RDF.type, OWL.Class)))
        owl_properties = len(list(graph.subjects(RDF.type, OWL.ObjectProperty))) + len(
            list(graph.subjects(RDF.type, OWL.DatatypeProperty))
        )

        # FIBO: OWL-heavy with formal ontology structure
        return owl_classes > 0 and owl_properties > 0

    def classify_entity_type(self, graph: Graph, subject: URIRef) -> Optional[str]:
        """
        Classify the entity type using FIBO rules.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to classify

        Returns:
            Entity type string or None if not applicable
        """
        # FIBO: OWL Classes are glossary terms, OWL Properties are structured properties
        if self.looks_like_glossary_term(graph, subject):
            return "glossary_term"
        elif self.looks_like_structured_property(graph, subject):
            return "structured_property"

        return None

    def looks_like_glossary_term(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like an OWL glossary term (FIBO style).

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

        # Must be an OWL Class
        is_owl_class = (uri, RDF.type, OWL.Class) in graph
        if not is_owl_class:
            return False

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
            (uri, RDF.type, ontology_type) in graph for ontology_type in ontology_types
        )
        if has_ontology_type:
            return False

        return True

    def matches_subject(self, graph: Graph, subject: URIRef) -> bool:
        """
        Check if a specific subject matches FIBO dialect.

        Args:
            graph: RDFLib Graph containing the subject
            subject: URIRef to check

        Returns:
            True if the subject matches FIBO dialect
        """
        # FIBO: OWL Class with label or OWL Property
        return self.looks_like_glossary_term(
            graph, subject
        ) or self.looks_like_structured_property(graph, subject)

    def looks_like_structured_property(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI looks like an OWL property (FIBO style).

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to check

        Returns:
            True if the URI looks like a structured property
        """
        # Prioritize owl:ObjectProperty as the primary identifier for structured properties
        property_types = [
            OWL.ObjectProperty,
            OWL.DatatypeProperty,
            OWL.FunctionalProperty,
        ]

        for property_type in property_types:
            if (uri, RDF.type, property_type) in graph:
                return True

        return False

    def _has_label(self, graph: Graph, uri: URIRef) -> bool:
        """Check if a URI has a label."""
        # Check for RDFS labels (FIBO uses rdfs:label)
        if (uri, RDFS.label, None) in graph:
            return True

        # Check for SKOS labels as fallback
        skos_labels = [SKOS.prefLabel, SKOS.altLabel, SKOS.hiddenLabel]
        for label_predicate in skos_labels:
            if (uri, label_predicate, None) in graph:
                return True

        return False
