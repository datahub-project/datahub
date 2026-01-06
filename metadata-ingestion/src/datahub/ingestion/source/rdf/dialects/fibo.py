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

    def __init__(self, include_provisional: bool = False):
        """
        Initialize FIBO dialect.

        Args:
            include_provisional: If True, include terms with provisional/work-in-progress status.
                                If False (default), exclude terms that haven't been fully approved.
        """
        self.include_provisional = include_provisional

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

        # Filter by workflow status if include_provisional is False
        if not self.include_provisional:
            if self._has_provisional_maturity_level(graph, uri):
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

    def extract_custom_properties(self, graph: Graph, uri: URIRef) -> dict:
        """
        Extract FIBO-specific custom properties from a URI.

        Extracts properties commonly used in FIBO ontologies:
        - cmns-av:adaptedFrom, cmns-av:explanatoryNote, cmns-av:synonym
        - rdfs:isDefinedBy (ontology URI - parent domain only)
        - dcterms:source (source references)
        - skos:example, skos:changeNote
        - owl:versionInfo
        - fibo:termIRI (full term IRI - critical for reverse export)

        Note: rdfs:isDefinedBy only provides the ontology URI, not the full term IRI.
        The full term IRI is stored as fibo:termIRI for reverse export purposes.

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to extract properties from (full term IRI)

        Returns:
            Dictionary of FIBO-specific custom properties
        """
        from rdflib import Literal, URIRef as URIRefType
        from rdflib.namespace import DCTERMS, OWL, RDFS, SKOS

        properties = {}

        # FIBO namespaces
        CMNS_AV = "https://www.omg.org/spec/Commons/AnnotationVocabulary/"

        # Single-value properties
        single_value_predicates = {
            f"{CMNS_AV}adaptedFrom": "fibo:adaptedFrom",
            str(OWL.versionInfo): "fibo:version",
        }

        for predicate_uri, prop_name in single_value_predicates.items():
            predicate = URIRefType(predicate_uri)
            for obj in graph.objects(uri, predicate):
                if obj:
                    properties[prop_name] = str(obj)
                    break  # Take first value only

        # Multi-value properties (join with semicolon)
        multi_value_predicates = {
            f"{CMNS_AV}explanatoryNote": "fibo:explanatoryNote",
            f"{CMNS_AV}synonym": "fibo:synonym",
            str(DCTERMS.source): "fibo:source",
            str(SKOS.example): "fibo:example",
            str(SKOS.changeNote): "fibo:changeNote",
        }

        for predicate_uri, prop_name in multi_value_predicates.items():
            predicate = URIRefType(predicate_uri)
            values = []
            for obj in graph.objects(uri, predicate):
                if obj:
                    # Handle both literals and URIs
                    if isinstance(obj, Literal):
                        values.append(str(obj))
                    else:
                        values.append(str(obj))
            if values:
                # Join multiple values with semicolon for readability
                properties[prop_name] = "; ".join(values)

        # rdfs:isDefinedBy - ontology URI (useful for sourceUrl mapping)
        is_defined_by_values = []
        for obj in graph.objects(uri, RDFS.isDefinedBy):
            if obj:
                is_defined_by_values.append(str(obj))
        if is_defined_by_values:
            # If multiple, use the first one (typically there's only one)
            properties["fibo:isDefinedBy"] = is_defined_by_values[0]
            # If multiple, also store all as a list
            if len(is_defined_by_values) > 1:
                properties["fibo:isDefinedByAll"] = "; ".join(is_defined_by_values)

        # Full term IRI - critical for reverse export
        # rdfs:isDefinedBy only gives the ontology URI, not the full term IRI
        properties["fibo:termIRI"] = str(uri)

        return properties

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

    def _has_provisional_maturity_level(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if a URI has provisional/work-in-progress status.

        For FIBO ontologies, this checks for 'Provisional' maturity level.
        Other ontologies may use similar workflow status properties.

        Args:
            graph: RDFLib Graph containing the URI
            uri: URIRef to check

        Returns:
            True if the URI has a provisional status (e.g., fibo-fnd-utl-av:hasMaturityLevel
            with value fibo-fnd-utl-av:Provisional)
        """
        from rdflib import URIRef as URIRefType

        # FIBO maturity level namespace
        FIBO_UTL_AV = "https://www.omg.org/spec/Commons/AnnotationVocabulary/"
        maturity_level_predicate = URIRefType(f"{FIBO_UTL_AV}hasMaturityLevel")
        provisional_value = URIRefType(f"{FIBO_UTL_AV}Provisional")

        # Check if this URI has the maturity level property set to Provisional
        for obj in graph.objects(uri, maturity_level_predicate):
            if obj == provisional_value:
                return True

        return False
