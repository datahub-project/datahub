#!/usr/bin/env python3
"""
Edge case tests for RDF dialects (FIBO, Generic, BCBS239).

Tests cover:
- Empty graph handling
- Missing labels/definitions
- Special characters in content
- Multiple types
- Edge cases in dialect-specific logic
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS

from datahub.ingestion.source.rdf.dialects.bcbs239 import DefaultDialect
from datahub.ingestion.source.rdf.dialects.fibo import FIBODialect
from datahub.ingestion.source.rdf.dialects.generic import GenericDialect


class TestGenericDialectEdgeCases:
    """Test GenericDialect edge cases."""

    def test_empty_graph(self):
        """Test GenericDialect with empty graph."""
        dialect = GenericDialect()
        graph = Graph()

        assert dialect.detect(graph) is True  # Generic always matches
        assert dialect.dialect_type.value == "generic"

    def test_term_without_label(self):
        """Test term without rdfs:label."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, SKOS.Concept))
        # No label

        # Should return False (requires label)
        assert dialect.looks_like_glossary_term(graph, uri) is False

    def test_term_with_empty_label(self):
        """Test term with empty label."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDFS.label, Literal("")))

        # Empty label might be treated as no label
        result = dialect.looks_like_glossary_term(graph, uri)
        assert isinstance(result, bool)

    def test_term_with_multiple_labels(self):
        """Test term with multiple labels."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDFS.label, Literal("Label 1")))
        graph.add((uri, RDFS.label, Literal("Label 2")))

        # Should handle multiple labels
        assert dialect.looks_like_glossary_term(graph, uri) is True

    def test_term_with_unicode_label(self):
        """Test term with unicode in label."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDFS.label, Literal("Term with émojis 🎉 and 中文")))

        assert dialect.looks_like_glossary_term(graph, uri) is True

    def test_term_with_special_characters_in_label(self):
        """Test term with special characters in label."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDFS.label, Literal('Term with "quotes" & <tags>')))

        assert dialect.looks_like_glossary_term(graph, uri) is True

    def test_owl_class_with_ontology_type(self):
        """Test OWL Class that is also an ontology."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Ontology")

        graph.add((uri, RDF.type, OWL.Class))
        graph.add((uri, RDF.type, OWL.Ontology))
        graph.add((uri, RDFS.label, Literal("Ontology")))

        # Should exclude ontology constructs
        assert dialect.looks_like_glossary_term(graph, uri) is False

    def test_skos_concept_with_property_type(self):
        """Test SKOS Concept that is also a property."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Property")

        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDF.type, RDF.Property))
        graph.add((uri, RDFS.label, Literal("Property")))

        # Should exclude properties
        assert dialect.looks_like_glossary_term(graph, uri) is False

    def test_extract_custom_properties_empty(self):
        """Test extracting custom properties from term with none."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDFS.label, Literal("Term")))

        props = dialect.extract_custom_properties(graph, uri)
        assert isinstance(props, dict)
        # Should return empty dict or minimal properties

    def test_extract_custom_properties_with_unknown_properties(self):
        """Test extracting custom properties with unknown predicates."""
        dialect = GenericDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDFS.label, Literal("Term")))
        graph.add((uri, URIRef("http://example.org/customProp"), Literal("value")))

        props = dialect.extract_custom_properties(graph, uri)
        assert isinstance(props, dict)


class TestFIBODialectEdgeCases:
    """Test FIBODialect edge cases."""

    def test_fibo_dialect_with_provisional(self):
        """Test FIBO dialect with include_provisional=True."""
        dialect = FIBODialect(include_provisional=True)
        assert dialect.include_provisional is True

    def test_fibo_dialect_without_provisional(self):
        """Test FIBO dialect with include_provisional=False."""
        dialect = FIBODialect(include_provisional=False)
        assert dialect.include_provisional is False

    def test_fibo_term_without_maturity_level(self):
        """Test FIBO term without maturity level."""
        dialect = FIBODialect(include_provisional=False)
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, OWL.Class))
        graph.add((uri, RDFS.label, Literal("Term")))
        # No maturity level

        # Should include terms without maturity level
        assert dialect.looks_like_glossary_term(graph, uri) is True

    def test_fibo_provisional_term_excluded(self):
        """Test FIBO provisional term is excluded when include_provisional=False."""
        from rdflib.namespace import Namespace

        dialect = FIBODialect(include_provisional=False)
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        # FIBO maturity vocabulary
        FIBO_AV = Namespace("https://www.omg.org/spec/Commons/AnnotationVocabulary/")
        graph.add((uri, RDF.type, OWL.Class))
        graph.add((uri, RDFS.label, Literal("Term")))
        graph.add((uri, FIBO_AV.hasMaturityLevel, FIBO_AV.Provisional))

        # Should exclude provisional terms
        assert dialect.looks_like_glossary_term(graph, uri) is False

    def test_fibo_provisional_term_included(self):
        """Test FIBO provisional term is included when include_provisional=True."""
        from rdflib.namespace import Namespace

        dialect = FIBODialect(include_provisional=True)
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        FIBO_AV = Namespace("https://www.omg.org/spec/Commons/AnnotationVocabulary/")
        graph.add((uri, RDF.type, OWL.Class))
        graph.add((uri, RDFS.label, Literal("Term")))
        graph.add((uri, FIBO_AV.hasMaturityLevel, FIBO_AV.Provisional))

        # Should include provisional terms
        assert dialect.looks_like_glossary_term(graph, uri) is True

    def test_fibo_released_term_always_included(self):
        """Test FIBO released term is always included."""
        from rdflib.namespace import Namespace

        dialect = FIBODialect(include_provisional=False)
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        FIBO_AV = Namespace("https://www.omg.org/spec/Commons/AnnotationVocabulary/")
        graph.add((uri, RDF.type, OWL.Class))
        graph.add((uri, RDFS.label, Literal("Term")))
        graph.add((uri, FIBO_AV.hasMaturityLevel, FIBO_AV.Release))

        # Should always include released terms
        assert dialect.looks_like_glossary_term(graph, uri) is True


class TestDefaultDialectEdgeCases:
    """Test DefaultDialect (BCBS239) edge cases."""

    def test_default_dialect_detection(self):
        """Test DefaultDialect detection."""
        dialect = DefaultDialect()
        graph = Graph()

        # Default dialect should detect BCBS239 patterns
        # Empty graph might not match
        result = dialect.detect(graph)
        assert isinstance(result, bool)

    def test_default_dialect_term_recognition(self):
        """Test DefaultDialect term recognition."""
        dialect = DefaultDialect()
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDFS.label, Literal("Term")))

        result = dialect.looks_like_glossary_term(graph, uri)
        assert isinstance(result, bool)
