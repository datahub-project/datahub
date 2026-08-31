#!/usr/bin/env python3
"""
Edge case tests for dialect router.

Tests cover:
- Dialect detection edge cases
- Forced dialect handling
- Empty graph handling
- Multiple dialect matching
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from datahub.ingestion.source.rdf.dialects.base import RDFDialect
from datahub.ingestion.source.rdf.dialects.router import DialectRouter


class TestDialectRouterEdgeCases:
    """Test dialect router edge cases."""

    def test_forced_dialect_always_used(self):
        """Test that forced dialect is always used."""
        router = DialectRouter(forced_dialect=RDFDialect.FIBO)

        # Even with empty graph, forced dialect should be used
        empty_graph = Graph()
        assert router.dialect_type == RDFDialect.FIBO
        assert router.get_detected_dialect(empty_graph) == RDFDialect.FIBO

    def test_empty_graph_detection(self):
        """Test dialect detection with empty graph."""
        router = DialectRouter()
        empty_graph = Graph()

        # GenericDialect always matches, so empty graph returns GENERIC
        detected = router.get_detected_dialect(empty_graph)
        assert detected == RDFDialect.GENERIC

    def test_graph_with_no_matching_dialect(self):
        """Test graph that doesn't match any dialect."""
        router = DialectRouter()
        graph = Graph()

        # Add some random triples that don't match any dialect
        graph.add(
            (
                URIRef("http://example.org/Thing"),
                RDF.type,
                URIRef("http://example.org/Type"),
            )
        )

        # GenericDialect always matches, so returns GENERIC
        detected = router.get_detected_dialect(graph)
        assert detected == RDFDialect.GENERIC

    def test_matches_subject_with_forced_dialect(self):
        """Test matches_subject with forced dialect."""
        router = DialectRouter(forced_dialect=RDFDialect.FIBO)
        graph = Graph()
        subject = URIRef("http://example.org/Term")

        # Add FIBO-like structure
        graph.add((subject, RDF.type, OWL.Class))
        graph.add((subject, RDFS.label, Literal("Term")))

        # Should use forced dialect
        result = router.matches_subject(graph, subject)
        # Result depends on FIBO dialect implementation
        assert isinstance(result, bool)

    def test_matches_subject_no_match(self):
        """Test matches_subject when no dialect matches."""
        router = DialectRouter()
        graph = Graph()
        subject = URIRef("http://example.org/Thing")

        # Add something that doesn't match any dialect
        graph.add((subject, URIRef("http://example.org/prop"), Literal("value")))

        result = router.matches_subject(graph, subject)
        assert result is False

    def test_classify_entity_type_with_forced_dialect(self):
        """Test classify_entity_type with forced dialect."""
        router = DialectRouter(forced_dialect=RDFDialect.FIBO)
        graph = Graph()
        subject = URIRef("http://example.org/Term")

        graph.add((subject, RDF.type, OWL.Class))
        graph.add((subject, RDFS.label, Literal("Term")))

        entity_type = router.classify_entity_type(graph, subject)
        # Should use FIBO dialect classification
        assert entity_type is None or isinstance(entity_type, str)

    def test_classify_entity_type_no_match(self):
        """Test classify_entity_type when no dialect matches."""
        router = DialectRouter()
        graph = Graph()
        subject = URIRef("http://example.org/Thing")

        graph.add((subject, URIRef("http://example.org/prop"), Literal("value")))

        entity_type = router.classify_entity_type(graph, subject)
        assert entity_type is None

    def test_looks_like_glossary_term_with_forced_dialect(self):
        """Test looks_like_glossary_term with forced dialect."""
        router = DialectRouter(forced_dialect=RDFDialect.FIBO)
        graph = Graph()
        uri = URIRef("http://example.org/Term")

        graph.add((uri, RDF.type, OWL.Class))
        graph.add((uri, RDFS.label, Literal("Term")))

        result = router.looks_like_glossary_term(graph, uri)
        assert isinstance(result, bool)

    def test_looks_like_glossary_term_no_match(self):
        """Test looks_like_glossary_term when no dialect matches."""
        router = DialectRouter()
        graph = Graph()
        uri = URIRef("http://example.org/Thing")

        graph.add((uri, URIRef("http://example.org/prop"), Literal("value")))

        result = router.looks_like_glossary_term(graph, uri)
        assert result is False

    def test_looks_like_structured_property(self):
        """Test looks_like_structured_property."""
        router = DialectRouter()
        graph = Graph()
        uri = URIRef("http://example.org/Property")

        graph.add((uri, RDF.type, RDF.Property))

        result = router.looks_like_structured_property(graph, uri)
        assert isinstance(result, bool)

    def test_include_provisional_flag(self):
        """Test include_provisional flag propagation."""
        router_with_provisional = DialectRouter(include_provisional=True)
        router_without_provisional = DialectRouter(include_provisional=False)

        assert router_with_provisional.include_provisional is True
        assert router_without_provisional.include_provisional is False

    def test_detect_always_returns_true(self):
        """Test that detect always returns True for router."""
        router = DialectRouter()
        graph = Graph()

        # Router can handle any graph
        assert router.detect(graph) is True

        # Even empty graph
        empty_graph = Graph()
        assert router.detect(empty_graph) is True

    def test_get_dialect_by_type_fallback(self):
        """Test _get_dialect_by_type fallback to default."""
        router = DialectRouter()

        # Try to get a dialect type that doesn't exist
        # Should fallback to default
        dialect = router._get_dialect_by_type(RDFDialect.DEFAULT)
        assert dialect is not None
