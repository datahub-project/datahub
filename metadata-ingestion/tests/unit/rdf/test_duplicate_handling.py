#!/usr/bin/env python3
"""
Unit tests for duplicate term definition handling.

Tests verify how the RDF source handles:
- Same URI defined multiple times (same subject, multiple triples)
- Same property with multiple values
- RDF graph merging behavior
"""

from rdflib import Graph, Literal, URIRef

from datahub.ingestion.source.rdf.entities.glossary_term.extractor import (
    GlossaryTermExtractor,
)


class TestDuplicateHandling:
    """Test how duplicate term definitions are handled."""

    def test_same_uri_defined_multiple_times(self):
        """Test that same URI defined multiple times is only extracted once."""
        graph = Graph()

        # Define the same term multiple times with different properties
        term_uri = URIRef("http://example.org/Term")
        rdf_type = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        skos_concept = URIRef("http://www.w3.org/2004/02/skos/core#Concept")
        pref_label = URIRef("http://www.w3.org/2004/02/skos/core#prefLabel")
        definition = URIRef("http://www.w3.org/2004/02/skos/core#definition")

        graph.add((term_uri, rdf_type, skos_concept))
        graph.add((term_uri, pref_label, Literal("First Definition")))
        graph.add((term_uri, definition, Literal("First definition")))

        # Define it again with different properties
        graph.add((term_uri, rdf_type, skos_concept))
        graph.add((term_uri, pref_label, Literal("Second Definition")))
        graph.add((term_uri, definition, Literal("Second definition")))

        extractor = GlossaryTermExtractor()
        terms = extractor.extract_all(graph)

        # Should only extract once (RDF graph merges triples with same subject)
        assert len(terms) == 1
        term = terms[0]

        # RDFLib merges all triples, so graph.objects() returns all values
        # The extractor takes the FIRST value found for each property
        # So it should have one of the definitions (implementation-dependent which one)
        assert term.name in ["First Definition", "Second Definition"]

    def test_same_property_multiple_values(self):
        """Test that multiple values for same property are handled."""
        graph = Graph()

        term_uri = URIRef("http://example.org/Term")
        rdf_type = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        skos_concept = URIRef("http://www.w3.org/2004/02/skos/core#Concept")
        pref_label = URIRef("http://www.w3.org/2004/02/skos/core#prefLabel")

        graph.add((term_uri, rdf_type, skos_concept))

        # Add multiple prefLabels (RDF allows this)
        graph.add((term_uri, pref_label, Literal("Label 1")))
        graph.add((term_uri, pref_label, Literal("Label 2")))
        graph.add((term_uri, pref_label, Literal("Label 3")))

        extractor = GlossaryTermExtractor()
        terms = extractor.extract_all(graph)

        assert len(terms) == 1
        term = terms[0]

        # The extractor takes the FIRST value found
        # graph.objects() returns all values, but _extract_name returns first
        assert term.name in ["Label 1", "Label 2", "Label 3"]
        # Implementation detail: which one depends on iteration order

    def test_rdf_graph_merges_triples(self):
        """Test that RDF graph automatically merges triples with same subject."""
        graph = Graph()

        term_uri = URIRef("http://example.org/Term")
        rdf_type = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        skos_concept = URIRef("http://www.w3.org/2004/02/skos/core#Concept")
        pref_label = URIRef("http://www.w3.org/2004/02/skos/core#prefLabel")
        definition = URIRef("http://www.w3.org/2004/02/skos/core#definition")

        # Add triples in separate "statements" - RDF graph merges them
        graph.add((term_uri, rdf_type, skos_concept))
        graph.add((term_uri, pref_label, Literal("Term Name")))
        graph.add((term_uri, definition, Literal("Definition 1")))
        graph.add((term_uri, definition, Literal("Definition 2")))

        extractor = GlossaryTermExtractor()
        terms = extractor.extract_all(graph)

        # Should extract once (same URI)
        assert len(terms) == 1

        # Definition should be one of the values (first one found)
        term = terms[0]
        assert (
            term.definition in ["Definition 1", "Definition 2"]
            or term.definition is None
        )

    def test_seen_uris_tracking(self):
        """Test that seen_uris set prevents duplicate extraction."""
        graph = Graph()

        term_uri = URIRef("http://example.org/Term")
        rdf_type = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        skos_concept = URIRef("http://www.w3.org/2004/02/skos/core#Concept")
        owl_class = URIRef("http://www.w3.org/2002/07/owl#Class")
        pref_label = URIRef("http://www.w3.org/2004/02/skos/core#prefLabel")

        # Add term with multiple types (common in RDF)
        graph.add((term_uri, rdf_type, skos_concept))
        graph.add((term_uri, rdf_type, owl_class))
        graph.add((term_uri, pref_label, Literal("Term")))

        extractor = GlossaryTermExtractor()
        terms = extractor.extract_all(graph)

        # Should only extract once even though it matches multiple term types
        assert len(terms) == 1
