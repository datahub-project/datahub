"""
Tests for GlossaryTermExtractor

Tests the extraction of glossary terms from RDF graphs.
"""

import unittest

from rdflib import RDF, RDFS, Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, SKOS

from datahub.ingestion.source.rdf.entities.glossary_term.extractor import (
    GlossaryTermExtractor,
)


class TestGlossaryTermExtractor(unittest.TestCase):
    """Test cases for GlossaryTermExtractor."""

    def setUp(self):
        """Set up test fixtures."""
        self.extractor = GlossaryTermExtractor()
        self.graph = Graph()

        # Common namespaces
        self.EX = Namespace("http://example.org/")
        self.graph.bind("ex", self.EX)

    def test_can_extract_skos_concept(self):
        """Test that SKOS Concepts are recognized as glossary terms."""
        uri = self.EX.TestTerm
        self.graph.add((uri, RDF.type, SKOS.Concept))
        self.graph.add((uri, SKOS.prefLabel, Literal("Test Term")))

        self.assertTrue(self.extractor.can_extract(self.graph, uri))

    def test_can_extract_owl_class(self):
        """Test that OWL Classes are recognized as glossary terms."""
        uri = self.EX.TestClass
        self.graph.add((uri, RDF.type, OWL.Class))
        self.graph.add((uri, RDFS.label, Literal("Test Class")))

        self.assertTrue(self.extractor.can_extract(self.graph, uri))

    def test_cannot_extract_without_label(self):
        """Test that entities without labels are not extracted."""
        uri = self.EX.NoLabelTerm
        self.graph.add((uri, RDF.type, SKOS.Concept))
        # No label added

        # Should still return True due to fallback to local name, but...
        # Let's test short label rejection
        uri2 = self.EX.AB  # Only 2 characters
        self.graph.add((uri2, RDF.type, SKOS.Concept))

        # Short names (< 3 chars) should be rejected
        self.assertFalse(self.extractor.can_extract(self.graph, uri2))

    def test_extract_basic_term(self):
        """Test extraction of basic glossary term properties."""
        uri = self.EX.AccountIdentifier
        self.graph.add((uri, RDF.type, SKOS.Concept))
        self.graph.add((uri, SKOS.prefLabel, Literal("Account Identifier")))
        self.graph.add(
            (uri, SKOS.definition, Literal("A unique identifier for an account"))
        )

        term = self.extractor.extract(self.graph, uri)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        self.assertEqual(term.name, "Account Identifier")
        self.assertEqual(term.definition, "A unique identifier for an account")
        self.assertEqual(term.source, str(uri))  # Original IRI stored in source
        self.assertIn("urn:li:glossaryTerm:", term.urn)  # URN generated
        self.assertIn("rdf:originalIRI", term.custom_properties)

    def test_extract_broader_relationship(self):
        """Test that glossary terms are extracted independently (relationships extracted separately)."""
        child = self.EX.ChildTerm
        parent = self.EX.ParentTerm

        self.graph.add((child, RDF.type, SKOS.Concept))
        self.graph.add((child, SKOS.prefLabel, Literal("Child Term")))
        self.graph.add((child, SKOS.broader, parent))

        self.graph.add((parent, RDF.type, SKOS.Concept))
        self.graph.add((parent, SKOS.prefLabel, Literal("Parent Term")))

        term = self.extractor.extract(self.graph, child)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Terms are now independent - relationships are extracted separately
        # Verify term was extracted correctly
        self.assertEqual(term.name, "Child Term")
        self.assertIn("urn:li:glossaryTerm:", term.urn)

    def test_extract_narrower_relationship(self):
        """Test that glossary terms are extracted independently (relationships extracted separately)."""
        parent = self.EX.ParentTerm
        child = self.EX.ChildTerm

        self.graph.add((parent, RDF.type, SKOS.Concept))
        self.graph.add((parent, SKOS.prefLabel, Literal("Parent Term")))
        self.graph.add((parent, SKOS.narrower, child))

        term = self.extractor.extract(self.graph, parent)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Terms are now independent - relationships are extracted separately
        # Verify term was extracted correctly
        self.assertEqual(term.name, "Parent Term")
        self.assertIn("urn:li:glossaryTerm:", term.urn)

    def test_no_related_relationship_extraction(self):
        """Test that glossary terms are extracted independently (relationships extracted separately)."""
        term1 = self.EX.Term1
        term2 = self.EX.Term2

        self.graph.add((term1, RDF.type, SKOS.Concept))
        self.graph.add((term1, SKOS.prefLabel, Literal("Term One")))
        self.graph.add((term1, SKOS.related, term2))  # Should be ignored

        term = self.extractor.extract(self.graph, term1)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Terms are now independent - relationships are extracted separately
        self.assertEqual(term.name, "Term One")

    def test_no_exact_match_relationship_extraction(self):
        """Test that glossary terms are extracted independently (relationships extracted separately)."""
        term1 = self.EX.Term1
        term2 = self.EX.Term2

        self.graph.add((term1, RDF.type, SKOS.Concept))
        self.graph.add((term1, SKOS.prefLabel, Literal("Term One")))
        self.graph.add((term1, SKOS.exactMatch, term2))  # Should be ignored

        term = self.extractor.extract(self.graph, term1)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Terms are now independent - relationships are extracted separately
        self.assertEqual(term.name, "Term One")

    def test_extract_all_terms(self):
        """Test extraction of all glossary terms from a graph."""
        # Add multiple terms
        for i in range(5):
            uri = URIRef(f"http://example.org/Term{i}")
            self.graph.add((uri, RDF.type, SKOS.Concept))
            self.graph.add((uri, SKOS.prefLabel, Literal(f"Term Number {i}")))

        terms = self.extractor.extract_all(self.graph)

        self.assertEqual(len(terms), 5)

    def test_extract_alternative_labels(self):
        """Test extraction of skos:altLabel."""
        uri = self.EX.MultiLabelTerm
        self.graph.add((uri, RDF.type, SKOS.Concept))
        self.graph.add((uri, SKOS.prefLabel, Literal("Primary Label")))
        self.graph.add((uri, SKOS.altLabel, Literal("Alternative One")))
        self.graph.add((uri, SKOS.altLabel, Literal("Alternative Two")))

        term = self.extractor.extract(self.graph, uri)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Alternative labels are now in custom_properties as comma-separated string
        self.assertIn("skos:altLabel", term.custom_properties)
        alt_labels = term.custom_properties["skos:altLabel"]
        self.assertIn("Alternative One", alt_labels)
        self.assertIn("Alternative Two", alt_labels)

    def test_extract_notation(self):
        """Test extraction of skos:notation."""
        uri = self.EX.NotatedTerm
        self.graph.add((uri, RDF.type, SKOS.Concept))
        self.graph.add((uri, SKOS.prefLabel, Literal("Notated Term")))
        self.graph.add((uri, SKOS.notation, Literal("NT-001")))

        term = self.extractor.extract(self.graph, uri)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Notation is now in custom_properties
        self.assertEqual(term.custom_properties.get("skos:notation"), "NT-001")

    def test_extract_scope_note(self):
        """Test extraction of skos:scopeNote."""
        uri = self.EX.ScopedTerm
        self.graph.add((uri, RDF.type, SKOS.Concept))
        self.graph.add((uri, SKOS.prefLabel, Literal("Scoped Term")))
        self.graph.add(
            (uri, SKOS.scopeNote, Literal("This term is used in banking contexts"))
        )

        term = self.extractor.extract(self.graph, uri)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Scope note is now in custom_properties
        self.assertEqual(
            term.custom_properties.get("skos:scopeNote"),
            "This term is used in banking contexts",
        )


class TestGlossaryTermExtractorMultipleRelationships(unittest.TestCase):
    """Test cases for multiple relationship extraction."""

    def setUp(self):
        """Set up test fixtures."""
        self.extractor = GlossaryTermExtractor()
        self.graph = Graph()
        self.EX = Namespace("http://example.org/")

    def test_extract_multiple_broader_relationships(self):
        """Test that glossary terms are extracted independently (relationships extracted separately)."""
        child = self.EX.ChildTerm
        parent1 = self.EX.Parent1
        parent2 = self.EX.Parent2

        self.graph.add((child, RDF.type, SKOS.Concept))
        self.graph.add((child, SKOS.prefLabel, Literal("Child Term")))
        self.graph.add((child, SKOS.broader, parent1))
        self.graph.add((child, SKOS.broader, parent2))

        term = self.extractor.extract(self.graph, child)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Terms are now independent - relationships are extracted separately
        self.assertEqual(term.name, "Child Term")
        self.assertIn("urn:li:glossaryTerm:", term.urn)

    def test_extract_mixed_broader_narrower(self):
        """Test that glossary terms are extracted independently (relationships extracted separately)."""
        middle = self.EX.MiddleTerm
        parent = self.EX.ParentTerm
        child = self.EX.ChildTerm

        self.graph.add((middle, RDF.type, SKOS.Concept))
        self.graph.add((middle, SKOS.prefLabel, Literal("Middle Term")))
        self.graph.add((middle, SKOS.broader, parent))
        self.graph.add((middle, SKOS.narrower, child))

        term = self.extractor.extract(self.graph, middle)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        # Terms are now independent - relationships are extracted separately
        self.assertEqual(term.name, "Middle Term")
        self.assertIn("urn:li:glossaryTerm:", term.urn)


if __name__ == "__main__":
    unittest.main()
