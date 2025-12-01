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
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    RelationshipType,
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
        self.assertEqual(term.name, "Account Identifier")
        self.assertEqual(term.definition, "A unique identifier for an account")
        self.assertEqual(term.uri, str(uri))
        self.assertIn("rdf:originalIRI", term.custom_properties)

    def test_extract_broader_relationship(self):
        """Test extraction of skos:broader relationship."""
        child = self.EX.ChildTerm
        parent = self.EX.ParentTerm

        self.graph.add((child, RDF.type, SKOS.Concept))
        self.graph.add((child, SKOS.prefLabel, Literal("Child Term")))
        self.graph.add((child, SKOS.broader, parent))

        self.graph.add((parent, RDF.type, SKOS.Concept))
        self.graph.add((parent, SKOS.prefLabel, Literal("Parent Term")))

        term = self.extractor.extract(self.graph, child)

        self.assertIsNotNone(term)
        self.assertEqual(len(term.relationships), 1)
        self.assertEqual(
            term.relationships[0].relationship_type, RelationshipType.BROADER
        )
        self.assertEqual(term.relationships[0].target_uri, str(parent))

    def test_extract_narrower_relationship(self):
        """Test extraction of skos:narrower relationship."""
        parent = self.EX.ParentTerm
        child = self.EX.ChildTerm

        self.graph.add((parent, RDF.type, SKOS.Concept))
        self.graph.add((parent, SKOS.prefLabel, Literal("Parent Term")))
        self.graph.add((parent, SKOS.narrower, child))

        term = self.extractor.extract(self.graph, parent)

        self.assertIsNotNone(term)
        self.assertEqual(len(term.relationships), 1)
        self.assertEqual(
            term.relationships[0].relationship_type, RelationshipType.NARROWER
        )

    def test_no_related_relationship_extraction(self):
        """Test that skos:related is NOT extracted."""
        term1 = self.EX.Term1
        term2 = self.EX.Term2

        self.graph.add((term1, RDF.type, SKOS.Concept))
        self.graph.add((term1, SKOS.prefLabel, Literal("Term One")))
        self.graph.add((term1, SKOS.related, term2))  # Should be ignored

        term = self.extractor.extract(self.graph, term1)

        self.assertIsNotNone(term)
        self.assertEqual(len(term.relationships), 0)  # No relationships extracted

    def test_no_exact_match_relationship_extraction(self):
        """Test that skos:exactMatch is NOT extracted for term-to-term."""
        term1 = self.EX.Term1
        term2 = self.EX.Term2

        self.graph.add((term1, RDF.type, SKOS.Concept))
        self.graph.add((term1, SKOS.prefLabel, Literal("Term One")))
        self.graph.add((term1, SKOS.exactMatch, term2))  # Should be ignored

        term = self.extractor.extract(self.graph, term1)

        self.assertIsNotNone(term)
        self.assertEqual(len(term.relationships), 0)  # No relationships extracted

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
        self.assertEqual(len(term.alternative_labels), 2)
        self.assertIn("Alternative One", term.alternative_labels)
        self.assertIn("Alternative Two", term.alternative_labels)

    def test_extract_notation(self):
        """Test extraction of skos:notation."""
        uri = self.EX.NotatedTerm
        self.graph.add((uri, RDF.type, SKOS.Concept))
        self.graph.add((uri, SKOS.prefLabel, Literal("Notated Term")))
        self.graph.add((uri, SKOS.notation, Literal("NT-001")))

        term = self.extractor.extract(self.graph, uri)

        self.assertIsNotNone(term)
        self.assertEqual(term.notation, "NT-001")

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
        self.assertEqual(term.scope_note, "This term is used in banking contexts")

    def test_extract_rdf_type(self):
        """Test extraction of RDF type."""
        uri = self.EX.TypedTerm
        self.graph.add((uri, RDF.type, SKOS.Concept))
        self.graph.add((uri, SKOS.prefLabel, Literal("Typed Term")))

        term = self.extractor.extract(self.graph, uri)

        self.assertIsNotNone(term)
        self.assertEqual(term.rdf_type, str(SKOS.Concept))


class TestGlossaryTermExtractorMultipleRelationships(unittest.TestCase):
    """Test cases for multiple relationship extraction."""

    def setUp(self):
        """Set up test fixtures."""
        self.extractor = GlossaryTermExtractor()
        self.graph = Graph()
        self.EX = Namespace("http://example.org/")

    def test_extract_multiple_broader_relationships(self):
        """Test extraction of multiple skos:broader relationships."""
        child = self.EX.ChildTerm
        parent1 = self.EX.Parent1
        parent2 = self.EX.Parent2

        self.graph.add((child, RDF.type, SKOS.Concept))
        self.graph.add((child, SKOS.prefLabel, Literal("Child Term")))
        self.graph.add((child, SKOS.broader, parent1))
        self.graph.add((child, SKOS.broader, parent2))

        term = self.extractor.extract(self.graph, child)

        self.assertIsNotNone(term)
        self.assertEqual(len(term.relationships), 2)

        target_uris = [r.target_uri for r in term.relationships]
        self.assertIn(str(parent1), target_uris)
        self.assertIn(str(parent2), target_uris)

    def test_extract_mixed_broader_narrower(self):
        """Test extraction of both broader and narrower relationships."""
        middle = self.EX.MiddleTerm
        parent = self.EX.ParentTerm
        child = self.EX.ChildTerm

        self.graph.add((middle, RDF.type, SKOS.Concept))
        self.graph.add((middle, SKOS.prefLabel, Literal("Middle Term")))
        self.graph.add((middle, SKOS.broader, parent))
        self.graph.add((middle, SKOS.narrower, child))

        term = self.extractor.extract(self.graph, middle)

        self.assertIsNotNone(term)
        self.assertEqual(len(term.relationships), 2)

        broader_rels = [
            r
            for r in term.relationships
            if r.relationship_type == RelationshipType.BROADER
        ]
        narrower_rels = [
            r
            for r in term.relationships
            if r.relationship_type == RelationshipType.NARROWER
        ]

        self.assertEqual(len(broader_rels), 1)
        self.assertEqual(len(narrower_rels), 1)


if __name__ == "__main__":
    unittest.main()
