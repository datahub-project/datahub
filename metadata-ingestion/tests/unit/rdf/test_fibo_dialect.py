#!/usr/bin/env python3
"""
Unit tests for FIBO dialect implementation.

Tests verify:
- Provisional filtering functionality
- Maturity level detection
- Glossary term identification
"""

import unittest

from rdflib import RDF, RDFS, Graph, Literal, URIRef
from rdflib.namespace import OWL

from datahub.ingestion.source.rdf.dialects.fibo import FIBODialect


class TestFIBODialect(unittest.TestCase):
    """Test cases for FIBODialect."""

    def setUp(self):
        """Set up test fixtures."""
        self.graph = Graph()

        # FIBO maturity level namespace
        self.FIBO_UTL_AV = "https://www.omg.org/spec/Commons/AnnotationVocabulary/"
        self.has_maturity_level = URIRef(f"{self.FIBO_UTL_AV}hasMaturityLevel")
        self.provisional = URIRef(f"{self.FIBO_UTL_AV}Provisional")
        self.release = URIRef(f"{self.FIBO_UTL_AV}Release")

        # Test URIs
        self.EX = URIRef("http://example.org/")
        self.released_term = self.EX + "ReleasedTerm"
        self.provisional_term = self.EX + "ProvisionalTerm"
        self.no_maturity_term = self.EX + "NoMaturityTerm"

    def test_looks_like_glossary_term_with_release_maturity(self):
        """Test that terms with Release maturity level are included."""
        dialect = FIBODialect(include_provisional=False)

        # Add a released term
        self.graph.add((self.released_term, RDF.type, OWL.Class))
        self.graph.add((self.released_term, RDFS.label, Literal("Released Term")))
        self.graph.add((self.released_term, self.has_maturity_level, self.release))

        self.assertTrue(
            dialect.looks_like_glossary_term(self.graph, self.released_term)
        )

    def test_looks_like_glossary_term_excludes_provisional_when_filtered(self):
        """Test that provisional terms are excluded when include_provisional is False."""
        dialect = FIBODialect(include_provisional=False)

        # Add a provisional term
        self.graph.add((self.provisional_term, RDF.type, OWL.Class))
        self.graph.add((self.provisional_term, RDFS.label, Literal("Provisional Term")))
        self.graph.add(
            (self.provisional_term, self.has_maturity_level, self.provisional)
        )

        self.assertFalse(
            dialect.looks_like_glossary_term(self.graph, self.provisional_term)
        )

    def test_looks_like_glossary_term_includes_provisional_when_enabled(self):
        """Test that provisional terms are included when include_provisional is True."""
        dialect = FIBODialect(include_provisional=True)

        # Add a provisional term
        self.graph.add((self.provisional_term, RDF.type, OWL.Class))
        self.graph.add((self.provisional_term, RDFS.label, Literal("Provisional Term")))
        self.graph.add(
            (self.provisional_term, self.has_maturity_level, self.provisional)
        )

        self.assertTrue(
            dialect.looks_like_glossary_term(self.graph, self.provisional_term)
        )

    def test_looks_like_glossary_term_no_maturity_level_included(self):
        """Test that terms without maturity level are included by default."""
        dialect = FIBODialect(include_provisional=False)

        # Add a term without maturity level
        self.graph.add((self.no_maturity_term, RDF.type, OWL.Class))
        self.graph.add((self.no_maturity_term, RDFS.label, Literal("No Maturity Term")))
        # No maturity level property

        self.assertTrue(
            dialect.looks_like_glossary_term(self.graph, self.no_maturity_term)
        )

    def test_has_provisional_maturity_level_detects_provisional(self):
        """Test that _has_provisional_maturity_level correctly detects provisional status."""
        dialect = FIBODialect()

        # Add a provisional term
        self.graph.add(
            (self.provisional_term, self.has_maturity_level, self.provisional)
        )

        self.assertTrue(
            dialect._has_provisional_maturity_level(self.graph, self.provisional_term)
        )

    def test_has_provisional_maturity_level_does_not_detect_release(self):
        """Test that _has_provisional_maturity_level does not detect Release status."""
        dialect = FIBODialect()

        # Add a released term
        self.graph.add((self.released_term, self.has_maturity_level, self.release))

        self.assertFalse(
            dialect._has_provisional_maturity_level(self.graph, self.released_term)
        )

    def test_has_provisional_maturity_level_handles_no_maturity_level(self):
        """Test that _has_provisional_maturity_level handles missing maturity level."""
        dialect = FIBODialect()

        # Term without maturity level
        self.graph.add((self.no_maturity_term, RDF.type, OWL.Class))
        self.graph.add((self.no_maturity_term, RDFS.label, Literal("No Maturity Term")))

        self.assertFalse(
            dialect._has_provisional_maturity_level(self.graph, self.no_maturity_term)
        )

    def test_mixed_maturity_levels_filtering(self):
        """Test filtering with mixed maturity levels."""
        dialect = FIBODialect(include_provisional=False)

        # Add released term
        self.graph.add((self.released_term, RDF.type, OWL.Class))
        self.graph.add((self.released_term, RDFS.label, Literal("Released Term")))
        self.graph.add((self.released_term, self.has_maturity_level, self.release))

        # Add provisional term
        self.graph.add((self.provisional_term, RDF.type, OWL.Class))
        self.graph.add((self.provisional_term, RDFS.label, Literal("Provisional Term")))
        self.graph.add(
            (self.provisional_term, self.has_maturity_level, self.provisional)
        )

        # Add term without maturity level
        self.graph.add((self.no_maturity_term, RDF.type, OWL.Class))
        self.graph.add((self.no_maturity_term, RDFS.label, Literal("No Maturity Term")))

        # Released term should be included
        self.assertTrue(
            dialect.looks_like_glossary_term(self.graph, self.released_term)
        )

        # Provisional term should be excluded
        self.assertFalse(
            dialect.looks_like_glossary_term(self.graph, self.provisional_term)
        )

        # Term without maturity level should be included
        self.assertTrue(
            dialect.looks_like_glossary_term(self.graph, self.no_maturity_term)
        )

    def test_default_include_provisional_is_false(self):
        """Test that default behavior excludes provisional terms."""
        dialect = FIBODialect()  # Default should be False

        # Add a provisional term
        self.graph.add((self.provisional_term, RDF.type, OWL.Class))
        self.graph.add((self.provisional_term, RDFS.label, Literal("Provisional Term")))
        self.graph.add(
            (self.provisional_term, self.has_maturity_level, self.provisional)
        )

        self.assertFalse(
            dialect.looks_like_glossary_term(self.graph, self.provisional_term)
        )
        self.assertFalse(dialect.include_provisional)


if __name__ == "__main__":
    unittest.main()
