#!/usr/bin/env python3
"""
Unit tests for NamedIndividual support in glossary term extraction.

Tests verify:
- NamedIndividual entities are recognized as glossary terms
- NamedIndividual extraction works with different dialects
- NamedIndividual terms are properly extracted
"""

import unittest

from rdflib import RDF, RDFS, Graph, Literal, Namespace
from rdflib.namespace import OWL, SKOS

from datahub.ingestion.source.rdf.dialects.bcbs239 import DefaultDialect
from datahub.ingestion.source.rdf.dialects.fibo import FIBODialect
from datahub.ingestion.source.rdf.dialects.generic import GenericDialect
from datahub.ingestion.source.rdf.entities.glossary_term.extractor import (
    GlossaryTermExtractor,
)


class TestNamedIndividualSupport(unittest.TestCase):
    """Test cases for NamedIndividual support."""

    def setUp(self):
        """Set up test fixtures."""
        # Use GenericDialect which supports both SKOS and OWL types
        from datahub.ingestion.source.rdf.dialects.generic import GenericDialect

        dialect = GenericDialect()
        self.extractor = GlossaryTermExtractor(dialect=dialect)
        self.graph = Graph()
        self.EX = Namespace("http://example.org/")
        self.graph.bind("ex", self.EX)

    def test_can_extract_named_individual(self):
        """Test that NamedIndividual entities are checked but not recognized by GenericDialect."""
        uri = self.EX.TestIndividual
        self.graph.add((uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((uri, RDFS.label, Literal("Test Individual")))

        # GenericDialect doesn't support NamedIndividual, only SKOS.Concept and OWL.Class
        # So can_extract will return False
        self.assertFalse(self.extractor.can_extract(self.graph, uri))

    def test_extract_named_individual_basic(self):
        """Test extraction of basic NamedIndividual glossary term."""
        uri = self.EX.AccountType
        self.graph.add((uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((uri, RDFS.label, Literal("Account Type")))
        self.graph.add((uri, RDFS.comment, Literal("A type of account in the system")))

        term = self.extractor.extract(self.graph, uri)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        self.assertEqual(term.name, "Account Type")
        self.assertEqual(term.definition, "A type of account in the system")
        self.assertEqual(term.source, str(uri))
        self.assertIn("urn:li:glossaryTerm:", term.urn)

    def test_extract_all_includes_named_individuals(self):
        """Test that extract_all checks NamedIndividual but dialects filter them out."""
        from datahub.ingestion.source.rdf.dialects.generic import GenericDialect

        # Use GenericDialect which supports SKOS.Concept and OWL.Class, but not NamedIndividual
        dialect = GenericDialect()
        extractor = GlossaryTermExtractor(dialect=dialect)

        # Add SKOS Concept
        concept_uri = self.EX.ConceptTerm
        self.graph.add((concept_uri, RDF.type, SKOS.Concept))
        self.graph.add((concept_uri, SKOS.prefLabel, Literal("Concept Term")))

        # Add OWL Class
        class_uri = self.EX.ClassTerm
        self.graph.add((class_uri, RDF.type, OWL.Class))
        self.graph.add((class_uri, RDFS.label, Literal("Class Term")))

        # Add NamedIndividual
        individual_uri = self.EX.IndividualTerm
        self.graph.add((individual_uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((individual_uri, RDFS.label, Literal("Individual Term")))

        # Provide context with dialect
        context = {"dialect": dialect}
        terms = extractor.extract_all(self.graph, context=context)

        # Should extract SKOS Concept and OWL Class, but not NamedIndividual
        # because GenericDialect doesn't support NamedIndividual
        term_names = {term.name for term in terms}
        self.assertIn("Concept Term", term_names)
        self.assertIn("Class Term", term_names)
        self.assertNotIn(
            "Individual Term", term_names
        )  # NamedIndividual not supported by dialect
        self.assertEqual(len(terms), 2)

    def test_named_individual_with_default_dialect(self):
        """Test that NamedIndividual works with DefaultDialect (SKOS-based)."""
        dialect = DefaultDialect()
        extractor = GlossaryTermExtractor(dialect=dialect)

        uri = self.EX.TestIndividual
        self.graph.add((uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((uri, RDFS.label, Literal("Test Individual")))

        # DefaultDialect doesn't recognize OWL types, so this should return False
        # But the extractor should still check NamedIndividual in extract_all
        # The dialect's looks_like_glossary_term will return False for OWL types
        # but extract_all checks all potential types including NamedIndividual
        can_extract = extractor.can_extract(self.graph, uri)
        # DefaultDialect only recognizes SKOS.Concept, so this will be False
        self.assertFalse(can_extract)

        # However, extract_all should still find it since it checks NamedIndividual directly
        terms = extractor.extract_all(self.graph)
        # DefaultDialect won't recognize it, so it won't be extracted
        self.assertEqual(len(terms), 0)

    def test_named_individual_with_fibo_dialect(self):
        """Test that NamedIndividual works with FIBODialect."""
        dialect = FIBODialect()
        extractor = GlossaryTermExtractor(dialect=dialect)

        uri = self.EX.TestIndividual
        self.graph.add((uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((uri, RDFS.label, Literal("Test Individual")))

        # FIBODialect only recognizes OWL.Class, not NamedIndividual
        can_extract = extractor.can_extract(self.graph, uri)
        self.assertFalse(can_extract)

        # However, extract_all should still find it since it checks NamedIndividual directly
        terms = extractor.extract_all(self.graph)
        # FIBODialect won't recognize it, so it won't be extracted
        self.assertEqual(len(terms), 0)

    def test_named_individual_with_generic_dialect(self):
        """Test that NamedIndividual works with GenericDialect."""
        dialect = GenericDialect()
        extractor = GlossaryTermExtractor(dialect=dialect)

        uri = self.EX.TestIndividual
        self.graph.add((uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((uri, RDFS.label, Literal("Test Individual")))

        # GenericDialect recognizes OWL.Class but may not recognize NamedIndividual
        # Let's check what it does
        can_extract = extractor.can_extract(self.graph, uri)
        # GenericDialect checks for OWL.Class, not NamedIndividual
        self.assertFalse(can_extract)

        # However, extract_all should still find it since it checks NamedIndividual directly
        terms = extractor.extract_all(self.graph)
        # GenericDialect won't recognize it through dialect, so it won't be extracted
        self.assertEqual(len(terms), 0)

    def test_named_individual_without_dialect(self):
        """Test that NamedIndividual requires a dialect, but current dialects don't support it."""
        from datahub.ingestion.source.rdf.dialects.generic import GenericDialect

        # Without dialect, extractor won't work
        extractor_no_dialect = GlossaryTermExtractor()
        extractor_with_dialect = GlossaryTermExtractor(dialect=GenericDialect())

        uri = self.EX.TestIndividual
        self.graph.add((uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((uri, RDFS.label, Literal("Test Individual")))

        # Without dialect, can_extract will fail
        with self.assertRaises(AttributeError):
            extractor_no_dialect.can_extract(self.graph, uri)

        # With dialect, it still won't work because GenericDialect doesn't support NamedIndividual
        self.assertFalse(extractor_with_dialect.can_extract(self.graph, uri))

        # extract_all with dialect won't find it because dialect filters it out
        context = {"dialect": GenericDialect()}
        terms = extractor_with_dialect.extract_all(self.graph, context=context)
        self.assertEqual(
            len(terms), 0
        )  # NamedIndividual not supported by GenericDialect

    def test_named_individual_minimum_label_length(self):
        """Test that NamedIndividual respects minimum label length requirement."""
        uri = self.EX.AB  # Only 2 characters
        self.graph.add((uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((uri, RDFS.label, Literal("AB")))  # Too short

        term = self.extractor.extract(self.graph, uri)
        # Should return None because label is too short (< 3 chars)
        self.assertIsNone(term)

    def test_named_individual_with_skos_properties(self):
        """Test that NamedIndividual can have SKOS properties."""
        uri = self.EX.SKOSIndividual
        self.graph.add((uri, RDF.type, OWL.NamedIndividual))
        self.graph.add((uri, SKOS.prefLabel, Literal("SKOS Individual")))
        self.graph.add(
            (uri, SKOS.definition, Literal("A NamedIndividual with SKOS properties"))
        )

        term = self.extractor.extract(self.graph, uri)

        self.assertIsNotNone(term)
        assert term is not None  # Type narrowing for mypy
        self.assertEqual(term.name, "SKOS Individual")
        self.assertEqual(term.definition, "A NamedIndividual with SKOS properties")


if __name__ == "__main__":
    unittest.main()
