"""
Tests for GlossaryTermConverter

Tests the conversion of RDF AST glossary terms to DataHub AST format.
"""

import unittest

from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    RDFGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.glossary_term.converter import (
    GlossaryTermConverter,
)
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    RDFRelationship,
    RelationshipType,
)


class TestGlossaryTermConverter(unittest.TestCase):
    """Test cases for GlossaryTermConverter."""

    def setUp(self):
        """Set up test fixtures."""
        self.converter = GlossaryTermConverter()

    def test_convert_basic_term(self):
        """Test conversion of a basic glossary term."""
        rdf_term = RDFGlossaryTerm(
            uri="http://example.org/glossary/AccountIdentifier",
            name="Account Identifier",
            definition="A unique identifier for an account",
            source="http://example.org",
            relationships=[],
            custom_properties={},
        )

        datahub_term = self.converter.convert(rdf_term)

        self.assertIsNotNone(datahub_term)
        self.assertEqual(datahub_term.name, "Account Identifier")
        self.assertEqual(datahub_term.definition, "A unique identifier for an account")
        self.assertIn("urn:li:glossaryTerm:", datahub_term.urn)

    def test_convert_preserves_original_iri(self):
        """Test that original IRI is preserved in custom properties."""
        rdf_term = RDFGlossaryTerm(
            uri="http://example.org/glossary/TestTerm",
            name="Test Term",
            relationships=[],
            custom_properties={},
        )

        datahub_term = self.converter.convert(rdf_term)

        self.assertIn("rdf:originalIRI", datahub_term.custom_properties)
        self.assertEqual(
            datahub_term.custom_properties["rdf:originalIRI"],
            "http://example.org/glossary/TestTerm",
        )

    def test_convert_skos_properties(self):
        """Test that SKOS properties are mapped to custom properties."""
        rdf_term = RDFGlossaryTerm(
            uri="http://example.org/glossary/SKOSTerm",
            name="SKOS Term",
            relationships=[],
            custom_properties={},
            notation="SKOS-001",
            scope_note="Used in financial contexts",
            alternative_labels=["Alt Label 1", "Alt Label 2"],
            hidden_labels=["Hidden 1"],
        )

        datahub_term = self.converter.convert(rdf_term)

        self.assertEqual(datahub_term.custom_properties["skos:notation"], "SKOS-001")
        self.assertEqual(
            datahub_term.custom_properties["skos:scopeNote"],
            "Used in financial contexts",
        )
        self.assertEqual(
            datahub_term.custom_properties["skos:altLabel"], "Alt Label 1,Alt Label 2"
        )
        self.assertEqual(datahub_term.custom_properties["skos:hiddenLabel"], "Hidden 1")

    def test_convert_with_broader_relationship(self):
        """Test conversion of term with broader relationship."""
        rdf_term = RDFGlossaryTerm(
            uri="http://example.org/glossary/ChildTerm",
            name="Child Term",
            relationships=[
                RDFRelationship(
                    source_uri="http://example.org/glossary/ChildTerm",
                    target_uri="http://example.org/glossary/ParentTerm",
                    relationship_type=RelationshipType.BROADER,
                )
            ],
            custom_properties={},
        )

        datahub_term = self.converter.convert(rdf_term)

        self.assertIsNotNone(datahub_term)
        self.assertEqual(len(datahub_term.relationships.get("broader", [])), 1)
        self.assertIn("urn:li:glossaryTerm:", datahub_term.relationships["broader"][0])

    def test_convert_all_terms(self):
        """Test conversion of multiple terms."""
        rdf_terms = [
            RDFGlossaryTerm(
                uri=f"http://example.org/glossary/Term{i}",
                name=f"Term {i}",
                relationships=[],
                custom_properties={},
            )
            for i in range(3)
        ]

        datahub_terms = self.converter.convert_all(rdf_terms)

        self.assertEqual(len(datahub_terms), 3)

    def test_collect_relationships_from_terms(self):
        """Test collection of relationships from multiple terms."""
        rdf_terms = [
            RDFGlossaryTerm(
                uri="http://example.org/glossary/Term1",
                name="Term 1",
                relationships=[
                    RDFRelationship(
                        source_uri="http://example.org/glossary/Term1",
                        target_uri="http://example.org/glossary/Parent1",
                        relationship_type=RelationshipType.BROADER,
                    )
                ],
                custom_properties={},
            ),
            RDFGlossaryTerm(
                uri="http://example.org/glossary/Term2",
                name="Term 2",
                relationships=[
                    RDFRelationship(
                        source_uri="http://example.org/glossary/Term2",
                        target_uri="http://example.org/glossary/Parent2",
                        relationship_type=RelationshipType.BROADER,
                    )
                ],
                custom_properties={},
            ),
        ]

        relationships = self.converter.collect_relationships(rdf_terms)

        self.assertEqual(len(relationships), 2)

    def test_collect_relationships_deduplicates(self):
        """Test that duplicate relationships are removed."""
        rdf_terms = [
            RDFGlossaryTerm(
                uri="http://example.org/glossary/Term1",
                name="Term 1",
                relationships=[
                    RDFRelationship(
                        source_uri="http://example.org/glossary/Term1",
                        target_uri="http://example.org/glossary/Parent",
                        relationship_type=RelationshipType.BROADER,
                    ),
                    RDFRelationship(
                        source_uri="http://example.org/glossary/Term1",
                        target_uri="http://example.org/glossary/Parent",
                        relationship_type=RelationshipType.BROADER,
                    ),
                ],
                custom_properties={},
            )
        ]

        relationships = self.converter.collect_relationships(rdf_terms)

        # Should deduplicate to 1
        self.assertEqual(len(relationships), 1)

    def test_path_segments_generated(self):
        """Test that path segments are generated from IRI."""
        rdf_term = RDFGlossaryTerm(
            uri="http://example.org/ontology/banking/AccountIdentifier",
            name="Account Identifier",
            relationships=[],
            custom_properties={},
        )

        datahub_term = self.converter.convert(rdf_term)

        self.assertIsNotNone(datahub_term.path_segments)
        self.assertIsInstance(datahub_term.path_segments, list)


class TestGlossaryTermConverterEdgeCases(unittest.TestCase):
    """Test edge cases for GlossaryTermConverter."""

    def setUp(self):
        """Set up test fixtures."""
        self.converter = GlossaryTermConverter()

    def test_convert_term_with_no_definition(self):
        """Test conversion when definition is None."""
        rdf_term = RDFGlossaryTerm(
            uri="http://example.org/glossary/NoDefTerm",
            name="No Definition Term",
            definition=None,
            relationships=[],
            custom_properties={},
        )

        datahub_term = self.converter.convert(rdf_term)

        self.assertIsNotNone(datahub_term)
        self.assertIsNone(datahub_term.definition)  # Should preserve None

    def test_convert_term_with_empty_relationships(self):
        """Test conversion when relationships list is empty."""
        rdf_term = RDFGlossaryTerm(
            uri="http://example.org/glossary/IsolatedTerm",
            name="Isolated Term",
            relationships=[],
            custom_properties={},
        )

        datahub_term = self.converter.convert(rdf_term)

        self.assertIsNotNone(datahub_term)
        self.assertEqual(len(datahub_term.relationships.get("broader", [])), 0)
        self.assertEqual(len(datahub_term.relationships.get("narrower", [])), 0)


if __name__ == "__main__":
    unittest.main()
