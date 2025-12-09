"""
Tests for Entity Registry and Direct Processing

Tests entity processing through the simplified architecture without EntityPipeline.
"""

import unittest

from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import SKOS

from datahub.ingestion.source.rdf.entities.registry import (
    create_default_registry,
)


class TestEntityRegistry(unittest.TestCase):
    """Test cases for EntityRegistry."""

    def test_create_default_registry(self):
        """Test that default registry includes glossary_term."""
        registry = create_default_registry()

        self.assertIn("glossary_term", registry.list_entity_types())
        # Glossary term no longer has a processor (no converter), but has extractor and mcp_builder
        self.assertIsNotNone(registry.get_extractor("glossary_term"))
        self.assertIsNotNone(registry.get_mcp_builder("glossary_term"))

    def test_get_processor(self):
        """Test getting a registered processor (may be None for entities without converters)."""
        registry = create_default_registry()

        # Glossary term no longer has a processor (no converter)
        # But extractor and mcp_builder should exist
        self.assertIsNotNone(registry.get_extractor("glossary_term"))
        self.assertIsNotNone(registry.get_mcp_builder("glossary_term"))

    def test_get_extractor(self):
        """Test getting a registered extractor."""
        registry = create_default_registry()

        extractor = registry.get_extractor("glossary_term")

        self.assertIsNotNone(extractor)
        self.assertEqual(extractor.entity_type, "glossary_term")

    def test_get_converter(self):
        """Test getting a registered converter (may be None for entities without converters)."""
        registry = create_default_registry()

        converter = registry.get_converter("glossary_term")

        # Glossary term no longer has a converter (extractor returns DataHub AST directly)
        self.assertIsNone(converter)

    def test_get_mcp_builder(self):
        """Test getting a registered MCP builder."""
        registry = create_default_registry()

        mcp_builder = registry.get_mcp_builder("glossary_term")

        self.assertIsNotNone(mcp_builder)
        self.assertEqual(mcp_builder.entity_type, "glossary_term")

    def test_get_nonexistent_processor(self):
        """Test getting a non-existent processor returns None."""
        registry = create_default_registry()

        processor = registry.get_processor("nonexistent")

        self.assertIsNone(processor)


class TestDirectEntityProcessing(unittest.TestCase):
    """Test cases for direct entity processing without EntityPipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.registry = create_default_registry()
        self.graph = Graph()
        self.EX = Namespace("http://example.org/")

        # Add some test glossary terms
        for i in range(3):
            uri = URIRef(f"http://example.org/Term{i}")
            self.graph.add((uri, RDF.type, SKOS.Concept))
            self.graph.add((uri, SKOS.prefLabel, Literal(f"Test Term {i}")))

    def test_extract_entities(self):
        """Test extracting entities directly using extractor."""
        extractor = self.registry.get_extractor("glossary_term")
        datahub_terms = extractor.extract_all(self.graph)

        self.assertEqual(len(datahub_terms), 3)
        for term in datahub_terms:
            self.assertIn("urn:li:glossaryTerm:", term.urn)

    def test_build_mcps(self):
        """Test building MCPs from DataHub AST entities."""
        extractor = self.registry.get_extractor("glossary_term")
        datahub_terms = extractor.extract_all(self.graph)

        mcp_builder = self.registry.get_mcp_builder("glossary_term")
        mcps = mcp_builder.build_all_mcps(datahub_terms)

        self.assertEqual(len(mcps), 3)  # One MCP per term

    def test_full_pipeline_direct(self):
        """Test full pipeline using registry directly."""
        extractor = self.registry.get_extractor("glossary_term")
        datahub_terms = extractor.extract_all(self.graph)

        mcp_builder = self.registry.get_mcp_builder("glossary_term")
        mcps = mcp_builder.build_all_mcps(datahub_terms)

        self.assertEqual(len(mcps), 3)


class TestDirectEntityProcessingRelationships(unittest.TestCase):
    """Test cases for relationship handling in direct processing."""

    def setUp(self):
        """Set up test fixtures with relationships."""
        self.registry = create_default_registry()
        self.graph = Graph()
        self.EX = Namespace("http://example.org/")

        # Add parent term
        parent = self.EX.ParentTerm
        self.graph.add((parent, RDF.type, SKOS.Concept))
        self.graph.add((parent, SKOS.prefLabel, Literal("Parent Term")))

        # Add child terms with broader relationships
        for i in range(2):
            child = URIRef(f"http://example.org/ChildTerm{i}")
            self.graph.add((child, RDF.type, SKOS.Concept))
            self.graph.add((child, SKOS.prefLabel, Literal(f"Child Term {i}")))
            self.graph.add((child, SKOS.broader, parent))

    def test_build_relationship_mcps(self):
        """Test building relationship MCPs directly."""
        from datahub.ingestion.source.rdf.entities.glossary_term.relationship_collector import (
            collect_relationships_from_terms,
        )

        extractor = self.registry.get_extractor("glossary_term")
        datahub_terms = extractor.extract_all(self.graph)
        relationships = collect_relationships_from_terms(datahub_terms)

        mcp_builder = self.registry.get_mcp_builder("glossary_term")
        rel_mcps = mcp_builder.build_relationship_mcps(relationships)

        # Should have 2 relationship MCPs (one for each child)
        self.assertEqual(len(rel_mcps), 2)

    def test_full_pipeline_with_relationships(self):
        """Test full pipeline produces both term and relationship MCPs."""
        from datahub.ingestion.source.rdf.entities.glossary_term.relationship_collector import (
            collect_relationships_from_terms,
        )

        extractor = self.registry.get_extractor("glossary_term")
        datahub_terms = extractor.extract_all(self.graph)

        mcp_builder = self.registry.get_mcp_builder("glossary_term")
        term_mcps = mcp_builder.build_all_mcps(datahub_terms)

        relationships = collect_relationships_from_terms(datahub_terms)
        rel_mcps = mcp_builder.build_relationship_mcps(relationships)

        # Should have 3 term MCPs + 2 relationship MCPs
        total_mcps = term_mcps + rel_mcps
        self.assertEqual(len(total_mcps), 5)


class TestDirectEntityProcessingIntegration(unittest.TestCase):
    """Integration tests for direct entity processing."""

    def test_processing_with_registry(self):
        """Test processing with registry."""
        registry = create_default_registry()

        graph = Graph()
        EX = Namespace("http://example.org/")

        uri = EX.TestTerm
        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, SKOS.prefLabel, Literal("Test Term")))

        extractor = registry.get_extractor("glossary_term")
        datahub_terms = extractor.extract_all(graph)
        mcp_builder = registry.get_mcp_builder("glossary_term")
        mcps = mcp_builder.build_all_mcps(datahub_terms)

        self.assertEqual(len(mcps), 1)

    def test_context_passing(self):
        """Test that context is passed through processing stages."""
        registry = create_default_registry()

        graph = Graph()
        EX = Namespace("http://example.org/")

        uri = EX.TestTerm
        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, SKOS.prefLabel, Literal("Test Term")))

        # Context with custom data
        context = {"test_key": "test_value"}

        # Should not raise errors with context
        extractor = registry.get_extractor("glossary_term")
        datahub_terms = extractor.extract_all(graph, context)
        mcp_builder = registry.get_mcp_builder("glossary_term")
        mcps = mcp_builder.build_all_mcps(datahub_terms, context)

        self.assertEqual(len(mcps), 1)


if __name__ == "__main__":
    unittest.main()
