"""
Tests for Entity Pipeline and Registry

Tests the orchestration of entity processing through the modular architecture.
"""

import unittest

from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import SKOS

from datahub.ingestion.source.rdf.entities.base import EntityProcessor
from datahub.ingestion.source.rdf.entities.pipeline import EntityPipeline
from datahub.ingestion.source.rdf.entities.registry import (
    EntityRegistry,
    create_default_registry,
)


class TestEntityRegistry(unittest.TestCase):
    """Test cases for EntityRegistry."""

    def test_create_default_registry(self):
        """Test that default registry includes glossary_term."""
        registry = create_default_registry()

        self.assertIn("glossary_term", registry.list_entity_types())
        self.assertTrue(registry.has_processor("glossary_term"))

    def test_get_processor(self):
        """Test getting a registered processor."""
        registry = create_default_registry()

        processor = registry.get_processor("glossary_term")

        self.assertIsNotNone(processor)
        self.assertIsInstance(processor, EntityProcessor)

    def test_get_extractor(self):
        """Test getting a registered extractor."""
        registry = create_default_registry()

        extractor = registry.get_extractor("glossary_term")

        self.assertIsNotNone(extractor)
        self.assertEqual(extractor.entity_type, "glossary_term")

    def test_get_converter(self):
        """Test getting a registered converter."""
        registry = create_default_registry()

        converter = registry.get_converter("glossary_term")

        self.assertIsNotNone(converter)
        self.assertEqual(converter.entity_type, "glossary_term")

    def test_get_mcp_builder(self):
        """Test getting a registered MCP builder."""
        registry = create_default_registry()

        mcp_builder = registry.get_mcp_builder("glossary_term")

        self.assertIsNotNone(mcp_builder)
        self.assertEqual(mcp_builder.entity_type, "glossary_term")

    def test_get_nonexistent_processor(self):
        """Test getting a non-existent processor returns None."""
        registry = EntityRegistry()

        processor = registry.get_processor("nonexistent")

        self.assertIsNone(processor)


class TestEntityPipeline(unittest.TestCase):
    """Test cases for EntityPipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.pipeline = EntityPipeline()
        self.graph = Graph()
        self.EX = Namespace("http://example.org/")

        # Add some test glossary terms
        for i in range(3):
            uri = URIRef(f"http://example.org/Term{i}")
            self.graph.add((uri, RDF.type, SKOS.Concept))
            self.graph.add((uri, SKOS.prefLabel, Literal(f"Test Term {i}")))

    def test_extract_entity_type(self):
        """Test extracting entities of a specific type."""
        rdf_terms = self.pipeline.extract_entity_type(self.graph, "glossary_term")

        self.assertEqual(len(rdf_terms), 3)

    def test_convert_entities(self):
        """Test converting RDF AST entities to DataHub AST."""
        rdf_terms = self.pipeline.extract_entity_type(self.graph, "glossary_term")
        datahub_terms = self.pipeline.convert_entities(rdf_terms, "glossary_term")

        self.assertEqual(len(datahub_terms), 3)
        for term in datahub_terms:
            self.assertIn("urn:li:glossaryTerm:", term.urn)

    def test_build_mcps(self):
        """Test building MCPs from DataHub AST entities."""
        rdf_terms = self.pipeline.extract_entity_type(self.graph, "glossary_term")
        datahub_terms = self.pipeline.convert_entities(rdf_terms, "glossary_term")
        mcps = self.pipeline.build_mcps(datahub_terms, "glossary_term")

        self.assertEqual(len(mcps), 3)  # One MCP per term

    def test_process_entity_type_full_pipeline(self):
        """Test processing entity type through full pipeline."""
        mcps = self.pipeline.process_entity_type(self.graph, "glossary_term")

        self.assertEqual(len(mcps), 3)

    def test_process_nonexistent_entity_type(self):
        """Test processing non-existent entity type returns empty list."""
        mcps = self.pipeline.process_entity_type(self.graph, "nonexistent")

        self.assertEqual(len(mcps), 0)


class TestEntityPipelineRelationships(unittest.TestCase):
    """Test cases for relationship handling in EntityPipeline."""

    def setUp(self):
        """Set up test fixtures with relationships."""
        self.pipeline = EntityPipeline()
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
        """Test building relationship MCPs."""
        rel_mcps = self.pipeline.build_relationship_mcps(self.graph)

        # Should have 2 relationship MCPs (one for each child)
        self.assertEqual(len(rel_mcps), 2)

    def test_full_pipeline_with_relationships(self):
        """Test full pipeline produces both term and relationship MCPs."""
        # Get term MCPs
        term_mcps = self.pipeline.process_entity_type(self.graph, "glossary_term")

        # Get relationship MCPs
        rel_mcps = self.pipeline.build_relationship_mcps(self.graph)

        # Should have 3 term MCPs + 2 relationship MCPs
        total_mcps = term_mcps + rel_mcps
        self.assertEqual(len(total_mcps), 5)


class TestEntityPipelineIntegration(unittest.TestCase):
    """Integration tests for EntityPipeline."""

    def test_pipeline_with_custom_registry(self):
        """Test pipeline with custom registry."""
        registry = create_default_registry()
        pipeline = EntityPipeline(registry=registry)

        graph = Graph()
        EX = Namespace("http://example.org/")

        uri = EX.TestTerm
        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, SKOS.prefLabel, Literal("Test Term")))

        mcps = pipeline.process_entity_type(graph, "glossary_term")

        self.assertEqual(len(mcps), 1)

    def test_pipeline_context_passing(self):
        """Test that context is passed through pipeline stages."""
        pipeline = EntityPipeline()

        graph = Graph()
        EX = Namespace("http://example.org/")

        uri = EX.TestTerm
        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, SKOS.prefLabel, Literal("Test Term")))

        # Context with custom data
        context = {"test_key": "test_value"}

        # Should not raise errors with context
        mcps = pipeline.process_entity_type(graph, "glossary_term", context)

        self.assertEqual(len(mcps), 1)


if __name__ == "__main__":
    unittest.main()
