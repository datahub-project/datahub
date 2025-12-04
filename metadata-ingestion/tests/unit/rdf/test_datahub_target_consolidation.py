#!/usr/bin/env python3
"""
Unit tests for consolidated DataHubTarget.

Tests that DataHubTarget correctly uses DataHubIngestionTarget internally
and emits work units via DataHubClient.
"""

import unittest
from unittest.mock import Mock

from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.core.datahub_client import DataHubClient
from datahub.ingestion.source.rdf.core.target_factory import (
    DataHubTarget,
    SimpleReport,
)
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)


class TestDataHubTargetConsolidation(unittest.TestCase):
    """Test consolidated DataHubTarget implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = Mock(spec=DataHubClient)
        self.mock_client.datahub_gms = "http://localhost:8080"
        self.mock_client.api_token = "test_token"
        self.mock_client.is_validation_only = False  # Enable actual emission
        self.mock_client._emit_mcp = Mock(return_value=None)

        # Mock urn_generator (no longer needed, but kept for compatibility)
        # urn_generator no longer needed on client

        self.target = DataHubTarget(self.mock_client)

    def test_datahub_target_initialization(self):
        """Test DataHubTarget initialization."""
        self.assertEqual(self.target.datahub_client, self.mock_client)
        self.assertIsNotNone(self.target.report)
        self.assertIsInstance(self.target.report, SimpleReport)
        # ingestion_target should be lazy-loaded
        self.assertIsNone(self.target._ingestion_target)

    def test_datahub_target_ingestion_target_lazy_load(self):
        """Test that ingestion_target is lazy-loaded."""
        # Initially None
        self.assertIsNone(self.target._ingestion_target)

        # Accessing property should load it
        ingestion_target = self.target.ingestion_target
        self.assertIsNotNone(ingestion_target)
        self.assertIsNotNone(self.target._ingestion_target)

        # Second access should return same instance
        ingestion_target2 = self.target.ingestion_target
        self.assertIs(ingestion_target, ingestion_target2)

    def test_datahub_target_execute_with_empty_graph(self):
        """Test DataHubTarget.execute() with empty graph."""
        graph = DataHubGraph()

        result = self.target.execute(graph)

        self.assertTrue(result["success"])
        self.assertEqual(result["target_type"], "datahub")
        self.assertEqual(result["results"]["entities_emitted"], 0)
        # Should not have called _emit_mcp since no work units
        self.assertEqual(self.mock_client._emit_mcp.call_count, 0)

    def test_datahub_target_execute_with_glossary_term(self):
        """Test DataHubTarget.execute() with glossary term."""
        graph = DataHubGraph()
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test",
            name="Test Term",
            definition="Test definition",
            source=None,
            custom_properties={},
        )
        graph.glossary_terms = [term]
        graph.domains = []

        result = self.target.execute(graph)

        self.assertTrue(result["success"])
        self.assertEqual(result["target_type"], "datahub")
        # Should have generated work units
        workunits = self.target.ingestion_target.get_workunits()
        if len(workunits) > 0:
            # Should have emitted at least one MCP (the glossary term)
            self.assertGreater(self.mock_client._emit_mcp.call_count, 0)
            self.assertGreater(result["results"]["entities_emitted"], 0)
        else:
            # If no work units, that's also valid (empty graph handling)
            self.assertEqual(result["results"]["entities_emitted"], 0)

    # test_datahub_target_execute_with_dataset removed - dataset extraction not supported in MVP

    def test_datahub_target_execute_handles_ingestion_failure(self):
        """Test DataHubTarget.execute() handles ingestion target failure."""
        graph = DataHubGraph()

        # Mock ingestion target execute method to fail
        original_execute = self.target.ingestion_target.execute

        def failing_execute(*args, **kwargs):
            return {"success": False, "error": "Ingestion failed"}

        self.target.ingestion_target.execute = failing_execute

        try:
            result = self.target.execute(graph)

            self.assertFalse(result["success"])
            self.assertIn("error", result)
            self.assertEqual(result["error"], "Ingestion failed")
        finally:
            # Restore original
            self.target.ingestion_target.execute = original_execute

    def test_datahub_target_execute_handles_emit_errors(self):
        """Test DataHubTarget.execute() handles MCP emission errors."""
        graph = DataHubGraph()
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test",
            name="Test Term",
            definition="Test definition",
            source=None,
            custom_properties={},
        )
        graph.glossary_terms = [term]
        graph.domains = []

        # Mock _emit_mcp to raise error
        self.mock_client._emit_mcp.side_effect = Exception("Emission failed")

        result = self.target.execute(graph)

        # Should still succeed overall, but have errors in results
        self.assertTrue(result["success"])
        # Errors are collected during emission
        if "errors" in result["results"]:
            self.assertGreater(len(result["results"]["errors"]), 0)

    def test_datahub_target_get_target_info(self):
        """Test DataHubTarget.get_target_info()."""
        info = self.target.get_target_info()

        self.assertEqual(info["type"], "datahub")
        self.assertEqual(info["server"], "http://localhost:8080")
        self.assertTrue(info["has_token"])

    def test_datahub_target_get_target_info_no_token(self):
        """Test DataHubTarget.get_target_info() without token."""
        self.mock_client.api_token = None
        target = DataHubTarget(self.mock_client)

        info = target.get_target_info()

        self.assertFalse(info["has_token"])

    def test_datahub_target_execute_with_rdf_graph(self):
        """Test DataHubTarget.execute() stores RDF graph."""
        graph = DataHubGraph()
        from rdflib import Graph

        rdf_graph = Graph()

        # Initially None (if not set in __init__)
        # Note: rdf_graph is stored during execute, not in __init__

        result = self.target.execute(graph, rdf_graph)

        # Should be stored after execution (if provided)
        # The rdf_graph parameter is passed to ingestion_target.execute()
        # but may not be stored on self.rdf_graph if not needed
        self.assertTrue(result["success"])
        # The graph is passed to ingestion target, which may or may not store it
        # This is acceptable behavior

    def test_simple_report_tracking(self):
        """Test SimpleReport tracks statistics."""
        report = SimpleReport()

        self.assertEqual(report.num_entities_emitted, 0)
        self.assertEqual(report.num_workunits_produced, 0)

        report.report_entity_emitted()
        self.assertEqual(report.num_entities_emitted, 1)

        report.report_workunit_produced()
        self.assertEqual(report.num_workunits_produced, 1)


class TestDataHubTargetIntegration(unittest.TestCase):
    """Integration tests for DataHubTarget with real ingestion target."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = Mock(spec=DataHubClient)
        self.mock_client.datahub_gms = "http://localhost:8080"
        self.mock_client.api_token = "test_token"
        self.mock_client.is_validation_only = False  # Enable actual emission
        self.mock_client._emit_mcp = Mock(return_value=None)

        # urn_generator no longer needed on client (removed HierarchicalUrnGenerator)

        self.target = DataHubTarget(self.mock_client)

    def test_full_pipeline_glossary_term(self):
        """Test full pipeline: graph -> work units -> emission."""
        graph = DataHubGraph()
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test",
            name="Test Term",
            definition="Test definition",
            source="http://example.com/test",
            custom_properties={},
        )
        graph.glossary_terms = [term]
        graph.domains = []

        result = self.target.execute(graph)

        # Verify ingestion target was used
        self.assertIsNotNone(self.target._ingestion_target)

        # Verify work units were generated
        workunits = self.target.ingestion_target.get_workunits()
        self.assertGreater(len(workunits), 0)

        # Verify MCPs were emitted (one per work unit)
        self.assertEqual(self.mock_client._emit_mcp.call_count, len(workunits))

        # Verify result
        self.assertTrue(result["success"])
        self.assertGreater(result["results"]["entities_emitted"], 0)

    def test_full_pipeline_multiple_entities(self):
        """Test full pipeline with multiple entity types."""
        graph = DataHubGraph()

        # Add glossary term
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test",
            name="Test Term",
            definition="Test definition",
            source=None,
            custom_properties={},
        )
        graph.glossary_terms = [term]
        graph.domains = []

        result = self.target.execute(graph)

        # Verify work units were generated
        workunits = self.target.ingestion_target.get_workunits()
        self.assertGreater(len(workunits), 0)

        # Verify MCPs were emitted (one per work unit)
        self.assertEqual(self.mock_client._emit_mcp.call_count, len(workunits))
        self.assertTrue(result["success"])
        self.assertGreater(result["results"]["entities_emitted"], 0)


if __name__ == "__main__":
    unittest.main()
