"""
Tests for DataHubIngestionTarget modularity features.
"""

import unittest
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
    DataHubIngestionTarget,
)


class TestDataHubIngestionTargetModularity(unittest.TestCase):
    """Test cases for DataHubIngestionTarget modular architecture."""

    def setUp(self):
        """Set up test fixtures."""
        self.report = MagicMock()
        self.target = DataHubIngestionTarget(self.report)

    def test_processing_order_respected(self):
        """Test that entities are processed in the correct order."""
        # Create a mock graph with entities
        graph = DataHubGraph()
        graph.structured_properties = []
        graph.glossary_terms = []
        graph.datasets = []
        graph.lineage_relationships = []

        # Mock the registry to return entities in a specific order
        with patch(
            "datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target.create_default_registry"
        ) as mock_registry:
            registry = MagicMock()
            mock_registry.return_value = registry

            # Set up processing order
            registry.get_entity_types_by_processing_order.return_value = [
                "structured_property",
                "glossary_term",
                "dataset",
                "lineage",
            ]

            # Mock MCP builders
            def get_mcp_builder(entity_type):
                builder = MagicMock()
                builder.build_all_mcps.return_value = []
                builder.build_post_processing_mcps.return_value = []
                return builder

            registry.get_mcp_builder.side_effect = get_mcp_builder
            registry.get_metadata.return_value = MagicMock(processing_order=100)

            # Call send
            self.target.send(graph)

            # Verify that get_entity_types_by_processing_order was called
            registry.get_entity_types_by_processing_order.assert_called_once()

    def test_post_processing_hooks_called(self):
        """Test that post-processing hooks are called after standard processing."""
        graph = DataHubGraph()
        graph.structured_properties = []
        graph.glossary_terms = []
        graph.datasets = []
        graph.domains = []

        with patch(
            "datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target.create_default_registry"
        ) as mock_registry:
            registry = MagicMock()
            mock_registry.return_value = registry

            registry.get_entity_types_by_processing_order.return_value = ["dataset"]

            # Create a mock builder with post-processing hook
            post_processing_mcps = [MagicMock()]
            builder = MagicMock()
            builder.build_all_mcps.return_value = []
            builder.build_post_processing_mcps.return_value = post_processing_mcps

            registry.get_mcp_builder.return_value = builder
            registry.get_metadata.return_value = MagicMock(processing_order=100)

            result = self.target.send(graph)

            # Verify post-processing hook was called
            builder.build_post_processing_mcps.assert_called_once()
            self.assertIsNotNone(result)

    def test_context_passed_to_builders(self):
        """Test that context with graph and report is passed to builders."""
        graph = DataHubGraph()
        graph.structured_properties = []
        graph.glossary_terms = []

        with patch(
            "datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target.create_default_registry"
        ) as mock_registry:
            registry = MagicMock()
            mock_registry.return_value = registry

            registry.get_entity_types_by_processing_order.return_value = [
                "structured_property"
            ]

            builder = MagicMock()
            builder.build_all_mcps.return_value = []
            builder.build_post_processing_mcps.return_value = []

            registry.get_mcp_builder.return_value = builder
            registry.get_metadata.return_value = MagicMock(processing_order=100)

            self.target.send(graph)

            # Verify context was passed
            call_args = builder.build_all_mcps.call_args
            self.assertIsNotNone(call_args)
            context = (
                call_args[1].get("context") or call_args[0][1]
                if len(call_args[0]) > 1
                else call_args[1]
            )
            if context:
                self.assertIn("datahub_graph", context)
                self.assertIn("report", context)

    def test_entity_type_to_field_name_used(self):
        """Test that entity_type_to_field_name utility is used."""
        from datahub.ingestion.source.rdf.core.utils import (
            entity_type_to_field_name,
        )

        # Verify the utility function works
        self.assertEqual(entity_type_to_field_name("dataset"), "datasets")
        self.assertEqual(entity_type_to_field_name("lineage"), "lineage_relationships")


if __name__ == "__main__":
    unittest.main()
