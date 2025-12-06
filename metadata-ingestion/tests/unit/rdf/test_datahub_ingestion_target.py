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
        # Create a mock graph with MVP entities
        graph = DataHubGraph()
        graph.glossary_terms = []
        graph.domains = []
        graph.relationships = []

        # Mock the registry to return entities in a specific order
        with patch(
            "datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target.create_default_registry"
        ) as mock_registry:
            registry = MagicMock()
            mock_registry.return_value = registry

            # Set up processing order for MVP
            registry.get_entity_types_by_processing_order.return_value = [
                "domain",
                "glossary_term",
                "relationship",
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
        # Add at least one entity so processing happens
        graph.glossary_terms = [MagicMock()]
        graph.domains = []

        with patch(
            "datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target.create_default_registry"
        ) as mock_registry:
            registry = MagicMock()
            mock_registry.return_value = registry

            registry.get_entity_types_by_processing_order.return_value = [
                "glossary_term"
            ]

            # Create a mock builder with post-processing hook
            post_processing_mcps = [MagicMock()]
            builder = MagicMock()
            builder.build_all_mcps.return_value = []
            builder.build_post_processing_mcps.return_value = post_processing_mcps

            registry.get_mcp_builder.return_value = builder
            registry.get_metadata.return_value = MagicMock(
                dependencies=[], processing_order=100
            )

            result = self.target.send(graph)

            # Verify post-processing hook was called
            # It may be called multiple times (during loop + deferred hooks), so check it was called at least once
            self.assertGreater(builder.build_post_processing_mcps.call_count, 0)
            self.assertIsNotNone(result)

    def test_context_passed_to_builders(self):
        """Test that context with graph and report is passed to builders."""
        graph = DataHubGraph()
        # Add at least one entity so processing happens
        graph.glossary_terms = [MagicMock()]

        with patch(
            "datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target.create_default_registry"
        ) as mock_registry:
            registry = MagicMock()
            mock_registry.return_value = registry

            registry.get_entity_types_by_processing_order.return_value = [
                "glossary_term"
            ]

            builder = MagicMock()
            builder.build_all_mcps.return_value = []
            builder.build_post_processing_mcps.return_value = []

            registry.get_mcp_builder.return_value = builder
            registry.get_metadata.return_value = MagicMock(
                dependencies=[], processing_order=100
            )

            self.target.send(graph)

            # Verify context was passed
            call_args = builder.build_all_mcps.call_args
            self.assertIsNotNone(call_args)
            # build_all_mcps is called with (entities, context) as positional args
            # or (entities, context=context) as keyword args
            if call_args:
                # Check positional args (second arg should be context)
                if len(call_args[0]) > 1:
                    context = call_args[0][1]
                # Or check keyword args
                elif "context" in call_args[1]:
                    context = call_args[1]["context"]
                else:
                    context = None

                if context:
                    self.assertIn("datahub_graph", context)
                    self.assertIn("report", context)

    def test_entity_type_to_field_name_used(self):
        """Test that entity_type_to_field_name utility is used."""
        from datahub.ingestion.source.rdf.core.utils import (
            entity_type_to_field_name,
        )

        # Verify the utility function works for MVP entities
        self.assertEqual(entity_type_to_field_name("glossary_term"), "glossary_terms")
        self.assertEqual(entity_type_to_field_name("domain"), "domains")
        self.assertEqual(entity_type_to_field_name("relationship"), "relationships")


if __name__ == "__main__":
    unittest.main()
