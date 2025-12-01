"""
Tests for entity processing order.
"""

import unittest
from unittest.mock import MagicMock

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.registry import EntityRegistry


class TestProcessingOrder(unittest.TestCase):
    """Test cases for entity processing order."""

    def setUp(self):
        """Set up test fixtures."""
        self.registry = EntityRegistry()

    def test_processing_order_default(self):
        """Test that processing_order defaults to 100."""
        metadata = EntityMetadata(
            entity_type="test_entity",
            cli_names=["test"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            export_targets=["pretty_print"],
        )
        self.assertEqual(metadata.processing_order, 100)

    def test_processing_order_custom(self):
        """Test custom processing_order values."""
        metadata = EntityMetadata(
            entity_type="test_entity",
            cli_names=["test"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            export_targets=["pretty_print"],
            processing_order=5,
        )
        self.assertEqual(metadata.processing_order, 5)

    def test_get_entity_types_by_processing_order(self):
        """Test that entities are returned in processing order."""
        # Register entities with different processing orders
        metadata1 = EntityMetadata(
            entity_type="entity_1",
            cli_names=["e1"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            export_targets=["pretty_print"],
            processing_order=10,
        )
        metadata2 = EntityMetadata(
            entity_type="entity_2",
            cli_names=["e2"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            export_targets=["pretty_print"],
            processing_order=5,
        )
        metadata3 = EntityMetadata(
            entity_type="entity_3",
            cli_names=["e3"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            export_targets=["pretty_print"],
            processing_order=15,
        )

        self.registry.register_metadata("entity_1", metadata1)
        self.registry.register_metadata("entity_2", metadata2)
        self.registry.register_metadata("entity_3", metadata3)

        ordered = self.registry.get_entity_types_by_processing_order()
        self.assertEqual(ordered, ["entity_2", "entity_1", "entity_3"])

    def test_get_entity_types_by_processing_order_same_order(self):
        """Test that entities with same processing_order are sorted by name."""
        metadata1 = EntityMetadata(
            entity_type="entity_b",
            cli_names=["eb"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            export_targets=["pretty_print"],
            processing_order=10,
        )
        metadata2 = EntityMetadata(
            entity_type="entity_a",
            cli_names=["ea"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            export_targets=["pretty_print"],
            processing_order=10,
        )

        self.registry.register_metadata("entity_b", metadata1)
        self.registry.register_metadata("entity_a", metadata2)

        ordered = self.registry.get_entity_types_by_processing_order()
        # Should be sorted by name when order is the same
        self.assertEqual(ordered, ["entity_a", "entity_b"])


if __name__ == "__main__":
    unittest.main()
