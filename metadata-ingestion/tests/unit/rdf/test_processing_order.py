"""
Tests for entity processing order using dependency-based topological sorting.
"""

import unittest
from unittest.mock import MagicMock

from datahub.ingestion.source.rdf.entities.base import EntityMetadata
from datahub.ingestion.source.rdf.entities.registry import EntityRegistry


class TestDependencyBasedOrdering(unittest.TestCase):
    """Test cases for dependency-based entity processing order."""

    def setUp(self):
        """Set up test fixtures."""
        self.registry = EntityRegistry()

    def test_simple_dependency_chain(self):
        """Test a simple linear dependency chain: A -> B -> C."""
        # A has no dependencies
        metadata_a = EntityMetadata(
            entity_type="a",
            cli_names=["a"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[],
        )
        # B depends on A
        metadata_b = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["a"],
        )
        # C depends on B
        metadata_c = EntityMetadata(
            entity_type="c",
            cli_names=["c"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["b"],
        )

        self.registry.register_metadata("a", metadata_a)
        self.registry.register_metadata("b", metadata_b)
        self.registry.register_metadata("c", metadata_c)

        ordered = self.registry.get_entity_types_by_processing_order()
        # Should be: a, b, c (test entities only)
        test_entities = [et for et in ordered if et in ["a", "b", "c"]]
        self.assertEqual(test_entities, ["a", "b", "c"])
        # Verify dependencies are satisfied
        self.assertLess(ordered.index("a"), ordered.index("b"))
        self.assertLess(ordered.index("b"), ordered.index("c"))

    def test_multiple_dependents(self):
        """Test multiple entities depending on the same entity."""
        # A has no dependencies
        metadata_a = EntityMetadata(
            entity_type="a",
            cli_names=["a"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[],
        )
        # B and C both depend on A
        metadata_b = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["a"],
        )
        metadata_c = EntityMetadata(
            entity_type="c",
            cli_names=["c"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["a"],
        )

        self.registry.register_metadata("a", metadata_a)
        self.registry.register_metadata("b", metadata_b)
        self.registry.register_metadata("c", metadata_c)

        ordered = self.registry.get_entity_types_by_processing_order()
        # A must come first
        self.assertEqual(ordered[0], "a")
        # B and C can come in any order after A
        self.assertIn("b", ordered)
        self.assertIn("c", ordered)
        self.assertLess(ordered.index("a"), ordered.index("b"))
        self.assertLess(ordered.index("a"), ordered.index("c"))

    def test_priority_ordering_for_root_nodes(self):
        """Test that domain has priority when it has no dependencies."""
        # Create a scenario where dependencies are used (to trigger priority ordering)
        metadata_domain = EntityMetadata(
            entity_type="domain",
            cli_names=["domain"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[],
        )
        metadata_other = EntityMetadata(
            entity_type="other",
            cli_names=["other"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[
                "domain"
            ],  # Add a dependency to trigger dependency-based sorting
        )

        self.registry.register_metadata("domain", metadata_domain)
        self.registry.register_metadata("other", metadata_other)

        ordered = self.registry.get_entity_types_by_processing_order()
        # domain should come before other (priority ordering)
        self.assertIn("domain", ordered[:1])
        # other should come after domain (it depends on domain)
        self.assertLess(ordered.index("domain"), ordered.index("other"))

    def test_real_world_dependencies(self):
        """Test the actual dependency structure used in MVP production."""
        # Register MVP entities
        metadata_domain = EntityMetadata(
            entity_type="domain",
            cli_names=["domain"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[],
        )
        metadata_glossary = EntityMetadata(
            entity_type="glossary_term",
            cli_names=["glossary"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["domain"],
        )
        metadata_relationship = EntityMetadata(
            entity_type="relationship",
            cli_names=["relationship"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["glossary_term"],
        )

        self.registry.register_metadata("domain", metadata_domain)
        self.registry.register_metadata("glossary_term", metadata_glossary)
        self.registry.register_metadata("relationship", metadata_relationship)

        ordered = self.registry.get_entity_types_by_processing_order()

        # Verify root node comes first
        self.assertIn("domain", ordered[:1])

        # Verify dependencies are satisfied
        domain_idx = ordered.index("domain")
        glossary_idx = ordered.index("glossary_term")
        relationship_idx = ordered.index("relationship")

        self.assertLess(domain_idx, glossary_idx)
        self.assertLess(glossary_idx, relationship_idx)

    def test_missing_dependency_handling(self):
        """Test that missing dependencies are handled gracefully."""
        # B depends on A, but A is not registered
        metadata_b = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["a"],  # A is not registered
        )

        self.registry.register_metadata("b", metadata_b)

        # Should not raise an error, but should log a warning
        # B should still be in the result (as a root node since its dependency is missing)
        ordered = self.registry.get_entity_types_by_processing_order()
        self.assertIn("b", ordered)

    def test_fallback_to_processing_order(self):
        """Test fallback to processing_order when no dependencies are specified."""
        # Entities with no dependencies specified should use processing_order
        metadata1 = EntityMetadata(
            entity_type="entity_1",
            cli_names=["e1"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            processing_order=10,
        )
        metadata2 = EntityMetadata(
            entity_type="entity_2",
            cli_names=["e2"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            processing_order=5,
        )

        self.registry.register_metadata("entity_1", metadata1)
        self.registry.register_metadata("entity_2", metadata2)

        ordered = self.registry.get_entity_types_by_processing_order()
        # Should be sorted by processing_order (test entities only)
        test_entities = [et for et in ordered if et.startswith("entity_")]
        self.assertEqual(test_entities, ["entity_2", "entity_1"])

    def test_mixed_dependencies_and_processing_order(self):
        """Test that dependencies take precedence over processing_order."""
        # A has no dependencies
        metadata_a = EntityMetadata(
            entity_type="a",
            cli_names=["a"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[],
            processing_order=100,  # High order, but should come first due to dependencies
        )
        # B depends on A
        metadata_b = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["a"],
            processing_order=1,  # Low order, but should come after A
        )

        self.registry.register_metadata("a", metadata_a)
        self.registry.register_metadata("b", metadata_b)

        ordered = self.registry.get_entity_types_by_processing_order()
        # A should come before B despite having higher processing_order (test entities only)
        test_entities = [et for et in ordered if et in ["a", "b"]]
        self.assertEqual(test_entities, ["a", "b"])

    def test_complex_dependency_graph(self):
        """Test a complex dependency graph with multiple levels."""
        # Level 0: No dependencies
        metadata_a = EntityMetadata(
            entity_type="a",
            cli_names=["a"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[],
        )
        # Level 1: Depend on A
        metadata_b = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["a"],
        )
        metadata_c = EntityMetadata(
            entity_type="c",
            cli_names=["c"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["a"],
        )
        # Level 2: Depend on B and C
        metadata_d = EntityMetadata(
            entity_type="d",
            cli_names=["d"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=["b", "c"],
        )

        self.registry.register_metadata("a", metadata_a)
        self.registry.register_metadata("b", metadata_b)
        self.registry.register_metadata("c", metadata_c)
        self.registry.register_metadata("d", metadata_d)

        ordered = self.registry.get_entity_types_by_processing_order()

        # Verify ordering constraints
        a_idx = ordered.index("a")
        b_idx = ordered.index("b")
        c_idx = ordered.index("c")
        d_idx = ordered.index("d")

        self.assertLess(a_idx, b_idx)
        self.assertLess(a_idx, c_idx)
        self.assertLess(b_idx, d_idx)
        self.assertLess(c_idx, d_idx)

    def test_entity_type_constants_in_dependencies(self):
        """Test that ENTITY_TYPE constants can be used in dependencies."""
        # Simulate using ENTITY_TYPE constants (which are just strings)
        DOMAIN_ENTITY_TYPE = "domain"

        metadata_domain = EntityMetadata(
            entity_type="domain",
            cli_names=["domain"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[],
        )
        metadata_glossary = EntityMetadata(
            entity_type="glossary_term",
            cli_names=["glossary"],
            rdf_ast_class=MagicMock(),
            datahub_ast_class=MagicMock(),
            dependencies=[DOMAIN_ENTITY_TYPE],  # Using constant
        )

        self.registry.register_metadata("domain", metadata_domain)
        self.registry.register_metadata("glossary_term", metadata_glossary)

        ordered = self.registry.get_entity_types_by_processing_order()
        # Domain should come before glossary_term (which depends on it)
        self.assertLess(ordered.index("domain"), ordered.index("glossary_term"))


class TestProcessingOrderBackwardCompatibility(unittest.TestCase):
    """Test backward compatibility with processing_order."""

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

    def test_fallback_to_processing_order_when_no_dependencies(self):
        """Test that processing_order is used when no dependencies are specified."""
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
        # Test entities only
        test_entities = [et for et in ordered if et.startswith("entity_")]
        self.assertEqual(test_entities, ["entity_2", "entity_1", "entity_3"])

    def test_same_processing_order_sorted_by_name(self):
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
        # Should be sorted by name when order is the same (test entities only)
        test_entities = [et for et in ordered if et.startswith("entity_")]
        self.assertEqual(test_entities, ["entity_a", "entity_b"])


if __name__ == "__main__":
    unittest.main()
