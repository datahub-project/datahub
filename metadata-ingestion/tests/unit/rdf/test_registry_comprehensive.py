#!/usr/bin/env python3
"""
Comprehensive tests for EntityRegistry to improve coverage.

Tests cover:
- Registration methods with error cases
- CLI name mapping
- Processing order with cycles
- Thread safety
- Edge cases
"""

import threading
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.rdf.entities.base import (
    EntityConverter,
    EntityExtractor,
    EntityMCPBuilder,
    EntityMetadata,
    EntityProcessor,
)
from datahub.ingestion.source.rdf.entities.registry import (
    EntityRegistry,
    create_default_registry,
)


class TestEntityRegistryRegistration:
    """Test entity registration methods."""

    def test_register_extractor_mismatch_raises_error(self):
        """Test registering extractor with mismatched entity_type raises error."""
        registry = EntityRegistry()
        extractor = MagicMock(spec=EntityExtractor)
        extractor.entity_type = "glossary_term"

        with pytest.raises(ValueError, match="does not match"):
            registry.register_extractor("relationship", extractor)

    def test_register_converter_mismatch_raises_error(self):
        """Test registering converter with mismatched entity_type raises error."""
        registry = EntityRegistry()
        converter = MagicMock(spec=EntityConverter)
        converter.entity_type = "glossary_term"

        with pytest.raises(ValueError, match="does not match"):
            registry.register_converter("relationship", converter)

    def test_register_mcp_builder_mismatch_raises_error(self):
        """Test registering MCP builder with mismatched entity_type raises error."""
        registry = EntityRegistry()
        mcp_builder = MagicMock(spec=EntityMCPBuilder)
        mcp_builder.entity_type = "glossary_term"

        with pytest.raises(ValueError, match="does not match"):
            registry.register_mcp_builder("relationship", mcp_builder)

    def test_register_processor_mismatch_raises_error(self):
        """Test registering processor with mismatched entity_type raises error."""
        registry = EntityRegistry()
        extractor = MagicMock(spec=EntityExtractor)
        extractor.entity_type = "test"
        converter = MagicMock(spec=EntityConverter)
        converter.entity_type = "test"
        mcp_builder = MagicMock(spec=EntityMCPBuilder)
        mcp_builder.entity_type = "test"
        processor: EntityProcessor = EntityProcessor(
            extractor=extractor, converter=converter, mcp_builder=mcp_builder
        )

        with pytest.raises(ValueError, match="does not match"):
            registry.register_processor("other", processor)

    def test_register_processor_component_mismatch_raises_error(self):
        """Test registering processor with component mismatch raises error."""
        registry = EntityRegistry()
        extractor = MagicMock(spec=EntityExtractor)
        extractor.entity_type = "test"
        converter = MagicMock(spec=EntityConverter)
        converter.entity_type = "other"  # Mismatch
        mcp_builder = MagicMock(spec=EntityMCPBuilder)
        mcp_builder.entity_type = "test"
        processor: EntityProcessor = EntityProcessor(
            extractor=extractor, converter=converter, mcp_builder=mcp_builder
        )

        with pytest.raises(ValueError, match="mismatch"):
            registry.register_processor("test", processor)

    def test_register_metadata_mismatch_raises_error(self):
        """Test registering metadata with mismatched entity_type raises error."""
        registry = EntityRegistry()
        metadata = EntityMetadata(
            entity_type="test",
            cli_names=["test"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
        )

        with pytest.raises(ValueError, match="does not match"):
            registry.register_metadata("other", metadata)

    def test_register_metadata_duplicate_cli_name_different_entity_raises_error(self):
        """Test registering metadata with duplicate CLI name for different entity raises error."""
        registry = EntityRegistry()
        metadata1 = EntityMetadata(
            entity_type="entity1",
            cli_names=["shared_name"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
        )
        metadata2 = EntityMetadata(
            entity_type="entity2",
            cli_names=["shared_name"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
        )

        registry.register_metadata("entity1", metadata1)
        with pytest.raises(ValueError, match="already registered"):
            registry.register_metadata("entity2", metadata2)

    def test_register_metadata_same_cli_name_same_entity_ok(self):
        """Test registering metadata with same CLI name for same entity is OK."""
        registry = EntityRegistry()
        metadata = EntityMetadata(
            entity_type="test",
            cli_names=["test"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
        )

        registry.register_metadata("test", metadata)
        # Re-registering should not raise error
        registry.register_metadata("test", metadata)


class TestEntityRegistryCLINameMapping:
    """Test CLI name to entity type mapping."""

    def test_get_entity_type_from_cli_name(self):
        """Test getting entity type from CLI name."""
        registry = create_default_registry()

        # Glossary term has CLI names
        entity_type = registry.get_entity_type_from_cli_name("glossary")
        assert entity_type == "glossary_term"

    def test_get_entity_type_from_cli_name_not_found(self):
        """Test getting entity type from non-existent CLI name."""
        registry = create_default_registry()

        entity_type = registry.get_entity_type_from_cli_name("nonexistent")
        assert entity_type is None

    def test_get_all_cli_choices(self):
        """Test getting all CLI choices."""
        registry = create_default_registry()

        cli_choices = registry.get_all_cli_choices()
        assert isinstance(cli_choices, list)
        assert len(cli_choices) > 0
        assert "glossary" in cli_choices or "relationship" in cli_choices


class TestEntityRegistryProcessingOrder:
    """Test entity processing order."""

    def test_get_entity_types_by_processing_order_no_dependencies(self):
        """Test processing order with no dependencies."""
        registry = EntityRegistry()
        metadata1 = EntityMetadata(
            entity_type="a",
            cli_names=["a"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=[],
            processing_order=2,
        )
        metadata2 = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=[],
            processing_order=1,
        )

        registry.register_metadata("a", metadata1)
        registry.register_metadata("b", metadata2)

        ordered = registry.get_entity_types_by_processing_order()
        # Should be sorted by processing_order, then alphabetically
        test_entities = [et for et in ordered if et in ["a", "b"]]
        assert test_entities == ["b", "a"]  # b has lower processing_order

    def test_get_entity_types_by_processing_order_with_dependencies(self):
        """Test processing order with dependencies."""
        registry = EntityRegistry()
        metadata_a = EntityMetadata(
            entity_type="a",
            cli_names=["a"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=[],
        )
        metadata_b = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=["a"],
        )

        registry.register_metadata("a", metadata_a)
        registry.register_metadata("b", metadata_b)

        ordered = registry.get_entity_types_by_processing_order()
        test_entities = [et for et in ordered if et in ["a", "b"]]
        assert test_entities == ["a", "b"]  # a must come before b

    def test_get_entity_types_by_processing_order_missing_dependency(self):
        """Test processing order with missing dependency."""
        registry = EntityRegistry()
        metadata_b = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=["nonexistent"],
        )

        registry.register_metadata("b", metadata_b)

        # Should handle gracefully and warn
        ordered = registry.get_entity_types_by_processing_order()
        assert "b" in ordered

    def test_get_entity_types_by_processing_order_cycle_detection(self):
        """Test processing order with circular dependency."""
        registry = EntityRegistry()
        metadata_a = EntityMetadata(
            entity_type="a",
            cli_names=["a"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=["b"],
        )
        metadata_b = EntityMetadata(
            entity_type="b",
            cli_names=["b"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=["a"],
        )

        registry.register_metadata("a", metadata_a)
        registry.register_metadata("b", metadata_b)

        # Should detect cycle and fall back to alphabetical order
        ordered = registry.get_entity_types_by_processing_order()
        test_entities = [et for et in ordered if et in ["a", "b"]]
        # Should fall back to sorted order
        assert sorted(test_entities) == test_entities

    def test_detect_cycle_simple(self):
        """Test cycle detection for simple cycle."""
        registry = EntityRegistry()
        dependency_graph = {"a": ["b"], "b": ["a"]}

        cycle = registry._detect_cycle(dependency_graph, ["a", "b"])
        assert cycle is not None
        assert len(cycle) > 0

    def test_detect_cycle_no_cycle(self):
        """Test cycle detection when no cycle exists."""
        registry = EntityRegistry()
        dependency_graph = {"a": ["b"], "b": []}

        cycle = registry._detect_cycle(dependency_graph, ["a", "b"])
        assert cycle is None

    def test_detect_cycle_complex(self):
        """Test cycle detection for complex cycle."""
        registry = EntityRegistry()
        dependency_graph = {"a": ["b"], "b": ["c"], "c": ["a"]}

        cycle = registry._detect_cycle(dependency_graph, ["a", "b", "c"])
        assert cycle is not None


class TestEntityRegistryThreadSafety:
    """Test thread safety of registry."""

    def test_create_default_registry_thread_safe(self):
        """Test that create_default_registry is thread-safe."""
        results = []

        def create_registry():
            registry = create_default_registry()
            results.append(registry)

        threads = [threading.Thread(target=create_registry) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # All should get the same singleton instance
        assert len(set(id(r) for r in results)) == 1

    def test_create_default_registry_multiple_calls_same_instance(self):
        """Test that multiple calls return same instance."""
        registry1 = create_default_registry()
        registry2 = create_default_registry()

        assert registry1 is registry2


class TestEntityRegistryEdgeCases:
    """Test edge cases for EntityRegistry."""

    def test_get_extractor_not_registered(self):
        """Test getting extractor for unregistered entity type."""
        registry = EntityRegistry()

        extractor = registry.get_extractor("nonexistent")
        assert extractor is None

    def test_get_converter_not_registered(self):
        """Test getting converter for unregistered entity type."""
        registry = EntityRegistry()

        converter = registry.get_converter("nonexistent")
        assert converter is None

    def test_get_mcp_builder_not_registered(self):
        """Test getting MCP builder for unregistered entity type."""
        registry = EntityRegistry()

        mcp_builder = registry.get_mcp_builder("nonexistent")
        assert mcp_builder is None

    def test_get_processor_not_registered(self):
        """Test getting processor for unregistered entity type."""
        registry = EntityRegistry()

        processor = registry.get_processor("nonexistent")
        assert processor is None

    def test_get_metadata_not_registered(self):
        """Test getting metadata for unregistered entity type."""
        registry = EntityRegistry()

        metadata = registry.get_metadata("nonexistent")
        assert metadata is None

    def test_has_processor_true(self):
        """Test has_processor returns True for registered processor."""
        registry = create_default_registry()

        # Relationship has a processor
        assert registry.has_processor("relationship")

    def test_has_processor_false(self):
        """Test has_processor returns False for entity without processor."""
        registry = create_default_registry()

        # Glossary term doesn't have a processor (no converter)
        assert not registry.has_processor("glossary_term")

    def test_list_entity_types(self):
        """Test listing all entity types."""
        registry = create_default_registry()

        entity_types = registry.list_entity_types()
        assert isinstance(entity_types, list)
        assert len(entity_types) > 0
        assert "glossary_term" in entity_types
        assert "relationship" in entity_types
        assert "domain" in entity_types

    def test_get_entity_types_by_processing_order_with_processing_order(self):
        """Test processing order respects processing_order field."""
        registry = EntityRegistry()
        metadata1 = EntityMetadata(
            entity_type="z",
            cli_names=["z"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=[],
            processing_order=1,
        )
        metadata2 = EntityMetadata(
            entity_type="a",
            cli_names=["a"],
            rdf_ast_class=None,
            datahub_ast_class=MagicMock(),
            dependencies=[],
            processing_order=2,
        )

        registry.register_metadata("z", metadata1)
        registry.register_metadata("a", metadata2)

        ordered = registry.get_entity_types_by_processing_order()
        test_entities = [et for et in ordered if et in ["a", "z"]]
        assert test_entities == ["z", "a"]  # z has lower processing_order
