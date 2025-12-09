"""
Simplified Entity Registry

Explicit registry for the 2-3 entity types currently supported.
Replaces the auto-discovery registry with simple explicit imports.
"""

import logging
from typing import Dict, List, Optional

from datahub.ingestion.source.rdf.entities.base import (
    EntityConverter,
    EntityExtractor,
    EntityMCPBuilder,
    EntityMetadata,
    EntityProcessor,
)

logger = logging.getLogger(__name__)


class SimpleEntityRegistry:
    """
    Simplified registry with explicit entity type registration.

    For 2-3 entity types, explicit imports are simpler than auto-discovery.
    """

    def __init__(self):
        self._extractors: Dict[str, EntityExtractor] = {}
        self._converters: Dict[str, EntityConverter] = {}
        self._mcp_builders: Dict[str, EntityMCPBuilder] = {}
        self._processors: Dict[str, EntityProcessor] = {}
        self._metadata: Dict[str, EntityMetadata] = {}
        self._cli_name_to_entity_type: Dict[str, str] = {}

        # Explicitly register the 2-3 entity types we support
        self._register_glossary_term()
        self._register_relationship()
        self._register_domain()

    def _register_glossary_term(self):
        """Register glossary_term entity."""
        from datahub.ingestion.source.rdf.entities.glossary_term import (
            ENTITY_METADATA,
            GlossaryTermExtractor,
            GlossaryTermMCPBuilder,
        )

        entity_type = "glossary_term"
        extractor = GlossaryTermExtractor()
        # No converter - extractor returns DataHub AST directly
        mcp_builder = GlossaryTermMCPBuilder()

        self._extractors[entity_type] = extractor
        # No converter registered - extractor returns DataHub AST directly
        self._mcp_builders[entity_type] = mcp_builder
        # EntityProcessor not used for glossary_term (no converter)
        self._metadata[entity_type] = ENTITY_METADATA

        # Register CLI names
        for cli_name in ENTITY_METADATA.cli_names:
            self._cli_name_to_entity_type[cli_name] = entity_type

        logger.debug(
            f"Registered {entity_type} (no converter - extractor returns DataHub AST directly)"
        )

    def _register_relationship(self):
        """Register relationship entity."""
        from datahub.ingestion.source.rdf.entities.relationship import (
            ENTITY_METADATA,
            RelationshipConverter,
            RelationshipExtractor,
            RelationshipMCPBuilder,
        )

        entity_type = "relationship"
        extractor = RelationshipExtractor()
        converter = RelationshipConverter()
        mcp_builder = RelationshipMCPBuilder()

        self._extractors[entity_type] = extractor
        self._converters[entity_type] = converter
        self._mcp_builders[entity_type] = mcp_builder
        self._processors[entity_type] = EntityProcessor(
            extractor=extractor,
            converter=converter,
            mcp_builder=mcp_builder,
        )
        self._metadata[entity_type] = ENTITY_METADATA

        # Register CLI names
        for cli_name in ENTITY_METADATA.cli_names:
            self._cli_name_to_entity_type[cli_name] = entity_type

        logger.debug(f"Registered {entity_type}")

    def _register_domain(self):
        """Register domain entity (data structure only, no extractor/converter)."""
        from datahub.ingestion.source.rdf.entities.domain import ENTITY_METADATA

        entity_type = "domain"
        # Domain is built from other entities, not extracted
        # No extractor or converter needed
        self._metadata[entity_type] = ENTITY_METADATA

        # Register CLI names
        for cli_name in ENTITY_METADATA.cli_names:
            self._cli_name_to_entity_type[cli_name] = entity_type

        logger.debug(f"Registered {entity_type} (data structure only)")

    def get_extractor(self, entity_type: str) -> Optional[EntityExtractor]:
        """Get the extractor for an entity type."""
        return self._extractors.get(entity_type)

    def get_converter(self, entity_type: str) -> Optional[EntityConverter]:
        """Get the converter for an entity type."""
        return self._converters.get(entity_type)

    def get_mcp_builder(self, entity_type: str) -> Optional[EntityMCPBuilder]:
        """Get the MCP builder for an entity type."""
        return self._mcp_builders.get(entity_type)

    def get_processor(self, entity_type: str) -> Optional[EntityProcessor]:
        """Get the processor for an entity type."""
        return self._processors.get(entity_type)

    def list_entity_types(self) -> List[str]:
        """List all registered entity types."""
        all_types = (
            set(self._extractors.keys())
            | set(self._converters.keys())
            | set(self._mcp_builders.keys())
            | set(self._metadata.keys())
        )
        return sorted(all_types)

    def has_processor(self, entity_type: str) -> bool:
        """Check if a processor is registered for an entity type."""
        return entity_type in self._processors

    def get_metadata(self, entity_type: str) -> Optional[EntityMetadata]:
        """Get metadata for an entity type."""
        return self._metadata.get(entity_type)

    def get_all_cli_choices(self) -> List[str]:
        """Get all CLI choice names from all registered entities."""
        all_cli_names = set()
        for metadata in self._metadata.values():
            all_cli_names.update(metadata.cli_names)
        return sorted(all_cli_names)

    def get_entity_type_from_cli_name(self, cli_name: str) -> Optional[str]:
        """Get the entity type name from a CLI name."""
        return self._cli_name_to_entity_type.get(cli_name)

    def get_entity_types_by_processing_order(self) -> List[str]:
        """
        Get all registered entity types sorted by dependencies.

        Uses topological sort based on dependencies in EntityMetadata.
        Falls back to alphabetical order if no dependencies specified.
        """
        entity_types = list(self._metadata.keys())

        # Build dependency graph
        dependency_graph: Dict[str, List[str]] = {}
        in_degree: Dict[str, int] = {}

        # Initialize
        for entity_type in entity_types:
            dependency_graph[entity_type] = []
            in_degree[entity_type] = 0

        # Build edges: if A depends on B, then B -> A (B must come before A)
        for entity_type, metadata in self._metadata.items():
            if metadata.dependencies:
                for dep in metadata.dependencies:
                    dep_str = dep if isinstance(dep, str) else str(dep)
                    if dep_str in dependency_graph:
                        dependency_graph[dep_str].append(entity_type)
                        in_degree[entity_type] += 1

        # Topological sort using Kahn's algorithm
        queue = [et for et in entity_types if in_degree[et] == 0]
        result = []

        # If no dependencies, return alphabetical order
        has_dependencies = any(
            metadata.dependencies for metadata in self._metadata.values()
        )
        if not has_dependencies:
            return sorted(entity_types)

        while queue:
            queue.sort()  # Deterministic ordering
            entity_type = queue.pop(0)
            result.append(entity_type)

            # Decrease in-degree of dependents
            for dependent in dependency_graph[entity_type]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Check for cycles
        if len(result) != len(entity_types):
            remaining = set(entity_types) - set(result)
            logger.warning(
                f"Circular dependency detected. Remaining: {remaining}. "
                f"Falling back to alphabetical order."
            )
            return sorted(entity_types)

        return result


# Create a singleton instance
_singleton_registry: Optional[SimpleEntityRegistry] = None


def create_default_registry() -> SimpleEntityRegistry:
    """
    Create or return the singleton registry instance.

    This maintains backward compatibility with the old registry interface.
    """
    global _singleton_registry
    if _singleton_registry is None:
        _singleton_registry = SimpleEntityRegistry()
    return _singleton_registry
