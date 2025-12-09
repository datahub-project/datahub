"""
Entity Registry

DEPRECATED: This auto-discovery registry is replaced by simple_registry.py
which uses explicit imports for the 2-3 entity types we support.

This module is kept for backward compatibility but delegates to SimpleEntityRegistry.
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


class EntityRegistry:
    """
    Central registry for entity processors.

    Manages registration and lookup of entity processing components
    (extractors, converters, MCP builders) for different entity types.

    Usage:
        registry = EntityRegistry()
        registry.register_processor('glossary_term', GlossaryTermProcessor())
        processor = registry.get_processor('glossary_term')
    """

    def __init__(self):
        self._extractors: Dict[str, EntityExtractor] = {}
        self._converters: Dict[str, EntityConverter] = {}
        self._mcp_builders: Dict[str, EntityMCPBuilder] = {}
        self._processors: Dict[str, EntityProcessor] = {}
        self._metadata: Dict[str, EntityMetadata] = {}
        self._cli_name_to_entity_type: Dict[
            str, str
        ] = {}  # Reverse mapping for CLI names

    def register_extractor(self, entity_type: str, extractor: EntityExtractor) -> None:
        """Register an extractor for an entity type."""
        self._extractors[entity_type] = extractor
        logger.debug(f"Registered extractor for {entity_type}")

    def register_converter(self, entity_type: str, converter: EntityConverter) -> None:
        """Register a converter for an entity type."""
        self._converters[entity_type] = converter
        logger.debug(f"Registered converter for {entity_type}")

    def register_mcp_builder(
        self, entity_type: str, mcp_builder: EntityMCPBuilder
    ) -> None:
        """Register an MCP builder for an entity type."""
        self._mcp_builders[entity_type] = mcp_builder
        logger.debug(f"Registered MCP builder for {entity_type}")

    def register_processor(self, entity_type: str, processor: EntityProcessor) -> None:
        """Register a complete processor for an entity type."""
        self._processors[entity_type] = processor
        # Also register individual components
        self._extractors[entity_type] = processor.extractor
        self._converters[entity_type] = processor.converter
        self._mcp_builders[entity_type] = processor.mcp_builder
        logger.debug(f"Registered processor for {entity_type}")

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
        # Union of all registered types
        all_types = (
            set(self._extractors.keys())
            | set(self._converters.keys())
            | set(self._mcp_builders.keys())
        )
        return sorted(all_types)

    def has_processor(self, entity_type: str) -> bool:
        """Check if a processor is registered for an entity type."""
        return entity_type in self._processors

    def register_metadata(self, entity_type: str, metadata: EntityMetadata) -> None:
        """
        Register metadata for an entity type.

        Args:
            entity_type: The entity type name
            metadata: The EntityMetadata instance
        """
        if metadata.entity_type != entity_type:
            raise ValueError(
                f"Metadata entity_type '{metadata.entity_type}' does not match provided entity_type '{entity_type}'"
            )

        self._metadata[entity_type] = metadata

        # Build reverse mapping from CLI names to entity type
        for cli_name in metadata.cli_names:
            if cli_name in self._cli_name_to_entity_type:
                logger.warning(
                    f"CLI name '{cli_name}' already mapped to '{self._cli_name_to_entity_type[cli_name]}', overwriting with '{entity_type}'"
                )
            self._cli_name_to_entity_type[cli_name] = entity_type

        logger.debug(
            f"Registered metadata for {entity_type} with CLI names: {metadata.cli_names}"
        )

    def get_metadata(self, entity_type: str) -> Optional[EntityMetadata]:
        """
        Get metadata for an entity type.

        Args:
            entity_type: The entity type name

        Returns:
            EntityMetadata if found, None otherwise
        """
        return self._metadata.get(entity_type)

    def get_all_cli_choices(self) -> List[str]:
        """
        Get all CLI choice names from all registered entities.

        Returns:
            Sorted list of all CLI names that can be used in CLI arguments
        """
        all_cli_names = set()
        for metadata in self._metadata.values():
            all_cli_names.update(metadata.cli_names)
        return sorted(all_cli_names)

    def get_entity_type_from_cli_name(self, cli_name: str) -> Optional[str]:
        """
        Get the entity type name from a CLI name.

        Args:
            cli_name: The CLI name (e.g., 'glossary', 'datasets')

        Returns:
            The entity type name (e.g., 'glossary_term', 'dataset') if found, None otherwise
        """
        return self._cli_name_to_entity_type.get(cli_name)

    def get_entity_types_by_processing_order(self) -> List[str]:
        """
        Get all registered entity types sorted by dependencies (topological sort).

        Entities are ordered such that dependencies are processed before dependents.
        Uses topological sorting based on the dependencies field in EntityMetadata.

        Falls back to processing_order if dependencies are not specified (backward compatibility).

        Returns:
            List of entity type names sorted by dependency order
        """
        # Build dependency graph
        entity_types = list(self._metadata.keys())
        dependency_graph: Dict[str, List[str]] = {}
        in_degree: Dict[str, int] = {}

        # Initialize
        for entity_type in entity_types:
            dependency_graph[entity_type] = []
            in_degree[entity_type] = 0

        # Build edges: if A depends on B, then B -> A (B must come before A)
        for entity_type, metadata in self._metadata.items():
            # Use dependencies if specified, otherwise fall back to processing_order
            if metadata.dependencies:
                for dep in metadata.dependencies:
                    # Normalize dependency to string (handles both string literals and ENTITY_TYPE constants)
                    dep_str = dep if isinstance(dep, str) else str(dep)
                    if dep_str in dependency_graph:
                        dependency_graph[dep_str].append(entity_type)
                        in_degree[entity_type] += 1
                    else:
                        logger.warning(
                            f"Entity '{entity_type}' depends on '{dep_str}', but '{dep_str}' is not registered. "
                            f"Ignoring dependency."
                        )

        # Topological sort using Kahn's algorithm
        queue = [et for et in entity_types if in_degree[et] == 0]
        result = []

        # If no dependencies specified, fall back to processing_order
        has_dependencies = any(
            metadata.dependencies for metadata in self._metadata.values()
        )
        if not has_dependencies:
            # Fallback to processing_order
            entity_types_with_order = [
                (entity_type, metadata.processing_order)
                for entity_type, metadata in self._metadata.items()
            ]
            entity_types_with_order.sort(key=lambda x: (x[1], x[0]))
            return [entity_type for entity_type, _ in entity_types_with_order]

        while queue:
            # Sort queue alphabetically for deterministic ordering
            queue.sort()
            entity_type = queue.pop(0)
            result.append(entity_type)

            # Decrease in-degree of dependents
            for dependent in dependency_graph[entity_type]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Check for cycles (shouldn't happen with valid dependencies)
        if len(result) != len(entity_types):
            remaining = set(entity_types) - set(result)
            logger.warning(
                f"Circular dependency detected or missing dependencies. "
                f"Remaining entities: {remaining}. "
                f"Falling back to processing_order."
            )
            # Fallback to processing_order
            entity_types_with_order = [
                (entity_type, metadata.processing_order)
                for entity_type, metadata in self._metadata.items()
            ]
            entity_types_with_order.sort(key=lambda x: (x[1], x[0]))
            return [entity_type for entity_type, _ in entity_types_with_order]

        return result


def _entity_type_to_class_name(entity_type: str, suffix: str) -> str:
    """
    Convert entity_type to class name following the naming convention.

    Examples:
        'glossary_term' + 'Extractor' -> 'GlossaryTermExtractor'
        'structured_property' + 'Converter' -> 'StructuredPropertyConverter'
        'data_product' + 'MCPBuilder' -> 'DataProductMCPBuilder'

    Args:
        entity_type: The entity type name (snake_case)
        suffix: The class suffix ('Extractor', 'Converter', 'MCPBuilder')

    Returns:
        PascalCase class name
    """
    # Convert snake_case to PascalCase
    parts = entity_type.split("_")
    pascal_case = "".join(word.capitalize() for word in parts)
    return f"{pascal_case}{suffix}"


def _register_entity_module(registry: EntityRegistry, entity_type: str, module) -> None:
    """
    Register an entity module's components.

    Args:
        registry: The registry to register into
        entity_type: The entity type name (must match folder name)
        module: The imported module

    Raises:
        ValueError: If required components are missing
    """
    # Get components using naming convention
    # Extractor and Converter are optional for built entities (e.g., domains)
    ExtractorClass = getattr(
        module, _entity_type_to_class_name(entity_type, "Extractor"), None
    )
    ConverterClass = getattr(
        module, _entity_type_to_class_name(entity_type, "Converter"), None
    )
    MCPBuilderClass = getattr(
        module, _entity_type_to_class_name(entity_type, "MCPBuilder"), None
    )
    metadata = getattr(module, "ENTITY_METADATA", None)

    # Validate required components exist
    # Note: MCPBuilder is optional for 'domain' since domains are data structure only, not ingested
    missing = []
    if MCPBuilderClass is None and entity_type != "domain":
        missing.append(f"{_entity_type_to_class_name(entity_type, 'MCPBuilder')}")
    if metadata is None:
        missing.append("ENTITY_METADATA")

    if missing:
        raise ValueError(
            f"Entity module '{entity_type}' is missing required components: {', '.join(missing)}. "
            f"See docs/ENTITY_PLUGIN_CONTRACT.md for the required plugin contract."
        )

    # Validate metadata entity_type matches
    assert metadata is not None  # Already validated above
    if metadata.entity_type != entity_type:
        raise ValueError(
            f"Entity module '{entity_type}' has ENTITY_METADATA.entity_type='{metadata.entity_type}'. "
            f"Entity type must match the folder name."
        )

    # Register MCP builder (required, except for domain which is data structure only)
    if MCPBuilderClass:
        mcp_builder = MCPBuilderClass()
        registry.register_mcp_builder(entity_type, mcp_builder)
    elif entity_type == "domain":
        # Domain is data structure only - no MCP builder needed
        logger.debug(
            "Domain module has no MCPBuilder (domains are data structure only, not ingested)"
        )

    # Register extractor and converter if they exist (optional for built entities)
    if ExtractorClass:
        extractor = ExtractorClass()
        registry.register_extractor(entity_type, extractor)
    if ConverterClass:
        converter = ConverterClass()
        registry.register_converter(entity_type, converter)

    # Create processor instance only if all components exist
    # Built entities (like domains) may not have extractor/converter
    if ExtractorClass and ConverterClass and MCPBuilderClass:
        try:
            processor = EntityProcessor(
                extractor=ExtractorClass(),
                converter=ConverterClass(),
                mcp_builder=MCPBuilderClass(),
            )
            registry.register_processor(entity_type, processor)
        except Exception as e:
            raise ValueError(
                f"Failed to instantiate processor components for '{entity_type}': {e}. "
                f"Ensure all components can be instantiated without required arguments."
            ) from e

    # Register metadata (always required)
    registry.register_metadata(entity_type, metadata)

    logger.debug(f"Auto-registered entity module: {entity_type}")


def create_default_registry():
    """
    Create a registry with entity processors.

    DEPRECATED: Now uses SimpleEntityRegistry with explicit imports
    instead of auto-discovery. This is simpler for 2-3 entity types.

    Returns:
        SimpleEntityRegistry with explicitly registered entities
    """
    # Use the simplified registry instead
    from datahub.ingestion.source.rdf.entities.simple_registry import (
        create_default_registry as create_simple_registry,
    )

    return create_simple_registry()
