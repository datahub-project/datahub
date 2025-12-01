"""
Entity Registry

Central registry for entity processors.
Allows dynamic registration and lookup of entity processing modules.

Auto-discovers entity modules by scanning the entities directory for modules
that export ENTITY_METADATA and required components.
"""

import importlib
import logging
import pkgutil
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
        Get all registered entity types sorted by processing_order.

        Entities with lower processing_order values are processed first.
        Entities without explicit ordering (default 100) are processed last.

        Returns:
            List of entity type names sorted by processing_order
        """
        entity_types_with_order = [
            (entity_type, metadata.processing_order)
            for entity_type, metadata in self._metadata.items()
        ]
        # Sort by processing_order, then by entity_type for stability
        entity_types_with_order.sort(key=lambda x: (x[1], x[0]))
        return [entity_type for entity_type, _ in entity_types_with_order]


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
    # Get required components using naming convention
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

    # Validate all required components exist
    missing = []
    if ExtractorClass is None:
        missing.append(f"{_entity_type_to_class_name(entity_type, 'Extractor')}")
    if ConverterClass is None:
        missing.append(f"{_entity_type_to_class_name(entity_type, 'Converter')}")
    if MCPBuilderClass is None:
        missing.append(f"{_entity_type_to_class_name(entity_type, 'MCPBuilder')}")
    if metadata is None:
        missing.append("ENTITY_METADATA")

    if missing:
        raise ValueError(
            f"Entity module '{entity_type}' is missing required components: {', '.join(missing)}. "
            f"See docs/ENTITY_PLUGIN_CONTRACT.md for the required plugin contract."
        )

    # Validate metadata entity_type matches
    if metadata.entity_type != entity_type:
        raise ValueError(
            f"Entity module '{entity_type}' has ENTITY_METADATA.entity_type='{metadata.entity_type}'. "
            f"Entity type must match the folder name."
        )

    # Create processor instance
    try:
        processor = EntityProcessor(
            extractor=ExtractorClass(),
            converter=ConverterClass(),
            mcp_builder=MCPBuilderClass(),
        )
    except Exception as e:
        raise ValueError(
            f"Failed to instantiate processor components for '{entity_type}': {e}. "
            f"Ensure all components can be instantiated without required arguments."
        ) from e

    # Register processor and metadata
    registry.register_processor(entity_type, processor)
    registry.register_metadata(entity_type, metadata)

    logger.debug(f"Auto-registered entity module: {entity_type}")


def create_default_registry() -> EntityRegistry:
    """
    Create a registry with all entity processors auto-discovered.

    Scans the entities directory for modules that export ENTITY_METADATA
    and required components (Extractor, Converter, MCPBuilder), then
    automatically registers them.

    Entity modules must follow the plugin contract:
    - Folder name matches entity_type
    - Exports {EntityName}Extractor, {EntityName}Converter, {EntityName}MCPBuilder
    - Exports ENTITY_METADATA instance

    See docs/ENTITY_PLUGIN_CONTRACT.md for details.

    Returns:
        EntityRegistry with all discovered entities registered
    """
    registry = EntityRegistry()

    # Get the entities package path
    import sys

    entities_package = sys.modules[__name__].__package__
    entities_module = sys.modules[entities_package]

    # Scan entities directory for subdirectories (entity modules)
    entity_modules_found = []
    for _finder, name, ispkg in pkgutil.iter_modules(
        entities_module.__path__, entities_package + "."
    ):
        if ispkg:  # Only process subdirectories (entity modules)
            # Skip special directories
            if name in ["__pycache__", "base", "registry", "pipeline"]:
                continue

            try:
                # Import the module
                module = importlib.import_module(name)

                # Check if it has ENTITY_METADATA (required for auto-discovery)
                if hasattr(module, "ENTITY_METADATA"):
                    entity_type = name.split(".")[-1]  # Get folder name
                    _register_entity_module(registry, entity_type, module)
                    entity_modules_found.append(entity_type)
                else:
                    logger.debug(
                        f"Skipping module '{name}': no ENTITY_METADATA found (not an entity module)"
                    )
            except Exception as e:
                logger.warning(f"Failed to auto-discover entity module '{name}': {e}")
                # Continue with other modules rather than failing completely

    if not entity_modules_found:
        logger.warning(
            "No entity modules were auto-discovered. Check that modules follow the plugin contract."
        )
    else:
        logger.info(
            f"Auto-discovered and registered {len(entity_modules_found)} entity types: {sorted(entity_modules_found)}"
        )

    return registry
