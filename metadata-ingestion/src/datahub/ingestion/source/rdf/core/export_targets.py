#!/usr/bin/env python3
"""
Export Target Types

Dynamically generated enum for specifying what to export from RDF graphs.
Export targets are registered by entity modules via EntityMetadata.

Each entity registers export target enum values through its CLI names.
Special system-level targets (ALL, DDL, OWNERSHIP) are also included.
"""

from enum import Enum
from typing import Dict


def _create_export_target_enum() -> type[Enum]:
    """
    Dynamically create ExportTarget enum from registered entities.

    Each entity's CLI names become ExportTarget enum values.
    For example, glossary_term with cli_names=['glossary', 'glossary_terms']
    creates ExportTarget.GLOSSARY = "glossary" and ExportTarget.GLOSSARY_TERMS = "glossary_terms"

    Returns:
        ExportTarget enum class with values from registered entities
    """
    # Import here to avoid circular dependencies
    from datahub.ingestion.source.rdf.entities.registry import create_default_registry

    registry = create_default_registry()

    # Start with special/system-level targets that aren't entity-specific
    enum_values: Dict[str, str] = {
        "ALL": "all",
        "ENTITIES": "entities",  # All entities
        "LINKS": "links",  # Relationships between entities
        "DDL": "ddl",  # DDL export (dataset-specific, but not an entity type)
        "OWNERSHIP": "ownership",  # Domain ownership information (not an entity type)
    }

    # Add entity-specific targets from registered entities
    # Each CLI name becomes an enum member
    for entity_type in registry.list_entity_types():
        metadata = registry.get_metadata(entity_type)
        if metadata and metadata.cli_names:
            for cli_name in metadata.cli_names:
                # Convert CLI name to UPPER_CASE for enum member name
                # Handle special characters by replacing with underscores
                enum_member_name = cli_name.upper().replace("-", "_")
                # Only add if not already present (avoid duplicates)
                if enum_member_name not in enum_values:
                    enum_values[enum_member_name] = cli_name

    # Create enum dynamically
    return Enum("ExportTarget", enum_values)


# Create the enum at module level
# This will be regenerated each time the module is imported, ensuring it reflects
# the current state of registered entities
ExportTarget = _create_export_target_enum()


def get_export_targets_for_entity(entity_type: str) -> list[str]:
    """
    Get export target enum values for a specific entity type.

    Args:
        entity_type: The entity type name (e.g., 'glossary_term', 'dataset')

    Returns:
        List of export target values (CLI names) for the entity
    """
    from datahub.ingestion.source.rdf.entities.registry import create_default_registry

    registry = create_default_registry()
    metadata = registry.get_metadata(entity_type)

    if metadata:
        return metadata.cli_names
    return []


def get_all_export_targets() -> list[str]:
    """
    Get all export target values from registered entities.

    Returns:
        List of all export target values (CLI names)
    """
    from datahub.ingestion.source.rdf.entities.registry import create_default_registry

    registry = create_default_registry()
    return registry.get_all_cli_choices()
