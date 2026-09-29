"""
Entity-based modular architecture for RDF-to-DataHub transpilation.

Each entity type (glossary_term, dataset, relationship, etc.) is self-contained
in its own module with:
- extractor.py: RDF Graph → RDF AST extraction
- converter.py: RDF AST → DataHub AST conversion
- mcp_builder.py: DataHub AST → MCP creation

This architecture follows the Open/Closed principle - adding new entity types
doesn't require modifying existing code.

## Adding a New Entity Type

To add a new entity type, create a folder in this directory following the
Entity Plugin Contract. The system will automatically discover and register it.

See docs/ENTITY_PLUGIN_CONTRACT.md for complete documentation on:
- Required folder structure
- Naming conventions
- Interface implementations
- ENTITY_METADATA structure
- Auto-discovery mechanism
- SPEC.md documentation requirements
"""

from datahub.ingestion.source.rdf.entities.base import (
    EntityConverter,
    EntityExtractor,
    EntityMCPBuilder,
    EntityProcessor,
)
from datahub.ingestion.source.rdf.entities.registry import (
    EntityRegistry,
    create_default_registry,
)

__all__ = [
    "EntityExtractor",
    "EntityConverter",
    "EntityMCPBuilder",
    "EntityProcessor",
    "EntityRegistry",
    "create_default_registry",
]
