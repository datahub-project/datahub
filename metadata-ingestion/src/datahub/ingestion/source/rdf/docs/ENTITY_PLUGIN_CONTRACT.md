# Entity Plugin Contract

## Overview

The rdf system uses a fully pluggable entity architecture. To add a new entity type, simply create a folder in `src/rdf/entities/` following this contract, and the system will automatically discover and register it. **No code changes are needed elsewhere.**

## Required Structure

Each entity module must follow this directory structure:

```
entities/
  your_entity/              # Folder name = entity_type (snake_case)
    __init__.py            # Must export ENTITY_METADATA and components
    extractor.py          # Must implement EntityExtractor
    converter.py          # Optional: Only if RDF AST → DataHub AST conversion needed
    mcp_builder.py        # Must implement EntityMCPBuilder
    ast.py               # Must define DataHub* AST class (RDF* AST optional)
    urn_generator.py     # Optional: entity-specific URN generator
    SPEC.md              # Required: Entity-specific specification documentation
```

**Note**: The `converter.py` file is optional. If your extractor returns `DataHub*` AST objects directly (like `glossary_term` does), you don't need a converter. The RDF AST layer is also optional - you can extract directly to DataHub AST.

## Required Exports in `__init__.py`

Your `__init__.py` must export these components:

1. **Extractor class**: `{EntityName}Extractor` (e.g., `GlossaryTermExtractor`) - **Required**
2. **Converter class**: `{EntityName}Converter` (e.g., `GlossaryTermConverter`) - **Optional** (only if using RDF AST)
3. **MCP Builder class**: `{EntityName}MCPBuilder` (e.g., `GlossaryTermMCPBuilder`) - **Required**
4. **ENTITY_METADATA**: `EntityMetadata` instance - **Required**

### Naming Convention

The system uses a strict naming convention to auto-discover components:

- **Entity folder**: `snake_case` (e.g., `glossary_term`, `data_product`)
- **Extractor class**: `{PascalCaseEntityName}Extractor` (e.g., `GlossaryTermExtractor`)
- **Converter class**: `{PascalCaseEntityName}Converter` (e.g., `GlossaryTermConverter`) - Optional
- **MCP Builder class**: `{PascalCaseEntityName}MCPBuilder` (e.g., `GlossaryTermMCPBuilder`)

**Conversion rule**: `snake_case` → `PascalCase` (underscores removed, each word capitalized)

- `glossary_term` → `GlossaryTerm`
- `relationship` → `Relationship`

## ENTITY_METADATA Structure

```python
from ..base import EntityMetadata
from .ast import DataHubYourEntity  # RDF AST class optional

ENTITY_METADATA = EntityMetadata(
    entity_type='your_entity',  # MUST match folder name exactly
    cli_names=['your_entity', 'your_entities'],  # CLI argument choices
    rdf_ast_class=None,  # Optional: RDF AST class if using RDF AST layer
    datahub_ast_class=DataHubYourEntity,  # DataHub AST class from ast.py
    export_targets=['pretty_print', 'file', 'datahub'],  # Supported export targets
    dependencies=[],  # List of entity types this depends on (for ordering)
    validation_rules={}  # Optional: entity-specific validation rules
)
```

### Field Descriptions

- **`entity_type`**: Must exactly match the folder name (e.g., if folder is `glossary_term`, this must be `'glossary_term'`)
- **`cli_names`**: List of strings that users can use in CLI arguments like `--export-only` and `--skip-export`
- **`rdf_ast_class`**: Optional RDF AST class. Set to `None` if extractor returns DataHub AST directly
- **`datahub_ast_class`**: The DataHub AST class that represents entities (required)
- **`dependencies`**: List of entity types this entity depends on (e.g., `['domain']`). Ensures correct processing order
- **`export_targets`**: List of export targets this entity supports (e.g., `'pretty_print'`, `'file'`, `'datahub'`, `'ddl'`)
- **`processing_order`**: Integer determining the order in which entities are processed during ingestion. Lower values are processed first. Default is 100. **Important**: Entities with dependencies on other entities should have higher `processing_order` values. For example:
  - Glossary terms: `processing_order=100` (may depend on domains for hierarchy)
  - Relationships: `processing_order=200` (depend on glossary terms existing first)
- **`validation_rules`**: Optional dictionary of entity-specific validation rules

## Required Interface Implementations

### EntityExtractor

**File**: `extractor.py`

Must implement `EntityExtractor[EntityT]` where `EntityT` is either `RDFYourEntity` or `DataHubYourEntity`:

**Option 1: Extract to RDF AST (requires converter)**

```python
from ..base import EntityExtractor
from .ast import RDFYourEntity

class YourEntityExtractor(EntityExtractor[RDFYourEntity]):
    def extract(self, graph: Graph, uri: URIRef, context: Dict[str, Any] = None) -> Optional[RDFYourEntity]:
        """Extract a single entity from the RDF graph."""
        # Implementation: extract entity from RDF
        pass
```

**Option 2: Extract directly to DataHub AST (no converter needed)**

```python
from ..base import EntityExtractor
from .ast import DataHubYourEntity

class YourEntityExtractor(EntityExtractor[DataHubYourEntity]):
    def extract(self, graph: Graph, uri: URIRef, context: Dict[str, Any] = None) -> Optional[DataHubYourEntity]:
        """Extract a single entity directly to DataHub AST."""
        # Implementation: extract and convert in one step
        pass
```

### EntityConverter (Optional)

**File**: `converter.py`

**Only needed if your extractor returns RDF AST objects.** If your extractor returns `DataHub*` AST directly (like `glossary_term`), you can skip this step.

Must implement `EntityConverter[RDFEntityT, DataHubEntityT]`:

```python
from ..base import EntityConverter
from .ast import RDFYourEntity, DataHubYourEntity

class YourEntityConverter(EntityConverter[RDFYourEntity, DataHubYourEntity]):
    @property
    def entity_type(self) -> str:
        """Return the entity type name."""
        return "your_entity"

    def convert(self, rdf_entity: RDFYourEntity, context: Dict[str, Any] = None) -> Optional[DataHubYourEntity]:
        """Convert a single RDF AST entity to DataHub AST."""
        # Implementation: convert RDF representation to DataHub representation
        pass

    def convert_all(self, rdf_entities: List[RDFYourEntity], context: Dict[str, Any] = None) -> List[DataHubYourEntity]:
        """Convert all RDF AST entities to DataHub AST."""
        # Implementation: convert list of entities
        pass
```

### EntityMCPBuilder

**File**: `mcp_builder.py`

Must implement `EntityMCPBuilder[DataHubEntityT]`:

```python
from ..base import EntityMCPBuilder
from .ast import DataHubYourEntity
from datahub.emitter.mcp import MetadataChangeProposalWrapper

class YourEntityMCPBuilder(EntityMCPBuilder[DataHubYourEntity]):
    @property
    def entity_type(self) -> str:
        """Return the entity type name."""
        return "your_entity"

    def build_mcps(self, entity: DataHubYourEntity, context: Dict[str, Any] = None) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for a single DataHub AST entity."""
        # Implementation: create MCPs for the entity
        pass

    def build_all_mcps(self, entities: List[DataHubYourEntity], context: Dict[str, Any] = None) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for all DataHub AST entities of this type."""
        # Implementation: create MCPs for all entities
        pass

    def build_post_processing_mcps(self, datahub_graph: Any, context: Dict[str, Any] = None) -> List[MetadataChangeProposalWrapper]:
        """
        Optional hook for building MCPs that depend on other entities.

        This method is called after all standard entities have been processed,
        allowing entities to handle cross-entity dependencies (e.g., dataset-domain
        associations, glossary nodes from domains, structured property value assignments).

        Args:
            datahub_graph: The complete DataHubGraph AST containing all entities
            context: Optional context with shared state (includes 'report' for entity counting)

        Returns:
            List of MetadataChangeProposalWrapper objects (empty list by default)

        Example use cases:
        - Creating glossary nodes from domain hierarchy (GlossaryTermMCPBuilder)
        - Processing term relationships after terms are created (RelationshipMCPBuilder)

        **Note**: Domains are data structure only, not ingested as DataHub domain entities
        """
        return []  # Default: no post-processing needed
```

## AST Classes

**File**: `ast.py`

Must define at minimum the `DataHub*` AST class. The `RDF*` AST class is optional (only needed if using a converter):

```python
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

# Optional: Only needed if extractor returns RDF AST
@dataclass
class RDFYourEntity:
    """RDF AST representation of your entity."""
    uri: str
    name: str
    # Add other fields as needed
    properties: Dict[str, Any] = field(default_factory=dict)
    custom_properties: Dict[str, Any] = field(default_factory=dict)

# Required: Always needed
@dataclass
class DataHubYourEntity:
    """DataHub AST representation of your entity."""
    urn: str
    name: str
    # Add other fields as needed
    properties: Dict[str, Any] = field(default_factory=dict)
    custom_properties: Dict[str, Any] = field(default_factory=dict)
```

**Example**: `glossary_term` doesn't use RDF AST - the extractor returns `DataHubGlossaryTerm` directly, so no `RDFGlossaryTerm` class is needed.

## URN Generator (Optional)

**File**: `urn_generator.py`

If your entity needs custom URN generation, create a URN generator:

```python
from ...core.urn_generator import UrnGeneratorBase
from urllib.parse import urlparse

class YourEntityUrnGenerator(UrnGeneratorBase):
    """URN generator for your entity type."""

    def generate_your_entity_urn(self, iri: str) -> str:
        """
        Generate a DataHub URN from an IRI.

        Args:
            iri: The RDF IRI

        Returns:
            DataHub URN
        """
        parsed = urlparse(iri)
        entity_name = self._preserve_iri_structure(parsed)
        return f"urn:li:yourEntity:{entity_name}"
```

Then use it in your extractor (if extracting directly to DataHub AST) or converter (if using RDF AST):

```python
from .urn_generator import YourEntityUrnGenerator

# If extracting directly to DataHub AST:
class YourEntityExtractor(EntityExtractor[DataHubYourEntity]):
    def __init__(self):
        self.urn_generator = YourEntityUrnGenerator()

# Or if using converter:
class YourEntityConverter(EntityConverter[...]):
    def __init__(self):
        self.urn_generator = YourEntityUrnGenerator()
```

## Auto-Discovery

Once you create the folder and implement the contract:

1. ✅ The system will **auto-discover** your entity on next import
2. ✅ CLI arguments will **automatically include** your `cli_names`
3. ✅ Export targets will **automatically include** your entity
4. ✅ Graph classes will **automatically have fields** for your entity
5. ✅ **No code changes needed** elsewhere in the codebase!

## Field Name Mapping

The system automatically maps entity types to field names in `RDFGraph` and `DataHubGraph`:

- `glossary_term` → `glossary_terms`
- `dataset` → `datasets`
- `lineage` → `lineage_relationships` (special case)
- `structured_property` → `structured_properties`
- `data_product` → `data_products`

**Default rule**: Pluralize by adding `'s'` (handles most cases)

## Special Fields

Some fields are not entity types but sub-components:

- `structured_property_values` - Sub-component of `structured_property`
- `lineage_activities` - Sub-component of `lineage`
- `cross_field_constraints` - Sub-component of `assertion`
- `domains` - Built from other entities, not extracted
- `metadata` - Special field for graph-level metadata

These are automatically initialized and don't need to be registered.

## Entity Specification Documentation

**File**: `SPEC.md`

Each entity module **must** include a `SPEC.md` file that documents:

- **Overview**: What the entity represents and its purpose
- **RDF Source Patterns**: How the entity is identified in RDF (types, properties, patterns)
- **Extraction and Conversion Logic**: How the entity is extracted and converted
- **DataHub Mapping**: How RDF properties map to DataHub fields
- **Examples**: RDF examples showing the entity in use
- **Limitations**: Any known limitations or constraints

The `SPEC.md` file should be comprehensive and serve as the authoritative reference for how the entity works. See existing entity `SPEC.md` files for examples:

- `src/rdf/entities/glossary_term/SPEC.md`
- `src/rdf/entities/dataset/SPEC.md`
- `src/rdf/entities/lineage/SPEC.md`

The main `docs/rdf-specification.md` provides high-level summaries and links to entity-specific specs for detailed information.

## Example: Complete Entity Module

See `src/rdf/entities/glossary_term/` as a reference implementation:

- ✅ Follows naming convention
- ✅ Exports all required components
- ✅ Defines `ENTITY_METADATA`
- ✅ Implements all three interfaces
- ✅ Includes URN generator
- ✅ Defines AST classes
- ✅ Includes `SPEC.md` documentation

## Processing Order and Cross-Entity Dependencies

### Processing Order

Entities are processed in the order specified by `processing_order` in `ENTITY_METADATA`. Lower values are processed first. This ensures that entities with dependencies are created after their dependencies.

**Standard Processing Order:**

1. **Structured properties** (`processing_order=1`) - Definitions must exist before values can be assigned
2. **Glossary terms** (`processing_order=2`) - May reference structured properties
3. **Relationships** (`processing_order=3`) - Depend on glossary terms existing
4. **Datasets** (`processing_order=4`) - May reference glossary terms and structured properties
5. **Lineage** (`processing_order=5`) - Depend on datasets existing
6. **Data products** (`processing_order=6`) - Depend on datasets
7. **Assertions** (`processing_order=7`) - Depend on datasets and fields

### Post-Processing Hooks

For cross-entity dependencies that can't be handled by processing order alone, implement `build_post_processing_mcps()`. This hook is called after all standard entities have been processed, giving you access to the complete `datahub_graph`.

**When to use post-processing hooks:**

- **Glossary nodes from domains**: Glossary nodes are created from domain hierarchy, which requires access to all domains
- **Dataset-domain associations**: Datasets need to be associated with domains after both are created
- **Structured property value assignments**: Values are assigned to entities after both the property definition and target entity exist

**Example: Dataset-Domain Associations**

```python
def build_post_processing_mcps(self, datahub_graph: Any, context: Dict[str, Any] = None) -> List[MetadataChangeProposalWrapper]:
    """Associate datasets with their domains."""
    mcps = []
    for domain in datahub_graph.domains:
        for dataset in domain.datasets:
            mcp = self.create_dataset_domain_association_mcp(
                str(dataset.urn), str(domain.urn)
            )
            mcps.append(mcp)
    return mcps
```

## Validation

The system validates your entity module on discovery:

- ✅ Checks for required components (Extractor, MCPBuilder, ENTITY_METADATA)
- ✅ Validates `ENTITY_METADATA.entity_type` matches folder name
- ✅ Validates `dependencies` is a list (if provided)
- ✅ Ensures components can be instantiated
- ✅ Logs warnings for missing or invalid components
- ✅ Converter is optional - only checked if `rdf_ast_class` is not None

## Troubleshooting

### Entity Not Discovered

- Check folder name matches `entity_type` in `ENTITY_METADATA`
- Verify `__init__.py` exports all required components
- Check class names follow naming convention
- Review logs for discovery errors

### Components Not Found

- Ensure class names match: `{PascalCaseEntityName}{ComponentType}`
- Verify classes are exported in `__all__` (optional but recommended)
- Check imports in `__init__.py` are correct
- Remember: Converter is optional if extractor returns DataHub AST directly

### Field Not Available in Graph

- Fields are created dynamically - ensure entity is registered
- Check `_entity_type_to_field_name()` mapping if field name seems wrong
- Verify `ENTITY_METADATA` is properly defined

## Best Practices

1. **Follow naming conventions strictly** - Auto-discovery depends on it
2. **Export everything in `__all__`** - Makes imports explicit
3. **Document your entity type** - Add docstrings explaining what it extracts
4. **Create comprehensive `SPEC.md`** - Document RDF patterns, extraction logic, and DataHub mappings
5. **Handle errors gracefully** - Return `None` or empty lists on failure
6. **Use context for shared state** - Pass URN generators, caches, etc. via context
7. **Test your entity module** - Create unit tests for each component

## Advanced: Cross-Entity Dependencies

If your entity needs to reference other entities (e.g., relationships between entities):

```python
# In converter.py
from ..other_entity.urn_generator import OtherEntityUrnGenerator

class YourEntityConverter(EntityConverter[...]):
    def __init__(self):
        self.urn_generator = YourEntityUrnGenerator()
        self.other_urn_generator = OtherEntityUrnGenerator()  # For cross-entity URNs
```

## Questions?

- See existing entity modules for examples
- Check `src/rdf/entities/base.py` for interface definitions
- Review `src/rdf/entities/registry.py` for discovery logic
