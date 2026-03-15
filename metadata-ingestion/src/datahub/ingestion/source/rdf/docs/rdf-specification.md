# RDF Specification: Business Glossary

Version: 2.0  
Date: December 2024

## Table of Contents

1. [Overview](#1-overview)
2. [Standards and Vocabularies](#2-standards-and-vocabularies)
3. [Glossaries and Business Terms](#3-glossaries-and-business-terms)
4. [Technical Implementation](#4-technical-implementation)
5. [DataHub Integration](#5-datahub-integration)
6. [Validation and Error Handling](#6-validation-and-error-handling)
7. [Common Patterns](#7-common-patterns)
8. [References](#8-references)

---

## 1. Overview

This specification defines an RDF vocabulary for creating business glossaries, designed for ingestion into data catalogs such as DataHub. It focuses on glossary modeling with term definitions, relationships, and hierarchical organization.

### 1.1 Goals

**Primary Goal: Business Glossaries**

- Define business terms with rich semantic relationships
- Support hierarchical organization of terms by domain
- Enable term-to-term relationships (broader/narrower/related)
- Provide reusable term definitions

**Removed for MVP:**

- Dataset modeling capabilities
- Dataset lineage tracking
- Data quality assertions
- Data products
- Structured properties

### 1.2 Design Principles

- Use existing W3C standards where possible (SKOS, OWL, RDFS)
- **Glossary-first approach**: Terms define business concepts
- Support hierarchical organization through domains
- Allow extension for domain-specific needs
- **Hybrid constraint modeling**: SHACL for validation, SKOS for semantic richness (when applicable)

---

## 2. Standards and Vocabularies

### 2.1 Required Vocabularies

| Prefix    | Namespace                               | Purpose                                                 |
| --------- | --------------------------------------- | ------------------------------------------------------- |
| `dcat`    | `http://www.w3.org/ns/dcat#`            | (Not used in MVP - reserved for future dataset support) |
| `dcterms` | `http://purl.org/dc/terms/`             | Dublin Core metadata terms                              |
| `sh`      | `http://www.w3.org/ns/shacl#`           | Structural schema and constraints                       |
| `xsd`     | `http://www.w3.org/2001/XMLSchema#`     | Standard datatypes                                      |
| `rdfs`    | `http://www.w3.org/2000/01/rdf-schema#` | Basic RDF schema terms                                  |
| `skos`    | `http://www.w3.org/2004/02/skos/core#`  | Semantic relationships and collections                  |
| `owl`     | `http://www.w3.org/2002/07/owl#`        | OWL classes, properties, and ontology constructs        |

### 2.2 Optional Vocabularies

| Prefix   | Namespace                          | Purpose                        |
| -------- | ---------------------------------- | ------------------------------ |
| `schema` | `http://schema.org/`               | Additional metadata properties |
| `vcard`  | `http://www.w3.org/2006/vcard/ns#` | Contact information            |
| `foaf`   | `http://xmlns.com/foaf/0.1/`       | Agent/person information       |

---

## 3. Glossaries and Business Terms

**Entity-Specific Specification**: See [`entities/glossary_term/SPEC.md`](../entities/glossary_term/SPEC.md)

The primary goal of RDF is to create comprehensive business glossaries that define terms and their relationships.

**Quick Reference**:

- **RDF Type**: `skos:Concept`
- **Required**: `skos:prefLabel` OR `rdfs:label` (≥3 characters), `skos:definition` OR `rdfs:comment`
- **Relationships**: `skos:broader`, `skos:narrower` (term-to-term)
- **Constraints**: SHACL constraints via dual-typed terms (`skos:Concept, sh:PropertyShape`)

---

**For complete glossary term specifications including term definitions, identification criteria, relationship mappings, IRI-to-URN conversion, constraint extraction, and the hybrid term-constraint pattern, see the [Glossary Term Specification](../entities/glossary_term/SPEC.md).**

---

## 4. Technical Implementation

### 4.1 IRI-to-URN Conversion Algorithm

The IRI-to-URN conversion follows a consistent pattern for all entity types:

```
Input: IRI (any valid IRI format)
Output: DataHub URN (urn:li:{entityType}:{path})
```

#### Step-by-Step Process

1. **Parse IRI**: Extract scheme, authority, path, and fragment
2. **Scheme Handling**:
   - HTTP/HTTPS schemes: Remove scheme portion
   - Custom schemes: Split on first `:` character
   - Other schemes: Handle based on `://` delimiter
3. **Path Preservation**: Preserve entire path structure after scheme removal
4. **Fragment Handling**: Preserve fragments as part of path structure
5. **URN Construction**: Build DataHub URN with preserved structure

#### Entity Type Mappings

- **Glossary Terms**: `urn:li:glossaryTerm:{path}`
- **Glossary Nodes**: `urn:li:glossaryNode:{path}`
- **Domains**: `urn:li:domain:{path}`

### 4.2 Constraint Extraction Algorithm

```python
def extract_constraints(graph, property_shape_uri):
    """Extract all constraints from a PropertyShape."""
    constraints = {}

    # Extract SHACL constraints
    constraints.update(extract_shacl_constraints(graph, property_shape_uri))

    # Extract SKOS enum constraints
    class_uri = get_class_uri(graph, property_shape_uri)
    if class_uri:
        enum_values = extract_enum_from_skos_collection(graph, class_uri)
        if enum_values:
            constraints['enum'] = enum_values

    return constraints

def extract_enum_values(graph, term_uri):
    """Extract enum values from SKOS Collections or OWL Enumerations."""
    enum_values = []

    # Try SKOS Collections first
    skos_values = extract_enum_from_skos_collection(graph, term_uri)
    if skos_values:
        return skos_values

    # Try OWL Enumerations
    owl_values = extract_enum_from_owl_enumeration(graph, term_uri)
    if owl_values:
        return owl_values

    return enum_values
```

### 8.3 Assertion Generation Algorithm

```python
def generate_assertions_from_constraints(constraints, field_context):
    """Generate DataHub assertions from extracted constraints."""
    assertions = []

    # Required field assertion
    if field_context.min_count > 0:
        assertions.append(create_not_null_assertion(field_context))

    # Length constraints
    if 'max_length' in constraints:
        assertions.append(create_length_assertion(constraints['max_length']))

    # Range constraints
    if 'min_inclusive' in constraints:
        assertions.append(create_range_assertion(constraints['min_inclusive'], 'min'))
    if 'max_inclusive' in constraints:
        assertions.append(create_range_assertion(constraints['max_inclusive'], 'max'))

    # Pattern constraints
    if 'pattern' in constraints:
        assertions.append(create_pattern_assertion(constraints['pattern']))

    # Enum constraints
    if 'enum' in constraints:
        assertions.append(create_enum_assertion(constraints['enum']))

    return assertions
```

### 4.4 Modular Architecture and Entity Registration

The rdf system uses a fully pluggable entity architecture where new entity types can be added without modifying core code.

#### 9.4.1 Entity Registry

The `EntityRegistry` provides centralized registration and lookup of entity processors:

```python
class EntityRegistry:
    """Central registry for entity processors and metadata."""

    def register_processor(self, entity_type: str, processor: EntityProcessor):
        """Register an entity processor."""

    def register_metadata(self, entity_type: str, metadata: EntityMetadata):
        """Register entity metadata."""

    def get_extractor(self, entity_type: str) -> EntityExtractor:
        """Get extractor for entity type."""

    def get_converter(self, entity_type: str) -> Optional[EntityConverter]:
        """Get converter for entity type (optional - may be None)."""

    def get_mcp_builder(self, entity_type: str) -> EntityMCPBuilder:
        """Get MCP builder for entity type."""

    def list_entity_types(self) -> List[str]:
        """List all registered entity types."""
```

#### 9.4.2 Entity Registration

Entity modules are explicitly registered in the registry. The system uses `EntityRegistry` that explicitly imports and registers entity types:

```python
class EntityRegistry:
    """Simplified registry with explicit entity type registration."""

    def __init__(self):
        # Explicitly register supported entity types
        self._register_glossary_term()
        self._register_relationship()
        self._register_domain()
```

**Entity Registration Requirements**:

- Entity folder must export `ENTITY_METADATA` instance
- Must export `{EntityName}Extractor` and `{EntityName}MCPBuilder` (required)
- Must export `{EntityName}Converter` only if using RDF AST layer (optional)
- Must follow naming conventions (see `ENTITY_PLUGIN_CONTRACT.md`)
- Must include `SPEC.md` file documenting the entity's RDF patterns, extraction logic, and DataHub mappings

**Note**: Converters are optional. Extractors can return `DataHub*` AST objects directly, eliminating the need for an RDF AST intermediate layer. This simplifies the architecture while maintaining flexibility.

#### 9.4.3 Dynamic Field Generation

`RDFGraph` and `DataHubGraph` classes dynamically initialize entity fields based on registered entity types:

```python
class RDFGraph:
    """Internal AST representation of the complete RDF graph."""
    def __init__(self):
        # Initialize entity fields dynamically from registry
        from ..entities.registry import create_default_registry
        registry = create_default_registry()

        # Initialize entity fields dynamically
        for entity_type, metadata in registry._metadata.items():
            field_name = _entity_type_to_field_name(entity_type)
            setattr(self, field_name, [])

        # Special fields (always present)
        self.metadata: Dict[str, Any] = {}
```

**Field Naming Convention**:

- `glossary_term` → `glossary_terms`
- `relationship` → `relationships`
- Default: pluralize entity type name

#### 9.4.4 Entity-Specific Specifications

Each entity module **must** include a `SPEC.md` file that provides comprehensive documentation:

- **Overview**: What the entity represents and its purpose
- **RDF Source Patterns**: How the entity is identified in RDF (types, properties, patterns)
- **Extraction and Conversion Logic**: Detailed explanation of extraction and conversion algorithms
- **DataHub Mapping**: Complete mapping of RDF properties to DataHub fields
- **Examples**: RDF examples showing the entity in use
- **Limitations**: Any known limitations or constraints

The main `rdf-specification.md` provides high-level summaries and links to entity-specific specs for detailed information. This modular documentation approach ensures:

- **Maintainability**: Entity-specific details are co-located with the code
- **Completeness**: Each entity has comprehensive, authoritative documentation
- **Discoverability**: Developers can find entity documentation alongside implementation

**Entity-Specific Specification Files**:

- `entities/glossary_term/SPEC.md` - Glossary terms and business vocabulary
- `entities/relationship/SPEC.md` - Term-to-term relationships
- `entities/domain/SPEC.md` - Domain organization

See `docs/ENTITY_PLUGIN_CONTRACT.md` for requirements when creating new entity modules.

#### 9.4.5 Entity-Specific URN Generators

Each entity type can define its own URN generator by inheriting from `UrnGeneratorBase`:

```python
from ...core.urn_generator import UrnGeneratorBase

class GlossaryTermUrnGenerator(UrnGeneratorBase):
    """Entity-specific URN generation for glossary terms."""

    def generate_glossary_term_urn(self, iri: str) -> str:
        # Implementation
        pass
```

**Shared Utilities**: `UrnGeneratorBase` provides shared methods:

- `_normalize_platform()` - Platform name normalization
- `derive_path_from_iri()` - IRI path extraction
- `generate_data_platform_urn()` - Platform URN generation
- `generate_corpgroup_urn_from_owner_iri()` - Owner group URN generation

### 4.5 Dynamic Export Target Generation

The `ExportTarget` enum is dynamically generated from registered entity metadata:

```python
def _create_export_target_enum() -> type[Enum]:
    """Dynamically create ExportTarget enum from registered entities."""
    registry = create_default_registry()

    enum_values = {
        'ALL': 'all',
        'ENTITIES': 'entities',
        'LINKS': 'links',
        'DDL': 'ddl',
    }

    # Add entity-specific targets from registered entities
    for entity_type in registry.list_entity_types():
        metadata = registry.get_metadata(entity_type)
        if metadata and metadata.cli_names:
            for cli_name in metadata.cli_names:
                enum_member_name = cli_name.upper().replace('-', '_')
                enum_values[enum_member_name] = cli_name

    return Enum('ExportTarget', enum_values)
```

**Result**: New entity types automatically appear in CLI choices without code changes.

---

## 5. DataHub Integration

### 5.1 Entity Type Mappings

| RDF Entity Type   | DataHub Entity Type | URN Format                   |
| ----------------- | ------------------- | ---------------------------- |
| `skos:Concept`    | `GlossaryTerm`      | `urn:li:glossaryTerm:{path}` |
| `skos:Collection` | `GlossaryNode`      | `urn:li:glossaryNode:{path}` |

---

## 5. DataHub Integration

### 5.1 Entity Type Mappings

| RDF Entity Type   | DataHub Entity Type | URN Format                   |
| ----------------- | ------------------- | ---------------------------- |
| `skos:Concept`    | `GlossaryTerm`      | `urn:li:glossaryTerm:{path}` |
| `skos:Collection` | `GlossaryNode`      | `urn:li:glossaryNode:{path}` |

---

## 6. Validation and Error Handling

### 11.1 RDF Validation

#### Required Format Validation

- Must have valid scheme (http, https, custom schemes)
- Must have non-empty path after scheme removal
- Must be parseable by URL parsing library

#### Entity Validation

- **Glossary Terms**: Must have label ≥3 characters, valid URI reference
- **Relationships**: Referenced entities must exist, no circular references

### 6.2 Constraint Validation

#### SHACL Constraint Validation

- `sh:pattern` must be valid regex
- `sh:minInclusive` ≤ `sh:maxInclusive`
- `sh:minLength` ≤ `sh:maxLength`
- `sh:minCount` ≥ 0, `sh:maxCount` ≥ `sh:minCount`

#### SKOS Collection Validation

- Collection members must have valid labels
- No circular membership relationships
- Collection must have proper SKOS type

### 6.3 Error Handling

#### Error Categories

1. **Parse Errors**: Malformed RDF, invalid syntax
2. **Validation Errors**: Invalid entities, broken references
3. **Constraint Errors**: Invalid constraint definitions
4. **API Errors**: DataHub connection, authentication issues

#### Error Recovery

- Non-fatal errors allow processing to continue
- Fatal errors stop processing with detailed messages
- All errors are logged with appropriate severity levels
- Partial results are preserved when possible

---

## 7. Common Patterns

### 7.1 Simple Custom Terms (Default Pattern)

```turtle
ex:creditScoreProperty a sh:PropertyShape ;
    sh:path ex:creditScore ;
    sh:datatype xsd:integer ;
    sh:minInclusive 300 ;
    sh:maxInclusive 850 ;
    sh:name "Credit Score" ;
    sh:description "FICO credit score" ;
    ex:sqlType "INTEGER" .
```

### 7.2 Enum Values with SKOS Collections

```turtle
# Parent concept
ex:Status a skos:Concept ;
    skos:prefLabel "Status" .

# Enum values
ex:Active a skos:Concept ;
    skos:prefLabel "Active" ;
    skos:memberOf ex:StatusCollection .

ex:Inactive a skos:Concept ;
    skos:prefLabel "Inactive" ;
    skos:memberOf ex:StatusCollection .

# Collection
ex:StatusCollection a skos:Collection ;
    skos:prefLabel "Status Collection" .
```

### 7.3 Pattern-Based Precision

```turtle
ex:currencyAmountProperty a sh:PropertyShape ;
    sh:path ex:amount ;
    sh:datatype xsd:decimal ;
    sh:pattern "^\\d{1,10}\\.\\d{2}$" ;  # DECIMAL(12,2)
    sh:minInclusive 0.00 ;
    sh:name "Currency Amount" ;
    ex:sqlType "DECIMAL(12,2)" .
```

### 7.4 Contextual Constraints

```turtle
# Required in one schema
ex:TradeSchema a sh:NodeShape ;
    sh:property [
        sh:node ex:brokerIdProperty ;
        sh:minCount 1 ;  # Required
        sh:maxCount 1
    ] .

# Optional in another schema
ex:QuoteSchema a sh:NodeShape ;
    sh:property [
        sh:node ex:brokerIdProperty ;
        sh:maxCount 1    # Optional
    ] .
```

### 7.5 Cross-Column Constraints

```turtle
# Simple cross-field constraints
ex:TradeShape a sh:NodeShape ;
    sh:targetClass ex:Trade ;

    # Date ordering constraint
    sh:property [
        sh:path ex:tradeDate ;
        sh:lessThan ex:settlementDate ;
        sh:message "Trade date must be before settlement date"@en
    ] ;

    # Currency inequality constraint
    sh:property [
        sh:path ex:buyCurrency ;
        sh:notEquals ex:sellCurrency ;
        sh:message "Buy currency must be different from sell currency"@en
    ] .

# Complex business rule with SPARQL
ex:TradeShape a sh:NodeShape ;
    sh:targetClass ex:Trade ;

    sh:sparql [
        sh:message "Large trades must have T+1 or later settlement"@en ;
        sh:select """
            PREFIX ex: <http://example.org/vocab#>
            SELECT $this ?amount ?tradeDate ?settlementDate
            WHERE {
                $this ex:amount ?amount ;
                      ex:tradeDate ?tradeDate ;
                      ex:settlementDate ?settlementDate .
                BIND((?settlementDate - ?tradeDate) / (24 * 60 * 60 * 1000) AS ?daysBetween)
                FILTER(?amount > 1000000 && ?daysBetween < 1)
            }
        """ ;
    ] .
```

---

## 8. References

- DCAT 3: https://www.w3.org/TR/vocab-dcat-3/
- SHACL: https://www.w3.org/TR/shacl/
- SKOS: https://www.w3.org/TR/skos-reference/
- Dublin Core: https://www.dublincore.org/specifications/dublin-core/dcmi-terms/
- Schema.org: https://schema.org/
- DataHub Assertions: https://datahubproject.io/docs/metadata/assertions/

---

## Appendix: Full Namespace Declarations

```turtle
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix schema: <http://schema.org/> .
@prefix vcard: <http://www.w3.org/2006/vcard/ns#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
```
