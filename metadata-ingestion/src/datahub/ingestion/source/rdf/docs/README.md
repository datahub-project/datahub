# RDF Documentation

## Overview

RDF is a lightweight RDF ontology ingestion system for DataHub. This documentation provides comprehensive guides for understanding how RDF concepts are mapped to DataHub entities.

## Quick Start

- [Main README](../README.md) - Installation, usage, and basic examples
- [Package Documentation](../README.md) - Core components and programmatic usage

## Detailed Specifications

### [RDF Specification](rdf-specification.md)

**Complete technical specification** - Precise mappings, algorithms, and implementation details:

- **Glossary Terms** (Section 3): SKOS concepts, relationships, constraints, IRI-to-URN conversion
- **Technical Implementation** (Section 4): URN generation, constraint extraction, modular architecture, entity registration
- **DataHub Integration** (Section 5): Entity mappings and integration
- **Validation and Error Handling** (Section 6): RDF validation, constraint validation, error handling
- **Common Patterns** (Section 7): Common RDF patterns for glossary terms
- **References** (Section 8): Standards and vocabulary references

**Purpose**: Precise technical specifications that ensure functionality isn't lost during refactoring.

## Examples

Example RDF files can be found in the test fixtures directory: `tests/unit/rdf/`

## Key Concepts

### Entity Identification Logic

**Glossary Terms** are identified by:

- Having labels (`rdfs:label` OR `skos:prefLabel` ≥3 chars)
- Being typed as: `owl:Class`, `owl:NamedIndividual`, `skos:Concept`, or custom class instances
- Excluding: `owl:Ontology` declarations

### Glossary Mapping

RDF glossaries are mapped to DataHub's glossary system through:

- **Terms**: Individual concepts with definitions and relationships
- **Nodes**: Container hierarchies for organizing terms (`skos:ConceptScheme`, `skos:Collection`)
- **Relationships**: Hierarchical (`skos:broader`), associative (`skos:related`), and external reference links

### Property Mapping Priority

**Term Properties:**

1. Name: `skos:prefLabel` → `rdfs:label`
2. Definition: `skos:definition` → `rdfs:comment`

### IRI-to-URN Transformation

RDF IRIs are transformed to DataHub URNs using:

- **Path-based hierarchy** for HTTP/HTTPS IRIs
- **Scheme preservation** for custom ontology schemes
- **Fragment handling** for term-specific identifiers

## Best Practices

### IRI Design

1. Use hierarchical paths: `/domain/subdomain/concept`
2. Avoid deep nesting (>5 levels)
3. Use consistent naming conventions
4. Include meaningful fragments

### Term Structure

1. Clear, descriptive `skos:prefLabel`
2. Comprehensive `skos:definition`
3. Logical `skos:broader` relationships
4. Consistent terminology across concepts

## Technical Implementation

### Modular Architecture

RDF uses a fully modular, pluggable entity architecture:

- **Explicit Registration**: Entity modules are explicitly registered in the registry
- **Dependency-Based Ordering**: Entities declare their dependencies via `dependencies` in `ENTITY_METADATA`
- **Post-Processing Hooks**: Cross-entity dependencies are handled via `build_post_processing_mcps()` hooks
- **Separation of Concerns**: Each entity module is self-contained with its own extractor, optional converter, and MCP builder
- **Streamlined Processing**: Extractors can return DataHub AST directly, eliminating unnecessary abstraction layers

**Processing Flow:**

1. Entities are processed in order (lowest `processing_order` first)
2. Standard MCPs are created for each entity type
3. Post-processing hooks are called for cross-entity dependencies
4. Special cases (non-registered entities) are handled separately

See [Entity Plugin Contract](ENTITY_PLUGIN_CONTRACT.md) for details on adding new entity types.

### URN Generation Algorithm

1. Parse IRI: Extract scheme, authority, path, and fragment
2. Scheme Handling: HTTP/HTTPS → DataHub URN format, Custom schemes → preserved
3. Path Processing: Split path into hierarchical components
4. Fragment Handling: Use fragment as final component
5. URN Construction: Build DataHub-compliant URN

### Validation Rules

- **IRI Validation**: Valid scheme, path components, fragment syntax
- **Property Validation**: Required properties, non-empty values, valid relationships
- **Hierarchy Validation**: No circular references, consistent naming, logical depth

### Error Handling

- **IRI Parsing Errors**: Invalid schemes, malformed paths, invalid fragments
- **Mapping Errors**: Missing properties, invalid values, broken references
- **DataHub API Errors**: Authentication, rate limiting, entity creation failures

## Additional Documentation

### [Background and Business Requirements](background.md)

Comprehensive business requirements document covering the background, motivation, problem statement, solution proposal, business justification, market opportunity, and success criteria for RDF. Essential reading for understanding the "why" behind RDF.

### [Entity Plugin Contract](ENTITY_PLUGIN_CONTRACT.md)

Complete guide for adding new entity types to rdf. Follow this contract to create pluggable entity modules that are automatically discovered and registered.

### Archived Documentation

Historical and proposal documents have been removed for MVP. Full feature set documentation is available in the `rdf-full-features` branch.

## Getting Help

For questions about RDF:

1. **Start with**: [RDF Specification](rdf-specification.md) - Complete technical reference
2. **Adding entities**: [Entity Plugin Contract](ENTITY_PLUGIN_CONTRACT.md) - Plugin development guide
3. **Examples**: Review the examples in the `examples/` directory
4. **Source code**: Examine the source code in `src/rdf/`
5. **CLI help**: Run the CLI with `--help` for command options
