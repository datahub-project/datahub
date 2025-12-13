# RDF User Stories and Acceptance Criteria

## Overview

This document provides detailed user stories with precise acceptance criteria for implementing RDF. Each story includes specific technical requirements, mapping rules, and validation criteria to ensure consistent implementation.

**Status**: This document has been updated to reflect current implementation status. Checked items `[x]` indicate completed features. Unchecked items `[ ]` indicate features not yet implemented or requiring verification.

**Last Updated**: December 2024

## Implementation Status Summary

- ✅ **Core Glossary Management** (Stories 1-8): ~95% complete

  - Format support: TTL, RDF/XML, JSON-LD (N-Triples pending)
  - Source support: File, folder (server sources pending)
  - Term detection, relationships, IRI-to-URN conversion: Complete
  - Domain management, glossary nodes, structured properties: Complete
  - CLI/API: Ingest command complete (list/delete commands pending)

- ✅ **Advanced Dataset and Lineage** (Stories 9-11): ~100% complete

  - Dataset processing, platform integration: Complete
  - Comprehensive lineage processing: Complete
  - Schema field processing: Complete

- ✅ **Experimental Features** (Story 12): ~100% complete

  - Dynamic routing with SPARQL queries: Complete

- ✅ **Technical Implementation** (Stories 13-15): ~95% complete
  - Streamlined architecture: Complete (simplified from three-phase)
  - Dependency injection framework: Complete
  - Validation and error handling: Complete (rollback/retry pending)

## Table of Contents

1. [Core Glossary Management Stories](#core-glossary-management-stories)
2. [Advanced Dataset and Lineage Stories](#advanced-dataset-and-lineage-stories)
3. [Experimental Features Stories](#experimental-features-stories)
4. [Technical Implementation Stories](#technical-implementation-stories)

---

## Core Glossary Management Stories

### Story 1: RDF Glossary Ingestion

**As a** data steward  
**I want to** ingest RDF glossaries from various sources and formats  
**So that** I can import my existing ontology into DataHub without manual configuration

#### Acceptance Criteria

**AC1.1: Format Support**

- [x] System supports TTL (Turtle) format with proper namespace handling
- [x] System supports RDF/XML format with namespace preservation
- [x] System supports JSON-LD format with context handling
- [ ] System supports N-Triples format with proper parsing
- [x] System validates RDF syntax and reports specific parsing errors

**AC1.2: Source Support**

- [x] System handles single file sources (`--source file.ttl`)
- [x] System handles directory sources (`--source /path/to/glossary/`)
- [ ] System handles server sources (`--source http://sparql.endpoint.com`)
- [x] System processes multiple files in directory recursively
- [x] System handles mixed format directories (TTL + RDF/XML)

**AC1.4: Error Handling**

- [x] System provides detailed error messages for malformed RDF
- [x] System continues processing after encountering non-fatal errors
- [x] System logs all processing steps for debugging
- [x] System validates file permissions and accessibility

---

### Story 2: Glossary Term Detection and Processing

**As a** data steward  
**I want to** automatically detect glossary terms from RDF  
**So that** I don't need to manually specify which resources are terms

#### Acceptance Criteria

**AC2.1: Term Detection Criteria**

- [x] System detects `skos:Concept` resources as glossary terms
- [x] System detects `owl:Class` resources as glossary terms
- [x] System detects `owl:NamedIndividual` resources as glossary terms
- [x] System detects custom class instances (any resource typed as instance of custom class)
- [x] System excludes `owl:Ontology` declarations from term detection
- [x] System requires terms to have labels (`rdfs:label` OR `skos:prefLabel` ≥3 characters)

**AC2.2: Property Extraction**

- [x] System extracts `skos:prefLabel` as primary name (preferred)
- [x] System falls back to `rdfs:label` if `skos:prefLabel` not available
- [x] System extracts `skos:definition` as primary description (preferred)
- [x] System falls back to `rdfs:comment` if `skos:definition` not available
- [x] System preserves language tags for multilingual support
- [x] System extracts custom properties and stores as metadata

**AC2.3: Validation Rules**

- [x] System validates that terms have valid URI references (not blank nodes)
- [x] System validates that labels are non-empty strings (≥3 characters)
- [x] System validates that definitions are non-empty strings
- [x] System reports validation errors with specific term URIs

---

### Story 3: SKOS Relationship Mapping

**As a** data steward  
**I want to** map SKOS relationships to DataHub glossary relationships  
**So that** my glossary hierarchy is preserved in DataHub

#### Acceptance Criteria

**AC3.1: Hierarchical Relationships**

- [x] System maps `skos:broader` to DataHub parent relationships
- [x] System maps `skos:narrower` to DataHub child relationships
- [x] System maps `skos:broadMatch` and `skos:narrowMatch` to hierarchy relationships
- [x] System creates bidirectional relationships automatically
- [x] System validates no circular references in hierarchy

**AC3.2: Associative Relationships**

- [x] System maps `skos:related` to DataHub related terms
- [x] System maps `skos:closeMatch` to DataHub related terms
- [x] System preserves relationship directionality
- [x] System handles multiple related terms per term

**AC3.3: External References**

- [x] System maps `skos:exactMatch` to DataHub external references
- [x] System maps `owl:sameAs` to DataHub external references
- [x] System preserves external reference URIs
- [x] System validates external reference format

**AC3.4: Relationship Validation**

- [x] System validates that referenced terms exist in the glossary
- [x] System reports broken relationship references
- [x] System handles missing referenced terms gracefully

---

### Story 4: IRI-to-URN Conversion

**As a** data steward  
**I want to** convert RDF IRIs to DataHub URNs  
**So that** my glossary terms have proper DataHub identifiers

#### Acceptance Criteria

**AC4.1: IRI Processing**

- [x] System processes HTTP/HTTPS IRIs by removing scheme and preserving path structure
- [x] System processes custom scheme IRIs by splitting on first `:` character
- [x] System handles various scheme formats (http://, https://, ftp://, custom:)
- [x] System preserves fragments as part of path structure
- [x] System handles empty path segments gracefully

**AC4.2: URN Generation**

- [x] System generates DataHub-compliant URNs for all entity types
- [x] System preserves original case and structure from IRI
- [x] System validates URN format compliance
- [x] System handles edge cases and error conditions
- [x] System follows consistent URN generation algorithm

**AC4.3: Validation and Error Handling**

- [x] System validates IRI format and scheme requirements
- [x] System provides detailed error messages for invalid IRIs
- [x] System handles malformed IRIs gracefully
- [x] System reports specific validation failures

---

### Story 5: Domain Management

**As a** data steward  
**I want to** automatically create DataHub domains from IRI hierarchy  
**So that** my glossary terms are organized in DataHub

#### Acceptance Criteria

**AC5.1: Domain Hierarchy Creation**

- [x] System creates domains for parent segments only (excludes term name)
- [x] System creates `urn:li:domain:example_com` for `https://example.com/finance/accounts`
- [x] System creates `urn:li:domain:finance` for `https://example.com/finance/accounts`
- [x] System assigns dataset `accounts` to `urn:li:domain:finance`
- [x] System handles deep hierarchies correctly

**AC5.2: Domain Naming Convention**

- [x] System converts `example.com` → `urn:li:domain:example_com`
- [x] System converts `finance` → `urn:li:domain:finance`
- [x] System converts `loan-trading` → `urn:li:domain:loan_trading`
- [x] System preserves original segment names for display
- [x] System validates domain URN format

**AC5.3: Domain Assignment**

- [x] System assigns glossary terms to leaf domain (most specific parent)
- [x] System creates parent-child relationships between domains
- [x] System handles shared domains correctly
- [x] System validates domain assignment logic

---

### Story 6: Glossary Node Support

**As a** data steward  
**I want to** process SKOS concept schemes and collections  
**So that** I can organize my glossary terms in DataHub

#### Acceptance Criteria

**AC6.1: Concept Scheme Processing**

- [x] System detects `skos:ConceptScheme` resources as glossary nodes
- [x] System maps `skos:prefLabel` → DataHub glossary node name
- [x] System maps `skos:definition` → DataHub glossary node description
- [x] System creates proper DataHub `GlossaryNode` entities
- [x] System generates URNs for concept schemes

**AC6.2: Collection Processing**

- [x] System detects `skos:Collection` resources as glossary nodes
- [x] System processes collection metadata (labels, descriptions)
- [x] System handles collection membership relationships
- [x] System creates DataHub glossary nodes for collections

**AC6.3: Node Relationships**

- [x] System maps `skos:broader` relationships for nodes
- [x] System creates parent-child relationships between nodes
- [x] System links terms to their containing nodes
- [x] System validates node hierarchy consistency

---

### Story 7: Structured Properties Support

**As a** data steward  
**I want to** attach structured properties to glossary terms  
**So that** I can add domain-specific metadata

#### Acceptance Criteria

**AC7.1: Property Detection**

- [x] System detects `rdf:Property` declarations with `rdfs:domain`
- [x] System maps `rdfs:domain` to appropriate DataHub entity types
- [x] System extracts `rdfs:label` as property name
- [x] System extracts `rdfs:comment` as property description
- [x] System identifies enum values from `rdfs:range` class instances

**AC7.2: Entity Type Mapping**

- [x] System maps `dcat:Dataset` domain → `dataset` entity type
- [x] System maps `skos:Concept` domain → `glossaryTerm` entity type
- [x] System maps `schema:Person` domain → `user` entity type
- [x] System maps `schema:Organization` domain → `corpGroup` entity type
- [x] System handles multiple domains per property

**AC7.3: Property Application**

- [x] System applies structured properties to appropriate entities
- [x] System validates property values against allowed values
- [x] System creates DataHub structured property definitions
- [x] System generates proper URNs for structured properties

---

### Story 8: CLI and API Interface

**As a** developer  
**I want to** use CLI commands and Python API  
**So that** I can integrate RDF into my workflows

#### Acceptance Criteria

**AC8.1: CLI Commands**

- [x] System provides `ingest` command with `--source`, `--export`, `--server`, `--token` options
- [ ] System provides `list` command to show existing glossary items
- [ ] System provides `delete` command to remove glossary terms/domains
- [x] System supports `--dry-run` flag for safe testing
- [x] System provides comprehensive help and usage examples

**AC8.2: Python API**

- [x] System provides `DataHubClient` class for API interactions
- [x] System provides `OntologyToDataHub` class for processing
- [x] System supports both dry run and live execution modes
- [x] System provides clear error handling and logging
- [x] System includes comprehensive API documentation

**AC8.3: Export Targets**

- [x] System supports `entities` target (datasets, glossary terms, properties)
- [x] System supports `links` target (relationships, associations)
- [x] System supports `lineage` target (lineage activities and relationships)
- [x] System supports `all` target (comprehensive export)
- [x] System maintains backward compatibility with legacy targets

---

## Advanced Dataset and Lineage Stories

### Story 9: Dataset Processing

**As a** data steward  
**I want to** process RDF datasets with platform integration  
**So that** I can manage my data assets in DataHub

#### Acceptance Criteria

**AC9.1: Dataset Detection**

- [x] System detects `void:Dataset` resources as datasets
- [x] System detects `dcterms:Dataset` resources as datasets
- [x] System detects `schema:Dataset` resources as datasets
- [x] System detects `dh:Dataset` resources as datasets
- [x] System validates dataset metadata requirements

**AC9.2: Dataset Properties**

- [x] System maps `dcterms:title` → dataset name (preferred)
- [x] System falls back to `schema:name` → dataset name
- [x] System falls back to `rdfs:label` → dataset name
- [x] System maps `dcterms:description` → dataset description
- [x] System maps `dcterms:creator` → dataset ownership
- [x] System maps `dcterms:created` → creation timestamp
- [x] System maps `dcterms:modified` → modification timestamp

**AC9.3: Platform Integration**

- [x] System maps `dcat:accessService` → platform identifier (preferred)
- [x] System maps `schema:provider` → platform identifier
- [x] System maps `void:sparqlEndpoint` → SPARQL platform
- [x] System maps `void:dataDump` → file platform
- [x] System extracts platform information from service URIs
- [x] System validates platform connection configurations

---

### Story 10: Comprehensive Lineage Processing

**As a** data steward  
**I want to** process PROV-O lineage relationships  
**So that** I can track data flow and dependencies

#### Acceptance Criteria

**AC10.1: Activity Processing**

- [x] System detects `prov:Activity` resources as DataHub DataJobs
- [x] System maps `rdfs:label` → activity name
- [x] System maps `dcterms:description` → activity description
- [x] System maps `prov:startedAtTime` → activity start time
- [x] System maps `prov:endedAtTime` → activity end time
- [x] System maps `prov:wasAssociatedWith` → user attribution

**AC10.2: Lineage Relationships**

- [x] System maps `prov:used` → upstream data dependencies
- [x] System maps `prov:generated` → downstream data products
- [x] System maps `prov:wasDerivedFrom` → direct derivation relationships
- [x] System maps `prov:wasGeneratedBy` → activity-to-entity relationships
- [x] System maps `prov:wasInfluencedBy` → downstream influences
- [x] System preserves activity mediation in lineage edges

**AC10.3: Field-Level Lineage**

- [x] System processes field-to-field mappings between datasets
- [x] System tracks data transformations at column level
- [x] System identifies unauthorized data flows
- [x] System supports complex ETL process documentation
- [x] System generates proper DataHub lineage URNs

---

### Story 11: Schema Field Processing

**As a** data steward  
**I want to** extract and map dataset schema fields  
**So that** I can document my data structure

#### Acceptance Criteria

**AC11.1: Field Detection**

- [x] System detects fields referenced via `dh:hasSchemaField`
- [x] System detects custom field properties
- [x] System requires field name via `dh:hasName`, `rdfs:label`, or custom `hasName`
- [x] System validates field identification criteria

**AC11.2: Field Properties**

- [x] System maps `dh:hasName` → field path
- [x] System maps `rdfs:label` → field display name
- [x] System maps `dh:hasDataType` → field data type
- [x] System maps `dh:isNullable` → nullable constraint
- [x] System maps `dh:hasGlossaryTerm` → associated glossary terms
- [x] System maps `rdfs:comment` → field description

**AC11.3: Data Type Mapping**

- [x] System maps `varchar`, `string` → `StringTypeClass`
- [x] System maps `date`, `datetime` → `DateTypeClass`
- [x] System maps `int`, `number`, `decimal` → `NumberTypeClass`
- [x] System maps `bool`, `boolean` → `BooleanTypeClass`
- [x] System defaults to `StringTypeClass` for unknown types
- [x] System validates data type constraints

---

## Experimental Features Stories

### Story 12: Dynamic Routing

**As a** developer  
**I want to** use SPARQL queries for dynamic entity detection  
**So that** I can process any RDF pattern without hardcoded logic

#### Acceptance Criteria

**AC12.1: Query-Based Detection**

- [x] System executes SPARQL queries to extract entities with types
- [x] System routes processing based on `entity_type` field in results
- [x] System processes generically using appropriate handlers
- [x] System eliminates need for separate processing methods per entity type

**AC12.2: Query Registry**

- [x] System maintains centralized SPARQL queries for each export target
- [x] System supports query customization for specialized use cases
- [x] System validates query syntax and execution
- [x] System provides query performance optimization

---

## Technical Implementation Stories

### Story 13: Streamlined Architecture (Simplified from Three-Phase)

**As a** developer  
**I want to** implement clean separation of concerns with minimal abstraction  
**So that** the system is maintainable, testable, and easy to understand

#### Acceptance Criteria

**AC13.1: RDF to DataHub AST (Simplified)**

- [x] System extracts entities directly from RDF graphs
- [x] System creates internal `DataHubGraph` representation
- [x] System extracts datasets, glossary terms, activities, properties
- [x] System handles various RDF patterns (SKOS, OWL, DCAT, PROV-O)
- [x] Extractors can return DataHub AST directly (no RDF AST layer required)
- [x] RDF AST layer is optional - only used when needed

**AC13.2: DataHub AST to MCPs**

- [x] System implements MCP builders for DataHub ingestion
- [x] System generates DataHub URNs with proper format
- [x] System converts RDF types to DataHub types
- [x] System prepares DataHub-specific metadata
- [x] System handles DataHub naming conventions

**AC13.3: Output Strategy**

- [x] System supports DataHub ingestion target
- [x] System supports pretty print output for debugging
- [x] System supports file export
- [x] System enables easy addition of new output formats

---

### Story 14: Dependency Injection Framework

**As a** developer  
**I want to** use dependency injection for modular architecture  
**So that** components can be easily swapped and tested

#### Acceptance Criteria

**AC14.1: RDF Loading (Simplified)**

- [x] System implements `load_rdf_graph()` function for RDF source loading
- [x] System supports file, folder, and URL sources
- [x] System provides consistent API for loading RDF graphs
- [x] System enables easy addition of new source types

**AC14.2: Query Factory**

- [x] System implements `QueryFactory` for query processing
- [x] System supports `SPARQLQuery`, `PassThroughQuery`, `FilterQuery`
- [x] System provides `QueryInterface` for consistent API
- [x] System enables query customization and optimization

**AC14.3: Target Factory**

- [x] System implements `TargetFactory` for output targets
- [x] System supports `DataHubTarget`, `PrettyPrintTarget`, `FileTarget`
- [x] System provides `TargetInterface` for consistent API
- [x] System enables easy addition of new output formats

---

### Story 15: Validation and Error Handling

**As a** developer  
**I want to** implement comprehensive validation  
**So that** the system provides clear error messages and graceful recovery

#### Acceptance Criteria

**AC15.1: RDF Validation**

- [x] System validates RDF syntax and structure
- [x] System reports specific parsing errors with line numbers
- [x] System validates namespace declarations
- [x] System handles malformed RDF gracefully

**AC15.2: Entity Validation**

- [x] System validates entity identification criteria
- [x] System validates property mappings and constraints
- [x] System validates relationship references
- [x] System reports validation errors with specific entity URIs

**AC15.3: DataHub Validation**

- [x] System validates DataHub URN format
- [x] System validates DataHub entity properties
- [x] System validates DataHub relationship constraints
- [x] System provides detailed error messages for DataHub API failures

**AC15.4: Error Recovery**

- [x] System continues processing after non-fatal errors
- [x] System logs all errors with appropriate severity levels
- [ ] System provides rollback capabilities for failed operations
- [ ] System supports retry mechanisms for transient failures

---

## Implementation Notes

### Technical Specifications

For detailed technical specifications including:

- **IRI-to-URN Conversion Algorithm**: Complete algorithm with pseudocode
- **Relationship Mapping Tables**: SKOS and PROV-O to DataHub mappings
- **Property Mapping Rules**: Priority chains and fallback rules
- **Validation Rules**: Comprehensive validation criteria
- **DataHub Integration**: Complete entity type mappings

See: [RDF Specification](rdf-specification.md)

### Development Guidelines

- **User Stories**: Focus on functional requirements and user value
- **Technical Specs**: Reference the technical specifications document for implementation details
- **Testing**: Each acceptance criteria should have corresponding test cases
- **Documentation**: Keep user stories focused on "what" and "why", not "how"
