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
- **Datasets** (Section 4): DCAT datasets, schema fields, platform integration
- **Platform Definitions** (Section 5): Platform service definitions and naming conventions
- **Lineage** (Section 6): PROV-O lineage processing with activities and relationships
- **Custom Properties** (Section 7): Structured property definitions and value assignments
- **Domain Ownership** (Section 8): Ownership groups and domain assignment
- **Technical Implementation** (Section 9): URN generation, constraint extraction, modular architecture, auto-discovery
- **DataHub Integration** (Section 10): Entity mappings, assertion creation, platform integration

**Purpose**: Precise technical specifications that ensure functionality isn't lost during refactoring.

## Examples

- [Examples Directory](../examples/README.md) - Sample RDF files and usage examples
- [BCBS239 Demo](../examples/bcbs239/README.md) - Banking regulatory compliance example

## Key Concepts

### Platform Mapping

**Preferred Method: `dcat:accessService`**

```turtle
ex:CustomerDatabase a void:Dataset ;
    dcterms:title "Customer Database" ;
    dcat:accessService <http://postgres.example.com> .
```

**Platform Extraction:**

- `http://postgres.example.com` → `postgres` (extracted from hostname)
- `"postgresql"` → `postgresql` (literal value used as-is)

**Benefits:**

- Standards compliant (W3C DCAT)
- Semantic clarity (represents access service)
- Tool integration (works with DCAT validators)
- Future proof (established semantic web standard)

### Entity Identification Logic

**Glossary Terms** are identified by:

- Having labels (`rdfs:label` OR `skos:prefLabel` ≥3 chars)
- Being typed as: `owl:Class`, `owl:NamedIndividual`, `skos:Concept`, or custom class instances
- Excluding: `owl:Ontology` declarations

**Datasets** are identified by:

- Having appropriate RDF type: `void:Dataset`, `dcterms:Dataset`, `schema:Dataset`, `dh:Dataset`
- Having basic metadata (name/title via priority mapping)
- Platform identification via `dcat:accessService` (preferred) or `schema:provider`

**Lineage Activities** are identified by:

- Being typed as `prov:Activity`
- Having upstream (`prov:used`) and downstream (`prov:generated`) relationships
- Having temporal information (`prov:startedAtTime`, `prov:endedAtTime`)
- Having user attribution (`prov:wasAssociatedWith`)

**Lineage Relationships** are identified by:

- `prov:used` - upstream data dependencies
- `prov:generated` - downstream data products
- `prov:wasDerivedFrom` - direct data derivations
- `prov:wasGeneratedBy` - activity-to-entity relationships
- `prov:wasInfluencedBy` - downstream influences

### Glossary Mapping

RDF glossaries are mapped to DataHub's glossary system through:

- **Terms**: Individual concepts with definitions and relationships
- **Nodes**: Container hierarchies for organizing terms (`skos:ConceptScheme`, `skos:Collection`)
- **Relationships**: Hierarchical (`skos:broader`), associative (`skos:related`), and external reference links

### Dataset Mapping

RDF datasets are mapped to DataHub's dataset system through:

- **Datasets**: Data entities with metadata and connections
- **Schema Fields**: Field definitions with types, constraints, and glossary associations
- **Platforms**: Data platform integration (SPARQL, databases, files)
- **Lineage Activities**: Data processing jobs with temporal and attribution information
- **Lineage Relationships**: Complete data flow mapping via PROV-O standard

### Property Mapping Priority

**Term Properties:**

1. Name: `skos:prefLabel` → `rdfs:label`
2. Definition: `skos:definition` → `rdfs:comment`

**Dataset Properties:**

1. Name: `dcterms:title` → `schema:name` → `rdfs:label` → custom `hasName`
2. Description: `dcterms:description` → `schema:description` → `rdfs:comment` → custom `hasDescription`
3. Identifier: `dcterms:identifier` → `dh:hasURN` → custom `hasIdentifier`

**Field Properties:**

1. Name: `dh:hasName` → `rdfs:label` → custom `hasName`
2. Type: `dh:hasDataType` → custom `hasDataType`
3. Description: `rdfs:comment` → custom `hasDescription`

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

### Dataset Documentation

1. Use clear, descriptive `dcterms:title`
2. Include comprehensive `dcterms:description`
3. Specify proper `dcterms:creator` and `dcterms:publisher`
4. Include creation and modification timestamps

### Lineage Documentation

1. Document all data dependencies with `prov:used`
2. Specify data generation with `prov:wasGeneratedBy`
3. Include user attribution with `prov:wasAssociatedWith`
4. Use proper timestamps for lineage events
5. Define activities with clear descriptions and temporal bounds
6. Map field-level dependencies for detailed lineage tracking

### Lineage Processing

RDF provides comprehensive lineage processing through PROV-O (Provenance Ontology):

**Activity Processing:**

- Extracts `prov:Activity` entities as DataHub data jobs
- Captures temporal information (`prov:startedAtTime`, `prov:endedAtTime`)
- Includes user attribution (`prov:wasAssociatedWith`)
- Generates hierarchical URNs for activities

**Relationship Processing:**

- Maps `prov:used` to upstream data dependencies
- Maps `prov:generated` to downstream data products
- Processes `prov:wasDerivedFrom` for direct derivations
- Handles `prov:wasGeneratedBy` for activity-to-entity links
- Supports `prov:wasInfluencedBy` for downstream influences

**Field-Level Lineage:**

- Captures field-to-field mappings between datasets
- Tracks data transformations at the column level
- Identifies unauthorized data flows and inconsistencies
- Supports complex ETL process documentation

## Data Governance Demonstration: Authorized vs Unauthorized Flows

RDF includes a comprehensive demonstration of how unauthorized data flows create inconsistencies between regulatory reports that should contain matching values.

### The Problem: Regulatory Report Inconsistencies

**Authorized Flow (FR Y-9C Report):**

```
Loan Trading → Aggregation Job → Finance Job → Risk Job → FR Y-9C Report
     ↓              ↓            ↓         ↓           ↓
  Multiple      Consolidated   Finance   Risk       Authorized
  Systems      Loan Data     Balances  Metrics    Regulatory
                                           ↓           ↓
                                    Validated    Same Line Items
                                    References   Same Values
```

**Unauthorized Flow (FFIEC 031 Report):**

```
Account Data → Performance Copy → FFIEC 031 Report
     ↓              ↓                   ↓
  Reference    Finance Copy        Different
  Data         (Unauthorized)       Line Items
                                          ↓
                                   Different Values
```

### Realistic Processing Jobs

The demonstration models actual enterprise data processing:

**Multi-Input ETL Jobs:**

- **Loan Aggregation**: 2+ inputs → Consolidated dataset (Daily Spark job)
- **Finance Processing**: 3+ inputs → Portfolio balances (Daily SQL job)
- **Risk Calculations**: 3+ inputs → Risk metrics (Daily Python/R job)
- **Regulatory Reporting**: Multiple inputs → FR Y-9C report (Monthly SAS job)

**Unauthorized Activities:**

- **Performance Copy**: Creates stale data copy (Unauthorized Pentaho job)
- **Alternative Reporting**: Uses unauthorized data sources (High-risk SAS job)

### Provenance-Ontology (PROV-O) Standards for Governance

**Rich Activity Metadata (W3C Standard):**

```turtle
<http://datahub.com/lineage/fr_y9c_reporting_job> a prov:RegulatoryActivity ;
    rdfs:label "FR Y-9C Regulatory Reporting Job" ;
    rdfs:comment "Monthly regulatory reporting job generating Federal Reserve Y-9C Call Report" ;
    prov:startedAtTime "2024-01-15T06:00:00Z"^^xsd:dateTime ;
    prov:wasAssociatedWith <http://org/teams/regulatory-reporting> ;
    dcterms:creator <http://org/teams/regulatory-reporting> ;
    prov:hasPrimarySource "regulatory-compliance" .
```

**Unauthorized Activity Markers (PROV-O Invalidation):**

```turtle
<http://datahub.com/lineage/ffiec_031_reporting_job> a prov:RegulatoryActivity ;
    rdfs:label "FFIEC 031 Reporting Job (UNAUTHORIZED INPUTS)" ;
    rdfs:comment "CRITICAL WARNING: FFIEC 031 report accidentally uses Finance performance copy" ;
    prov:invalidatedBy <http://gov/detection/data-inconsistency> ;
    dcterms:description "WARNING: Uses unauthorized Finance performance copy - FED VALIDATION RISK HIGH" ;
    dcterms:isReferencedBy <http://gov/compliance/regulatory-violations> .
```

### Expected Inconsistencies

| Line Item               | FR Y-9C (Authorized)  | FFIEC 031 (Unauthorized)    | Impact                       |
| ----------------------- | --------------------- | --------------------------- | ---------------------------- |
| Total Loan Count        | 15,423 (consolidated) | 12,891 (stale copy)         | ❌ Regulatory mismatch       |
| Commercial Loans        | $2.3B (current)       | $1.8B (outdated)            | ❌ Capital calculation error |
| Account Classifications | Validated (latest)    | Outdated (performance copy) | ❌ Audit findings            |

### Business Value

This demonstration showcases:

1. **Realistic Processing**: Models actual multi-input ETL jobs with scheduling and technology
2. **Clear Business Impact**: Shows how authorization violations create regulatory inconsistencies
3. **Governance Integration**: Demonstrates DataHub's data governance capabilities
4. **Risk Management**: Highlights critical data integrity issues that affect compliance
5. **Audit Trail**: Provides complete provenance tracking for regulatory examinations

**DataHub Visualization**: Creates compelling lineage graphs showing authorized (green) vs unauthorized (red) data flows, making governance issues immediately visible to stakeholders.

**Example Usage**: Run `python -m rdf.scripts.datahub_rdf --source examples/bcbs239/` to see the full demonstration in DataHub.

### Standard RDF Properties vs DataHub Extensions

The lineage schema demonstrates **cross-platform compatibility** by using only W3C-standard predicates instead of proprietary DataHub ontology:

| **DataHub Property**        | **Standard RDF Predicate**       | **Purpose**               |
| --------------------------- | -------------------------------- | ------------------------- |
| `dh:hasBusinessProcess`     | `prov:hasPrimarySource`          | Business context          |
| `dh:hasActivityType`        | `rdfs:subClassOf prov:Activity`  | Activity classification   |
| `dh:hasTransformationType`  | `prov:used` patterns             | Transformation indicators |
| `dh:hasSchedule`            | `prov:startedAtTime/endedAtTime` | Temporal context          |
| `dh:hasOwner`               | `prov:wasAssociatedWith`         | Team/user attribution     |
| `dh:hasTechnology`          | `dcterms:creator` + comments     | Technology context        |
| `dh:hasAuthorizationStatus` | `prov:invalidatedBy`             | Governance markers        |

**Benefits of Standard RDF Approach:**

- ✅ **Cross-platform compatibility** - Works with any RDF-compliant system
- ✅ **W3C standardized** - Uses PROV-O (Provenance) and Dublin Core predicates
- ✅ **Better interoperability** - Semantic web compliant
- ✅ **Future-proof** - Not dependent on proprietary ontologies
- ✅ **Pure lineage modeling** - Focus on provenance rather than implementation details

## Technical Implementation

### Modular Architecture

RDF uses a fully modular, pluggable entity architecture:

- **Auto-Discovery**: Entity modules are automatically discovered and registered
- **Processing Order**: Entities declare their processing order via `processing_order` in `ENTITY_METADATA`
- **Post-Processing Hooks**: Cross-entity dependencies are handled via `build_post_processing_mcps()` hooks
- **Separation of Concerns**: Each entity module is self-contained with its own extractor, converter, and MCP builder

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

### [SHACL Migration Guide](SHACL_MIGRATION_GUIDE.md)

Guide for migrating from legacy SKOS approach to modern SHACL approach for dataset field definitions.

### Archived Documentation

Historical and proposal documents are archived in `docs/archive/`:

- `RDF_GLOSSARY_MAPPING.md` - Consolidated into main specification
- `RDF_DATASET_MAPPING.md` - Consolidated into main specification
- `TRANSPILER_ARCHITECTURE.md` - Consolidated into main specification
- Other historical/proposal documents

## Getting Help

For questions about RDF:

1. **Start with**: [RDF Specification](rdf-specification.md) - Complete technical reference
2. **Adding entities**: [Entity Plugin Contract](ENTITY_PLUGIN_CONTRACT.md) - Plugin development guide
3. **Examples**: Review the examples in the `examples/` directory
4. **Source code**: Examine the source code in `src/rdf/`
5. **CLI help**: Run the CLI with `--help` for command options
