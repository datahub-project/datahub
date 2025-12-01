# RDF Specification: Business Glossary and Dataset Modeling

Version: 2.0  
Date: December 2024

## Table of Contents

1. [Overview](#1-overview)
2. [Standards and Vocabularies](#2-standards-and-vocabularies)
3. [Glossaries and Business Terms](#3-glossaries-and-business-terms)
4. [Datasets](#4-datasets)
5. [Platform Definitions](#5-platform-definitions)
6. [Dataset Lineage](#6-dataset-lineage)
7. [Custom Properties](#7-custom-properties)
8. [Domain Ownership](#8-domain-ownership)
9. [Technical Implementation](#9-technical-implementation)
10. [DataHub Integration](#10-datahub-integration)
11. [Validation and Error Handling](#11-validation-and-error-handling)
12. [Common Patterns](#12-common-patterns)
13. [References](#13-references)

---

## 1. Overview

This specification defines a comprehensive RDF vocabulary for creating business glossaries and describing datasets, designed for ingestion into data catalogs such as DataHub. It combines glossary modeling with dataset schema definition capabilities.

### 1.1 Goals

**Primary Goal: Business Glossaries**

- Define business terms with rich semantic relationships
- Support hierarchical organization of terms by domain
- Enable term-to-term relationships (broader/narrower/related)
- Provide reusable term definitions across datasets

**Secondary Goal: Dataset Modeling**

- Provide rich catalog-level metadata (title, description, ownership, keywords)
- Define precise structural schemas (fields, types, constraints)
- Enable reusable field/property definitions across datasets
- Support technology-specific type information (e.g., SQL types)
- Reference glossary terms for field definitions

**Supporting Capabilities**

- Track dataset lineage and field-level lineage
- Support custom properties on both terms and datasets
- Enable validation of dataset instances against schemas
- Generate data quality assertions from constraint definitions

### 1.2 Design Principles

- Use existing W3C standards where possible (SKOS, DCAT, SHACL)
- **Glossary-first approach**: Terms define business concepts, datasets reference terms
- Separate glossary definitions from dataset schemas
- Support reusable term definitions across multiple datasets
- Allow extension for domain-specific needs
- **Hybrid constraint modeling**: SHACL for validation, SKOS for semantic richness
- **Assertion-first approach**: Generate DataHub assertions from RDF constraints

---

## 2. Standards and Vocabularies

### 2.1 Required Vocabularies

| Prefix    | Namespace                               | Purpose                                          |
| --------- | --------------------------------------- | ------------------------------------------------ |
| `dcat`    | `http://www.w3.org/ns/dcat#`            | Dataset catalog metadata                         |
| `dcterms` | `http://purl.org/dc/terms/`             | Dublin Core metadata terms                       |
| `sh`      | `http://www.w3.org/ns/shacl#`           | Structural schema and constraints                |
| `xsd`     | `http://www.w3.org/2001/XMLSchema#`     | Standard datatypes                               |
| `rdfs`    | `http://www.w3.org/2000/01/rdf-schema#` | Basic RDF schema terms                           |
| `skos`    | `http://www.w3.org/2004/02/skos/core#`  | Semantic relationships and collections           |
| `owl`     | `http://www.w3.org/2002/07/owl#`        | OWL classes, properties, and ontology constructs |

### 2.2 Optional Vocabularies

| Prefix   | Namespace                          | Purpose                        |
| -------- | ---------------------------------- | ------------------------------ |
| `schema` | `http://schema.org/`               | Additional metadata properties |
| `vcard`  | `http://www.w3.org/2006/vcard/ns#` | Contact information            |
| `foaf`   | `http://xmlns.com/foaf/0.1/`       | Agent/person information       |

---

## 3. Glossaries and Business Terms

**Entity-Specific Specification**: See [`src/rdf/entities/glossary_term/SPEC.md`](../src/rdf/entities/glossary_term/SPEC.md)

The primary goal of RDF is to create comprehensive business glossaries that define terms and their relationships. These terms are then referenced by datasets to provide semantic meaning to data fields.

**Quick Reference**:

- **RDF Type**: `skos:Concept`
- **Required**: `skos:prefLabel` OR `rdfs:label` (≥3 characters), `skos:definition` OR `rdfs:comment`
- **Relationships**: `skos:broader`, `skos:narrower` (term-to-term), `skos:exactMatch` (field-to-term)
- **Constraints**: SHACL constraints via dual-typed terms (`skos:Concept, sh:PropertyShape`)

---

**For complete glossary term specifications including term definitions, identification criteria, relationship mappings, IRI-to-URN conversion, constraint extraction, and the hybrid term-constraint pattern, see the [Glossary Term Specification](../src/rdf/entities/glossary_term/SPEC.md).**

---

## 4. Datasets

**Entity-Specific Specification**: See [`src/rdf/entities/dataset/SPEC.md`](../src/rdf/entities/dataset/SPEC.md)

Datasets represent data sources with catalog metadata and structural schemas. They reference glossary terms to provide semantic meaning to their fields.

**Quick Reference**:

- **RDF Type**: `dcat:Dataset`
- **Required**: `dcterms:title`, `dcterms:conformsTo` (links to `sh:NodeShape`), `dcat:accessService` (links to platform)
- **Schema**: Fields defined via `sh:PropertyShape` in referenced `sh:NodeShape`
- **Platform**: Detected via `dcat:accessService` → platform service definition
- **Domain**: Auto-assigned from IRI path hierarchy

---

**For complete dataset specifications including schema discovery, field definitions, platform integration, constraints, and domain assignment, see the [Dataset Specification](../src/rdf/entities/dataset/SPEC.md).**

---

## 5. Platform Definitions

### 5.1 Platform Service Definitions

Platform services define the data platforms used by datasets. They should be defined with proper semantic properties to ensure correct DataHub integration.

**Required Properties**:

- `rdf:type` → `dcat:DataService`
- `dcterms:title` → DataHub-compatible platform name (lowercase)
- `rdfs:label` → Descriptive platform name for display
- `dcterms:description` → Platform description
- `dcat:endpointURL` → Platform endpoint URL

**Optional Properties**:

- `schema:provider` → Platform provider organization
- `dcterms:type` → Platform type (e.g., "Database", "Cloud Data Warehouse")
- `dcterms:created` → Creation date
- `dcterms:modified` → Last modification date

### 5.2 Platform Naming Conventions

Platform names in `dcterms:title` should follow DataHub's standard naming conventions:

**Database Platforms**:

- `postgres` (not "PostgreSQL")
- `mysql` (not "MySQL")
- `oracle` (not "Oracle")
- `sql_server` (not "SQL Server")
- `db2` (not "DB2")
- `sybase` (not "Sybase")

**Cloud Data Platforms**:

- `snowflake` (not "Snowflake")
- `bigquery` (not "BigQuery")
- `redshift` (not "Redshift")
- `databricks` (not "Databricks")

**Big Data Platforms**:

- `teradata` (not "Teradata")
- `hive` (not "Hive")
- `spark` (not "Spark")
- `hadoop` (not "Hadoop")

**Streaming Platforms**:

- `kafka` (not "Kafka")
- `pulsar` (not "Pulsar")

**Storage Platforms**:

- `s3` (not "S3")
- `gcs` (not "GCS")
- `azure_blob` (not "Azure Blob Storage")

### 5.3 Platform Definition Examples

```turtle
# PostgreSQL Platform
<http://DataHubFinancial.com/PLATFORMS/postgres> a dcat:DataService ;
    rdfs:label "PostgreSQL Database Platform" ;
    dcterms:title "postgres" ;
    dcterms:description "PostgreSQL database platform for loan trading data" ;
    schema:provider <http://DataHubFinancial.com/PLATFORMS/oracle-corporation> ;
    dcat:endpointURL <http://postgres.platforms.com:5432> ;
    dcterms:type "Database" ;
    dcterms:created "2024-01-01"^^xsd:date ;
    dcterms:modified "2024-01-01"^^xsd:date .

# Snowflake Platform
<http://DataHubFinancial.com/PLATFORMS/snowflake> a dcat:DataService ;
    rdfs:label "Snowflake Data Platform" ;
    dcterms:title "snowflake" ;
    dcterms:description "Snowflake cloud data platform for risk management and analytics" ;
    schema:provider <http://DataHubFinancial.com/PLATFORMS/snowflake-inc> ;
    dcat:endpointURL <https://snowflake.platforms.com:443> ;
    dcterms:type "Cloud Data Warehouse" ;
    dcterms:created "2024-01-01"^^xsd:date ;
    dcterms:modified "2024-01-01"^^xsd:date .

# Teradata Platform
<http://DataHubFinancial.com/PLATFORMS/teradata> a dcat:DataService ;
    rdfs:label "Teradata Data Warehouse Platform" ;
    dcterms:title "teradata" ;
    dcterms:description "Teradata data warehouse platform for analytical workloads" ;
    schema:provider <http://DataHubFinancial.com/PLATFORMS/teradata-corporation> ;
    dcat:endpointURL <http://teradata.platforms.com:1025> ;
    dcterms:type "Data Warehouse" ;
    dcterms:created "2024-01-01"^^xsd:date ;
    dcterms:modified "2024-01-01"^^xsd:date .
```

### 5.4 Platform Provider Organizations

Platform providers should be defined as organizations:

```turtle
# Oracle Corporation
<http://DataHubFinancial.com/PLATFORMS/oracle-corporation> a schema:Organization ;
    rdfs:label "Oracle Corporation" ;
    dcterms:description "Oracle Corporation - Database and cloud services provider" ;
    schema:name "Oracle Corporation" ;
    schema:url <https://www.oracle.com> .

# Snowflake Inc.
<http://DataHubFinancial.com/PLATFORMS/snowflake-inc> a schema:Organization ;
    rdfs:label "Snowflake Inc." ;
    dcterms:description "Snowflake Inc. - Cloud data platform provider" ;
    schema:name "Snowflake Inc." ;
    schema:url <https://www.snowflake.com> .
```

### 5.5 Platform Categories

Platforms can be categorized for better organization:

```turtle
# Database Platform Category
<http://DataHubFinancial.com/PLATFORMS/database-platforms> a rdfs:Class ;
    rdfs:label "Database Platforms" ;
    rdfs:comment "Category for traditional database platforms" ;
    rdfs:subClassOf dcat:DataService .

# Cloud Data Platform Category
<http://DataHubFinancial.com/PLATFORMS/cloud-data-platforms> a rdfs:Class ;
    rdfs:label "Cloud Data Platforms" ;
    rdfs:comment "Category for cloud-based data warehouse platforms" ;
    rdfs:subClassOf dcat:DataService .

# Platform categorization
<http://DataHubFinancial.com/PLATFORMS/postgres> rdf:type <http://DataHubFinancial.com/PLATFORMS/database-platforms> .
<http://DataHubFinancial.com/PLATFORMS/snowflake> rdf:type <http://DataHubFinancial.com/PLATFORMS/cloud-data-platforms> .
```

## 6. Dataset Lineage

**Entity-Specific Specification**: See [`src/rdf/entities/lineage/SPEC.md`](../src/rdf/entities/lineage/SPEC.md)

Dataset lineage tracks how data flows between datasets and processing activities, providing complete visibility into data transformations and dependencies.

**Quick Reference**:

- **RDF Properties**: `prov:used`, `prov:generated`, `prov:wasDerivedFrom`, `prov:wasGeneratedBy`, `prov:wasInfluencedBy`
- **Activities**: `prov:Activity` resources become DataHub `DataJob` entities
- **Field-Level**: Field-to-field lineage via fragment URIs (e.g., `dataset#field_name`)

---

**For complete lineage specifications including dataset-to-dataset lineage, field-level lineage, activity processing, and relationship types, see the [Lineage Specification](../src/rdf/entities/lineage/SPEC.md).**

---

## 7. Custom Properties

**Entity-Specific Specification**: See [`src/rdf/entities/structured_property/SPEC.md`](../src/rdf/entities/structured_property/SPEC.md)

Custom properties provide a powerful way to attach typed, validated metadata to both glossary terms and datasets. The system automatically detects structured properties from RDF ontologies and maps them to appropriate DataHub entity types.

**Quick Reference**:

- **RDF Types**: `owl:ObjectProperty`, `owl:DatatypeProperty`, `rdf:Property`
- **Entity Mapping**: `rdfs:domain` determines target DataHub entity type (`dcat:Dataset` → `dataset`, `skos:Concept` → `glossaryTerm`)
- **URN Format**: `urn:li:structuredProperty:{property_name}`

---

**For complete structured property specifications including property detection, entity type mapping, value assignments, and common patterns, see the [Structured Property Specification](../src/rdf/entities/structured_property/SPEC.md).**

---

## 8. Domain Ownership

Domain ownership provides a comprehensive governance model for data assets by defining ownership groups and assigning them to domains using the DPROD standard.

### 8.1 Ownership Model

The ownership model uses **group-based ownership** rather than individual ownership, providing better scalability and governance. Ownership can be assigned to:

- **Domains**: Organizational units that contain datasets, glossary terms, and data products
- **Term Groups**: Collections of related glossary terms (skos:Collection)

**Owner Types:**
Owner types are defined as strings via `dh:hasOwnerType` property. The system supports:

- Standard types: `BUSINESS_OWNER`, `DATA_STEWARD`, `TECHNICAL_OWNER`
- Custom types: Any owner type string defined in DataHub UI (e.g., `CUSTOM_OWNER_TYPE`, `DATA_CUSTODIAN`)

**Standard Owner Types:**

- **Business Owners**: Strategic accountability for data assets
- **Data Stewards**: Operational responsibility for data quality
- **Technical Owners**: Technical responsibility for data infrastructure

**Custom Owner Types:**
DataHub allows organizations to define custom owner types in the UI. These can be specified in RDF using `dh:hasOwnerType` with any string value. The system will pass these custom types directly to DataHub without hardcoded restrictions.

**Group Registration:**

- Owner groups are automatically registered as DataHub corpGroup entities
- Groups are created before ownership assignment to ensure proper references
- Group metadata (labels, descriptions) is extracted from RDF definitions

### 8.2 Owner Group Definitions

Owner groups are defined as RDF resources with rich metadata:

```turtle
@prefix dh: <http://datahub.com/ontology/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

# Finance Domain Owner Groups
<http://DataHubFinancial.com/FINANCE/Business_Owners> a dh:BusinessOwner ;
    rdfs:label "Finance Business Owners" ;
    rdfs:comment "Business leadership team for Finance domain" ;
    dh:hasOwnerType "BUSINESS_OWNER" ;
    dh:hasResponsibility "Strategic accountability for financial data governance" ;
    dh:hasDepartment "Finance" ;
    dh:hasApprovalAuthority "true"^^xsd:boolean .

<http://DataHubFinancial.com/FINANCE/Data_Stewards> a dh:DataSteward ;
    rdfs:label "Finance Data Governance Team" ;
    rdfs:comment "Data stewards responsible for finance data quality" ;
    dh:hasOwnerType "DATA_STEWARD" ;
    dh:hasResponsibility "Operational data quality management for finance systems" ;
    dh:hasDepartment "Finance" ;
    dh:hasApprovalAuthority "false"^^xsd:boolean .

<http://DataHubFinancial.com/FINANCE/Technical_Owners> a dh:TechnicalOwner ;
    rdfs:label "Finance Technology Team" ;
    rdfs:comment "Technical team managing finance systems" ;
    dh:hasOwnerType "TECHNICAL_OWNER" ;
    dh:hasResponsibility "Technical infrastructure and system maintenance" ;
    dh:hasDepartment "Finance IT" ;
    dh:hasApprovalAuthority "false"^^xsd:boolean .
```

### 8.3 Domain and Term Group Ownership Assignment

Domains and term groups are assigned owners using the DPROD standard `dprod:dataOwner` property:

```turtle
@prefix dprod: <https://ekgf.github.io/dprod/> .
@prefix dh: <http://datahub.com/ontology/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

# Finance Domain with Ownership
<http://DataHubFinancial.com/FINANCE/> a dh:Domain ;
    rdfs:label "Finance Domain" ;
    rdfs:comment "Financial reporting and accounting domain" ;
    dprod:dataOwner <http://DataHubFinancial.com/FINANCE/Business_Owners> ;
    dprod:dataOwner <http://DataHubFinancial.com/FINANCE/Data_Stewards> ;
    dprod:dataOwner <http://DataHubFinancial.com/FINANCE/Technical_Owners> .

# Term Group with Ownership
accounts:Counterparty_Type_Collection a skos:Collection ;
    skos:prefLabel "Counterparty Type Collection" ;
    skos:definition "Collection of valid counterparty types for data validation." ;
    dprod:dataOwner <http://DataHubFinancial.com/REFERENCE_DATA/Business_Owners> ;
    dprod:dataOwner <http://DataHubFinancial.com/REFERENCE_DATA/Data_Stewards> ;
    skos:member accounts:Bank ;
    skos:member accounts:Corporate .
```

### 8.4 Ownership Properties

The DataHub ontology defines the following ownership properties:

| Property                  | Type          | Description                                                                        |
| ------------------------- | ------------- | ---------------------------------------------------------------------------------- |
| `dh:hasOwnerType`         | `xsd:string`  | Owner type string (supports standard types and custom types defined in DataHub UI) |
| `dh:hasResponsibility`    | `xsd:string`  | Description of responsibilities                                                    |
| `dh:hasDepartment`        | `xsd:string`  | Organizational department                                                          |
| `dh:hasApprovalAuthority` | `xsd:boolean` | Whether owner has approval authority                                               |

### 8.5 Ownership Export

Ownership information can be exported using the CLI:

```bash
# Export ownership as JSON
python -m rdf.scripts.datahub_rdf --source data.ttl --ownership-output ownership.json --ownership-format json

# Export ownership as CSV
python -m rdf.scripts.datahub_rdf --source data.ttl --ownership-output ownership.csv --ownership-format csv

# Export ownership as YAML
python -m rdf.scripts.datahub_rdf --source data.ttl --ownership-output ownership.yaml --ownership-format yaml
```

### 8.6 Ownership Export Formats

#### JSON Format

```json
{
  "export_timestamp": "2024-12-19T10:30:00",
  "ownership_count": 3,
  "ownership": [
    {
      "owner_uri": "http://DataHubFinancial.com/FINANCE/Business_Owners",
      "owner_type": "BUSINESS_OWNER",
      "owner_label": "Finance Business Owners",
      "owner_description": "Business leadership team for Finance domain",
      "owner_department": "Finance",
      "owner_responsibility": "Strategic accountability for financial data governance",
      "owner_approval_authority": true,
      "entity_uri": "http://DataHubFinancial.com/FINANCE/",
      "entity_type": "domain"
    }
  ]
}
```

#### CSV Format

```csv
owner_uri,owner_type,owner_label,owner_description,owner_department,owner_responsibility,owner_approval_authority,entity_uri,entity_type
http://DataHubFinancial.com/FINANCE/Business_Owners,BUSINESS_OWNER,Finance Business Owners,Business leadership team for Finance domain,Finance,Strategic accountability for financial data governance,true,http://DataHubFinancial.com/FINANCE/,domain
```

### 8.7 Domain-Based Namespace Structure

Owner groups are organized under their respective domain namespaces:

```
Domain Namespaces:
├── http://DataHubFinancial.com/FINANCE/
│   ├── Business_Owners, Data_Stewards, Technical_Owners
│   └── (domain resources)
├── http://DataHubFinancial.com/TRADING/
│   ├── Business_Owners, Data_Stewards, Technical_Owners
│   ├── LOANS/Business_Owners, Data_Stewards, Technical_Owners
│   └── (domain resources)
├── http://DataHubFinancial.com/REFERENCE_DATA/
│   ├── Business_Owners, Data_Stewards, Technical_Owners
│   └── (domain resources)
└── ...
```

### 8.8 DataHub Integration

The ownership system integrates with DataHub through automatic group creation and ownership assignment:

**1. Group Creation Process:**

- Owner groups are automatically registered as DataHub corpGroup entities
- Group metadata (name, description) is extracted from RDF definitions
- Groups are created before ownership assignment to ensure they exist

**2. IRI to URN Conversion:**

- **Owner IRI**: `http://DataHubFinancial.com/FINANCE/Business_Owners`
- **DataHub URN**: `urn:li:corpGroup:business_owners`
- **Owner Type**: `BUSINESS_OWNER` (mapped to DataHub OwnershipTypeClass)

**3. Group Registration Example:**

```python
# Owner groups are automatically created in DataHub
group_urn = f"urn:li:corpGroup:{group_name}"
corp_group = CorpGroupClass(info=CorpGroupInfoClass(
    displayName=group_name,
    description=group_description
))
```

**4. Ownership Assignment:**

- Groups are assigned as owners to domains using DataHub's ownership system
- Multiple owner types per domain (Business, Data Steward, Technical)
- Full metadata preserved (responsibilities, departments, approval authority)

### 8.9 Ownership Inheritance (Future)

Future implementation will support ownership inheritance from domains to:

- Datasets within the domain
- Glossary terms within the domain
- Data products within the domain

This provides automatic governance assignment based on domain membership.

## 9. Technical Implementation

### 9.1 IRI-to-URN Conversion Algorithm

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
- **Datasets**: `urn:li:dataset:({platform_urn},{path},{environment})`
- **Domains**: `urn:li:domain:{path}`

### 9.2 Constraint Extraction Algorithm

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

### 9.4 Modular Architecture and Auto-Discovery

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

    def get_converter(self, entity_type: str) -> EntityConverter:
        """Get converter for entity type."""

    def get_mcp_builder(self, entity_type: str) -> EntityMCPBuilder:
        """Get MCP builder for entity type."""

    def list_entity_types(self) -> List[str]:
        """List all registered entity types."""
```

#### 9.4.2 Auto-Discovery

Entity modules are automatically discovered by scanning the `entities/` directory:

```python
def create_default_registry() -> EntityRegistry:
    """
    Create a registry with all entity processors auto-discovered.

    Scans the entities directory for modules that export ENTITY_METADATA
    and required components (Extractor, Converter, MCPBuilder), then
    automatically registers them.
    """
    registry = EntityRegistry()

    # Auto-discover entity modules
    for finder, name, ispkg in pkgutil.iter_modules(entities_module.__path__):
        if ispkg:  # Only process subdirectories (entity modules)
            if hasattr(module, 'ENTITY_METADATA'):
                _register_entity_module(registry, entity_type, module)

    return registry
```

**Auto-Discovery Requirements**:

- Entity folder must export `ENTITY_METADATA` instance
- Must export `{EntityName}Extractor`, `{EntityName}Converter`, `{EntityName}MCPBuilder`
- Must follow naming conventions (see `ENTITY_PLUGIN_CONTRACT.md`)
- Must include `SPEC.md` file documenting the entity's RDF patterns, extraction logic, and DataHub mappings

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
        self.owner_groups: List[RDFOwnerGroup] = []
        self.ownership: List[RDFOwnership] = []
        self.metadata: Dict[str, Any] = {}
```

**Field Naming Convention**:

- `glossary_term` → `glossary_terms`
- `dataset` → `datasets`
- `lineage` → `lineage_relationships` (special case)
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

- `src/rdf/entities/glossary_term/SPEC.md` - Glossary terms and business vocabulary
- `src/rdf/entities/dataset/SPEC.md` - Datasets and schema definitions
- `src/rdf/entities/lineage/SPEC.md` - Dataset and field-level lineage
- `src/rdf/entities/structured_property/SPEC.md` - Custom structured properties
- `src/rdf/entities/assertion/SPEC.md` - Data quality assertions
- `src/rdf/entities/data_product/SPEC.md` - Data products
- `src/rdf/entities/relationship/SPEC.md` - Term-to-term relationships
- `src/rdf/entities/domain/SPEC.md` - Domain organization

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

### 9.5 Dynamic Export Target Generation

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
        'OWNERSHIP': 'ownership',
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

## 10. DataHub Integration

### 10.1 Entity Type Mappings

| RDF Entity Type   | DataHub Entity Type | URN Format                                 |
| ----------------- | ------------------- | ------------------------------------------ |
| `skos:Concept`    | `GlossaryTerm`      | `urn:li:glossaryTerm:{path}`               |
| `skos:Collection` | `GlossaryNode`      | `urn:li:glossaryNode:{path}`               |
| `dcat:Dataset`    | `Dataset`           | `urn:li:dataset:({platform},{path},{env})` |
| `prov:Activity`   | `DataJob`           | `urn:li:dataJob:{path}`                    |

### 10.2 Assertion Creation

**All assertions are created as Column Assertions** using DataHub's `FieldValuesAssertion` API. Column Assertions are field-level assertions that validate data quality constraints on specific dataset columns.

#### 10.2.1 Column Assertion API

Assertions are created using DataHub's `FieldValuesAssertion` high-level API, which generates proper Column Assertions visible in the DataHub UI:

```python
from datahub.api.entities.assertion.field_assertion import FieldValuesAssertion
from datahub.api.entities.assertion.assertion_operator import (
    MatchesRegexOperator, GreaterThanOrEqualToOperator,
    LessThanOrEqualToOperator, NotNullOperator, InOperator
)

# Create Column Assertion for a field
field_assertion = FieldValuesAssertion(
    type="field",  # Required: must be "field" for Column Assertions
    entity=dataset_urn,  # Dataset URN
    field=field_name,  # Field/column name
    condition=condition,  # Assertion condition (operator)
    exclude_nulls=True,
    failure_threshold={"type": "count", "value": 0},  # Fail on any violation
    description=description
)

# Get assertion info aspect
assertion_info = field_assertion.get_assertion_info()

# Create MCP
mcp = MetadataChangeProposalWrapper(
    entityUrn=assertion_urn,
    aspect=assertion_info
)
```

#### 10.2.2 Supported Assertion Operators

The following operators are supported and mapped to DataHub assertion conditions:

| RDF Operator               | DataHub Condition              | Use Case                                |
| -------------------------- | ------------------------------ | --------------------------------------- |
| `NOT_NULL`                 | `NotNullOperator`              | Required field validation               |
| `MATCHES` / `REGEX_MATCH`  | `MatchesRegexOperator`         | Pattern validation (string fields only) |
| `GREATER_THAN_OR_EQUAL_TO` | `GreaterThanOrEqualToOperator` | Minimum value constraint                |
| `LESS_THAN_OR_EQUAL_TO`    | `LessThanOrEqualToOperator`    | Maximum value/length constraint         |
| `IN`                       | `InOperator`                   | Enum/allowed values constraint          |

#### 10.2.3 Assertion Scope

- **Field-level assertions only**: Only assertions with a `field_name` are created as Column Assertions
- **Dataset-level assertions**: Assertions without a `field_name` are skipped (not supported)
- **Pattern constraints**: Only applied to string fields (decimal/integer/float patterns are removed)

### 10.3 Platform Integration

#### Platform Detection Rules

1. **Preferred**: `dcat:accessService` → look up platform using semantic properties (`dcterms:title`, `rdfs:label`)
2. **Fallback**: `dcterms:creator` → use creator as platform name
3. **Legacy**: `void:sparqlEndpoint` → use "sparql" as platform
4. **Default**: If no platform can be determined, defaults to `"logical"` (for logical/conceptual datasets)

#### Platform Name Extraction Process

1. **Semantic Lookup**: Query the platform service URI for `dcterms:title` property
2. **Fallback to Label**: If no title, use `rdfs:label` property
3. **URI Parsing**: If no semantic properties, fall back to parsing the URI
4. **Normalization**: Convert platform name to lowercase for DataHub compatibility
5. **Default Assignment**: If platform cannot be determined through any of the above methods, assign `"logical"` as the default platform

#### Platform URN Generation

- Format: `urn:li:dataPlatform:{platform_name}`
- Platform names are extracted from semantic properties and normalized to lowercase
- Platform names should match DataHub's standard naming conventions
- **Default Platform**: Datasets without an explicit platform definition default to `"logical"`, which is appropriate for logical/conceptual datasets that don't have a physical platform association. This default is applied centrally during URN generation to ensure consistent behavior across all dataset processing.

#### Implementation Details

```python
def _get_platform_name_from_service(self, graph: Graph, service_uri: URIRef) -> Optional[str]:
    """
    Extract platform name from a service URI using semantic properties.

    Looks for dcterms:title first, then falls back to rdfs:label.
    Normalizes the platform name to lowercase for DataHub compatibility.
    """
    platform_name = None

    # First try dcterms:title (preferred)
    for title in graph.objects(service_uri, DCTERMS.title):
        if isinstance(title, Literal):
            platform_name = str(title).strip()
            break

    # Fallback to rdfs:label
    if not platform_name:
        for label in graph.objects(service_uri, RDFS.label):
            if isinstance(label, Literal):
                platform_name = str(label).strip()
                break

    # Normalize platform name to lowercase for DataHub compatibility
    if platform_name:
        return platform_name.lower().strip()

    return None
```

---

## 11. Validation and Error Handling

### 11.1 RDF Validation

#### Required Format Validation

- Must have valid scheme (http, https, custom schemes)
- Must have non-empty path after scheme removal
- Must be parseable by URL parsing library

#### Entity Validation

- **Glossary Terms**: Must have label ≥3 characters, valid URI reference
- **Datasets**: Must have appropriate RDF type, name/title, valid URI
- **Relationships**: Referenced entities must exist, no circular references

### 11.2 Constraint Validation

#### SHACL Constraint Validation

- `sh:pattern` must be valid regex
- `sh:minInclusive` ≤ `sh:maxInclusive`
- `sh:minLength` ≤ `sh:maxLength`
- `sh:minCount` ≥ 0, `sh:maxCount` ≥ `sh:minCount`

#### SKOS Collection Validation

- Collection members must have valid labels
- No circular membership relationships
- Collection must have proper SKOS type

### 11.3 Error Handling

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

## 12. Common Patterns

### 12.1 Simple Custom Terms (Default Pattern)

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

### 12.2 Enum Values with SKOS Collections

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

### 12.3 Pattern-Based Precision

```turtle
ex:currencyAmountProperty a sh:PropertyShape ;
    sh:path ex:amount ;
    sh:datatype xsd:decimal ;
    sh:pattern "^\\d{1,10}\\.\\d{2}$" ;  # DECIMAL(12,2)
    sh:minInclusive 0.00 ;
    sh:name "Currency Amount" ;
    ex:sqlType "DECIMAL(12,2)" .
```

### 12.4 Contextual Constraints

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

### 12.5 Cross-Column Constraints

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

## 13. References

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
