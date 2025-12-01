# Dataset Specification

**Part of**: [RDF Specification](../../../../docs/rdf-specification.md)

This document specifies how RDF datasets are extracted, converted, and mapped to DataHub dataset entities.

## Overview

Datasets represent data sources with catalog metadata and structural schemas. They reference glossary terms to provide semantic meaning to their fields.

## Dataset Definitions

Datasets are defined using DCAT (Data Catalog Vocabulary) with rich metadata.

**RDF Type**: `dcat:Dataset`

**Required Properties**:

- `dcterms:title` - Dataset title
- `dcterms:conformsTo` - **Primary link** to `sh:NodeShape` defining the dataset's schema structure
- `dcat:accessService` - Link to platform service definition

**Recommended Properties**:

- `dcterms:description` - Detailed description
- `dcterms:publisher` - Organization or team responsible
- `dcterms:creator` - Individual creator
- `dcterms:created` - Creation date
- `dcterms:modified` - Last modification date
- `dcat:keyword` - Searchable keywords
- `dcat:theme` - Thematic categorization
- `dcterms:identifier` - Unique identifier
- `dcat:contactPoint` - Contact for questions

**Example**:

```turtle
accounts:AccountDataset a dcat:Dataset ;
    dcterms:title "Account Master" ;
    dcterms:description "Master account data with counterparty information" ;
    dcterms:conformsTo accounts:AccountSchema ;  # Links to schema definition
    dcat:accessService platforms:postgres ;       # Links to platform
    dcterms:publisher "Finance Team" ;
    dcterms:created "2024-01-01"^^xsd:date ;
    dcat:keyword "accounts", "counterparty", "reference-data" .
```

## Schema Discovery

**Required**: Datasets must link to their schema definitions using `dcterms:conformsTo` pointing to a `sh:NodeShape`. This is the only supported method.

**Schema Linking Pattern**:

```turtle
ex:TradeTable a dcat:Dataset ;
    dcterms:title "Trade Table" ;
    dcterms:conformsTo ex:TradeSchema .

ex:TradeSchema a sh:NodeShape ;
    sh:property [ ... ] .
```

**Requirements**:

- The dataset must have a `dcterms:conformsTo` property
- The value of `dcterms:conformsTo` must be a URI reference to a `sh:NodeShape`
- The referenced NodeShape must exist and be typed as `sh:NodeShape`

**Error Handling**: If a dataset lacks `dcterms:conformsTo` or references a non-existent/invalid NodeShape, schema fields will not be extracted and a warning will be logged.

## Dataset-to-Term Relationships

Dataset fields reference glossary terms using `skos:exactMatch` to provide semantic meaning.

**Field-to-Term Mapping**:

```turtle
# Field definition referencing glossary term
<http://COUNTERPARTY_MASTER/Counterparty_Master#legal_name> a schema:PropertyValue ;
    schema:name "LEGAL_NM" ;
    schema:description "Legal name of the counterparty entity" ;
    schema:unitText "VARCHAR(200)" ;
    skos:exactMatch accounts:Legal_Name .
```

**Benefits**:

- Fields inherit semantic meaning from glossary terms
- Consistent terminology across datasets
- Automatic glossary term usage tracking
- Data lineage through shared concepts

## Schema Definitions

Dataset schemas define field structure using SHACL NodeShapes. Schemas are linked to datasets via `dcterms:conformsTo`.

**RDF Type**: `sh:NodeShape`

**Required Properties**:

- `sh:property` - References to property shapes (one or more)

**Recommended Properties**:

- `rdfs:label` - Human-readable schema name
- `sh:targetClass` - The RDF class instances must belong to (optional when using `dcterms:conformsTo`)

**Example**:

```turtle
accounts:AccountSchema a sh:NodeShape ;
    rdfs:label "Account Master Schema" ;
    sh:property [
        sh:node accounts:Account_ID ;   # Reference to reusable property shape
        sh:minCount 1 ;                  # Required field (contextual constraint)
        sh:maxCount 1
    ] ;
    sh:property [
        sh:node accounts:counterpartyTypeProperty ;
        sh:minCount 1 ;
        sh:maxCount 1
    ] .
```

## Field Definitions (PropertyShapes)

Field definitions are reusable PropertyShapes that contain intrinsic constraints and can optionally reference glossary terms.

**RDF Type**: `sh:PropertyShape` (optionally combined with `skos:Concept`)

### Field Extraction Methods

Fields are extracted from schemas using **two patterns**:

| Pattern               | Description                                                      | Use Case                                 |
| --------------------- | ---------------------------------------------------------------- | ---------------------------------------- |
| **Direct Properties** | `sh:path`, `sh:datatype`, `sh:name` directly on property shape   | Simple inline field definitions          |
| **sh:node Reference** | Property shape uses `sh:node` to reference a reusable definition | Semantic glossary terms with constraints |

**Pattern 1: Direct Properties (Simple)**

```turtle
ex:Schema a sh:NodeShape ;
    sh:property [
        sh:path ex:tradeId ;
        sh:name "Trade ID" ;
        sh:datatype xsd:string ;
        sh:maxLength 20
    ] .
```

**Pattern 2: sh:node Reference (Recommended)**

This pattern allows glossary terms to be both semantic concepts AND carry SHACL constraints:

```turtle
# Glossary term that's also a property shape (dual-typed)
ex:Account_ID a skos:Concept, sh:PropertyShape ;
    skos:prefLabel "Account ID" ;
    skos:definition "Unique account identifier" ;
    sh:path ex:accountId ;
    sh:datatype xsd:string ;
    sh:maxLength 20 ;
    sh:name "Account ID" .

# Schema references the term via sh:node
ex:AccountSchema a sh:NodeShape ;
    sh:property [
        sh:node ex:Account_ID ;    # Reference to the glossary term/property shape
        sh:minCount 1 ;             # Contextual constraint (required in this schema)
        sh:maxCount 1
    ] .
```

**Benefits of sh:node Pattern**:

- **Single source of truth**: Field definition and glossary term are the same entity
- **Automatic glossary linking**: Fields automatically associate with glossary terms
- **Reusability**: Same field definition used across multiple schemas
- **Contextual constraints**: `sh:minCount`/`sh:maxCount` can vary per schema

### Field Property Resolution

When extracting field properties, the system checks **both** the inline property shape **and** any `sh:node` reference:

| Property         | Priority Order                                 |
| ---------------- | ---------------------------------------------- |
| `sh:name`        | 1. Inline property shape, 2. sh:node reference |
| `sh:datatype`    | 1. Inline property shape, 2. sh:node reference |
| `sh:path`        | 1. Inline property shape, 2. sh:node reference |
| `sh:description` | 1. Inline property shape, 2. sh:node reference |

If no `sh:name` is found but `sh:node` references a URI, the field name is derived from the URI's local name.

### PropertyShape Properties

**Required Properties** (on either inline shape or sh:node reference):

- `sh:datatype` OR `sh:class` - Data type constraint
- `sh:name` OR derivable from `sh:path` or `sh:node` URI - Field name

**Recommended Properties**:

- `sh:name` - Human-readable field name
- `sh:description` - Detailed field description
- `sh:minLength` / `sh:maxLength` - String length constraints
- `sh:pattern` - Regular expression for validation
- `sh:minInclusive` / `sh:maxInclusive` - Numeric range constraints

**Custom Extension Properties**:

- `ex:sqlType` - Technology-specific type (e.g., "VARCHAR(16)", "INTEGER")
- `ex:nativeType` - Alternative for non-SQL types

### XSD Type Mapping

XSD datatypes are mapped to DataHub field types:

| XSD Type                                 | DataHub Type | Notes           |
| ---------------------------------------- | ------------ | --------------- |
| `xsd:string`                             | `string`     | VARCHAR         |
| `xsd:integer`, `xsd:int`, `xsd:long`     | `number`     | INTEGER/BIGINT  |
| `xsd:decimal`, `xsd:float`, `xsd:double` | `number`     | NUMERIC/DECIMAL |
| `xsd:boolean`                            | `boolean`    | BOOLEAN         |
| `xsd:date`                               | `date`       | DATE            |
| `xsd:dateTime`                           | `datetime`   | TIMESTAMP       |
| `xsd:time`                               | `time`       | TIME            |

## Dataset Constraints

Dataset schemas can specify contextual constraints that vary by dataset context.

### Required/Optional Fields

Fields can be required or optional depending on dataset context:

```turtle
# Required field in one schema
accounts:TradeSchema a sh:NodeShape ;
    sh:property [
        sh:node accounts:brokerIdProperty ;
        sh:minCount 1 ;              # Required
        sh:maxCount 1
    ] .

# Optional field in another schema
accounts:QuoteSchema a sh:NodeShape ;
    sh:property [
        sh:node accounts:brokerIdProperty ;
        sh:maxCount 1                # Optional (no minCount)
    ] .
```

### Cross-Column Constraints

Datasets can have constraints that validate relationships between multiple fields:

```turtle
# Simple cross-field constraints
accounts:TradeShape a sh:NodeShape ;
    sh:targetClass accounts:Trade ;

    # Date ordering constraint
    sh:property [
        sh:path accounts:tradeDate ;
        sh:lessThan accounts:settlementDate ;
        sh:message "Trade date must be before settlement date"@en
    ] ;

    # Currency inequality constraint
    sh:property [
        sh:path accounts:buyCurrency ;
        sh:notEquals accounts:sellCurrency ;
        sh:message "Buy currency must be different from sell currency"@en
    ] .
```

## Platform Integration

Datasets are assigned to platforms based on their access methods using semantic properties from platform definitions.

**Platform Detection Rules**:

1. **Preferred**: `dcat:accessService` → look up platform using semantic properties (`dcterms:title`, `rdfs:label`)
2. **Fallback**: `dcterms:creator` → use creator as platform name
3. **Legacy**: `void:sparqlEndpoint` → use "sparql" as platform
4. **Default**: If no platform can be determined, defaults to `"logical"` (for logical/conceptual datasets)

**Platform Definition Requirements**:

- Platform services must be defined with proper semantic properties
- `dcterms:title` should contain the DataHub-compatible platform name (lowercase)
- `rdfs:label` can contain a descriptive name for display purposes

**Platform URN Generation**:

- Format: `urn:li:dataPlatform:{platform_name}`
- Platform names are extracted from semantic properties and normalized to lowercase
- Platform names should match DataHub's standard naming conventions (e.g., `postgres`, `mysql`, `oracle`)
- **Default Platform**: Datasets without an explicit platform definition default to `"logical"`, which is appropriate for logical/conceptual datasets that don't have a physical platform association

**Example Platform Definition**:

```turtle
# Platform service definition
<http://DataHubFinancial.com/PLATFORMS/postgres> a dcat:DataService ;
    rdfs:label "PostgreSQL Database Platform" ;
    dcterms:title "postgres" ;
    dcterms:description "PostgreSQL database platform for loan trading data" ;
    dcat:endpointURL <http://postgres.platforms.com:5432> .

# Dataset using the platform
<http://BANK/TRADING/Loans> a dcat:Dataset ;
    dcat:accessService <http://DataHubFinancial.com/PLATFORMS/postgres> ;
    dcterms:title "Loan Trading Data" .
```

## Domain Assignment

Datasets are automatically assigned to domains based on their IRI paths, following the same pattern as glossary terms.

**Domain Assignment Process**:

1. **IRI Analysis**: Extract parent path segments from dataset IRI (exclude dataset name)
2. **Domain Generation**: Create domain for each parent segment
3. **Hierarchy Building**: Establish parent-child relationships
4. **Dataset Assignment**: Assign dataset to the leaf domain (most specific parent)

**Example**:

```turtle
# Dataset with IRI: https://bank.com/finance/accounts/customer_data
# Creates domains: bank.com → finance → accounts
# Dataset assigned to: urn:li:domain:accounts
```
