# RDF Dataset Mapping Reference

## Overview

This document provides detailed technical specifications for how RDF dataset concepts are mapped to DataHub dataset entities, including datasets, lineage activities, lineage relationships, and platform connections.

## Dataset Mapping

### Dataset Identification Criteria

The system identifies RDF resources as "datasets" using these criteria:

**Required Conditions:**

- Must have appropriate RDF type declaration
- Must have basic metadata (name/title)

**Included RDF Types:**

- `void:Dataset` - VOID dataset declarations
- `dcterms:Dataset` - Dublin Core dataset declarations
- `schema:Dataset` - Schema.org dataset declarations
- `dh:Dataset` - Native DataHub dataset declarations

**Property Mapping Priority:**

1. **Name**: `dcterms:title` → `schema:name` → `rdfs:label` → custom `hasName`
2. **Description**: `dcterms:description` → `schema:description` → `rdfs:comment` → custom `hasDescription`
3. **Identifier**: `dcterms:identifier` → `dh:hasURN` → custom `hasIdentifier`

### Core Entity Mappings

| RDF Concept           | DataHub Entity | Description                     |
| --------------------- | -------------- | ------------------------------- |
| `void:Dataset`        | `Dataset`      | Dataset entities                |
| `dcterms:Dataset`     | `Dataset`      | Alternative dataset declaration |
| `schema:Dataset`      | `Dataset`      | Schema.org dataset              |
| `dh:Dataset`          | `Dataset`      | Native DataHub dataset          |
| `void:sparqlEndpoint` | `Platform`     | SPARQL endpoint platform        |
| `void:dataDump`       | `Platform`     | File-based data platform        |
| `schema:provider`     | `Platform`     | Data platform provider          |
| `dh:hasSchemaField`   | `SchemaField`  | Dataset schema fields           |
| `dh:hasGlossaryTerm`  | `GlossaryTerm` | Field glossary associations     |

### Property Mappings

#### Basic Dataset

```turtle
ex:CustomerDatabase a void:Dataset ;
    dcterms:title "Customer Database" ;
    dcterms:description "Main customer information database" ;
    dcterms:creator ex:ITDepartment ;
    dcterms:created "2023-01-01"^^xsd:date ;
    dcterms:modified "2023-06-15"^^xsd:date ;
    void:sparqlEndpoint <http://sparql.example.com/customer> ;
    void:dataDump <http://data.example.com/customer.nt> ;
    void:triples 1500000 ;
    void:entities 50000 .
```

**Maps to DataHub Dataset:**

- `dcterms:title` → `name` (dataset name)
- `dcterms:description` → `description` (dataset description)
- `dcterms:creator` → `ownership` (dataset owner)
- `dcterms:created` → `created` (creation timestamp)
- `dcterms:modified` → `lastModified` (modification timestamp)
- `void:sparqlEndpoint` → `connection` (SPARQL endpoint)
- `void:dataDump` → `connection` (data dump URL)
- `void:triples` → `statistics` (triple count)
- `void:entities` → `statistics` (entity count)

#### Dataset with Platform

```turtle
ex:CustomerTable a void:Dataset ;
    dcterms:title "Customer Table" ;
    dcterms:description "Customer data table in PostgreSQL" ;
    dcat:accessService <http://postgres.example.com> ;
    schema:provider ex:DatabasePlatform ;
    schema:distribution ex:CustomerDataDistribution ;
    schema:url <https://database.example.com/customer_table> ;
    schema:version "2.1" ;
    schema:license <https://creativecommons.org/licenses/by/4.0/> .
```

**Maps to DataHub Dataset:**

- `dcat:accessService` → `platform` (data platform - preferred method)
- `schema:provider` → `platform` (data platform)
- `schema:distribution` → `connection` (data distribution)
- `schema:url` → `connection` (dataset URL)
- `schema:version` → `version` (dataset version)
- `schema:license` → `license` (dataset license)

### Schema Field Mapping

The system supports two approaches for defining dataset schema fields:

#### **Approach 1: Legacy SKOS Approach** (Simple Fields)

**Field Identification Criteria:**

- Must be referenced via `schema:DataCatalog` and `schema:PropertyValue`
- Must have field name via `schema:name`
- Must have glossary term mapping via `skos:exactMatch`

**Example:**

```turtle
<http://COUNTERPARTY_MASTER/Counterparty_Master#schema_def> a schema:DataCatalog ;
    schema:variableMeasured <http://COUNTERPARTY_MASTER/Counterparty_Master#legal_name> .

<http://COUNTERPARTY_MASTER/Counterparty_Master#legal_name> a schema:PropertyValue ;
    schema:name "LEGAL_NM" ;
    schema:description "Legal name of the counterparty entity" ;
    schema:unitText "VARCHAR(200)" ;
    skos:exactMatch counterparty:Legal_Name .
```

**Field Property Mappings:**
| RDF Property | DataHub Field Property | Description |
|--------------|------------------------|-------------|
| `schema:name` | `fieldPath` | Field name/identifier |
| `schema:description` | `description` | Field description |
| `schema:unitText` | `type` | Field data type |
| `skos:exactMatch` | `glossaryTerms` | Associated glossary terms |

#### **Approach 2: Modern SHACL Approach** (Complex Fields)

**Field Identification Criteria:**

- Must be referenced via `dcterms:conformsTo` pointing to `sh:NodeShape`
- Must have `sh:PropertyShape` definitions
- Must have glossary term mapping via `sh:class`

**Example:**

```turtle
<http://DataHubFinancial.com/REFERENCE_DATA/ACCOUNTS/Account_Details> a dcat:Dataset ;
    dcterms:conformsTo <http://DataHubFinancial.com/REFERENCE_DATA/ACCOUNTS/Account_Details_Schema> .

<http://DataHubFinancial.com/REFERENCE_DATA/ACCOUNTS/Account_Details_Schema> a sh:NodeShape ;
    sh:property [
        sh:node accounts:accountIdProperty ;
        sh:minCount 1 ;
        sh:maxCount 1
    ] .

accounts:accountIdProperty a sh:PropertyShape ;
    sh:path accounts:accountId ;
    sh:class accounts:Account_ID ;
    sh:datatype xsd:string ;
    sh:maxLength 20 ;
    sh:name "Account ID" ;
    sh:description "Unique identifier for the account" ;
    ex:sqlType "VARCHAR(20)" .
```

**Field Property Mappings:**
| RDF Property | DataHub Field Property | Description |
|--------------|------------------------|-------------|
| `sh:name` | `fieldPath` | Field name/identifier |
| `sh:description` | `description` | Field description |
| `sh:datatype` | `type` | Field data type |
| `sh:class` | `glossaryTerms` | Associated glossary terms |
| `sh:maxLength` | `maxLength` | Maximum field length |
| `sh:minCount` | `minCount` | Minimum occurrence count |
| `sh:maxCount` | `maxCount` | Maximum occurrence count |
| `ex:sqlType` | `sqlType` | SQL-specific type information |

**When to Use Each Approach:**

- **SKOS Approach**: Simple fields, basic descriptions, no validation requirements
- **SHACL Approach**: Complex fields, validation rules, constraints, business logic

**Data Type Mapping:**

- `varchar`, `string`, `xsd:string` → `StringTypeClass`
- `date`, `datetime`, `xsd:date` → `DateTypeClass`
- `int`, `number`, `decimal`, `xsd:decimal` → `NumberTypeClass`
- `bool`, `boolean`, `xsd:boolean` → `BooleanTypeClass`
- Default → `StringTypeClass`

### Platform Mapping

| RDF Property          | DataHub Platform | Description                          |
| --------------------- | ---------------- | ------------------------------------ |
| `dcat:accessService`  | Platform URN     | Data platform identifier (preferred) |
| `schema:provider`     | Platform URN     | Data platform identifier             |
| `void:sparqlEndpoint` | SPARQL Platform  | SPARQL endpoint platform             |
| `void:dataDump`       | File Platform    | File-based data platform             |
| `schema:distribution` | Custom Platform  | Data distribution platform           |

## Lineage Mapping

### Lineage Identification Criteria

The system identifies lineage relationships using these criteria:

**Required Conditions:**

- Must have PROV-O activity declarations (`prov:Activity`)
- Must have upstream/downstream entity relationships
- Must have temporal information (`prov:startedAtTime`, `prov:endedAtTime`)

**Included PROV-O Types:**

- `prov:Activity` - Data processing activities
- `prov:Entity` - Data entities (datasets)
- `prov:Agent` - Processing agents (users)

**Lineage Relationship Types:**

- `prov:used` - Upstream data dependencies
- `prov:generated` - Downstream data products
- `prov:wasDerivedFrom` - Direct derivation relationships
- `prov:wasGeneratedBy` - Activity-to-entity relationships
- `prov:wasAssociatedWith` - User associations
- `prov:wasAttributedTo` - User attribution

### Core Entity Mappings

| RDF Concept                | DataHub Entity | Description                  |
| -------------------------- | -------------- | ---------------------------- |
| `prov:Activity`            | `DataJob`      | Data processing activities   |
| `prov:Entity`              | `Dataset`      | Data entities                |
| `prov:Agent`               | `User`         | Processing agents            |
| `dh:hasTransformationType` | `DataJob`      | Transformation metadata      |
| `dh:hasBusinessProcess`    | `DataJob`      | Business process metadata    |
| `dh:hasActivityType`       | `DataJob`      | Activity type classification |

### Property Mappings

#### Upstream Lineage

```turtle
ex:CustomerReport a prov:Entity ;
    prov:wasDerivedFrom ex:CustomerDatabase ;
    prov:wasGeneratedBy ex:ReportGenerationJob ;
    prov:wasAttributedTo ex:DataAnalyst ;
    prov:generatedAtTime "2023-06-20T10:30:00Z"^^xsd:dateTime .

ex:ReportGenerationJob a prov:Activity ;
    prov:used ex:CustomerDatabase ;
    prov:used ex:CustomerGlossary ;
    prov:wasAssociatedWith ex:DataAnalyst ;
    prov:startedAtTime "2023-06-20T09:00:00Z"^^xsd:dateTime ;
    prov:endedAtTime "2023-06-20T10:30:00Z"^^xsd:dateTime .
```

**Maps to DataHub Lineage:**

- `prov:wasDerivedFrom` → upstream dataset lineage
- `prov:wasGeneratedBy` → data job lineage
- `prov:used` → data dependencies
- `prov:wasAssociatedWith` → user associations
- `prov:wasAttributedTo` → user attribution
- `prov:generatedAtTime` → lineage timestamp
- `prov:startedAtTime` → job start time
- `prov:endedAtTime` → job end time

#### Downstream Lineage

```turtle
ex:CustomerDatabase a prov:Entity ;
    prov:wasInfluencedBy ex:DataIngestionJob ;
    prov:wasAttributedTo ex:DataEngineer ;
    prov:wasGeneratedBy ex:ETLProcess ;
    prov:generatedAtTime "2023-01-01T00:00:00Z"^^xsd:dateTime .
```

**Maps to DataHub Lineage:**

- `prov:wasInfluencedBy` → downstream processing lineage
- `prov:wasAttributedTo` → user attribution
- `prov:wasGeneratedBy` → data job lineage
- `prov:generatedAtTime` → lineage timestamp

### Lineage Types

#### Dataset-to-Dataset Lineage

```turtle
ex:ProcessedCustomerData a prov:Entity ;
    prov:wasDerivedFrom ex:RawCustomerData ;
    prov:wasGeneratedBy ex:DataCleaningJob ;
    prov:wasInfluencedBy ex:DataValidationJob .
```

#### Dataset-to-Job Lineage

```turtle
ex:CustomerETLJob a prov:Activity ;
    prov:used ex:CustomerDatabase ;
    prov:generated ex:CustomerDataMart ;
    prov:wasAssociatedWith ex:ETLEngineer .
```

#### Complex Lineage Chains

```turtle
ex:RawData a prov:Entity ;
    prov:wasGeneratedBy ex:DataIngestionJob .

ex:CleanedData a prov:Entity ;
    prov:wasDerivedFrom ex:RawData ;
    prov:wasGeneratedBy ex:DataCleaningJob .

ex:AggregatedData a prov:Entity ;
    prov:wasDerivedFrom ex:CleanedData ;
    prov:wasGeneratedBy ex:DataAggregationJob .
```

## Relationship Mapping

### Core Relationship Types

| RDF Property         | DataHub Relationship | Description                 |
| -------------------- | -------------------- | --------------------------- |
| `owl:sameAs`         | External Reference   | Identity relationships      |
| `rdfs:subPropertyOf` | Property Hierarchy   | Property inheritance        |
| `skos:exactMatch`    | Term Equivalence     | Exact term matches          |
| `skos:closeMatch`    | Term Similarity      | Similar term matches        |
| `skos:broadMatch`    | Term Hierarchy       | Broader term relationships  |
| `skos:narrowMatch`   | Term Hierarchy       | Narrower term relationships |
| `dcterms:isPartOf`   | Dataset Hierarchy    | Dataset containment         |
| `dcterms:hasPart`    | Dataset Hierarchy    | Dataset components          |

### Property Mappings

#### External References

```turtle
ex:CustomerDataset owl:sameAs <http://catalog.example.com/customer> ;
    skos:exactMatch ex:ClientDatabase ;
    skos:closeMatch ex:CustomerInformationSystem .
```

**Maps to DataHub Relationships:**

- `owl:sameAs` → `externalReferences` (identity relationships)
- `skos:exactMatch` → `externalReferences` (exact matches)
- `skos:closeMatch` → `relatedDatasets` (similar datasets)

#### Dataset Hierarchy

```turtle
ex:CustomerDatabase dcterms:hasPart ex:CustomerTable ;
    dcterms:hasPart ex:CustomerView ;
    dcterms:isPartOf ex:EnterpriseDataWarehouse .

ex:CustomerTable dcterms:isPartOf ex:CustomerDatabase .
ex:CustomerView dcterms:isPartOf ex:CustomerDatabase .
```

**Maps to DataHub Relationships:**

- `dcterms:hasPart` → child datasets (component relationships)
- `dcterms:isPartOf` → `parentDatasets` (containment relationships)

## Custom Property Handling

### Additional Properties

```turtle
ex:CustomerDatabase a void:Dataset ;
    dcterms:title "Customer Database" ;
    dcterms:description "Main customer information database" ;
    rdfs:comment "This dataset contains all customer-related information" ;
    dcterms:source "Internal Data Warehouse" ;
    dcterms:publisher ex:DataTeam ;
    dcterms:rights "Internal Use Only" ;
    dcterms:language "en" ;
    dcterms:coverage "Global" ;
    dcterms:spatial "Worldwide" ;
    dcterms:temporal "2020-2023" .
```

**Maps to DataHub Properties:**

- `rdfs:comment` → additional description text
- `dcterms:source` → provenance information
- `dcterms:publisher` → publisher information
- `dcterms:rights` → usage rights
- `dcterms:language` → language specification
- `dcterms:coverage` → coverage information
- `dcterms:spatial` → spatial coverage
- `dcterms:temporal` → temporal coverage

## Domain Mapping

### Overview

Domain mapping creates hierarchical domain structures in DataHub based on dataset IRIs, following the same pattern as glossary term hierarchy creation. Each segment of the IRI path becomes a domain, creating a complete hierarchy from root to leaf.

### Domain Creation Logic

**IRI Path Segmentation:**

- Uses `derive_path_from_iri(iri, include_last=False)` to extract parent segments only
- Creates domains for parent segments, excluding the dataset name
- Follows the same hierarchy logic as glossary terms (dataset name is the entity, not a domain)

**Domain Hierarchy Examples:**

#### Simple Domain Structure

```turtle
ex:CustomerDatabase a void:Dataset ;
    dcterms:title "Customer Database" ;
    dh:hasIRI "https://example.com/finance/accounts" .
```

**Creates Domain Hierarchy:**

- `https://example.com/finance/accounts` → `urn:li:domain:example_com`
- `https://example.com/finance/accounts` → `urn:li:domain:finance`
- Dataset `accounts` assigned to `urn:li:domain:finance`

#### Complex Domain Structure

```turtle
ex:LoanTradingSystem a void:Dataset ;
    dcterms:title "Loan Trading" ;
    dh:hasIRI "https://bank.com/trading/loans/equities" .
```

**Creates Domain Hierarchy:**

- `https://bank.com/trading/loans/equities` → `urn:li:domain:bank_com`
- `https://bank.com/trading/loans/equities` → `urn:li:domain:trading`
- `https://bank.com/trading/loans/equities` → `urn:li:domain:loans`
- Dataset `equities` assigned to `urn:li:domain:loans`

### Domain Assignment Process

#### Automatic Domain Creation

1. **IRI Analysis**: Extract parent path segments from dataset IRI (exclude dataset name)
2. **Domain Generation**: Create domain for each parent segment
3. **Hierarchy Building**: Establish parent-child relationships
4. **Dataset Assignment**: Assign dataset to the leaf domain (most specific parent)

#### Domain Naming Convention

- **Clean Names**: Replace `.`, `-` with `_` and convert to lowercase
- **URN Format**: `urn:li:domain:{clean_name}`
- **Display Names**: Preserve original segment names for display

**Examples:**

- `example.com` → `urn:li:domain:example_com`
- `finance` → `urn:li:domain:finance`
- `loan-trading` → `urn:li:domain:loan_trading`

### Domain Reuse and Sharing

**Shared Domains:**
Datasets with common IRI prefixes share the same domain hierarchy:

```turtle
ex:CustomerAccounts a void:Dataset ;
    dh:hasIRI "https://example.com/finance/accounts" .

ex:CustomerLoans a void:Dataset ;
    dh:hasIRI "https://example.com/finance/loans" .
```

**Shared Domain Structure:**

- Both datasets share: `urn:li:domain:example_com` and `urn:li:domain:finance`
- Each gets its own leaf domain: `urn:li:domain:accounts` and `urn:li:domain:loans`

### Domain Mapping Examples

#### Financial Services Domain

```turtle
ex:FR_Y9C_Report a void:Dataset ;
    dcterms:title "Federal Reserve Y-9C Report" ;
    dh:hasIRI "https://federalreserve.gov/regulatory/reports/y9c" .
```

**Domain Hierarchy:**

- `urn:li:domain:federalreserve_gov` (Root domain)
- `urn:li:domain:regulatory` (Regulatory domain)
- `urn:li:domain:reports` (Reports domain)
- Dataset `y9c` assigned to `urn:li:domain:reports`

#### Multi-Platform Domain

```turtle
ex:CustomerDataWarehouse a void:Dataset ;
    dcterms:title "Customer Data Warehouse" ;
    dh:hasIRI "https://data.company.com/warehouse/customer" .

ex:CustomerAnalytics a void:Dataset ;
    dcterms:title "Customer Analytics" ;
    dh:hasIRI "https://analytics.company.com/insights/customer" .
```

**Domain Structure:**

- `urn:li:domain:data_company_com` and `urn:li:domain:analytics_company_com` (Platform domains)
- `urn:li:domain:warehouse` and `urn:li:domain:insights` (Service domains)
- Dataset `customer` assigned to `urn:li:domain:warehouse` and `urn:li:domain:insights` respectively

### Domain Configuration

#### Domain Properties

Each domain is created with:

- **Name**: Clean version of the IRI segment
- **Description**: Auto-generated description based on segment
- **Parent Domain**: Reference to parent domain (if not root)
- **Custom Properties**: Additional metadata as needed

#### Domain Assignment

- **Automatic**: Datasets are automatically assigned to their leaf domain
- **Manual Override**: Can be disabled with `--no-domains` flag
- **Preview Mode**: Dry run shows domain assignment preview

### Best Practices

#### Domain Design

1. **Consistent Naming**: Use consistent IRI patterns across related datasets
2. **Logical Hierarchy**: Design IRI paths to reflect business hierarchy
3. **Domain Reuse**: Leverage shared domains for related datasets
4. **Clear Segmentation**: Use meaningful path segments for domain names

#### IRI Structure Recommendations

```
https://{organization}.com/{department}/{system}/{component}
```

**Examples:**

- `https://bank.com/finance/loans/equities` → 4-level domain hierarchy
- `https://bank.com/regulatory/reports/y9c` → 4-level domain hierarchy
- `https://bank.com/trading/systems` → 3-level domain hierarchy

## Structured Properties Mapping

### Overview

Structured properties provide a powerful way to attach typed, validated metadata to DataHub entities. The system automatically detects structured properties from RDF ontologies and maps them to appropriate DataHub entity types based on the `rdfs:domain` property.

### Entity Type Detection

The system automatically determines which DataHub entity types a structured property applies to based on the RDF `rdfs:domain` property:

| RDF Domain            | DataHub Entity Type | Description            |
| --------------------- | ------------------- | ---------------------- |
| `dcat:Dataset`        | `dataset`           | Dataset entities       |
| `skos:Concept`        | `glossaryTerm`      | Glossary term entities |
| `schema:Person`       | `user`              | User entities          |
| `schema:Organization` | `corpGroup`         | Group entities         |
| `schema:DataCatalog`  | `dataPlatform`      | Platform entities      |

### Property Definition Structure

Structured properties are defined using standard RDF patterns:

```turtle
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix bcbs: <http://BCBS239/GOVERNANCE/> .

# Dataset authorization property
bcbs:authorized a rdf:Property ;
    rdfs:domain dcat:Dataset ;
    rdfs:range bcbs:AuthorizationType ;
    rdfs:label "authorized" ;
    rdfs:comment "The authorization type of this dataset" .

# Glossary term compliance property
bcbs:complianceStatus a rdf:Property ;
    rdfs:domain skos:Concept ;
    rdfs:range bcbs:ComplianceStatus ;
    rdfs:label "compliance status" ;
    rdfs:comment "The compliance status of this glossary term" .
```

### Enum Value Definition

Enum values are defined as instances of the range class:

```turtle
# Authorization types for datasets
bcbs:AuthorizationType a rdfs:Class ;
    rdfs:label "Authorization Type" ;
    rdfs:comment "Enumeration of authorization types for datasets" .

bcbs:Source a bcbs:AuthorizationType ;
    rdfs:label "Source" ;
    rdfs:comment "Dataset is an authorized source of data" .

bcbs:Distributor a bcbs:AuthorizationType ;
    rdfs:label "Distributor" ;
    rdfs:comment "Dataset is an authorized distributor of data" .
```

### DataHub Configuration Requirements

**CRITICAL**: Structured properties MUST be configured with specific DataHub settings to ensure they appear in filters, sidebar, and as badges. The following configuration is mandatory:

#### Required DataHub Search Configuration

```python
search_config = DataHubSearchConfigClass(
    enableAutocomplete=True,    # Enable autocomplete in search
    addToFilters=True,          # Show in filter panels
    queryByDefault=True,        # Include in default queries
    fieldType=SearchFieldTypeClass.TEXT
)
```

#### Required StructuredPropertyDefinitionClass Configuration

```python
datahub_definition = StructuredPropertyDefinitionClass(
    qualifiedName=qualified_name,
    displayName=property_name,           # Human-readable name
    description=property_definition['description'],
    valueType=property_definition['value_type'],
    cardinality=PropertyCardinalityClass.SINGLE,
    entityTypes=property_definition['entity_types'],  # List of DataHub entity type URNs
    allowedValues=allowed_values,         # Enum values if applicable
    searchConfiguration=search_config     # REQUIRED: Search configuration above
)
```

#### Configuration Validation Rules

1. **Entity Types**: Must be proper DataHub entity type URNs (e.g., `urn:li:entityType:datahub.dataset`)

   - ❌ **INVALID**: `["urn:li:entityType:datahub.dataset", "Dataset"]` (mixed URNs and strings)
   - ✅ **VALID**: `["urn:li:entityType:datahub.dataset"]` (only proper URNs)

2. **Search Configuration**: All three flags must be `True`:

   - `enableAutocomplete=True` - Required for search autocomplete
   - `addToFilters=True` - Required for filter panels
   - `queryByDefault=True` - Required for default search inclusion

3. **Display Configuration**:
   - `displayName` should be human-readable (e.g., "Authorized" not "authorized")
   - `description` should provide business context

#### Common Configuration Errors

**Error**: `Failed to retrieve entity with urn Dataset, invalid urn`
**Cause**: Entity types contain literal strings instead of proper DataHub URNs
**Fix**: Ensure only proper DataHub entity type URNs are used

**Error**: Structured properties not appearing in UI
**Cause**: Missing or incorrect search configuration
**Fix**: Ensure all three search configuration flags are set to `True`

#### Example: Complete Working Configuration

```python
# Correct entity type mapping
entity_types = ["urn:li:entityType:datahub.dataset"]

# Correct search configuration
search_config = DataHubSearchConfigClass(
    enableAutocomplete=True,
    addToFilters=True,
    queryByDefault=True,
    fieldType=SearchFieldTypeClass.TEXT
)

# Correct property definition
datahub_definition = StructuredPropertyDefinitionClass(
    qualifiedName="BCBS239/GOVERNANCE/authorized",
    displayName="Authorized",
    description="The authorization type of this dataset (Source or Distributor)",
    valueType=StringTypeClass(),
    cardinality=PropertyCardinalityClass.SINGLE,
    entityTypes=entity_types,
    allowedValues=[PropertyValueClass(value="Source"), PropertyValueClass(value="Distributor")],
    searchConfiguration=search_config
)
```

#### ⚠️ **CRITICAL PRESERVATION REQUIREMENTS**

**DO NOT MODIFY** the search configuration without explicit approval. Any changes to the following parameters will break structured property visibility in the DataHub UI:

- `enableAutocomplete=True` - **MUST REMAIN TRUE**
- `addToFilters=True` - **MUST REMAIN TRUE**
- `queryByDefault=True` - **MUST REMAIN TRUE**

**Regression Prevention**: Before any changes to `DataHubSearchConfigClass` or `StructuredPropertyDefinitionClass`, verify that:

1. All three search configuration flags remain `True`
2. Entity types contain only proper DataHub URNs (no literal strings)
3. The `searchConfiguration` parameter is always included

### Property Value Assignment

Property values are assigned to entities using the same RDF property:

```turtle
# Assign authorization to a dataset
ex:CustomerDatabase bcbs:authorized bcbs:Source .

# Assign compliance status to a glossary term
ex:CustomerID bcbs:complianceStatus bcbs:Compliant .
```

### Compliance Status Enumeration

```turtle
# Compliance statuses for glossary terms
bcbs:ComplianceStatus a rdfs:Class ;
    rdfs:label "Compliance Status" ;
    rdfs:comment "Enumeration of compliance statuses for glossary terms" .

bcbs:Compliant a bcbs:ComplianceStatus ;
    rdfs:label "Compliant" ;
    rdfs:comment "Term meets compliance requirements" .

bcbs:NonCompliant a bcbs:ComplianceStatus ;
    rdfs:label "Non-Compliant" ;
    rdfs:comment "Term does not meet compliance requirements" .
```

### Property Application

Structured properties are applied to entities using simple RDF assertions:

```turtle
# Apply authorization to datasets
<http://DataHubFinancial.com/TRADING/LOANS/TRADING/Loan_Trading> a dcat:Dataset ;
    bcbs:authorized bcbs:Source .

<http://DataHubFinancial.com/TRADING/LOANS/HUB/Consolidated_Loans> a dcat:Dataset ;
    bcbs:authorized bcbs:Distributor .

# Apply compliance status to glossary terms
<http://GLOSSARY/Customer_ID> a skos:Concept ;
    bcbs:complianceStatus bcbs:Compliant .

<http://GLOSSARY/Sensitive_Data> a skos:Concept ;
    bcbs:complianceStatus bcbs:NonCompliant .
```

## Enhanced Glossary Term Extraction

### Overview

The system now extracts comprehensive metadata from glossary terms, preserving all RDF properties that are useful for exporting and downstream processing.

### Extracted Properties

| Property               | RDF Source                            | Description            | Example                                                                 |
| ---------------------- | ------------------------------------- | ---------------------- | ----------------------------------------------------------------------- |
| **URI**                | Original IRI                          | Complete original URI  | `http://DataHubFinancial.com/CDE/CRITICAL_DATA_ELEMENTS/Reporting_Date` |
| **Name**               | `skos:prefLabel`                      | Primary label          | `"Reporting Date"`                                                      |
| **Definition**         | `skos:definition`                     | Term definition        | `"Date of regulatory reporting period..."`                              |
| **RDF Type**           | `rdf:type`                            | Original RDF type      | `"Concept"`                                                             |
| **Alternative Labels** | `skos:altLabel`                       | Alternative names      | `["Client ID", "Customer Number"]`                                      |
| **Hidden Labels**      | `skos:hiddenLabel`                    | Hidden/internal names  | `["CustID"]`                                                            |
| **Notation**           | `skos:notation`                       | Short code/notation    | `"CUST-001"`                                                            |
| **Scope Note**         | `skos:scopeNote`                      | Usage context          | `"This is used across all customer-facing systems"`                     |
| **Relationships**      | `skos:broader`, `skos:narrower`, etc. | Semantic relationships | `[RDFRelationship(...)]`                                                |
| **Custom Properties**  | Any literal properties                | Additional metadata    | `{"prefLabel": "Customer ID", ...}`                                     |

### Example: Complete Glossary Term Extraction

```turtle
# RDF Source
test:CustomerID a skos:Concept ;
    skos:prefLabel "Customer ID" ;
    skos:altLabel "Client ID" ;
    skos:altLabel "Customer Number" ;
    skos:hiddenLabel "CustID" ;
    skos:notation "CUST-001" ;
    skos:definition "Unique identifier for a customer" ;
    skos:scopeNote "This is used across all customer-facing systems" .
```

**Extracted Properties:**

```python
RDFGlossaryTerm(
    uri="http://TEST/CustomerID",
    name="Customer ID",
    definition="Unique identifier for a customer",
    rdf_type="Concept",
    alternative_labels=["Client ID", "Customer Number"],
    hidden_labels=["CustID"],
    notation="CUST-001",
    scope_note="This is used across all customer-facing systems",
    relationships=[],  # Semantic relationships
    properties={...}   # All literal properties
)
```

### Benefits for Exporting

1. **Complete Metadata Preservation**: All RDF properties are captured for full fidelity
2. **Multiple Label Support**: Alternative and hidden labels preserved for search/discovery
3. **Notation Support**: Short codes preserved for system integration
4. **Context Preservation**: Scope notes provide usage context
5. **Type Information**: Original RDF type preserved for validation
6. **Export Flexibility**: Rich metadata enables various export formats and use cases

### Auto-Detection Process

The system automatically:

1. **Scans for Properties**: Finds all `rdf:Property` declarations
2. **Detects Domain**: Reads `rdfs:domain` to determine target entity types
3. **Identifies Enums**: Finds instances of the `rdfs:range` class as enum values
4. **Extracts Metadata**: Uses `rdfs:label` and `rdfs:comment` for descriptions
5. **Registers Properties**: Creates DataHub structured property definitions
6. **Applies Values**: Assigns property values to entities

### Multi-Entity Support

The same structured property can be applied to multiple entity types by using multiple `rdfs:domain` declarations:

```turtle
# Property that applies to both datasets and glossary terms
bcbs:classification a rdf:Property ;
    rdfs:domain dcat:Dataset ;
    rdfs:domain skos:Concept ;
    rdfs:range bcbs:ClassificationLevel ;
    rdfs:label "classification" ;
    rdfs:comment "Security classification level" .

bcbs:ClassificationLevel a rdfs:Class .
bcbs:Public a bcbs:ClassificationLevel .
bcbs:Internal a bcbs:ClassificationLevel .
bcbs:Confidential a bcbs:ClassificationLevel .
bcbs:Restricted a bcbs:ClassificationLevel .
```

This creates a structured property that applies to both `dataset` and `glossaryTerm` entities in DataHub.

### Property Characteristics

Additional property characteristics can be specified:

```turtle
# Functional property (one-to-one relationship)
bcbs:authorized a owl:FunctionalProperty .

# Transitive property
bcbs:partOf a owl:TransitiveProperty .

# Symmetric property
bcbs:relatedTo a owl:SymmetricProperty .
```

### Namespace Handling

The system automatically extracts namespace prefixes from RDF `@prefix` declarations:

```turtle
@prefix bcbs: <http://BCBS239/GOVERNANCE/> .
@prefix fibo: <http://www.omg.org/spec/FIBO/> .
@prefix custom: <http://company.com/ontology/> .
```

Properties are registered with their namespace prefix (e.g., `bcbs:authorized`, `fibo:hasCurrency`, `custom:businessValue`).

### Validation and Constraints

The system validates:

- **Required Properties**: Must have `rdfs:domain` and `rdfs:range`
- **Valid Domains**: Must map to supported DataHub entity types
- **Enum Values**: Must have at least one instance of the range class
- **Namespace**: Must have valid namespace prefix
- **Metadata**: Must have `rdfs:label` or property name

### Best Practices

#### Property Design

1. **Clear Naming**: Use descriptive property names
2. **Consistent Domains**: Use standard RDF vocabularies for domains
3. **Meaningful Enums**: Create enum values that are self-explanatory
4. **Comprehensive Metadata**: Include labels and comments
5. **Namespace Organization**: Use consistent namespace prefixes

#### Entity Type Selection

1. **Dataset Properties**: Use `dcat:Dataset` for dataset-specific metadata
2. **Glossary Properties**: Use `skos:Concept` for term-specific metadata
3. **User Properties**: Use `schema:Person` for user-specific metadata
4. **Group Properties**: Use `schema:Organization` for group-specific metadata
5. **Platform Properties**: Use `schema:DataCatalog` for platform-specific metadata

#### Enum Design

1. **Exhaustive Values**: Include all possible enum values
2. **Clear Labels**: Use descriptive labels for enum values
3. **Consistent Naming**: Follow consistent naming conventions
4. **Documentation**: Include comments explaining each enum value
5. **Hierarchical Structure**: Use subclasses for complex enum hierarchies

### Examples

#### BCBS 239 Compliance

```turtle
# Dataset authorization
bcbs:authorized a rdf:Property ;
    rdfs:domain dcat:Dataset ;
    rdfs:range bcbs:AuthorizationType ;
    rdfs:label "authorized" ;
    rdfs:comment "BCBS 239 authorization level for datasets" .

bcbs:AuthorizationType a rdfs:Class .
bcbs:Source a bcbs:AuthorizationType ;
    rdfs:label "Authorized Source" .
bcbs:Distributor a bcbs:AuthorizationType ;
    rdfs:label "Authorized Distributor" .

# Application to datasets
<http://DataHubFinancial.com/TRADING/LOANS/TRADING/Loan_Trading> a dcat:Dataset ;
    bcbs:authorized bcbs:Source .
```

#### Data Quality Metrics

```turtle
# Data quality for multiple entity types
quality:dataQualityScore a rdf:Property ;
    rdfs:domain dcat:Dataset ;
    rdfs:domain skos:Concept ;
    rdfs:range quality:QualityLevel ;
    rdfs:label "data quality score" ;
    rdfs:comment "Data quality assessment score" .

quality:QualityLevel a rdfs:Class .
quality:Excellent a quality:QualityLevel .
quality:Good a quality:QualityLevel .
quality:Fair a quality:QualityLevel .
quality:Poor a quality:QualityLevel .

# Application to datasets and terms
<http://DATA/Customer_DB> a dcat:Dataset ;
    quality:dataQualityScore quality:Good .

<http://GLOSSARY/Customer_ID> a skos:Concept ;
    quality:dataQualityScore quality:Excellent .
```

## Technical Implementation Details

### URN Generation Algorithm

1. **Parse Dataset IRI**: Extract scheme, authority, path, and fragment
2. **Scheme Handling**:
   - HTTP/HTTPS: Convert to DataHub URN format using path hierarchy
   - Custom schemes: Preserve as-is for dataset-specific schemes
3. **Path Processing**: Split path into hierarchical components
4. **Fragment Handling**: Use fragment as final component if present
5. **URN Construction**: Build DataHub-compliant dataset URN

### Platform Processing

#### Platform Identification

```turtle
ex:CustomerDatabase dcat:accessService <http://postgres.example.com> ;
    schema:provider ex:PostgreSQLPlatform ;
    void:sparqlEndpoint <http://sparql.example.com/customer> ;
    void:dataDump <http://data.example.com/customer.nt> .
```

**Creates DataHub Platform Mapping:**

- `dcat:accessService` → `urn:li:dataPlatform:postgres` (preferred method)
- `schema:provider` → `urn:li:dataPlatform:postgresql`
- `void:sparqlEndpoint` → `urn:li:dataPlatform:sparql`
- `void:dataDump` → `urn:li:dataPlatform:file`

#### Connection Processing

- `dcat:accessService` creates platform connections (preferred method)
- SPARQL endpoints create SPARQL platform connections
- Data dumps create file platform connections
- Database providers create database platform connections
- Custom distributions create custom platform connections

#### Platform Extraction Logic

The system extracts platform information from `dcat:accessService` using the following logic:

**Service URI Processing:**

```turtle
ex:CustomerDatabase dcat:accessService <http://postgres.example.com> .
ex:AnalyticsDB dcat:accessService <http://bigquery.example.com> .
ex:DataWarehouse dcat:accessService <http://snowflake.example.com> .
```

**Platform Extraction:**

- `http://postgres.example.com` → `postgres` (extracted from hostname)
- `http://bigquery.example.com` → `bigquery` (extracted from hostname)
- `http://snowflake.example.com` → `snowflake` (extracted from hostname)

**Literal Value Processing:**

```turtle
ex:CustomerDatabase dcat:accessService "postgresql" .
ex:AnalyticsDB dcat:accessService "bigquery" .
```

**Platform Extraction:**

- `"postgresql"` → `postgresql` (used as-is)
- `"bigquery"` → `bigquery` (used as-is)

**Benefits of `dcat:accessService`:**

- **Standards Compliant**: Uses W3C DCAT standard
- **Semantic Clarity**: Represents the service that provides access to the dataset
- **Tool Integration**: Works with existing DCAT tools and validators
- **Future Proof**: Follows established semantic web standards

### Validation Rules

#### Dataset Validation

- Must have valid dataset type (`void:Dataset`, `dcterms:Dataset`, `schema:Dataset`)
- Required properties must be present (`dcterms:title`)
- Property values must be non-empty strings
- Timestamps must be valid date/time formats
- URLs must be valid URI formats

#### Lineage Validation

- Lineage relationships must reference valid entities
- No circular references in lineage chains
- Timestamps must be chronologically consistent
- Agents must reference valid users

#### Platform Validation

- Platform references must be valid platform URNs
- Connection properties must be valid connection types
- Endpoint URLs must be accessible
- Data dump URLs must be valid file references

### Validation Rules

#### Dataset Identification Validation

- **Type Validation**: Must be `void:Dataset`, `dcterms:Dataset`, `schema:Dataset`, or `dh:Dataset`
- **Metadata Validation**: Must have name/title via priority mapping
- **URI Validation**: Must be valid URI reference

#### Schema Field Validation

- **Field Reference**: Must be referenced via `dh:hasSchemaField` or custom field properties
- **Field Name**: Must have field name via `dh:hasName`, `rdfs:label`, or custom `hasName`
- **Type Validation**: Data types must be valid DataHub schema types
- **Constraint Validation**: Constraints must be valid (nullable, length, etc.)

#### Lineage Validation

- **Activity Validation**: Must be typed as `prov:Activity`
- **Relationship Validation**: Must have upstream (`prov:used`) and downstream (`prov:generated`) relationships
- **Temporal Validation**: Must have `prov:startedAtTime` and `prov:endedAtTime`
- **Agent Validation**: Must have `prov:wasAssociatedWith` or `prov:wasAttributedTo`

### Error Handling

#### Dataset Processing Errors

- Missing dataset type declarations
- Invalid dataset metadata (empty names, descriptions)
- Unsupported platform configurations
- Schema field extraction failures

#### Lineage Processing Errors

- Missing PROV-O activity declarations
- Incomplete lineage relationships
- Invalid temporal information
- Broken entity references

#### Platform Integration Errors

- Unsupported platform types
- Invalid connection configurations
- Authentication failures
- Data access permissions

#### Mapping Errors

- Missing required properties
- Invalid property values (empty strings, malformed data)
- Broken relationship references
- Unsupported RDF patterns

### Best Practices

#### Dataset Design

1. Use clear, descriptive `dcterms:title`
2. Include comprehensive `dcterms:description`
3. Specify proper `dcterms:creator` and `dcterms:publisher`
4. Include creation and modification timestamps
5. Use standard dataset vocabularies (VOID, DC Terms, Schema.org)

#### Lineage Documentation

1. Document all data dependencies with `prov:used`
2. Specify data generation with `prov:wasGeneratedBy`
3. Include user attribution with `prov:wasAssociatedWith`
4. Use proper timestamps for lineage events
5. Maintain consistent lineage chains

#### Platform Integration

1. Use `dcat:accessService` for platform identification (preferred method)
2. Use appropriate platform types for different data sources
3. Include connection details for data access
4. Specify data distribution methods
5. Document platform-specific configurations
6. Maintain platform consistency across related datasets

#### Relationship Management

1. Use `owl:sameAs` for true identity relationships
2. Use `skos:exactMatch` for equivalent datasets
3. Use `dcterms:isPartOf` for dataset containment
4. Use `prov:wasDerivedFrom` for lineage relationships
5. Maintain bidirectional consistency where appropriate

## Lineage Processing

### Overview

RDF provides comprehensive lineage processing through PROV-O (Provenance Ontology), enabling detailed tracking of data flow, transformations, and dependencies across datasets and processing activities.

### Lineage Activity Mapping

#### Activity Identification Criteria

**Required Conditions:**

- Must be typed as `prov:Activity`
- Must have a name or label
- Should have temporal information

**Included Properties:**

- `prov:startedAtTime` - Activity start timestamp
- `prov:endedAtTime` - Activity end timestamp
- `prov:wasAssociatedWith` - User/agent attribution
- `rdfs:label` or `dcterms:title` - Activity name
- `dcterms:description` - Activity description

#### Activity Processing Example

```turtle
ex:LoanAggregationActivity a prov:Activity ;
    rdfs:label "Loan Data Aggregation" ;
    dcterms:description "ETL process that aggregates loan trading data from multiple front office systems" ;
    prov:startedAtTime "2024-01-01T06:00:00+00:00"^^xsd:dateTime ;
    prov:endedAtTime "2024-01-01T06:30:00+00:00"^^xsd:dateTime ;
    prov:wasAssociatedWith ex:DataEngineeringTeam .
```

**DataHub Mapping:**

- Activity → DataHub DataJob entity
- URN: `urn:li:dataJob:datahub.com/lineage/loan_aggregation_activity`
- Temporal information preserved
- User attribution maintained

### Lineage Relationship Mapping

#### Relationship Types

| PROV-O Property        | DataHub Mapping      | Description                |
| ---------------------- | -------------------- | -------------------------- |
| `prov:used`            | Upstream dependency  | Data consumed by activity  |
| `prov:generated`       | Downstream product   | Data produced by activity  |
| `prov:wasDerivedFrom`  | Direct derivation    | Direct data transformation |
| `prov:wasGeneratedBy`  | Activity-to-entity   | Entity created by activity |
| `prov:wasInfluencedBy` | Downstream influence | Indirect data influence    |

#### Relationship Processing Example

```turtle
# Activity uses upstream data
ex:LoanAggregationActivity prov:used ex:LoanTradingDataset ;
                          prov:used ex:AccountDetailsDataset .

# Activity generates downstream data
ex:LoanAggregationActivity prov:generated ex:ConsolidatedLoansDataset .

# Direct derivation relationship
ex:ConsolidatedLoansDataset prov:wasDerivedFrom ex:LoanTradingDataset .
```

**DataHub Mapping:**

- Relationships → DataHub LineageEdge entities
- Source and target URNs generated
- Activity mediation preserved
- Relationship types mapped to DataHub lineage types

### Field-Level Lineage

#### Field Mapping Processing

RDF supports detailed field-level lineage tracking:

```turtle
# Field-level lineage mapping
ex:AccountIdFieldMapping a prov:Activity ;
    rdfs:label "Account ID Field Mapping" ;
    dcterms:description "Reference data pattern: all systems import account_id directly from Account Details" ;
    prov:used ex:AccountDetailsDataset#account_id ;
    prov:generated ex:ConsolidatedLoansDataset#account_id ;
    prov:generated ex:FinanceLoanBalancesDataset#account_id ;
    prov:generated ex:RiskLoanRiskManagementDataset#account_id .
```

**Benefits:**

- Tracks data transformations at column level
- Identifies data quality issues
- Supports impact analysis
- Enables compliance reporting

### Activity-Mediated Relationships

#### Mediation Detection

The system automatically detects activities that mediate lineage relationships:

```turtle
# Activity-mediated relationship
ex:ETLJob a prov:Activity ;
    prov:used ex:SourceDataset ;
    prov:generated ex:TargetDataset .

# Direct relationship (mediated by activity)
ex:TargetDataset prov:wasGeneratedBy ex:ETLJob .
```

**Processing Logic:**

1. Identify activities with `prov:used` and `prov:generated` relationships
2. Link direct relationships to mediating activities
3. Preserve activity context in lineage edges
4. Generate proper DataHub lineage URNs

### Lineage URN Generation

#### Activity URNs

Activities receive hierarchical URNs based on their IRI structure:

```turtle
# Input IRI
ex:LoanAggregationActivity

# Generated URN
urn:li:dataJob:datahub.com/lineage/loan_aggregation_activity
```

#### Relationship URNs

Lineage relationships reference dataset URNs with activity mediation:

```turtle
# Source dataset URN
urn:li:dataset:(postgres,LOANS/TRADING/Loan_Trading,PROD)

# Target dataset URN
urn:li:dataset:(hive,LOANS/HUB/Consolidated_Loans,PROD)

# Activity URN (if mediated)
urn:li:dataJob:datahub.com/lineage/loan_aggregation_activity
```

### Lineage Processing Features

#### Comprehensive Coverage

- **Activity Processing**: Complete PROV-O activity extraction
- **Relationship Processing**: All major PROV-O relationship types
- **Field-Level Tracking**: Column-to-column lineage mapping
- **Temporal Information**: Start/end times and user attribution
- **Mediation Detection**: Automatic activity-relationship linking

#### Data Quality Features

- **Unauthorized Flow Detection**: Identifies problematic data flows
- **Consistency Checking**: Validates lineage relationships
- **Impact Analysis**: Tracks downstream effects of changes
- **Compliance Reporting**: Supports regulatory requirements

#### Integration Features

- **DataHub Native**: Direct integration with DataHub lineage system
- **Pretty Print Support**: Human-readable lineage visualization
- **Export Capabilities**: Multiple output formats
- **Validation**: Comprehensive lineage validation

### Best Practices

#### Lineage Documentation

1. **Activity Definition**: Use clear, descriptive names and descriptions
2. **Temporal Bounds**: Include start and end times for activities
3. **User Attribution**: Specify responsible users/teams
4. **Field Mapping**: Document field-level transformations
5. **Dependency Tracking**: Map all upstream and downstream relationships

#### PROV-O Usage

1. **Standard Compliance**: Use standard PROV-O properties
2. **Consistent Naming**: Maintain consistent activity and dataset naming
3. **Complete Coverage**: Document all significant data flows
4. **Validation**: Validate lineage relationships for consistency
5. **Maintenance**: Keep lineage information current

#### Performance Considerations

1. **Batch Processing**: Process lineage in batches for large datasets
2. **Incremental Updates**: Support incremental lineage updates
3. **Caching**: Cache frequently accessed lineage information
4. **Optimization**: Optimize queries for lineage traversal
5. **Monitoring**: Monitor lineage processing performance
