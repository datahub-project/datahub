# Data Product Specification

**Part of**: [RDF Specification](../../../../docs/rdf-specification.md)

This document specifies how RDF data products are extracted, converted, and mapped to DataHub data product entities.

## Overview

Data products represent logical groupings of datasets that together provide a complete business capability. They are defined using the Data Product Ontology (DPROD) vocabulary.

## RDF Source Pattern

Data products are identified by the `dprod:DataProduct` type:

```turtle
ex:LoanTradingProduct a dprod:DataProduct ;
    rdfs:label "Loan Trading Data Product" ;
    rdfs:comment "Complete data product for loan trading operations" ;
    dprod:hasDomain ex:TradingDomain ;
    dprod:dataOwner ex:FinanceTeam ;
    dprod:asset ex:LoanDataset ;
    dprod:asset ex:CounterpartyDataset .
```

## Required Properties

- **RDF Type**: `dprod:DataProduct` (required)
- **Name**: `rdfs:label` OR `dcterms:title` (required)

## Recommended Properties

- **Description**: `rdfs:comment` OR `dcterms:description`
- **Domain**: `dprod:hasDomain` - Reference to domain IRI or path
- **Owner**: `dprod:dataOwner` - Reference to owner entity
- **Assets**: `dprod:asset` - References to dataset URIs (one or more)

## Property Extraction

### Name Extraction

Priority order:

1. `rdfs:label`
2. `dcterms:title`

### Description Extraction

Priority order:

1. `rdfs:comment`
2. `dcterms:description`

### Domain Extraction

**Supported Property**: `dprod:hasDomain` only

The domain can be specified as:

- **IRI format**: Full URI reference to a domain
- **Path format**: String path like `"TRADING/FIXED_INCOME"`

**Example**:

```turtle
ex:Product a dprod:DataProduct ;
    dprod:hasDomain ex:TradingDomain .  # IRI format

# OR

ex:Product a dprod:DataProduct ;
    dprod:hasDomain "TRADING/FIXED_INCOME" .  # Path format
```

### Owner Extraction

**Supported Property**: `dprod:dataOwner`

The owner is extracted as an IRI reference. Owner type can be specified via:

- **Primary**: `dh:hasOwnerType` property on the owner entity (supports custom types)
- **Fallback**: RDF type mapping:
  - `dh:BusinessOwner` → `"BUSINESS_OWNER"`
  - `dh:DataSteward` → `"DATA_STEWARD"`
  - `dh:TechnicalOwner` → `"TECHNICAL_OWNER"`

**Example**:

```turtle
ex:FinanceTeam a dh:BusinessOwner ;
    rdfs:label "Finance Team" .

ex:Product a dprod:DataProduct ;
    dprod:dataOwner ex:FinanceTeam .
```

### Asset Extraction

**Supported Property**: `dprod:asset`

Assets are dataset URIs. Each asset can optionally specify a platform via `dcat:accessService`:

```turtle
ex:LoanDataset a dcat:Dataset ;
    dcat:accessService ex:PostgresPlatform .

ex:Product a dprod:DataProduct ;
    dprod:asset ex:LoanDataset .
```

**Platform Detection**:

- Extracted from `dcat:accessService` → `dcterms:title` of the service
- If no platform is found, defaults to `"logical"` during URN generation

## DataHub Integration

### URN Generation

Data product URNs are generated from the product name:

- Format: `urn:li:dataProduct:{product_name}`
- Product name is normalized (spaces replaced, special characters handled)

### Domain URN Conversion

Domain references are converted to DataHub domain URNs:

- **IRI format**: Converted to path segments, then to domain URN
- **Path format**: Directly converted to domain URN

Format: `urn:li:domain:({path_segments})`

### Owner URN Conversion

Owner IRIs are converted to DataHub CorpGroup URNs:

- Format: `urn:li:corpGroup:{owner_name}`
- Owner name extracted from owner IRI or label

### Asset URN Conversion

Asset dataset URIs are converted to DataHub dataset URNs:

- Uses standard dataset URN generation: `urn:li:dataset:({platform},{path},{environment})`
- Platform extracted from `dcat:accessService` or defaults to `"logical"`

## Example

**RDF**:

```turtle
ex:LoanTradingProduct a dprod:DataProduct ;
    rdfs:label "Loan Trading Data Product" ;
    rdfs:comment "Complete data product for loan trading operations" ;
    dprod:hasDomain "TRADING/LOANS" ;
    dprod:dataOwner ex:FinanceTeam ;
    dprod:asset ex:LoanDataset ;
    dprod:asset ex:CounterpartyDataset .

ex:FinanceTeam a dh:BusinessOwner ;
    rdfs:label "Finance Team" .

ex:LoanDataset a dcat:Dataset ;
    dcterms:title "Loan Master" ;
    dcat:accessService ex:PostgresPlatform .

ex:PostgresPlatform a dcat:DataService ;
    dcterms:title "postgres" .
```

**DataHub**:

- Product URN: `urn:li:dataProduct:Loan_Trading_Data_Product`
- Domain URN: `urn:li:domain:(TRADING,LOANS)`
- Owner URN: `urn:li:corpGroup:Finance_Team`
- Asset URNs:
  - `urn:li:dataset:(urn:li:dataPlatform:postgres,Loan_Master,PROD)`
  - `urn:li:dataset:(urn:li:dataPlatform:logical,CounterpartyDataset,PROD)`
