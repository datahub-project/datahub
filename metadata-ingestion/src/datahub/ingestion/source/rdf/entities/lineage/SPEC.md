# Lineage Specification

**Part of**: [RDF Specification](../../../../docs/rdf-specification.md)

This document specifies how RDF lineage relationships are extracted, converted, and mapped to DataHub lineage entities.

## Overview

Dataset lineage tracks how data flows between datasets and processing activities, providing complete visibility into data transformations and dependencies.

## Dataset-to-Dataset Lineage

Direct lineage relationships between datasets using PROV-O (Provenance Ontology).

**RDF Properties**:

- `prov:wasDerivedFrom` - Direct derivation relationship
- `prov:wasInfluencedBy` - Indirect influence relationship
- `prov:wasGeneratedBy` - Activity that created the data
- `prov:used` - Data consumed by an activity

**Example**:

```turtle
# Direct derivation
accounts:ProcessedCustomerData a dcat:Dataset ;
    dcterms:title "Processed Customer Data" ;
    prov:wasDerivedFrom accounts:RawCustomerData ;
    prov:wasGeneratedBy accounts:DataCleaningJob .

# Activity-mediated lineage
accounts:DataCleaningJob a prov:Activity ;
    prov:used accounts:RawCustomerData ;
    prov:generated accounts:ProcessedCustomerData ;
    prov:wasAssociatedWith accounts:DataEngineer .
```

## Field-Level Lineage

Detailed lineage tracking at the field level, showing how individual fields are transformed between datasets.

**Field Lineage Mapping**:

```turtle
# Field-level lineage activity
accounts:AccountIdFieldMapping a prov:Activity ;
    rdfs:label "Account ID Field Mapping" ;
    dcterms:description "Reference data pattern: all systems import account_id directly from Account Details" ;
    prov:used accounts:AccountDetailsDataset#account_id ;
    prov:generated accounts:ConsolidatedLoansDataset#account_id ;
    prov:generated accounts:FinanceLoanBalancesDataset#account_id ;
    prov:generated accounts:RiskLoanRiskManagementDataset#account_id .
```

**Benefits**:

- Tracks data transformations at column level
- Identifies data quality issues
- Supports impact analysis
- Enables compliance reporting

## Activity-Mediated Relationships

Activities that mediate lineage relationships provide context about data processing.

**Activity Types**:

- **Data Jobs**: ETL processes, data transformations
- **Data Flows**: Streaming processes, real-time processing
- **Manual Processes**: Human-driven data operations

**Example**:

```turtle
# Complex lineage chain
accounts:RawData a dcat:Dataset ;
    prov:wasGeneratedBy accounts:DataIngestionJob .

accounts:CleanedData a dcat:Dataset ;
    prov:wasDerivedFrom accounts:RawData ;
    prov:wasGeneratedBy accounts:DataCleaningJob .

accounts:AggregatedData a dcat:Dataset ;
    prov:wasDerivedFrom accounts:CleanedData ;
    prov:wasGeneratedBy accounts:DataAggregationJob .
```

## Lineage Relationship Types

**Core Relationship Types**:

| PROV-O Property        | DataHub Mapping      | Description                |
| ---------------------- | -------------------- | -------------------------- |
| `prov:used`            | Upstream dependency  | Data consumed by activity  |
| `prov:generated`       | Downstream product   | Data produced by activity  |
| `prov:wasDerivedFrom`  | Direct derivation    | Direct data transformation |
| `prov:wasGeneratedBy`  | Activity-to-entity   | Entity created by activity |
| `prov:wasInfluencedBy` | Downstream influence | Indirect data influence    |

## Lineage Processing

The system automatically processes lineage relationships and creates appropriate DataHub lineage edges:

**Processing Steps**:

1. **Relationship Detection**: Identify PROV-O relationships in RDF
2. **URN Generation**: Convert dataset IRIs to DataHub URNs
3. **Activity Creation**: Create DataJob entities for activities
4. **Lineage Edge Creation**: Establish upstream/downstream relationships
5. **Field Mapping**: Create fine-grained lineage for field-level relationships

**DataHub Integration**:

- Dataset URNs: `urn:li:dataset:({platform},{path},{environment})`
- DataJob URNs: `urn:li:dataJob:{path}`
- Lineage edges with temporal and attribution information
