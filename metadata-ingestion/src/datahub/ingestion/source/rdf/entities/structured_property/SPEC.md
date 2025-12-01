# Structured Property Specification

**Part of**: [RDF Specification](../../../../docs/rdf-specification.md)

This document specifies how RDF structured properties are extracted, converted, and mapped to DataHub structured property entities.

## Overview

Custom properties provide a powerful way to attach typed, validated metadata to both glossary terms and datasets. The system automatically detects structured properties from RDF ontologies and maps them to appropriate DataHub entity types.

## Structured Properties Overview

Structured properties are identified using OWL and RDF property types. The system recognizes properties defined as:

**Property Type Indicators** (in priority order):

1. `owl:ObjectProperty` - Properties relating entities to other entities
2. `owl:DatatypeProperty` - Properties relating entities to data values
3. `rdf:Property` - Generic RDF properties

**RDF Pattern (using owl:ObjectProperty)**:

```turtle
ex:hasBusinessOwner a owl:ObjectProperty ;
    rdfs:label "Business Owner" ;
    rdfs:domain dcat:Dataset ;
    rdfs:range schema:Person .
```

**RDF Pattern (using owl:DatatypeProperty)**:

```turtle
ex:dataClassification a owl:DatatypeProperty ;
    rdfs:label "Data Classification" ;
    rdfs:domain dcat:Dataset ;
    rdfs:range xsd:string .
```

**RDF Pattern (using rdf:Property)**:

```turtle
ex:customProperty a rdf:Property ;
    rdfs:label "Custom Property" ;
    rdfs:domain ex:TargetEntityType ;
    rdfs:range xsd:string .
```

## Entity Type Detection

The system automatically determines which DataHub entity types a structured property applies to based on the RDF `rdfs:domain` property:

| RDF Domain            | DataHub Entity Type | Description            |
| --------------------- | ------------------- | ---------------------- |
| `dcat:Dataset`        | `dataset`           | Dataset entities       |
| `skos:Concept`        | `glossaryTerm`      | Glossary term entities |
| `schema:Person`       | `user`              | User entities          |
| `schema:Organization` | `corpGroup`         | Group entities         |
| `schema:DataCatalog`  | `dataPlatform`      | Platform entities      |

## Property Definition Structure

**Basic Property Definition (DatatypeProperty)**:

```turtle
ex:dataClassification a owl:DatatypeProperty ;
    rdfs:label "Data Classification" ;
    rdfs:comment "Classification level for data sensitivity" ;
    rdfs:domain dcat:Dataset ;
    rdfs:range xsd:string .
```

**Property with Cardinality (ObjectProperty)**:

```turtle
ex:businessOwner a owl:ObjectProperty ;
    rdfs:label "Business Owner" ;
    rdfs:comment "Primary business owner of the dataset" ;
    rdfs:domain dcat:Dataset ;
    rdfs:range schema:Person ;
    rdfs:cardinality 1 .
```

## Property Value Assignments

Properties are assigned to entities using standard RDF patterns:

**Dataset Property Assignment**:

```turtle
accounts:AccountDataset a dcat:Dataset ;
    dcterms:title "Account Master" ;
    ex:dataClassification "CONFIDENTIAL" ;
    ex:businessOwner accounts:FinanceManager ;
    ex:retentionPeriod "P7Y" .  # 7 years retention
```

**Glossary Term Property Assignment**:

```turtle
accounts:Customer_ID a skos:Concept ;
    skos:prefLabel "Customer Identifier" ;
    ex:dataClassification "PII" ;
    ex:regulatoryScope "GDPR" ;
    ex:encryptionRequired true .
```

## Property Processing

The system automatically processes structured properties:

**Processing Steps**:

1. **Property Detection**: Identify properties with `rdfs:domain`
2. **Entity Type Mapping**: Map RDF domains to DataHub entity types
3. **URN Generation**: Create structured property URNs
4. **Value Assignment**: Apply property values to entities
5. **DataHub Integration**: Create structured property assignments

**DataHub Integration**:

- Property URNs: `urn:li:structuredProperty:{property_name}`
- Value assignments with proper typing
- Automatic deduplication of property values

## Common Property Patterns

**Data Classification Properties**:

```turtle
ex:dataClassification a owl:DatatypeProperty ;
    rdfs:label "Data Classification" ;
    rdfs:domain dcat:Dataset ;
    rdfs:range xsd:string .

ex:confidentialityLevel a owl:DatatypeProperty ;
    rdfs:label "Confidentiality Level" ;
    rdfs:domain skos:Concept ;
    rdfs:range xsd:string .
```

**Business Metadata Properties**:

```turtle
ex:businessOwner a owl:ObjectProperty ;
    rdfs:label "Business Owner" ;
    rdfs:domain dcat:Dataset ;
    rdfs:range schema:Person .

ex:dataSteward a owl:ObjectProperty ;
    rdfs:label "Data Steward" ;
    rdfs:domain skos:Concept ;
    rdfs:range schema:Person .
```

**Technical Metadata Properties**:

```turtle
ex:retentionPeriod a owl:DatatypeProperty ;
    rdfs:label "Retention Period" ;
    rdfs:domain dcat:Dataset ;
    rdfs:range xsd:duration .

ex:encryptionRequired a owl:DatatypeProperty ;
    rdfs:label "Encryption Required" ;
    rdfs:domain skos:Concept ;
    rdfs:range xsd:boolean .
```
