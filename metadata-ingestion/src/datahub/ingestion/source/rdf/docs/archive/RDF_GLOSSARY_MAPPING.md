# RDF Glossary Mapping Reference

## Overview

This document provides detailed technical specifications for how RDF glossary concepts are mapped to DataHub glossary entities, including terms, nodes, relationships, and IRI transformations.

## Glossary Mapping

### Term Identification Criteria

The system identifies RDF individuals as "terms" using these criteria:

**Required Conditions:**

- Must have a label: `rdfs:label` OR `skos:prefLabel` (≥3 characters)
- Must be a URI reference (not blank node or literal)
- Must have appropriate RDF type

**Included RDF Types:**

- `owl:Class` - OWL classes
- `owl:NamedIndividual` - OWL named individuals
- `skos:Concept` - SKOS concepts
- **Custom class instances** - Any resource typed as instance of custom class

**Excluded RDF Types:**

- `owl:Ontology` - Ontology declarations (not terms)

**Definition Extraction Priority:**

1. `skos:definition` (preferred)
2. `rdfs:comment` (fallback)

### Core Entity Mappings

| RDF Concept            | DataHub Entity | Description                          |
| ---------------------- | -------------- | ------------------------------------ |
| `skos:Concept`         | `GlossaryTerm` | Individual glossary terms            |
| `skos:ConceptScheme`   | `GlossaryNode` | Container nodes for organizing terms |
| `skos:Collection`      | `GlossaryNode` | Grouped collections of terms         |
| `owl:Class`            | `GlossaryTerm` | OWL classes as terms                 |
| `owl:NamedIndividual`  | `GlossaryTerm` | OWL individuals as terms             |
| Custom class instances | `GlossaryTerm` | Domain-specific concept instances    |

### Field-to-Concept Mapping Approaches

The system supports two approaches for mapping dataset fields to glossary terms:

#### **Approach 1: Legacy SKOS Approach** (Simple Fields)

**Mapping Method:**

- Fields reference glossary terms via `skos:exactMatch`
- Glossary terms defined as `skos:Concept` with `skos:prefLabel` and `skos:definition`

**Example:**

```turtle
# Field definition
<http://COUNTERPARTY_MASTER/Counterparty_Master#legal_name> a schema:PropertyValue ;
    schema:name "LEGAL_NM" ;
    schema:description "Legal name of the counterparty entity" ;
    skos:exactMatch counterparty:Legal_Name .

# Glossary term definition
counterparty:Legal_Name a skos:Concept ;
    skos:prefLabel "Legal Name" ;
    skos:definition "Full legal name of the counterparty entity" .
```

**Result:** Field `LEGAL_NM` maps to glossary term `Legal_Name`

#### **Approach 2: Modern SHACL Approach** (Complex Fields)

**Mapping Method:**

- Fields reference glossary terms via `sh:class` in `sh:PropertyShape`
- Glossary terms defined as `skos:Concept` with `skos:prefLabel` and `skos:definition`

**Example:**

```turtle
# Field definition
accounts:accountIdProperty a sh:PropertyShape ;
    sh:path accounts:accountId ;
    sh:class accounts:Account_ID ;
    sh:datatype xsd:string ;
    sh:maxLength 20 ;
    sh:name "Account ID" ;
    sh:description "Unique identifier for the account" .

# Glossary term definition
accounts:Account_ID a skos:Concept ;
    skos:prefLabel "Account ID" ;
    skos:definition "Unique identifier for a financial account" .
```

**Result:** Field `Account ID` maps to glossary term `Account_ID`

**When to Use Each Approach:**

- **SKOS Approach**: Simple fields, basic descriptions, no validation requirements
- **SHACL Approach**: Complex fields, validation rules, constraints, business logic

### Property Mappings

#### Glossary Terms

```turtle
ex:CustomerName a skos:Concept ;
    skos:prefLabel "Customer Name"@en ;
    skos:definition "The legal name of a customer entity" ;
    skos:broader ex:CustomerData ;
    skos:related ex:CustomerID ;
    skos:exactMatch fibo:CustomerName ;
    skos:closeMatch ex:ClientName ;
    owl:sameAs <http://schema.org/name> .
```

**Maps to DataHub GlossaryTerm:**

- `skos:prefLabel` → `name` (display name)
- `skos:definition` → `description` (term definition)
- `skos:broader` → `parentNodes` (hierarchical relationships)
- `skos:related` → `relatedTerms` (associative relationships)
- `skos:exactMatch` → `externalReferences` (exact external mappings)
- `skos:closeMatch` → `relatedTerms` (similar terms)
- `owl:sameAs` → `externalReferences` (identity relationships)

#### Glossary Nodes

```turtle
ex:CustomerData a skos:ConceptScheme ;
    skos:prefLabel "Customer Data"@en ;
    skos:definition "Data related to customer entities" ;
    skos:broader ex:DataClassification ;
    skos:narrower ex:CustomerName ;
    skos:narrower ex:CustomerID .
```

**Maps to DataHub GlossaryNode:**

- `skos:prefLabel` → `name` (node display name)
- `skos:definition` → `description` (node description)
- `skos:broader` → `parentNodes` (hierarchical structure)
- `skos:narrower` → child terms (inferred from broader relationships)

### Relationship Mapping

**Hierarchical Relationships:**

- `skos:broader` → Parent hierarchy (broader term)
- `skos:narrower` → Child hierarchy (narrower term)
- `skos:broadMatch` → Parent hierarchy (broader match)
- `skos:narrowMatch` → Child hierarchy (narrower match)

**Associative Relationships:**

- `skos:related` → Related terms (associative)
- `skos:closeMatch` → Related terms (similar concepts)

**External References:**

- `skos:exactMatch` → External references (exact matches)
- `owl:sameAs` → External references (identity relationships)

**Custom Properties:**

- Custom relationship properties → Related terms (domain-specific)
- Custom external properties → External references (domain-specific)

### IRI-to-URN Transformation

#### HTTP/HTTPS IRIs

```
Input:  http://example.com/finance/credit-risk
Output: urn:li:glossaryTerm:(finance,credit-risk)

Input:  https://bank.com/regulatory/capital-adequacy
Output: urn:li:glossaryTerm:(regulatory,capital-adequacy)

Input:  http://example.com/domain/subdomain/concept/subconcept
Output: urn:li:glossaryTerm:(domain,subdomain,concept,subconcept)
```

#### Custom Schemes

```
Input:  fibo:FinancialInstrument
Output: fibo:FinancialInstrument (preserved as-is)

Input:  myorg:CustomerData
Output: myorg:CustomerData (preserved as-is)

Input:  trading:term/Customer_Name
Output: trading:term/Customer_Name (preserved as-is)
```

#### Fragment-based IRIs

```
Input:  http://example.com/glossary#CustomerName
Output: urn:li:glossaryTerm:(glossary,CustomerName)

Input:  https://bank.com/terms#CreditRisk
Output: urn:li:glossaryTerm:(terms,CreditRisk)

Input:  http://example.com/ontology#FinancialInstrument
Output: urn:li:glossaryTerm:(ontology,FinancialInstrument)
```

## Relationship Mapping

### Core Relationship Types

| RDF Property       | DataHub Relationship | Description                  |
| ------------------ | -------------------- | ---------------------------- |
| `skos:broader`     | Parent Hierarchy     | Broader term relationships   |
| `skos:narrower`    | Child Hierarchy      | Narrower term relationships  |
| `skos:related`     | Related Terms        | Associative relationships    |
| `skos:exactMatch`  | External Reference   | Exact term matches           |
| `skos:closeMatch`  | Related Terms        | Similar term matches         |
| `skos:broadMatch`  | Parent Hierarchy     | Broader match relationships  |
| `skos:narrowMatch` | Child Hierarchy      | Narrower match relationships |
| `owl:sameAs`       | External Reference   | Identity relationships       |

### Property Mappings

#### Hierarchical Relationships

```turtle
ex:CustomerData skos:broader ex:PersonalData ;
    skos:narrower ex:CustomerName ;
    skos:narrower ex:CustomerID ;
    skos:broadMatch ex:ClientData ;
    skos:narrowMatch ex:CustomerProfile .
```

**Maps to DataHub Relationships:**

- `skos:broader` → `parentNodes` (parent relationships)
- `skos:narrower` → child terms (child relationships)
- `skos:broadMatch` → `parentNodes` (broader match relationships)
- `skos:narrowMatch` → child terms (narrower match relationships)

#### Associative Relationships

```turtle
ex:CustomerName skos:related ex:CustomerID ;
    skos:related ex:CustomerAddress ;
    skos:closeMatch ex:ClientName ;
    skos:closeMatch ex:AccountHolderName .
```

**Maps to DataHub Relationships:**

- `skos:related` → `relatedTerms` (associative relationships)
- `skos:closeMatch` → `relatedTerms` (similar terms)

#### External References

```turtle
ex:CustomerName skos:exactMatch fibo:CustomerName ;
    owl:sameAs <http://schema.org/name> ;
    owl:sameAs <http://www.w3.org/2006/vcard/ns#fn> .
```

**Maps to DataHub Relationships:**

- `skos:exactMatch` → `externalReferences` (exact matches)
- `owl:sameAs` → `externalReferences` (identity relationships)

## Custom Property Handling

### Additional Properties

```turtle
ex:CustomerName a skos:Concept ;
    skos:prefLabel "Customer Name" ;
    skos:definition "The legal name of a customer entity" ;
    rdfs:comment "This term represents the primary identifier for customer entities" ;
    dcterms:source "Internal Business Glossary v2.1" ;
    dcterms:created "2023-01-15"^^xsd:date ;
    dcterms:modified "2023-06-20"^^xsd:date ;
    skos:scopeNote "Applies to all customer types including individuals and organizations" .
```

**Maps to DataHub Properties:**

- `rdfs:comment` → additional description text
- `dcterms:source` → provenance information
- `dcterms:created` → creation timestamp
- `dcterms:modified` → modification timestamp
- `skos:scopeNote` → usage notes

## Technical Implementation Details

### URN Generation Algorithm

1. **Parse IRI**: Extract scheme, authority, path, and fragment
2. **Scheme Handling**:
   - HTTP/HTTPS: Convert to DataHub URN format using path hierarchy
   - Custom schemes: Preserve as-is for ontology-specific schemes
3. **Path Processing**: Split path into hierarchical components
4. **Fragment Handling**: Use fragment as final component if present
5. **URN Construction**: Build DataHub-compliant URN with proper escaping

### Hierarchy Processing

#### Automatic Parent Creation

```turtle
ex:CustomerName skos:broader ex:CustomerData .
ex:CustomerData skos:broader ex:PersonalData .
ex:PersonalData skos:broader ex:DataClassification .
```

**Creates DataHub Hierarchy:**

- `urn:li:glossaryNode:DataClassification`
- `urn:li:glossaryNode:(DataClassification,PersonalData)`
- `urn:li:glossaryNode:(DataClassification,PersonalData,CustomerData)`
- `urn:li:glossaryTerm:(DataClassification,PersonalData,CustomerData,CustomerName)`

#### Bidirectional Relationships

- Parent-child relationships are created bidirectionally
- `skos:broader` creates both parent and child links
- `skos:narrower` is inferred from broader relationships

### Validation Rules

#### Term Identification Validation

- **Label Validation**: Must have `rdfs:label` OR `skos:prefLabel` (≥3 characters)
- **Type Validation**: Must be `owl:Class`, `owl:NamedIndividual`, `skos:Concept`, or custom class instance
- **Exclusion Validation**: Must NOT be `owl:Ontology` declaration
- **URI Validation**: Must be valid URI reference (not blank node)

#### IRI Validation

- Must have valid scheme (http, https, or custom)
- Path components must be valid identifiers
- Fragment must be valid identifier (if present)
- Custom schemes must follow naming conventions

#### Property Validation

- Required properties must be present (`skos:prefLabel` OR `rdfs:label`)
- Property values must be non-empty strings
- Relationships must reference valid entities
- Language tags are preserved for multilingual support

#### Hierarchy Validation

- No circular references in broader relationships
- Consistent naming conventions across hierarchy
- Logical hierarchy depth (max 5 levels recommended)
- Proper escaping of special characters in URNs

#### Definition Validation

- Must have `skos:definition` OR `rdfs:comment`
- Definition must be non-empty string
- Multiple definitions are supported (first one used)

### Error Handling

#### IRI Parsing Errors

- Invalid scheme format
- Malformed path structure
- Invalid fragment syntax
- Unsupported IRI patterns

#### Mapping Errors

- Missing required properties (`skos:prefLabel`)
- Invalid property values (empty strings)
- Broken relationship references
- Invalid language tag formats

#### DataHub API Errors

- Authentication failures
- Rate limiting
- Entity creation failures
- Relationship creation failures

## Best Practices

#### IRI Design

1. Use hierarchical paths: `/domain/subdomain/concept`
2. Avoid deep nesting (>5 levels)
3. Use consistent naming conventions
4. Include meaningful fragments
5. Use lowercase with hyphens for path components

#### Term Structure

1. Clear, descriptive `skos:prefLabel`
2. Comprehensive `skos:definition`
3. Logical `skos:broader` relationships
4. Consistent terminology across concepts
5. Include language tags for multilingual support

#### Hierarchy Design

1. Start with broad categories
2. Create logical subdivisions
3. Avoid circular references
4. Maintain consistent depth
5. Use meaningful node names

#### Relationship Management

1. Use `skos:exactMatch` for true equivalences
2. Use `skos:closeMatch` for similar concepts
3. Use `skos:related` for associative relationships
4. Use `owl:sameAs` for external identity
5. Maintain bidirectional consistency
