# Glossary Term Specification

**Part of**: [RDF Specification](../../docs/rdf-specification.md)

This document specifies how RDF glossary terms are extracted, converted, and mapped to DataHub glossary entities.

## Overview

The primary goal of RDF is to create comprehensive business glossaries that define terms and their relationships. These terms are then referenced by datasets to provide semantic meaning to data fields.

## Term Definitions

Business terms are defined using SKOS (Simple Knowledge Organization System) concepts, providing rich semantic metadata and relationships.

**RDF Type**: `skos:Concept`

**Required Properties**:

- `skos:prefLabel` OR `rdfs:label` - Human-readable term name (≥3 characters)
- `skos:definition` OR `rdfs:comment` - Detailed term definition

**Recommended Properties**:

- `skos:altLabel` - Alternative names for the term
- `skos:hiddenLabel` - Hidden labels for search
- `skos:notation` - Code or identifier for the term
- `skos:scopeNote` - Additional context or usage notes

**Example**:

```turtle
accounts:Customer_ID a skos:Concept ;
    skos:prefLabel "Customer Identifier" ;
    skos:definition "Unique identifier assigned to customer accounts for tracking and reference purposes" ;
    skos:notation "CUST_ID" ;
    skos:scopeNote "Used across all customer-facing systems" .
```

## Term Identification Criteria

The system identifies RDF resources as glossary terms using these criteria:

**Required Conditions**:

- Must have a label: `rdfs:label` OR `skos:prefLabel` (≥3 characters)
- Must be a URI reference (not blank node or literal)
- Must have appropriate RDF type

**Included RDF Types**:

- `owl:Class` - OWL classes
- `owl:NamedIndividual` - OWL named individuals
- `skos:Concept` - SKOS concepts
- **Custom class instances** - Any resource typed as instance of custom class

**Excluded RDF Types**:

- `owl:Ontology` - Ontology declarations (not terms)

## Term Relationships

Terms can have rich semantic relationships using SKOS properties:

**Hierarchical Relationships**:

- `skos:broader` - Parent term (more general)
- `skos:narrower` - Child term (more specific)
- `skos:broadMatch` - Broader match relationship
- `skos:narrowMatch` - Narrower match relationship

**Associative Relationships**:

- `skos:related` - Related terms (associative)
- `skos:closeMatch` - Similar concepts

**External References**:

- `skos:exactMatch` - Exact term matches
- `owl:sameAs` - Identity relationships

**Example**:

```turtle
accounts:Customer_ID a skos:Concept ;
    skos:prefLabel "Customer Identifier" ;
    skos:broader accounts:Customer_Data ;
    skos:related accounts:Account_ID ;
    skos:exactMatch external:CustomerIdentifier .

accounts:Customer_Data a skos:Concept ;
    skos:prefLabel "Customer Data" ;
    skos:narrower accounts:Customer_ID ;
    skos:narrower accounts:Customer_Name .
```

## Domain Hierarchy

Terms are automatically organized into domain hierarchies based on their IRI paths, creating logical groupings for business organization.

**Domain Creation Logic**:

- Uses IRI path segments to create hierarchical domains
- Each segment becomes a domain level
- Terms are assigned to their leaf domain (most specific)

**Example**:

```turtle
# Term with IRI: https://bank.com/finance/accounts/customer_id
# Creates domains: bank.com → finance → accounts
# Term assigned to: urn:li:domain:accounts
```

## IRI-to-URN Conversion

Terms are converted from RDF IRIs to DataHub URNs using consistent patterns:

**HTTP/HTTPS IRIs**:

```
Input:  http://example.com/finance/credit-risk
Output: urn:li:glossaryTerm:(finance,credit-risk)
```

**Custom Schemes**:

```
Input:  fibo:FinancialInstrument
Output: fibo:FinancialInstrument (preserved as-is)
```

**Fragment-based IRIs**:

```
Input:  http://example.com/glossary#CustomerName
Output: urn:li:glossaryTerm:(glossary,CustomerName)
```

## RDF-to-DataHub Mapping Specifications

For testing and verification, every RDF concept must have a precise mapping to DataHub concepts. This section provides the exact specifications for how RDF glossary terms and relationships are interpreted into DataHub.

### Term Entity Mapping

**RDF Term Identification**:

- **Required**: `skos:prefLabel` OR `rdfs:label` (≥3 characters)
- **Required**: Valid URI reference (not blank node or literal)
- **Required**: Appropriate RDF type (`skos:Concept`, `owl:Class`, `owl:NamedIndividual`, or custom class instance)
- **Excluded**: `owl:Ontology` declarations

**DataHub Entity Creation**:

```python
# RDF Term → DataHub GlossaryTerm
term_urn = generate_glossary_term_urn(term_iri)
glossary_term = GlossaryTermClass(
    urn=term_urn,
    name=extract_preferred_label(graph, term_iri),
    description=extract_definition(graph, term_iri),
    definition=extract_definition(graph, term_iri)
)
```

### Property Mapping Specifications

**Core Property Mappings**:

| RDF Property                                                 | DataHub Field                               | Extraction Priority     | Validation Rule                                                                                    |
| ------------------------------------------------------------ | ------------------------------------------- | ----------------------- | -------------------------------------------------------------------------------------------------- |
| `skos:prefLabel`                                             | `name`                                      | 1st priority            | ≥3 characters, non-empty                                                                           |
| `rdfs:label`                                                 | `name`                                      | 2nd priority (fallback) | ≥3 characters, non-empty                                                                           |
| `skos:definition`                                            | `description`                               | 1st priority            | Non-empty string                                                                                   |
| `rdfs:comment`                                               | `description`                               | 2nd priority (fallback) | Non-empty string                                                                                   |
| `skos:notation`                                              | `customProperties`                          | Optional                | String value                                                                                       |
| `skos:scopeNote`                                             | `customProperties`                          | Optional                | String value                                                                                       |
| `skos:altLabel`                                              | `customProperties`                          | Optional                | Array of strings                                                                                   |
| `skos:hiddenLabel`                                           | `customProperties`                          | Optional                | Array of strings                                                                                   |
| `sh:datatype` + `sh:minInclusive` + `sh:maxInclusive` + etc. | `customProperties['shacl:dataConstraints']` | Optional                | Human-readable constraint description (requires dual-typed term: `skos:Concept, sh:PropertyShape`) |

**Property Extraction Algorithm**:

```python
def extract_preferred_label(graph: Graph, uri: URIRef) -> str:
    """Extract term name with priority order."""
    # Priority 1: skos:prefLabel
    pref_label = graph.value(uri, SKOS.prefLabel)
    if pref_label and len(str(pref_label)) >= 3:
        return str(pref_label)

    # Priority 2: rdfs:label
    label = graph.value(uri, RDFS.label)
    if label and len(str(label)) >= 3:
        return str(label)

    raise ValueError(f"No valid label found for {uri}")

def extract_definition(graph: Graph, uri: URIRef) -> Optional[str]:
    """Extract term definition with priority order."""
    # Priority 1: skos:definition
    definition = graph.value(uri, SKOS.definition)
    if definition:
        return str(definition)

    # Priority 2: rdfs:comment
    comment = graph.value(uri, RDFS.comment)
    if comment:
        return str(comment)

    return None
```

### Relationship Mapping Specifications

**Supported Relationship Types**:

This implementation only supports `skos:broader` and `skos:narrower` for term-to-term relationships:

| RDF Property    | DataHub Relationship                                  | Processing Rule       | When to Use                                                                             |
| --------------- | ----------------------------------------------------- | --------------------- | --------------------------------------------------------------------------------------- |
| `skos:broader`  | `isRelatedTerms` (child) + `hasRelatedTerms` (parent) | Bidirectional mapping | Use when term A is a broader concept than term B (e.g., "Animal" is broader than "Dog") |
| `skos:narrower` | Inferred from `broader`                               | Inferred from broader | Use when term A is a narrower concept than term B (inverse of broader)                  |

**DataHub Relationship Mapping**:

| DataHub Field     | UI Display | Semantic Meaning                     | Source                                  |
| ----------------- | ---------- | ------------------------------------ | --------------------------------------- |
| `isRelatedTerms`  | "Inherits" | Child term inherits from parent term | `skos:broader` (child points to parent) |
| `hasRelatedTerms` | "Contains" | Parent term contains child terms     | `skos:broader` (parent has children)    |

**Important Notes**:

- Only `skos:broader` and `skos:narrower` are supported for term-to-term relationships
- `skos:related` and `skos:closeMatch` are **not supported** and will be ignored
- `skos:exactMatch` is **excluded** from term-to-term relationship extraction (only used for field-to-term mappings)
- `skos:broader` creates bidirectional relationships: child → parent via `isRelatedTerms` (inherits), and parent → children via `hasRelatedTerms` (contains)

**External References** (Field-to-Term Only):

| RDF Property      | DataHub Relationship                                   | Processing Rule | When to Use                                                                                                             |
| ----------------- | ------------------------------------------------------ | --------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `skos:exactMatch` | `externalReferences` (for field-to-term mappings only) | Direct mapping  | **Only for field-to-term mappings**, not term-to-term. Use when a dataset field exactly matches a glossary term concept |
| `owl:sameAs`      | `externalReferences`                                   | Direct mapping  | Use when two URIs refer to the exact same concept (identity relationship)                                               |

**Term-to-Term Relationship Processing**:

- Only `skos:broader` and `skos:narrower` are extracted and processed
- `skos:related`, `skos:closeMatch`, and `skos:exactMatch` are **not supported** for term-to-term relationships
- `skos:exactMatch` is reserved exclusively for field-to-term mappings

### IRI-to-URN Conversion Specifications

**Conversion Rules**:

| IRI Pattern                       | Conversion Rule              | DataHub URN Format                    | Example                                                                               |
| --------------------------------- | ---------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------- |
| `http://domain.com/path/term`     | Remove scheme, preserve path | `urn:li:glossaryTerm:(path,term)`     | `http://bank.com/finance/customer_id` → `urn:li:glossaryTerm:(finance,customer_id)`   |
| `https://domain.com/path/term`    | Remove scheme, preserve path | `urn:li:glossaryTerm:(path,term)`     | `https://bank.com/finance/customer_id` → `urn:li:glossaryTerm:(finance,customer_id)`  |
| `custom:term`                     | Preserve as-is               | `custom:term`                         | `fibo:FinancialInstrument` → `fibo:FinancialInstrument`                               |
| `http://domain.com/glossary#term` | Extract fragment, use path   | `urn:li:glossaryTerm:(glossary,term)` | `http://bank.com/glossary#Customer_ID` → `urn:li:glossaryTerm:(glossary,Customer_ID)` |

**Conversion Algorithm**:

```python
def generate_glossary_term_urn(iri: str) -> str:
    """Convert IRI to DataHub glossary term URN with exact rules."""
    parsed = urlparse(iri)

    if parsed.scheme in ['http', 'https']:
        # HTTP/HTTPS: Remove scheme, preserve path
        path = parsed.path.strip('/')
        if parsed.fragment:
            # Fragment-based: use fragment as term name
            return f"urn:li:glossaryTerm:({path},{parsed.fragment})"
        else:
            # Path-based: use last segment as term name
            segments = path.split('/')
            return f"urn:li:glossaryTerm:({','.join(segments)})"

    elif ':' in iri and not iri.startswith('http'):
        # Custom scheme: preserve as-is
        return iri

    else:
        raise ValueError(f"Invalid IRI format: {iri}")
```

### Domain Assignment Specifications

**Domain Creation Rules**:

- Extract parent path segments from term IRI (exclude term name)
- Create domain for each parent segment
- Assign term to leaf domain (most specific parent)

**Domain Assignment Algorithm**:

```python
def assign_term_to_domain(term_iri: str) -> str:
    """Assign term to domain based on IRI path."""
    parsed = urlparse(term_iri)
    path_segments = parsed.path.strip('/').split('/')

    # Remove last segment (term name) to get parent path
    parent_segments = path_segments[:-1]

    if parent_segments:
        domain_path = '/'.join(parent_segments)
        return f"urn:li:domain:{domain_path}"
    else:
        return None  # No domain assignment
```

### Validation Rules

**Term Validation**:

1. **Label Validation**: Must have `skos:prefLabel` OR `rdfs:label` ≥3 characters
2. **Type Validation**: Must be `skos:Concept`, `owl:Class`, `owl:NamedIndividual`, or custom class instance
3. **URI Validation**: Must be valid URI reference (not blank node)
4. **Exclusion Validation**: Must NOT be `owl:Ontology` declaration

**Relationship Validation**:

1. **Target Validation**: All relationship targets must be valid term URIs
2. **Circular Reference Check**: No circular `skos:broader` relationships
3. **URN Generation**: All target URIs must successfully convert to DataHub URNs

**Domain Validation**:

1. **Path Validation**: IRI path segments must be valid identifiers
2. **Hierarchy Validation**: Domain hierarchy must be logical and consistent
3. **Assignment Validation**: Terms must be assigned to appropriate leaf domains

## Term Constraints

Terms can have data constraints defined using SHACL and SKOS patterns for validation and business rules.

### Enum Constraints

**SKOS Collections Approach** (Recommended for Simple Enums):

```turtle
# Define the parent concept
accounts:Counterparty_Type a skos:Concept ;
    skos:prefLabel "Counterparty Type" ;
    skos:definition "The classification of a counterparty." .

# Define individual enum values
accounts:Bank a skos:Concept ;
    skos:prefLabel "Bank" ;
    skos:definition "A financial institution." ;
    skos:memberOf accounts:Counterparty_Type_Collection .

accounts:Corporate a skos:Concept ;
    skos:prefLabel "Corporate" ;
    skos:definition "A corporation." ;
    skos:memberOf accounts:Counterparty_Type_Collection .

# Define the collection
accounts:Counterparty_Type_Collection a skos:Collection ;
    skos:prefLabel "Counterparty Type Collection" ;
    skos:definition "Valid counterparty types for validation." .
```

**OWL Enumeration Pattern** (For Complex Enums with Ordering):

```turtle
# Define the enumeration type
ex:Priority a owl:Class ;
    rdfs:label "Priority"@en ;
    owl:equivalentClass [
        a owl:Class ;
        owl:oneOf (ex:Low ex:Medium ex:High ex:Critical)
    ] .

# Define enumeration members with ordering
ex:Low a owl:NamedIndividual , ex:Priority ;
    skos:notation "LOW" ;
    skos:prefLabel "Low"@en ;
    rdf:value 0 ;
    skos:definition "Low priority items should be addressed after higher priority items"@en .
```

### Data Type Constraints

Terms can specify data type constraints for validation. **Important**: Constraints are only extracted from terms that are dual-typed as both `skos:Concept` and `sh:PropertyShape` (see Hybrid Term-Constraint Pattern below).

```turtle
accounts:Risk_Weight a skos:Concept, sh:PropertyShape ;
    skos:prefLabel "Risk Weight" ;
    skos:definition "Risk weight percentage for capital adequacy." ;
    sh:datatype xsd:decimal ;
    sh:pattern "^\\d{1,3}\\.\\d{2}$" ;  # DECIMAL(5,2) precision
    sh:minInclusive 0.00 ;
    sh:maxInclusive 100.00 .
```

**Constraint Storage**:

- Extracted SHACL constraints are stored as a `shacl:dataConstraints` custom property on the glossary term
- The constraint description is a human-readable string combining all constraint types (datatype, min/max, length, pattern)
- Format: `"{term_name} must be {datatype}, between {min} and {max}"` or similar descriptive text
- Example: `"Risk Weight must be decimal, between 0.00 and 100.00"`

**Supported Constraint Types**:

- `sh:datatype` - Data type (string, integer, decimal, date, boolean)
- `sh:minInclusive` / `sh:maxInclusive` - Numeric range constraints
- `sh:minLength` / `sh:maxLength` - String length constraints
- `sh:pattern` - Regular expression pattern validation

## Hybrid Term-Constraint Pattern

The hybrid pattern combines SKOS concepts with SHACL PropertyShapes to create complete semantic definitions with embedded constraints. This approach aligns with the principle of "single source of truth" while allowing for domain-specific variations through constraint narrowing.

### When to Use the Combined Pattern

Use the combined `skos:Concept, sh:PropertyShape` pattern for **invariant business concepts** with standardized constraints that are unlikely to change across domains or contexts.

**Ideal Candidates**:

- Industry-standard identifiers (CUSIP, ISIN, LEI)
- Regulatory-defined concepts (Entity Identifier, Risk Weight)
- Fixed-format business identifiers (Account ID, Counterparty ID)
- Universal business rules embedded in concept definitions

**Example - Invariant Identifier (CUSIP)**:

```turtle
security:CUSIP a skos:Concept, sh:PropertyShape ;
    skos:prefLabel "CUSIP" ;
    skos:definition "Committee on Uniform Securities Identification Procedures - 9 character alphanumeric code" ;
    sh:path security:cusip ;
    sh:datatype xsd:string ;
    sh:pattern "^[0-9]{3}[0-9A-Z]{5}[0-9]$" ;
    sh:maxLength 9 ;
    sh:minLength 9 ;
    sh:name "CUSIP" ;
    sh:description "Committee on Uniform Securities Identification Procedures number" ;
    ex:sqlType "VARCHAR(9)" .
```

**Key Characteristics**:

- Single definition combining semantic meaning and validation rules
- No `sh:class` self-reference needed (the concept _is_ the PropertyShape)
- All SKOS properties for semantic richness (prefLabel, definition)
- All SHACL properties for validation (datatype, pattern, constraints)

### When to Use Constraint Narrowing

Use constraint narrowing with `skos:broader` for **domain-specific variations** where the core business concept has different constraints depending on context, product type, or regulatory requirements.

**Ideal Candidates**:

- Concepts with regulatory variations by product (LTV ratios, interest rates)
- Business rules that differ by domain (credit limits, pricing rules)
- Constraints that are context-dependent but semantically related

**Example - Constraint Narrowing (Loan-to-Value)**:

**Core Business Concept** (finance.ttl):

```turtle
fin:Loan_To_Value a skos:Concept, sh:PropertyShape ;
    skos:prefLabel "Loan-to-Value Ratio" ;
    skos:definition "Ratio of loan amount to collateral value. Business rule allows 0-200% to accommodate over-collateralized loans." ;
    sh:path fin:loanToValue ;
    sh:datatype xsd:decimal ;
    sh:minInclusive 0.00 ;          # Core business truth: 0-200%
    sh:maxInclusive 200.00 ;
    sh:pattern "^\\d{1,3}\\.\\d{2}$" ;
    sh:name "Loan-to-Value Ratio" ;
    sh:description "Ratio of loan amount to collateral value, expressed as percentage" ;
    ex:sqlType "DECIMAL(5,2)" .
```

**Domain-Specific Narrowing - Commercial Lending** (commercial_lending.ttl):

```turtle
commercial:Loan_To_Value a skos:Concept, sh:PropertyShape ;
    skos:prefLabel "Commercial Loan LTV" ;
    skos:definition "Loan-to-Value ratio for commercial loans. Regulatory limits typically 60-80%." ;
    skos:broader fin:Loan_To_Value ;  # ← Inherits from core concept
    sh:path commercial:loanToValue ;
    sh:datatype xsd:decimal ;
    sh:minInclusive 60.00 ;         # ← Narrowed: 60-80%
    sh:maxInclusive 80.00 ;
    sh:pattern "^\\d{1,3}\\.\\d{2}$" ;  # ← Must redeclare all constraints
    sh:name "Commercial Loan LTV" ;
    sh:description "Loan-to-Value ratio for commercial loans (typically 60-80% per regulatory limits)" ;
    ex:sqlType "DECIMAL(5,2)" .
```

**Key Characteristics**:

- `skos:broader` links to the core concept (semantic inheritance)
- **All SHACL constraints must be explicitly redefined** (no automatic SHACL inheritance)
- Narrowed concepts override specific constraints (min/max ranges)
- Pattern and datatype constraints are typically preserved but must be restated

### SHACL Inheritance Limitations

**Important**: SHACL does not automatically inherit properties from `sh:class` references. When creating narrowed concepts:

1. **Must Redeclare**: `sh:datatype`, `sh:pattern`, all min/max constraints
2. **Cannot Rely On**: Automatic inheritance from broader concept's SHACL properties
3. **Best Practice**: Copy all SHACL properties from broader concept, then modify only what needs to narrow

### Benefits of the Hybrid Approach

**Single Source of Truth**:

- Core business concepts define the "truth" (e.g., LTV can be 0-200%)
- Constraints are embedded directly in the concept definition
- No separation between semantic meaning and technical validation

**Domain Flexibility**:

- Narrowed concepts allow practical business rules (e.g., 60-80% for commercial loans)
- `skos:broader` provides clear traceability to the core truth
- Supports regulatory variations without duplicating semantic definitions

**Semantic Completeness**:

- SKOS properties provide rich business context (prefLabel, definition, broader)
- SHACL properties provide technical validation (datatype, pattern, constraints)
- Combined approach eliminates redundancy between separate term and PropertyShape definitions

**Traceability**:

- `skos:broader` relationships show inheritance hierarchy
- DataHub can visualize relationships between core and narrowed concepts
- Clear distinction between business truth and domain-specific reality

### Decision Matrix

| Scenario                                 | Recommended Approach | Example                                                          |
| ---------------------------------------- | -------------------- | ---------------------------------------------------------------- |
| Industry standard format (never changes) | Combined Pattern     | CUSIP (always 9 chars), ISIN (always 12 chars)                   |
| Regulatory identifier (fixed format)     | Combined Pattern     | Entity Identifier (10 digits), LEI (20 chars)                    |
| Core business concept (universal)        | Combined Pattern     | Account ID, Counterparty ID, Security ID                         |
| Context-dependent constraints            | Constraint Narrowing | LTV (varies by loan type), Interest Rate (varies by product)     |
| Domain-specific business rules           | Constraint Narrowing | Credit Limit (varies by customer type), Pricing (varies by tier) |
| Concept with multiple valid ranges       | Constraint Narrowing | Risk Weight (0-100% core, narrowed by asset class)               |
