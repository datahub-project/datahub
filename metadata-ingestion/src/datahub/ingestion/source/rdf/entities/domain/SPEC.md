# Domain Specification

**Part of**: [RDF Specification](../../docs/rdf-specification.md)

This document specifies how DataHub domains are constructed from entity IRI paths.

## Overview

Domains are **not extracted from RDF graphs**. Instead, they are **constructed** from the IRI path segments of glossary terms. Domains provide hierarchical organization for business entities.

**Important**: Domains are **not registered entities** (no `ENTITY_METADATA`). They are built by the `DomainBuilder` class from existing entities.

## Domain Construction Logic

### Path Segment Extraction

Domains are created from the **parent path segments** of entity IRIs:

1. **Extract IRI path**: Parse entity IRI to get path segments
2. **Remove entity name**: Exclude the last segment (entity name itself)
3. **Create domain hierarchy**: Each parent segment becomes a domain level
4. **Assign entities**: Entities are assigned to their immediate parent domain (leaf domain)

### Example

**Entity IRI**: `https://bank.com/finance/accounts/customer_id`

**Path Segments**: `['bank.com', 'finance', 'accounts', 'customer_id']`

**Parent Segments** (for domain creation): `['bank.com', 'finance', 'accounts']`

**Domains Created**:

- `bank.com` (root domain)
- `finance` (child of `bank.com`)
- `accounts` (child of `finance`, leaf domain)

**Entity Assignment**: Term assigned to `accounts` domain (most specific parent)

## Domain Hierarchy

Domains form a hierarchical tree structure:

```
bank.com (root)
  └── finance
      └── accounts (leaf - contains entities)
```

### Parent-Child Relationships

- Each domain has an optional `parent_domain_urn`
- Root domains have no parent
- Child domains reference their parent via `parent_domain_urn`

## Domain Creation Rules

### Domains with Glossary Terms

**Rule**: Domains that have **glossary terms** in their hierarchy are created.

- Domains are created when they contain glossary terms
- Domains provide hierarchical organization for business vocabulary

### Entity Assignment

Entities are assigned to their **immediate parent domain** (leaf domain):

- **Glossary Terms**: Assigned to the domain corresponding to their parent path

**Example**:

- Term: `https://bank.com/finance/accounts/customer_id` → Assigned to `accounts` domain

## URN Generation

Domain URNs are generated from path segments:

**Format**: `urn:li:domain:({path_segments})`

**Example**:

- Path: `('bank.com', 'finance', 'accounts')`
- URN: `urn:li:domain:(bank.com,finance,accounts)`

### Path Segment Tuple

Path segments are represented as tuples:

- `('bank.com',)` - Root domain
- `('bank.com', 'finance')` - Second-level domain
- `('bank.com', 'finance', 'accounts')` - Third-level domain (leaf)

## Domain Properties

### Required Properties

- **URN**: Generated from path segments
- **Name**: Last segment of the path (e.g., `"accounts"`)

### Optional Properties

- **Parent Domain URN**: Reference to parent domain (if not root)
- **Description**: Can be set from domain metadata if available
- **Glossary Terms**: List of terms assigned to this domain

## DataHub Integration

### Domain MCP Creation

Domains are created via DataHub MCPs:

1. **Domain Properties MCP**: Creates the domain entity with name, description
2. **Domain Hierarchy MCP**: Establishes parent-child relationships
3. **Domain Hierarchy MCP**: Establishes parent-child relationships

## Example

**Input Entities**:

- Term: `https://bank.com/finance/accounts/customer_id`

**Domains Created**:

```python
DataHubDomain(
    urn="urn:li:domain:(bank.com,finance,accounts)",
    name="accounts",
    parent_domain_urn="urn:li:domain:(bank.com,finance)",
    glossary_terms=[...],  # customer_id term
)

DataHubDomain(
    urn="urn:li:domain:(bank.com,finance)",
    name="finance",
    parent_domain_urn="urn:li:domain:(bank.com)",
    glossary_terms=[],
)

DataHubDomain(
    urn="urn:li:domain:(bank.com)",
    name="bank.com",
    parent_domain_urn=None,  # Root domain
    glossary_terms=[],
)
```

## Limitations

1. **No RDF Extraction**: Domains are not extracted from RDF - they are constructed
2. **Glossary Term Requirement**: Domains without glossary terms are not created
3. **Path-Based Only**: Domain structure is derived solely from IRI paths
4. **No Explicit Domain Definitions**: RDF does not contain explicit domain definitions - they are inferred

## Relationship to Other Entities

- **Glossary Terms**: Provide path segments for domain construction and determine which domains are created
