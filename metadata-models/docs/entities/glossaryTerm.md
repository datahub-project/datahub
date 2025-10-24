# GlossaryTerm

A GlossaryTerm represents a standardized business definition or vocabulary term that can be associated with data assets across your organization. GlossaryTerms are the fundamental building blocks of DataHub's Business Glossary feature, enabling teams to establish and maintain a shared vocabulary for describing data concepts.

In practice, GlossaryTerms allow you to:

- Define business terminology with clear, authoritative definitions
- Create relationships between related business concepts (inheritance, containment, etc.)
- Tag data assets (datasets, dashboards, charts, etc.) with standardized business terms
- Establish governance and ownership over business vocabulary
- Link to external resources and documentation

For example, a GlossaryTerm might define "Customer Lifetime Value (CLV)" with a precise business definition, relate it to other terms like "Revenue" and "Customer", and be applied to specific dataset columns that store CLV calculations.

## Identity

GlossaryTerms are uniquely identified by a single field: their **name**. This name serves as the persistent identifier for the term throughout its lifecycle.

### URN Structure

The URN (Uniform Resource Name) for a GlossaryTerm follows this pattern:

```
urn:li:glossaryTerm:<term_name>
```

Where:

- `<term_name>`: A unique string identifier for the term. This can be human-readable (e.g., "CustomerLifetimeValue") or a generated ID (e.g., "clv-001" or a UUID).

### Examples

```
# Simple term name
urn:li:glossaryTerm:Revenue

# Hierarchical naming convention (common pattern)
urn:li:glossaryTerm:Finance.Revenue
urn:li:glossaryTerm:Classification.PII
urn:li:glossaryTerm:Classification.Confidential

# UUID-based identifier
urn:li:glossaryTerm:41516e31-0acb-fd90-76ff-fc2c98d2d1a3

# Descriptive identifier
urn:li:glossaryTerm:CustomerLifetimeValue
```

### Best Practices for Term Names

1. **Use hierarchical notation**: Prefix terms with their category (e.g., `Classification.PII`, `Finance.Revenue`) to indicate structure even though the name is flat.
2. **Be consistent**: Choose a naming convention (camelCase, dot notation, etc.) and apply it uniformly.
3. **Keep it permanent**: The term name is the identifier and should not change. Use the `name` field in `glossaryTermInfo` for the display name.
4. **Consider organization**: While the URN is flat, you can use glossaryNodes (term groups) to create hierarchical organization in the UI.

## Important Capabilities

### Core Business Definition (glossaryTermInfo)

The `glossaryTermInfo` aspect contains the essential business information about a term:

- **definition** (required): The authoritative business definition of the term. This should be clear, concise, and provide sufficient context for anyone to understand the term's meaning.
- **name**: The display name shown in the UI. This can be more human-friendly than the URN identifier (e.g., "Customer Lifetime Value" vs. "CustomerLifetimeValue").
- **parentNode**: A reference to a GlossaryNode (term group) that acts as a folder for organizing terms hierarchically.
- **termSource**: Indicates whether the term is "INTERNAL" (defined within your organization) or "EXTERNAL" (from an external standard like FIBO).
- **sourceRef**: A reference identifier for external term sources (e.g., "FIBO" for Financial Industry Business Ontology).
- **sourceUrl**: A URL pointing to the external definition of the term.
- **customProperties**: Key-value pairs for additional metadata specific to your organization.

Example:

```python
{
  "name": "Customer Lifetime Value",
  "definition": "The total revenue a business can expect from a single customer account throughout the business relationship.",
  "termSource": "INTERNAL",
  "parentNode": "urn:li:glossaryNode:Finance"
}
```

### Term Relationships (glossaryRelatedTerms)

GlossaryTerms support several relationship types that help model the semantic connections between business concepts:

#### 1. IsA Relationships (Inheritance)

Indicates that one term is a specialized type of another term. This creates an "Is-A" hierarchy where more specific terms inherit the characteristics of broader terms.

**Use case**: `Email` IsA `PersonalInformation`, `SocialSecurityNumber` IsA `PersonalInformation`

#### 2. HasA Relationships (Containment)

Indicates that one term contains or is composed of another term. This creates a "Has-A" relationship where a complex concept consists of simpler parts.

**Use case**: `Address` HasA `ZipCode`, `Address` HasA `Street`, `Address` HasA `City`

#### 3. Values Relationships

Defines the allowed values for an enumerated term. Useful for controlled vocabularies where a term has a fixed set of valid values.

**Use case**: `ColorEnum` HasValues `Red`, `Green`, `Blue`

#### 4. RelatedTo Relationships

General-purpose relationship for terms that are semantically related but don't fit the other categories.

**Use case**: `Revenue` RelatedTo `Profit`, `Customer` RelatedTo `Account`

### Hierarchical Organization

GlossaryTerms can be organized hierarchically through **GlossaryNodes** (term groups). The `parentNode` field in `glossaryTermInfo` establishes this relationship:

```
GlossaryNode: Classification
  ├── GlossaryTerm: Sensitive
  ├── GlossaryTerm: Confidential
  └── GlossaryTerm: HighlyConfidential

GlossaryNode: PersonalInformation
  ├── GlossaryTerm: Email
  ├── GlossaryTerm: Address
  └── GlossaryTerm: PhoneNumber
```

This hierarchy is visible in the DataHub UI and helps users navigate large glossaries.

### Applying Terms to Data Assets

GlossaryTerms become valuable when applied to actual data assets. Terms can be attached to:

- Datasets (tables, views, files)
- Dataset fields (columns)
- Dashboards
- Charts
- Data Jobs
- Containers
- And many other entity types

When a term is applied to a data asset, it creates a **TermedWith** relationship, which enables:

- Discovery: Find all assets tagged with a specific business concept
- Governance: Track which assets contain sensitive data types
- Documentation: Provide business context for technical assets
- Compliance: Identify datasets subject to regulatory requirements

## Code Examples

### Creating a GlossaryTerm

<details>
<summary>Python SDK: Create a basic GlossaryTerm</summary>

```python
{{ inline /metadata-ingestion/examples/library/glossary_term_create.py show_path_as_comment }}
```

</details>

<details>
<summary>Python SDK: Create a GlossaryTerm with full metadata</summary>

```python
{{ inline /metadata-ingestion/examples/library/glossary_term_create_with_metadata.py show_path_as_comment }}
```

</details>

### Managing Term Relationships

<details>
<summary>Python SDK: Add relationships between GlossaryTerms</summary>

```python
{{ inline /metadata-ingestion/examples/library/glossary_term_add_relationships.py show_path_as_comment }}
```

</details>

### Applying Terms to Assets

<details>
<summary>Python SDK: Add a GlossaryTerm to a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_term.py show_path_as_comment }}
```

</details>

<details>
<summary>Python SDK: Add a GlossaryTerm to a dataset column</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_column_term.py show_path_as_comment }}
```

</details>

### Querying GlossaryTerms

<details>
<summary>REST API: Get a GlossaryTerm by URN</summary>

```bash
# Fetch a GlossaryTerm entity
curl -X GET 'http://localhost:8080/entities/urn%3Ali%3AglossaryTerm%3ACustomerLifetimeValue' \
  -H 'Authorization: Bearer <token>'

# Response includes all aspects:
# - glossaryTermKey (identity)
# - glossaryTermInfo (definition, name, etc.)
# - glossaryRelatedTerms (relationships)
# - ownership (who owns this term)
# - institutionalMemory (links to documentation)
# - etc.
```

</details>

<details>
<summary>REST API: Search for assets tagged with a term</summary>

```bash
# Find all datasets tagged with a specific term
curl -X POST 'http://localhost:8080/entities?action=search' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
    "entity": "dataset",
    "input": "*",
    "filter": {
      "or": [
        {
          "and": [
            {
              "field": "glossaryTerms",
              "value": "urn:li:glossaryTerm:Classification.PII",
              "condition": "EQUAL"
            }
          ]
        }
      ]
    },
    "start": 0,
    "count": 10
  }'
```

</details>

<details>
<summary>Python SDK: Query terms applied to a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_query_terms.py show_path_as_comment }}
```

</details>

### Bulk Operations

<details>
<summary>YAML Ingestion: Create multiple terms from a Business Glossary file</summary>

```yaml
# business_glossary.yml
version: "1"
source: MyOrganization
owners:
  users:
    - datahub
nodes:
  - name: Classification
    description: Data classification categories
    terms:
      - name: PII
        description: Personally Identifiable Information
      - name: Confidential
        description: Confidential business data
      - name: Public
        description: Publicly available data

  - name: Finance
    description: Financial domain terms
    terms:
      - name: Revenue
        description: Total income from business operations
      - name: Profit
        description: Financial gain after expenses
        related_terms:
          - Finance.Revenue
# Ingest using the DataHub CLI:
# datahub ingest -c business_glossary.yml
```

See the [Business Glossary Source](../../../generated/ingestion/sources/business-glossary.md) documentation for the full YAML format specification.

</details>

## Integration Points

### Relationship with GlossaryNode

GlossaryNodes (term groups) provide hierarchical organization for GlossaryTerms. Think of GlossaryNodes as folders and GlossaryTerms as files within those folders.

- A GlossaryTerm can have at most one parent GlossaryNode (specified via `parentNode` in `glossaryTermInfo`)
- GlossaryNodes can contain both GlossaryTerms and other GlossaryNodes (creating nested hierarchies)
- Terms at the root level (no parent) appear at the top of the glossary

### Application to Data Assets

GlossaryTerms can be applied to most entity types in DataHub through the `glossaryTerms` aspect:

**Supported entities:**

- dataset, schemaField (dataset columns)
- dashboard, chart
- dataJob, dataFlow
- mlModel, mlFeature, mlFeatureTable, mlPrimaryKey
- notebook
- container
- dataProduct, application
- erModelRelationship, businessAttribute

When you apply a term to an entity, DataHub creates:

1. A `glossaryTerms` aspect on the target entity containing the term association
2. A **TermedWith** relationship edge in the graph
3. A searchable index entry allowing you to find all assets with that term

### GraphQL API

The GraphQL API provides rich querying and mutation capabilities for GlossaryTerms:

**Queries:**

- Fetch term details with related entities
- Browse terms hierarchically
- Search terms by name or definition
- Get all entities tagged with a term

**Mutations:**

- `createGlossaryTerm`: Create a new term
- `addTerms`, `addTerm`: Apply terms to entities
- `removeTerm`, `batchRemoveTerms`: Remove terms from entities
- `updateParentNode`: Move a term to a different parent group

See the [GraphQL API documentation](../../../api/graphql/overview.md) for detailed examples.

### Integration with Search and Discovery

GlossaryTerms enhance discoverability in multiple ways:

1. **Faceted Search**: Users can filter search results by glossary terms
2. **Term Propagation**: When a term is applied at the dataset level, it can be inherited by downstream assets
3. **Related Entities**: The term's page shows all assets tagged with that term
4. **Autocomplete**: Terms are suggested as users type in search or when tagging assets

### Governance and Access Control

GlossaryTerms support fine-grained access control through DataHub's policy system:

- **Manage Direct Glossary Children**: Permission to create/edit/delete terms directly under a specific term group
- **Manage All Glossary Children**: Permission to manage any term within a term group's entire subtree
- Standard entity policies (view, edit, delete) apply to individual terms

See the [Business Glossary documentation](../../../glossary/business-glossary.md) for details on privileges.

## Notable Exceptions

### Term Name vs Display Name

The URN identifier (`name` in `glossaryTermKey`) is separate from the display name (`name` in `glossaryTermInfo`). Best practice:

- **URN name**: Use a stable, unchanging identifier (e.g., "clv-001", "Classification.PII")
- **Display name**: Use a human-friendly label that can be updated (e.g., "Customer Lifetime Value", "Personally Identifiable Information")

### External Term Sources

When using terms from external standards (FIBO, ISO, industry glossaries):

- Set `termSource` to "EXTERNAL"
- Populate `sourceRef` with the standard name (e.g., "FIBO")
- Include `sourceUrl` linking to the authoritative definition
- Consider using the external standard's identifier as your URN name for consistency

### Term Relationships vs Hierarchy

Don't confuse:

- **Parent-child hierarchy** (via `parentNode` → GlossaryNode): Organizational structure for browsing
- **Semantic relationships** (via `glossaryRelatedTerms`): Meaning connections between concepts

A term can have a `parentNode` for organization (e.g., term "Email" under node "PersonalInformation") AND semantic relationships (e.g., "Email" IsA "PII", "Email" RelatedTo "Contact").

### Schema Metadata on GlossaryTerm

GlossaryTerms support the `schemaMetadata` aspect, which is rarely used but can be helpful for defining structured attributes on terms themselves. This is an advanced feature for when terms need to carry typed properties beyond simple custom properties.

### Deprecation Behavior

When a GlossaryTerm is deprecated (via the `deprecation` aspect):

- The term remains in the system and its relationships are preserved
- Assets tagged with the term retain those associations
- The UI displays a deprecation warning
- The term may be hidden from autocomplete and suggestions
- Consider creating a new term and migrating assets rather than reusing deprecated term names
