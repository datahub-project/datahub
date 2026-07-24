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


**Python SDK: Create a basic GlossaryTerm**

```python
# Inlined from /metadata-ingestion/examples/library/glossary_term_create.py
from datahub.sdk import DataHubClient
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

term = GlossaryTerm(
    id="a1b2c3d4",
    display_name="Customer Lifetime Value",
    definition="The total revenue a business can expect from a single customer account throughout the business relationship. This metric helps prioritize customer retention efforts and marketing spend.",
)

client.entities.upsert(term)

```




**Python SDK: Create a GlossaryTerm with full metadata**

```python
# Inlined from /metadata-ingestion/examples/library/glossary_term_create_with_metadata.py
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn
from datahub.sdk import DataHubClient, GlossaryNodeUrn
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

term = GlossaryTerm(
    id="3a4b5c6d",
    display_name="Personally Identifiable Information",
    definition="Information that can be used to identify, contact, or locate a single person, or to identify an individual in context. Examples include name, email address, phone number, and social security number.",
    parent_node=GlossaryNodeUrn("2f3a4b5c"),
    custom_properties={
        "sensitivity_level": "HIGH",
        "data_retention_period": "7_years",
        "regulatory_framework": "GDPR,CCPA",
    },
    owners=[CorpUserUrn("datahub"), CorpGroupUrn("privacy-team")],
    links=[
        (
            "https://wiki.company.com/privacy/pii-guidelines",
            "Internal PII Handling Guidelines",
        ),
        ("https://gdpr.eu/", "GDPR Official Documentation"),
    ],
)

client.entities.upsert(term)

```



### Managing Term Relationships


**Python SDK: Add relationships between GlossaryTerms**

```python
# Inlined from /metadata-ingestion/examples/library/glossary_term_add_relationships.py
from datahub.metadata.urns import GlossaryTermUrn
from datahub.sdk import DataHubClient
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

# Email — is a type of PII and Sensitive, related to PhoneNumber and Contact
email_term = GlossaryTerm(
    id="1a2b3c4d",
    display_name="Email",
    definition="Email addresses that can identify individuals.",
    is_a=[
        GlossaryTermUrn("a1b2c3d4"),  # Classification.PII
        GlossaryTermUrn("b2c3d4e5"),  # Classification.Sensitive
    ],
    related_terms=[
        GlossaryTermUrn("c3d4e5f6"),  # PersonalInformation.PhoneNumber
        GlossaryTermUrn("d4e5f6a7"),  # PersonalInformation.Contact
    ],
)

# Address — is a type of PII, has components (ZipCode, Street, City, Country)
address_term = GlossaryTerm(
    id="5e6f7a8b",
    display_name="Address",
    definition="Physical addresses that can identify individuals.",
    is_a=[GlossaryTermUrn("a1b2c3d4")],  # Classification.PII
    has_a=[
        GlossaryTermUrn("e5f6a7b8"),  # PersonalInformation.ZipCode
        GlossaryTermUrn("f6a7b8c9"),  # PersonalInformation.Street
        GlossaryTermUrn("a7b8c9d0"),  # PersonalInformation.City
        GlossaryTermUrn("b8c9d0e1"),  # PersonalInformation.Country
    ],
)

# ColorEnum — an enumeration with fixed allowed values
color_enum_term = GlossaryTerm(
    id="9c0d1e2f",
    display_name="Color",
    definition="An enumeration of allowed color values.",
    values=[
        GlossaryTermUrn("c9d0e1f2"),  # Colors.Red
        GlossaryTermUrn("d0e1f2a3"),  # Colors.Green
        GlossaryTermUrn("e1f2a3b4"),  # Colors.Blue
        GlossaryTermUrn("f2a3b4c5"),  # Colors.Yellow
    ],
)

client.entities.upsert(email_term)
client.entities.upsert(address_term)
client.entities.upsert(color_enum_term)

```



### Applying Terms to Assets


**Python SDK: Add a GlossaryTerm to a dataset**

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_term.py
from typing import List, Optional, Union

from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn


def add_terms_to_dataset(
    client: DataHubClient,
    dataset_urn: DatasetUrn,
    term_urns: List[Union[GlossaryTermUrn, str]],
) -> None:
    """
    Add glossary terms to a dataset.

    Args:
        client: DataHub client to use
        dataset_urn: URN of the dataset to update
        term_urns: List of term URNs or term names to add
    """
    dataset = client.entities.get(dataset_urn)

    for term in term_urns:
        if isinstance(term, str):
            resolved_term_urn = client.resolve.term(name=term)
            dataset.add_term(resolved_term_urn)
        else:
            dataset.add_term(term)

    client.entities.update(dataset)


def main(client: Optional[DataHubClient] = None) -> None:
    """
    Main function to add terms to dataset example.

    Args:
        client: Optional DataHub client (for testing). If not provided, creates one from env.
    """
    client = client or DataHubClient.from_env()

    dataset_urn = DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")

    # Add terms using both URN and name resolution
    add_terms_to_dataset(
        client=client,
        dataset_urn=dataset_urn,
        term_urns=[
            GlossaryTermUrn("Classification.HighlyConfidential"),
            "PII",  # Will be resolved by name
        ],
    )


if __name__ == "__main__":
    main()

```




**Python SDK: Add a GlossaryTerm to a dataset column**

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_column_term.py
from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)

dataset["address.zipcode"].add_term(GlossaryTermUrn("Classification.Location"))

client.entities.update(dataset)

```



### Querying GlossaryTerms


**REST API: Get a GlossaryTerm by URN**

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




**REST API: Search for assets tagged with a term**

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




**Python SDK: Query terms applied to a dataset**

```python
# Inlined from /metadata-ingestion/examples/library/dataset_query_terms.py
from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)

print(dataset.terms)

```



### Bulk Operations


**YAML Ingestion: Create multiple terms from a Business Glossary file**

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



## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### glossaryTermKey
Key for a GlossaryTerm



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | The term name, which serves as a unique id | Searchable (id) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTermKey"
  },
  "name": "GlossaryTermKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "enableAutocomplete": true,
        "fieldName": "id",
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "The term name, which serves as a unique id"
    }
  ],
  "doc": "Key for a GlossaryTerm"
}
```





#### glossaryTermInfo
Properties associated with a GlossaryTerm



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| id | string |  | Optional id for the term | Searchable |
| name | string |  | Display name of the term | Searchable |
| definition | string | ✓ | Definition of business term. | Searchable |
| parentNode | string |  | Parent node of the glossary term | Searchable, → IsPartOf |
| termSource | string | ✓ | Source of the Business Term (INTERNAL or EXTERNAL) with default value as INTERNAL | Searchable |
| sourceRef | string |  | External Reference to the business-term | Searchable |
| sourceUrl | string |  | The abstracted URL such as https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/Fin... |  |
| rawSchema | string |  | Schema definition of the glossary term | ⚠️ Deprecated |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTermInfo"
  },
  "name": "GlossaryTermInfo",
  "namespace": "com.linkedin.glossary",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "id",
      "default": null,
      "doc": "Optional id for the term"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": [
        "null",
        "string"
      ],
      "name": "name",
      "default": null,
      "doc": "Display name of the term"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "searchTier": 2
      },
      "type": "string",
      "name": "definition",
      "doc": "Definition of business term."
    },
    {
      "Relationship": {
        "entityTypes": [
          "glossaryNode"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "fieldName": "parentNode",
        "fieldType": "URN",
        "hasValuesFieldName": "hasParentNode"
      },
      "java": {
        "class": "com.linkedin.common.urn.GlossaryNodeUrn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parentNode",
      "default": null,
      "doc": "Parent node of the glossary term"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "termSource",
      "doc": "Source of the Business Term (INTERNAL or EXTERNAL) with default value as INTERNAL"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "sourceRef",
      "default": null,
      "doc": "External Reference to the business-term"
    },
    {
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "sourceUrl",
      "default": null,
      "doc": "The abstracted URL such as https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/CashInstrument."
    },
    {
      "deprecated": true,
      "type": [
        "null",
        "string"
      ],
      "name": "rawSchema",
      "default": null,
      "doc": "Schema definition of the glossary term"
    }
  ],
  "doc": "Properties associated with a GlossaryTerm"
}
```





#### ownership
Ownership information of an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| owners | Owner[] | ✓ | List of owners of the entity. |  |
| ownerTypes | map |  | Ownership type to Owners map, populated via mutation hook. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the record and when. A value of 0 in the time field indi... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false,
                "searchTier": 2
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/$key": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
}
```





#### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| removed | boolean | ✓ | Whether the entity has been removed (soft-deleted). Kept for backward compatibility. When lifecyc... | Searchable |
| lifecycleStage | string |  | The lifecycle stage of the entity, referencing a lifecycleStageType entity. When null, the entity... | Searchable |
| lifecycleLastUpdated | [AuditStamp](#auditstamp) |  | Attribution for the lifecycle stage transition — who moved the entity into its current stage and ... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "status"
  },
  "name": "Status",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "removed",
      "default": false,
      "doc": "Whether the entity has been removed (soft-deleted).\nKept for backward compatibility. When lifecycleStage is set to a stage\nwith hideInSearch=true, this field is NOT automatically synced \u2014 the\nsearch layer uses lifecycleStage settings directly."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "lifecycleStage",
      "default": null,
      "doc": "The lifecycle stage of the entity, referencing a lifecycleStageType entity.\nWhen null, the entity is in its default active state (visible in search).\nWhen set, the referenced lifecycle stage's settings determine behavior\n(e.g., hideInSearch=true excludes the entity from default search results).\n\nUsers can override default filtering by explicitly filtering on this field."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AuditStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "actor",
              "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "impersonator",
              "default": null,
              "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "message",
              "default": null,
              "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
            }
          ],
          "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
        }
      ],
      "name": "lifecycleLastUpdated",
      "default": null,
      "doc": "Attribution for the lifecycle stage transition \u2014 who moved the entity\ninto its current stage and when. Populated automatically by the\nsetLifecycleStage mutation; should be set by any code path that\nwrites the lifecycleStage field."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```





#### browsePaths
Shared aspect containing Browse Paths to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| paths | string[] | ✓ | A list of valid browse paths for the entity.  Browse paths are expected to be forward slash-separ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePaths"
  },
  "name": "BrowsePaths",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "browsePaths",
          "fieldType": "BROWSE_PATH"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "paths",
      "doc": "A list of valid browse paths for the entity.\n\nBrowse paths are expected to be forward slash-separated strings. For example: 'prod/snowflake/datasetName'"
    }
  ],
  "doc": "Shared aspect containing Browse Paths to be indexed for an entity."
}
```





#### glossaryRelatedTerms
Has A / Is A lineage information about a glossary Term reporting the lineage



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| isRelatedTerms | string[] |  | The relationship Is A with glossary term | Searchable, → IsA |
| hasRelatedTerms | string[] |  | The relationship Has A with glossary term | Searchable, → HasA |
| values | string[] |  | The relationship Has Value with glossary term. These are fixed value a term has. For example a Co... | Searchable, → HasValue |
| relatedTerms | string[] |  | The relationship isRelatedTo with glossary term | Searchable, → IsRelatedTo |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryRelatedTerms"
  },
  "name": "GlossaryRelatedTerms",
  "namespace": "com.linkedin.glossary",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "glossaryTerm"
          ],
          "name": "IsA"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "isRelatedTerms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "isRelatedTerms",
      "default": null,
      "doc": "The relationship Is A with glossary term"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "glossaryTerm"
          ],
          "name": "HasA"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "hasRelatedTerms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "hasRelatedTerms",
      "default": null,
      "doc": "The relationship Has A with glossary term"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "glossaryTerm"
          ],
          "name": "HasValue"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "values",
          "fieldType": "URN"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "values",
      "default": null,
      "doc": "The relationship Has Value with glossary term.\nThese are fixed value a term has. For example a ColorEnum where RED, GREEN and YELLOW are fixed values."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "glossaryTerm"
          ],
          "name": "IsRelatedTo"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "relatedTerms",
          "fieldType": "URN"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "relatedTerms",
      "default": null,
      "doc": "The relationship isRelatedTo with glossary term"
    }
  ],
  "doc": "Has A / Is A lineage information about a glossary Term reporting the lineage"
}
```





#### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| elements | InstitutionalMemoryMetadata[] | ✓ | List of records that represent institutional memory of an entity. Each record consists of a link,... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
            },
            {
              "type": {
                "type": "record",
                "name": "AuditStamp",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": "long",
                    "name": "time",
                    "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": "string",
                    "name": "actor",
                    "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "impersonator",
                    "default": null,
                    "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "message",
                    "default": null,
                    "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                  }
                ],
                "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
              },
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
}
```





#### schemaMetadata
SchemaMetadata to describe metadata related to store schema



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| schemaName | string | ✓ | Schema name e.g. PageViewEvent, identity.Profile, ams.account_management_tracking |  |
| platform | string | ✓ | Standardized platform urn where schema is defined. The data platform Urn (urn:li:platform:{platfo... |  |
| version | long | ✓ | Every change to SchemaMetadata in the resource results in a new version. Version is server assign... |  |
| created | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of... |  |
| lastModified | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the last modification of this resource/association/sub-resource. I... |  |
| deleted | [AuditStamp](#auditstamp) |  | An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically,... |  |
| dataset | string |  | Dataset this schema metadata is associated with. |  |
| cluster | string |  | The cluster this schema metadata resides from |  |
| hash | string | ✓ | the SHA1 hash of the schema content |  |
| platformSchema | union |  | The native schema in the dataset's platform. |  |
| fields | SchemaField[] | ✓ | Client provided a list of fields from document schema. |  |
| primaryKeys | string[] |  | Client provided list of fields that define primary keys to access record. Field order defines hie... |  |
| foreignKeysSpecs | map |  | Map captures all the references schema makes to external datasets. Map key is ForeignKeySpecName ... | ⚠️ Deprecated |
| foreignKeys | ForeignKeyConstraint[] |  | List of foreign key constraints for the schema |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "schemaMetadata"
  },
  "name": "SchemaMetadata",
  "namespace": "com.linkedin.schema",
  "fields": [
    {
      "validate": {
        "strlen": {
          "max": 500,
          "min": 1
        }
      },
      "type": "string",
      "name": "schemaName",
      "doc": "Schema name e.g. PageViewEvent, identity.Profile, ams.account_management_tracking"
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.DataPlatformUrn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Standardized platform urn where schema is defined. The data platform Urn (urn:li:platform:{platform_name})"
    },
    {
      "type": "long",
      "name": "version",
      "doc": "Every change to SchemaMetadata in the resource results in a new version. Version is server assigned. This version is differ from platform native schema version."
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "created",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.DatasetUrn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "dataset",
      "default": null,
      "doc": "Dataset this schema metadata is associated with."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "cluster",
      "default": null,
      "doc": "The cluster this schema metadata resides from"
    },
    {
      "type": "string",
      "name": "hash",
      "doc": "the SHA1 hash of the schema content"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "EspressoSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "documentSchema",
              "doc": "The native espresso document schema."
            },
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The espresso table schema definition."
            }
          ],
          "doc": "Schema text of an espresso table schema."
        },
        {
          "type": "record",
          "name": "OracleDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The native schema in the dataset's platform. This is a human readable (json blob) table schema."
            }
          ],
          "doc": "Schema holder for oracle data definition language that describes an oracle table."
        },
        {
          "type": "record",
          "name": "MySqlDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The native schema in the dataset's platform. This is a human readable (json blob) table schema."
            }
          ],
          "doc": "Schema holder for MySql data definition language that describes an MySql table."
        },
        {
          "type": "record",
          "name": "PrestoDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "rawSchema",
              "doc": "The raw schema in the dataset's platform. This includes the DDL and the columns extracted from DDL."
            }
          ],
          "doc": "Schema holder for presto data definition language that describes a presto view."
        },
        {
          "type": "record",
          "name": "KafkaSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "documentSchema",
              "doc": "The native kafka document schema. This is a human readable avro document schema."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "documentSchemaType",
              "default": null,
              "doc": "The native kafka document schema type. This can be AVRO/PROTOBUF/JSON."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "keySchema",
              "default": null,
              "doc": "The native kafka key schema as retrieved from Schema Registry"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "keySchemaType",
              "default": null,
              "doc": "The native kafka key schema type. This can be AVRO/PROTOBUF/JSON."
            }
          ],
          "doc": "Schema holder for kafka schema."
        },
        {
          "type": "record",
          "name": "BinaryJsonSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "schema",
              "doc": "The native schema text for binary JSON file format."
            }
          ],
          "doc": "Schema text of binary JSON schema."
        },
        {
          "type": "record",
          "name": "OrcSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "schema",
              "doc": "The native schema for ORC file format."
            }
          ],
          "doc": "Schema text of an ORC schema."
        },
        {
          "type": "record",
          "name": "Schemaless",
          "namespace": "com.linkedin.schema",
          "fields": [],
          "doc": "The dataset has no specific schema associated with it"
        },
        {
          "type": "record",
          "name": "KeyValueSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "keySchema",
              "doc": "The raw schema for the key in the key-value store."
            },
            {
              "type": "string",
              "name": "valueSchema",
              "doc": "The raw schema for the value in the key-value store."
            }
          ],
          "doc": "Schema text of a key-value store schema."
        },
        {
          "type": "record",
          "name": "OtherSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "rawSchema",
              "doc": "The native schema in the dataset's platform."
            }
          ],
          "doc": "Schema holder for undefined schema types."
        }
      ],
      "name": "platformSchema",
      "doc": "The native schema in the dataset's platform."
    },
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "SchemaField",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "Searchable": {
                "boostScore": 1.0,
                "fieldName": "fieldPaths",
                "fieldType": "TEXT",
                "queryByDefault": "true"
              },
              "type": "string",
              "name": "fieldPath",
              "doc": "Flattened name of the field. Field is computed from jsonPath field."
            },
            {
              "Deprecated": true,
              "type": [
                "null",
                "string"
              ],
              "name": "jsonPath",
              "default": null,
              "doc": "Flattened name of a field in JSON Path notation."
            },
            {
              "type": "boolean",
              "name": "nullable",
              "default": false,
              "doc": "Indicates if this field is optional or nullable"
            },
            {
              "Searchable": {
                "boostScore": 0.1,
                "fieldName": "fieldDescriptions",
                "fieldType": "TEXT",
                "sanitizeRichText": true
              },
              "type": [
                "null",
                "string"
              ],
              "name": "description",
              "default": null,
              "doc": "Description"
            },
            {
              "Deprecated": true,
              "Searchable": {
                "boostScore": 0.2,
                "fieldName": "fieldLabels",
                "fieldType": "TEXT"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "label",
              "default": null,
              "doc": "Label of the field. Provides a more human-readable name for the field than field path. Some sources will\nprovide this metadata but not all sources have the concept of a label. If just one string is associated with\na field in a source, that is most likely a description.\n\nNote that this field is deprecated and is not surfaced in the UI."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "created",
              "default": null,
              "doc": "An AuditStamp corresponding to the creation of this schema field."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An AuditStamp corresponding to the last modification of this schema field."
            },
            {
              "type": {
                "type": "record",
                "name": "SchemaFieldDataType",
                "namespace": "com.linkedin.schema",
                "fields": [
                  {
                    "type": [
                      {
                        "type": "record",
                        "name": "BooleanType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Boolean field type."
                      },
                      {
                        "type": "record",
                        "name": "FixedType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Fixed field type."
                      },
                      {
                        "type": "record",
                        "name": "StringType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "String field type."
                      },
                      {
                        "type": "record",
                        "name": "BytesType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Bytes field type."
                      },
                      {
                        "type": "record",
                        "name": "NumberType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Number data type: long, integer, short, etc.."
                      },
                      {
                        "type": "record",
                        "name": "DateType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Date field type."
                      },
                      {
                        "type": "record",
                        "name": "TimeType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Time field type. This should also be used for datetimes."
                      },
                      {
                        "type": "record",
                        "name": "EnumType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Enum field type."
                      },
                      {
                        "type": "record",
                        "name": "NullType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Null field type."
                      },
                      {
                        "type": "record",
                        "name": "MapType",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "keyType",
                            "default": null,
                            "doc": "Key type in a map"
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "valueType",
                            "default": null,
                            "doc": "Type of the value in a map"
                          }
                        ],
                        "doc": "Map field type."
                      },
                      {
                        "type": "record",
                        "name": "ArrayType",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": [
                              "null",
                              {
                                "type": "array",
                                "items": "string"
                              }
                            ],
                            "name": "nestedType",
                            "default": null,
                            "doc": "List of types this array holds."
                          }
                        ],
                        "doc": "Array field type."
                      },
                      {
                        "type": "record",
                        "name": "UnionType",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": [
                              "null",
                              {
                                "type": "array",
                                "items": "string"
                              }
                            ],
                            "name": "nestedTypes",
                            "default": null,
                            "doc": "List of types in union type."
                          }
                        ],
                        "doc": "Union field type."
                      },
                      {
                        "type": "record",
                        "name": "RecordType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Record field type."
                      }
                    ],
                    "name": "type",
                    "doc": "Data platform specific types"
                  }
                ],
                "doc": "Schema field data types"
              },
              "name": "type",
              "doc": "Platform independent field type of the field."
            },
            {
              "type": "string",
              "name": "nativeDataType",
              "doc": "The native type of the field in the dataset's platform as declared by platform schema."
            },
            {
              "type": "boolean",
              "name": "recursive",
              "default": false,
              "doc": "There are use cases when a field in type B references type A. A field in A references field of type B. In such cases, we will mark the first field as recursive."
            },
            {
              "Relationship": {
                "/tags/*/tag": {
                  "entityTypes": [
                    "tag"
                  ],
                  "name": "SchemaFieldTaggedWith"
                }
              },
              "Searchable": {
                "/tags/*/attribution/actor": {
                  "fieldName": "fieldTagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/tags/*/attribution/source": {
                  "fieldName": "fieldTagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/tags/*/attribution/time": {
                  "fieldName": "fieldTagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                },
                "/tags/*/tag": {
                  "boostScore": 0.5,
                  "fieldName": "fieldTags",
                  "fieldType": "URN"
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "Aspect": {
                    "name": "globalTags"
                  },
                  "name": "GlobalTags",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "Relationship": {
                        "/*/tag": {
                          "entityTypes": [
                            "tag"
                          ],
                          "name": "TaggedWith"
                        }
                      },
                      "Searchable": {
                        "/*/tag": {
                          "addToFilters": true,
                          "boostScore": 0.5,
                          "fieldName": "tags",
                          "fieldType": "URN",
                          "filterNameOverride": "Tagged With",
                          "hasValuesFieldName": "hasTags",
                          "queryByDefault": true,
                          "searchTier": 2
                        }
                      },
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "TagAssociation",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "java": {
                                "class": "com.linkedin.common.urn.TagUrn"
                              },
                              "type": "string",
                              "name": "tag",
                              "doc": "Urn of the applied tag"
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "context",
                              "default": null,
                              "doc": "Additional context about the association"
                            },
                            {
                              "Searchable": {
                                "/actor": {
                                  "fieldName": "tagAttributionActors",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/source": {
                                  "fieldName": "tagAttributionSources",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/time": {
                                  "fieldName": "tagAttributionDates",
                                  "fieldType": "DATETIME",
                                  "queryByDefault": false
                                }
                              },
                              "type": [
                                "null",
                                {
                                  "type": "record",
                                  "name": "MetadataAttribution",
                                  "namespace": "com.linkedin.common",
                                  "fields": [
                                    {
                                      "type": "long",
                                      "name": "time",
                                      "doc": "When this metadata was updated."
                                    },
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.Urn"
                                      },
                                      "type": "string",
                                      "name": "actor",
                                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                                    },
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.Urn"
                                      },
                                      "type": [
                                        "null",
                                        "string"
                                      ],
                                      "name": "source",
                                      "default": null,
                                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                                    },
                                    {
                                      "type": {
                                        "type": "map",
                                        "values": "string"
                                      },
                                      "name": "sourceDetail",
                                      "default": {},
                                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                                    }
                                  ],
                                  "doc": "Information about who, why, and how this metadata was applied"
                                }
                              ],
                              "name": "attribution",
                              "default": null,
                              "doc": "Information about who, why, and how this metadata was applied"
                            }
                          ],
                          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
                        }
                      },
                      "name": "tags",
                      "doc": "Tags associated with a given entity"
                    }
                  ],
                  "doc": "Tag aspect used for applying tags to an entity"
                }
              ],
              "name": "globalTags",
              "default": null,
              "doc": "Tags associated with the field"
            },
            {
              "Relationship": {
                "/terms/*/urn": {
                  "entityTypes": [
                    "glossaryTerm"
                  ],
                  "name": "SchemaFieldWithGlossaryTerm"
                }
              },
              "Searchable": {
                "/terms/*/attribution/actor": {
                  "fieldName": "fieldTermAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/terms/*/attribution/source": {
                  "fieldName": "fieldTermAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/terms/*/attribution/time": {
                  "fieldName": "fieldTermAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                },
                "/terms/*/urn": {
                  "boostScore": 0.5,
                  "fieldName": "fieldGlossaryTerms",
                  "fieldType": "URN"
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "Aspect": {
                    "name": "glossaryTerms"
                  },
                  "name": "GlossaryTerms",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "GlossaryTermAssociation",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "Relationship": {
                                "entityTypes": [
                                  "glossaryTerm"
                                ],
                                "name": "TermedWith"
                              },
                              "Searchable": {
                                "addToFilters": true,
                                "fieldName": "glossaryTerms",
                                "fieldType": "URN",
                                "filterNameOverride": "Glossary Term",
                                "hasValuesFieldName": "hasGlossaryTerms",
                                "includeSystemModifiedAt": true,
                                "systemModifiedAtFieldName": "termsModifiedAt"
                              },
                              "java": {
                                "class": "com.linkedin.common.urn.GlossaryTermUrn"
                              },
                              "type": "string",
                              "name": "urn",
                              "doc": "Urn of the applied glossary term"
                            },
                            {
                              "java": {
                                "class": "com.linkedin.common.urn.Urn"
                              },
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "actor",
                              "default": null,
                              "doc": "The user URN which will be credited for adding associating this term to the entity"
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "context",
                              "default": null,
                              "doc": "Additional context about the association"
                            },
                            {
                              "Searchable": {
                                "/actor": {
                                  "fieldName": "termAttributionActors",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/source": {
                                  "fieldName": "termAttributionSources",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/time": {
                                  "fieldName": "termAttributionDates",
                                  "fieldType": "DATETIME",
                                  "queryByDefault": false
                                }
                              },
                              "type": [
                                "null",
                                "com.linkedin.common.MetadataAttribution"
                              ],
                              "name": "attribution",
                              "default": null,
                              "doc": "Information about who, why, and how this metadata was applied"
                            }
                          ],
                          "doc": "Properties of an applied glossary term."
                        }
                      },
                      "name": "terms",
                      "doc": "The related business terms"
                    },
                    {
                      "type": "com.linkedin.common.AuditStamp",
                      "name": "auditStamp",
                      "doc": "Audit stamp containing who reported the related business term"
                    }
                  ],
                  "doc": "Related business terms information"
                }
              ],
              "name": "glossaryTerms",
              "default": null,
              "doc": "Glossary terms associated with the field"
            },
            {
              "type": "boolean",
              "name": "isPartOfKey",
              "default": false,
              "doc": "For schema fields that are part of complex keys, set this field to true\nWe do this to easily distinguish between value and key fields"
            },
            {
              "type": [
                "null",
                "boolean"
              ],
              "name": "isPartitioningKey",
              "default": null,
              "doc": "For Datasets which are partitioned, this determines the partitioning key.\nNote that multiple columns can be part of a partitioning key, but currently we do not support\nrendering the ordered partitioning key."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "jsonProps",
              "default": null,
              "doc": "For schema fields that have other properties that are not modeled explicitly,\nuse this field to serialize those properties into a JSON string"
            }
          ],
          "doc": "SchemaField to describe metadata related to dataset schema."
        }
      },
      "name": "fields",
      "doc": "Client provided a list of fields from document schema."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "primaryKeys",
      "default": null,
      "doc": "Client provided list of fields that define primary keys to access record. Field order defines hierarchical espresso keys. Empty lists indicates absence of primary key access patter. Value is a SchemaField@fieldPath."
    },
    {
      "deprecated": "Use foreignKeys instead.",
      "type": [
        "null",
        {
          "type": "map",
          "values": {
            "type": "record",
            "name": "ForeignKeySpec",
            "namespace": "com.linkedin.schema",
            "fields": [
              {
                "type": [
                  {
                    "type": "record",
                    "name": "DatasetFieldForeignKey",
                    "namespace": "com.linkedin.schema",
                    "fields": [
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.DatasetUrn"
                        },
                        "type": "string",
                        "name": "parentDataset",
                        "doc": "dataset that stores the resource."
                      },
                      {
                        "type": {
                          "type": "array",
                          "items": "string"
                        },
                        "name": "currentFieldPaths",
                        "doc": "List of fields in hosting(current) SchemaMetadata that conform a foreign key. List can contain a single entry or multiple entries if several entries in hosting schema conform a foreign key in a single parent dataset."
                      },
                      {
                        "type": "string",
                        "name": "parentField",
                        "doc": "SchemaField@fieldPath that uniquely identify field in parent dataset that this field references."
                      }
                    ],
                    "doc": "For non-urn based foregin keys."
                  },
                  {
                    "type": "record",
                    "name": "UrnForeignKey",
                    "namespace": "com.linkedin.schema",
                    "fields": [
                      {
                        "type": "string",
                        "name": "currentFieldPath",
                        "doc": "Field in hosting(current) SchemaMetadata."
                      }
                    ],
                    "doc": "If SchemaMetadata fields make any external references and references are of type com.linkedin.common.Urn or any children, this models can be used to mark it."
                  }
                ],
                "name": "foreignKey",
                "doc": "Foreign key definition in metadata schema."
              }
            ],
            "doc": "Description of a foreign key in a schema."
          }
        }
      ],
      "name": "foreignKeysSpecs",
      "default": null,
      "doc": "Map captures all the references schema makes to external datasets. Map key is ForeignKeySpecName typeref."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "ForeignKeyConstraint",
            "namespace": "com.linkedin.schema",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the constraint, likely provided from the source"
              },
              {
                "Relationship": {
                  "/*": {
                    "entityTypes": [
                      "schemaField"
                    ],
                    "name": "ForeignKeyTo"
                  }
                },
                "type": {
                  "type": "array",
                  "items": "string"
                },
                "name": "foreignFields",
                "doc": "Fields the constraint maps to on the foreign dataset"
              },
              {
                "type": {
                  "type": "array",
                  "items": "string"
                },
                "name": "sourceFields",
                "doc": "Fields the constraint maps to on the source dataset"
              },
              {
                "Relationship": {
                  "entityTypes": [
                    "dataset"
                  ],
                  "name": "ForeignKeyToDataset"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "foreignDataset",
                "doc": "Reference to the foreign dataset for ease of lookup"
              }
            ],
            "doc": "Description of a foreign key constraint in a schema."
          }
        }
      ],
      "name": "foreignKeys",
      "default": null,
      "doc": "List of foreign key constraints for the schema"
    }
  ],
  "doc": "SchemaMetadata to describe metadata related to store schema"
}
```





#### deprecation
Deprecation status of an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| deprecated | boolean | ✓ | Whether the entity is deprecated. | Searchable |
| decommissionTime | long |  | The time user plan to decommission this entity. |  |
| note | string | ✓ | Additional information about the entity deprecation plan, such as the wiki, doc, RB. |  |
| actor | string | ✓ | The user URN which will be credited for modifying this deprecation content. |  |
| replacement | string |  |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "deprecation"
  },
  "name": "Deprecation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "BOOLEAN",
        "filterNameOverride": "Deprecated",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the entity is deprecated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this entity."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the entity deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actor",
      "doc": "The user URN which will be credited for modifying this deprecation content."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "replacement",
      "default": null
    }
  ],
  "doc": "Deprecation status of an entity"
}
```





#### domains
Links from an Asset to its Domains



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| domains | string[] | ✓ | The Domains attached to an Asset | Searchable, → AssociatedWith |
| domainAssociations | DomainAssociation[] |  | Additional per-domain association metadata such as attribution and propagation source. A superset... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains",
    "schemaVersion": 2
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DomainAssociation",
            "namespace": "com.linkedin.domain",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "domain",
                "doc": "Urn of the associated domain. Corresponds to an entry in the parallel domains array."
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "context",
                "default": null,
                "doc": "Additional context about the association"
              },
              {
                "Searchable": {
                  "/actor": {
                    "fieldName": "domainAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/source": {
                    "fieldName": "domainAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/time": {
                    "fieldName": "domainAttributionDates",
                    "fieldType": "DATETIME",
                    "queryByDefault": false
                  }
                },
                "type": [
                  "null",
                  {
                    "type": "record",
                    "name": "MetadataAttribution",
                    "namespace": "com.linkedin.common",
                    "fields": [
                      {
                        "type": "long",
                        "name": "time",
                        "doc": "When this metadata was updated."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": "string",
                        "name": "actor",
                        "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "source",
                        "default": null,
                        "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                      },
                      {
                        "type": {
                          "type": "map",
                          "values": "string"
                        },
                        "name": "sourceDetail",
                        "default": {},
                        "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                      }
                    ],
                    "doc": "Information about who, why, and how this metadata was applied"
                  }
                ],
                "name": "attribution",
                "default": null,
                "doc": "Information about who, why, and how this domain was applied.\nsourceDetail may carry flags such as 'propagated'='true' when set via glossary tree propagation."
              }
            ],
            "doc": "Properties of an applied domain association."
          }
        }
      ],
      "name": "domainAssociations",
      "default": null,
      "doc": "Additional per-domain association metadata such as attribution and propagation source.\nA superset of the domains field; entries correspond by domain URN.\nInitial migration handled by the DomainsMigrationMutator;\nthe two fields are kept in sync via the DomainsSyncMutationHook."
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```





#### applications
Links from an Asset to its Applications



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| applications | string[] | ✓ | The Applications attached to an Asset | Searchable, → AssociatedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "applications"
  },
  "name": "Applications",
  "namespace": "com.linkedin.application",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "application"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "applications",
          "fieldType": "URN",
          "filterNameOverride": "Application",
          "hasValuesFieldName": "hasApplication"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "applications",
      "doc": "The Applications attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Applications"
}
```





#### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| properties | StructuredPropertyValueAssignment[] | ✓ | Custom property bag. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```





#### forms
Forms that are assigned to this entity to be filled out



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| incompleteForms | [FormAssociation](#formassociation)[] | ✓ | All incomplete forms assigned to the entity. | Searchable |
| completedForms | [FormAssociation](#formassociation)[] | ✓ | All complete forms assigned to the entity. | Searchable |
| verifications | FormVerificationAssociation[] | ✓ | Verifications that have been applied to the entity via completed forms. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "forms"
  },
  "name": "Forms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "incompleteFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "incompleteFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "incompleteFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "incompleteForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied form"
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "FormPromptAssociation",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "string",
                      "name": "id",
                      "doc": "The id for the prompt. This must be GLOBALLY UNIQUE."
                    },
                    {
                      "type": {
                        "type": "record",
                        "name": "AuditStamp",
                        "namespace": "com.linkedin.common",
                        "fields": [
                          {
                            "type": "long",
                            "name": "time",
                            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                          },
                          {
                            "java": {
                              "class": "com.linkedin.common.urn.Urn"
                            },
                            "type": "string",
                            "name": "actor",
                            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                          },
                          {
                            "java": {
                              "class": "com.linkedin.common.urn.Urn"
                            },
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "impersonator",
                            "default": null,
                            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "message",
                            "default": null,
                            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                          }
                        ],
                        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                      },
                      "name": "lastModified",
                      "doc": "The last time this prompt was touched for the entity (set, unset)"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FormPromptFieldAssociations",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": {
                                    "type": "record",
                                    "name": "FieldFormPromptAssociation",
                                    "namespace": "com.linkedin.common",
                                    "fields": [
                                      {
                                        "type": "string",
                                        "name": "fieldPath",
                                        "doc": "The field path on a schema field."
                                      },
                                      {
                                        "type": "com.linkedin.common.AuditStamp",
                                        "name": "lastModified",
                                        "doc": "The last time this prompt was touched for the field on the entity (set, unset)"
                                      }
                                    ],
                                    "doc": "Information about the status of a particular prompt for a specific schema field\non an entity."
                                  }
                                }
                              ],
                              "name": "completedFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are not yet complete for this form."
                            },
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "com.linkedin.common.FieldFormPromptAssociation"
                                }
                              ],
                              "name": "incompleteFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are complete for this form."
                            }
                          ],
                          "doc": "Information about the field-level prompt associations on a top-level prompt association."
                        }
                      ],
                      "name": "fieldAssociations",
                      "default": null,
                      "doc": "Optional information about the field-level prompt associations."
                    }
                  ],
                  "doc": "Information about the status of a particular prompt.\nNote that this is where we can add additional information about individual responses:\nactor, timestamp, and the response itself."
                }
              },
              "name": "incompletePrompts",
              "default": [],
              "doc": "A list of prompts that are not yet complete for this form."
            },
            {
              "type": {
                "type": "array",
                "items": "com.linkedin.common.FormPromptAssociation"
              },
              "name": "completedPrompts",
              "default": [],
              "doc": "A list of prompts that have been completed for this form."
            }
          ],
          "doc": "Properties of an applied form."
        }
      },
      "name": "incompleteForms",
      "doc": "All incomplete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "completedFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "completedFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "completedFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "completedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.FormAssociation"
      },
      "name": "completedForms",
      "doc": "All complete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/form": {
          "fieldName": "verifiedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormVerificationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "form",
              "doc": "The urn of the form that granted this verification."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An audit stamp capturing who and when verification was applied for this form."
            }
          ],
          "doc": "An association between a verification and an entity that has been granted\nvia completion of one or more forms of type 'VERIFICATION'."
        }
      },
      "name": "verifications",
      "default": [],
      "doc": "Verifications that have been applied to the entity via completed forms."
    }
  ],
  "doc": "Forms that are assigned to this entity to be filled out"
}
```





#### testResults
Information about a Test Result



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| failing | [TestResult](#testresult)[] | ✓ | Results that are failing | Searchable, → IsFailing |
| passing | [TestResult](#testresult)[] | ✓ | Results that are passing | Searchable, → IsPassing |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
}
```





#### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| typeNames | string[] | ✓ | The names of the specific types. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
}
```





#### displayProperties
Properties related to how the entity is displayed in the Datahub UI



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| colorHex | string |  | The color associated with the entity in Hex. For example #FFFFFF. |  |
| icon | IconProperties |  | The icon associated with the entity |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "displayProperties"
  },
  "name": "DisplayProperties",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "colorHex",
      "default": null,
      "doc": "The color associated with the entity in Hex. For example #FFFFFF."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "IconProperties",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "MATERIAL": "Material UI"
                },
                "name": "IconLibrary",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "MATERIAL"
                ],
                "doc": "Enum of possible icon sources"
              },
              "name": "iconLibrary",
              "doc": "The source of the icon: e.g. Antd, Material, etc"
            },
            {
              "type": "string",
              "name": "name",
              "doc": "The name of the icon"
            },
            {
              "type": "string",
              "name": "style",
              "doc": "Any modifier for the icon, this will be library-specific, e.g. filled/outlined, etc"
            }
          ],
          "doc": "Properties describing an icon associated with an entity"
        }
      ],
      "name": "icon",
      "default": null,
      "doc": "The icon associated with the entity"
    }
  ],
  "doc": "Properties related to how the entity is displayed in the Datahub UI"
}
```





#### assetSettings
Settings associated with this asset



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| assetSummary | AssetSummarySettings |  | Information related to the asset summary for this asset |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "assetSettings"
  },
  "name": "AssetSettings",
  "namespace": "com.linkedin.settings.asset",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AssetSummarySettings",
          "namespace": "com.linkedin.settings.asset",
          "fields": [
            {
              "Relationship": {
                "/*/template": {
                  "entityTypes": [
                    "dataHubPageTemplate"
                  ],
                  "name": "HasSummaryTemplate"
                }
              },
              "type": [
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "AssetSummarySettingsTemplate",
                    "namespace": "com.linkedin.settings.asset",
                    "fields": [
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": "string",
                        "name": "template",
                        "doc": "The urn of the template"
                      }
                    ],
                    "doc": "Object containing the template and any additional info for asset summary settings"
                  }
                },
                "null"
              ],
              "name": "templates",
              "default": [],
              "doc": "The list of templates applied to this asset in order. Right now we only expect one."
            }
          ],
          "doc": "Information related to the asset summary for this asset"
        }
      ],
      "name": "assetSummary",
      "default": null,
      "doc": "Information related to the asset summary for this asset"
    }
  ],
  "doc": "Settings associated with this asset"
}
```





### Common Types

These types are used across multiple aspects in this entity.

#### AuditStamp

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.

**Fields:**

- `time` (long): When did the resource/association/sub-resource move into the specific lifecyc...
- `actor` (string): The entity (e.g. a member URN) which will be credited for moving the resource...
- `impersonator` (string?): The entity (e.g. a service URN) which performs the change on behalf of the Ac...
- `message` (string?): Additional context around how DataHub was informed of the particular change. ...

#### FormAssociation

Properties of an applied form.

**Fields:**

- `urn` (string): Urn of the applied form
- `incompletePrompts` (FormPromptAssociation[]): A list of prompts that are not yet complete for this form.
- `completedPrompts` (FormPromptAssociation[]): A list of prompts that have been completed for this form.

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...


### Relationships

#### Self
These are the relationships to itself, stored in this entity's aspects
- IsA (via `glossaryRelatedTerms.isRelatedTerms`)
- HasA (via `glossaryRelatedTerms.hasRelatedTerms`)
- HasValue (via `glossaryRelatedTerms.values`)
- IsRelatedTo (via `glossaryRelatedTerms.relatedTerms`)
- SchemaFieldWithGlossaryTerm (via `schemaMetadata.fields.glossaryTerms`)
- TermedWith (via `schemaMetadata.fields.glossaryTerms.terms.urn`)
#### Outgoing
These are the relationships stored in this entity's aspects
- IsPartOf

   - GlossaryNode via `glossaryTermInfo.parentNode`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- SchemaFieldTaggedWith

   - Tag via `schemaMetadata.fields.globalTags`
- TaggedWith

   - Tag via `schemaMetadata.fields.globalTags.tags`
- ForeignKeyTo

   - SchemaField via `schemaMetadata.foreignKeys.foreignFields`
- ForeignKeyToDataset

   - Dataset via `schemaMetadata.foreignKeys.foreignDataset`
- AssociatedWith

   - Domain via `domains.domains`
   - Application via `applications.applications`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
- HasSummaryTemplate

   - DataHubPageTemplate via `assetSettings.assetSummary.templates`
#### Incoming
These are the relationships stored in other entity's aspects
- SchemaFieldWithGlossaryTerm

   - Dataset via `schemaMetadata.fields.glossaryTerms`
   - Chart via `inputFields.fields.schemaField.glossaryTerms`
   - Dashboard via `inputFields.fields.schemaField.glossaryTerms`
- TermedWith

   - Dataset via `schemaMetadata.fields.glossaryTerms.terms.urn`
   - Dataset via `editableSchemaMetadata.editableSchemaFieldInfo.glossaryTerms.terms.urn`
   - Dataset via `glossaryTerms.terms.urn`
   - DataJob via `glossaryTerms.terms.urn`
   - DataFlow via `glossaryTerms.terms.urn`
   - Chart via `glossaryTerms.terms.urn`
   - Chart via `inputFields.fields.schemaField.glossaryTerms.terms.urn`
   - Dashboard via `glossaryTerms.terms.urn`
   - Dashboard via `inputFields.fields.schemaField.glossaryTerms.terms.urn`
   - Notebook via `glossaryTerms.terms.urn`
   - Container via `glossaryTerms.terms.urn`
- EditableSchemaFieldWithGlossaryTerm

   - Dataset via `editableSchemaMetadata.editableSchemaFieldInfo.glossaryTerms`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
