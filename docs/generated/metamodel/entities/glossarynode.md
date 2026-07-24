# GlossaryNode

A GlossaryNode represents a hierarchical grouping or category within DataHub's Business Glossary. GlossaryNodes act as folders or containers that organize GlossaryTerms into a logical structure, making it easier to navigate and manage large business glossaries.

In practice, GlossaryNodes allow you to:

- Create hierarchical categories for organizing business terminology
- Build multi-level taxonomies (e.g., Finance > Revenue > Recurring Revenue)
- Establish ownership and governance over specific glossary sections
- Apply metadata consistently across related terms within a category
- Manage permissions at the category level

For example, you might create a GlossaryNode called "Finance" containing terms like "Revenue", "Profit", and "EBITDA", with a nested GlossaryNode "Compliance" underneath containing "SOX", "GDPR", and "CCPA" terms.

## Identity

GlossaryNodes are uniquely identified by a single field: their **name**. This name serves as the persistent identifier for the node throughout its lifecycle.

### URN Structure

The URN (Uniform Resource Name) for a GlossaryNode follows this pattern:

```
urn:li:glossaryNode:<node_name>
```

Where:

- `<node_name>`: A unique string identifier for the node. This can be human-readable (e.g., "Finance") or a generated ID (e.g., "fin-category-001" or a UUID).

### Examples

```
# Simple node name
urn:li:glossaryNode:Finance

# Hierarchical naming convention (common pattern)
urn:li:glossaryNode:Finance.Revenue
urn:li:glossaryNode:Classification
urn:li:glossaryNode:Classification.DataSensitivity

# UUID-based identifier
urn:li:glossaryNode:41516e31-0acb-fd90-76ff-fc2c98d2d1a3

# Descriptive identifier
urn:li:glossaryNode:PersonalInformation
```

### Best Practices for Node Names

1. **Use hierarchical notation**: Prefix nodes with their parent category (e.g., `Finance.Revenue`, `Classification.PII`) to indicate structure even though the name is flat.
2. **Be consistent**: Choose a naming convention (camelCase, dot notation, etc.) and apply it uniformly across your glossary.
3. **Keep it permanent**: The node name is the identifier and should not change. Use the `name` field in `glossaryNodeInfo` for the display name.
4. **Consider depth**: While nesting is supported, keep hierarchies manageable (typically 2-4 levels deep) for usability.

## Important Capabilities

### Core Node Information (glossaryNodeInfo)

The `glossaryNodeInfo` aspect contains the essential information about a glossary node:

- **definition** (required): A description of what this node/category represents. This helps users understand the purpose and scope of terms within this node.
- **name**: The display name shown in the UI. This can be more human-friendly than the URN identifier (e.g., "Financial Metrics" vs. "FinancialMetrics").
- **parentNode**: A reference to another GlossaryNode that acts as the parent in the hierarchy. This creates the tree structure visible in the UI.
- **id**: An optional identifier field that can store an external reference or alternate ID.
- **customProperties**: Key-value pairs for additional metadata specific to your organization.

Example:

```python
{
  "name": "Financial Metrics",
  "definition": "Category for all financial and accounting-related business terms including revenue, costs, and profitability measures.",
  "parentNode": "urn:li:glossaryNode:Finance"
}
```

### Hierarchical Structure

GlossaryNodes support arbitrary nesting through the `parentNode` field, creating tree structures:

```
GlossaryNode: DataGovernance
  ├── GlossaryNode: Classification
  │   ├── GlossaryTerm: Public
  │   ├── GlossaryTerm: Internal
  │   └── GlossaryTerm: Confidential
  │
  ├── GlossaryNode: PersonalInformation
  │   ├── GlossaryNode: DirectIdentifiers
  │   │   ├── GlossaryTerm: Email
  │   │   └── GlossaryTerm: SSN
  │   └── GlossaryNode: IndirectIdentifiers
  │       ├── GlossaryTerm: IPAddress
  │       └── GlossaryTerm: DeviceID
  │
  └── GlossaryNode: Compliance
      ├── GlossaryTerm: GDPR
      └── GlossaryTerm: CCPA
```

Key characteristics:

- A GlossaryNode can have at most one parent node (single inheritance)
- A GlossaryNode can contain both GlossaryTerms and child GlossaryNodes
- Nodes at the root level (no parent) appear at the top of the glossary hierarchy
- Moving a node automatically moves all its descendants

### Ownership and Governance

GlossaryNodes support standard ownership metadata through the `ownership` aspect. Ownership at the node level can represent:

- Stewardship responsibility for maintaining the category and its terms
- Subject matter expertise for the business domain
- Accountability for term quality and accuracy within the category

Ownership is particularly powerful for GlossaryNodes because:

- Owners can be granted special permissions (Manage Direct Children, Manage All Children)
- Ownership can cascade to terms within the node
- It establishes clear accountability for glossary sections

### Documentation and Links

GlossaryNodes support the `institutionalMemory` aspect, allowing you to:

- Link to external documentation (Confluence pages, wikis, etc.)
- Reference governance policies or standards
- Point to training materials or style guides
- Maintain a history of important links related to the category

This is especially useful for top-level nodes representing major domains or initiatives.

## Code Examples

### Creating a GlossaryNode


**Python SDK: Create a root-level GlossaryNode**

```python
# Inlined from /metadata-ingestion/examples/library/glossary_node_create.py
from datahub.sdk import DataHubClient, GlossaryNode

client = DataHubClient.from_env()

# Top-level glossary node
node = GlossaryNode(
    id="7f3d2c1a",
    display_name="Financial Metrics",
    definition="Category for all financial and accounting-related business terms including revenue, costs, and profitability measures.",
)

# Child node nested under Finance
child_node = GlossaryNode(
    id="4b5e6f7a",
    display_name="Revenue Metrics",
    definition="Metrics related to revenue recognition and reporting.",
    parent_node=node,
)

client.entities.upsert(node)
client.entities.upsert(child_node)

```




**Python SDK: Create a nested GlossaryNode with parent**

```python
# Inlined from /metadata-ingestion/examples/library/glossary_node_create_nested.py
from datahub.sdk import DataHubClient, GlossaryNode

client = DataHubClient.from_env()

parent_node = GlossaryNode(
    id="7f3d2c1a",
    display_name="Finance",
    definition="Top-level category for financial metrics and terms.",
)

child_node = GlossaryNode(
    id="4b5e6f7a",
    display_name="Revenue Metrics",
    definition="Metrics related to revenue recognition and reporting.",
    parent_node=parent_node,
)

client.entities.upsert(parent_node)
client.entities.upsert(child_node)

```



### Managing Hierarchy


**Python SDK: Build a multi-level glossary hierarchy**

```python
# Inlined from /metadata-ingestion/examples/library/glossary_term_create_hierarchy.py
from datahub.sdk import DataHubClient, GlossaryNode
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

# Create multi-level hierarchy:
# DataGovernance
#   ├── Classification
#   │   ├── Public (term)
#   │   └── Confidential (term)
#   └── PersonalInformation
#       ├── Email (term)
#       └── SSN (term)

root = GlossaryNode(
    id="8b9c0d1e",
    display_name="Data Governance",
    definition="Top-level governance structure for data classification and management.",
)

classification = GlossaryNode(
    id="2f3a4b5c",
    display_name="Classification",
    definition="Data classification categories.",
    parent_node=root,
)

pii = GlossaryNode(
    id="6d7e8f90",
    display_name="Personal Information",
    definition="Personal and sensitive data categories.",
    parent_node=root,
)

terms = [
    GlossaryTerm(
        id="abcdef12",
        display_name="Public",
        definition="Publicly available data with no restrictions.",
        parent_node=classification,
    ),
    GlossaryTerm(
        id="34567890",
        display_name="Confidential",
        definition="Restricted access data for internal use only.",
        parent_node=classification,
    ),
    GlossaryTerm(
        id="abcd1234",
        display_name="Email Address",
        definition="Email addresses that can identify individuals.",
        parent_node=pii,
    ),
    GlossaryTerm(
        id="5678abcd",
        display_name="Social Security Number",
        definition="Social Security Numbers - highly sensitive personal identifiers.",
        parent_node=pii,
    ),
]

for entity in [root, classification, pii, *terms]:
    client.entities.upsert(entity)

```



### Adding Ownership


**Python SDK: Add an owner to a GlossaryNode**

```python
# Inlined from /metadata-ingestion/examples/library/glossary_node_add_owner.py
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient, GlossaryNodeUrn

client = DataHubClient.from_env()

node = client.entities.get(GlossaryNodeUrn("7f3d2c1a"))
node.add_owner(CorpUserUrn("jdoe"))
client.entities.upsert(node)

```



### Querying GlossaryNodes


**REST API: Get a GlossaryNode by URN**

```bash
# Fetch a GlossaryNode entity
curl -X GET 'http://localhost:8080/entities/urn%3Ali%3AglossaryNode%3AFinance' \
  -H 'Authorization: Bearer <token>'

# Response includes all aspects:
# - glossaryNodeKey (identity)
# - glossaryNodeInfo (definition, name, parentNode, etc.)
# - ownership (who owns this node)
# - institutionalMemory (links to documentation)
# - etc.
```




**GraphQL: Query root-level GlossaryNodes**

```graphql
query GetRootGlossaryNodes {
  getRootGlossaryNodes {
    nodes {
      urn
      properties {
        name
        definition
      }
      ownership {
        owners {
          owner {
            ... on CorpUser {
              urn
              username
            }
          }
        }
      }
    }
  }
}
```




**GraphQL: Query children of a GlossaryNode**

```graphql
query GetGlossaryNodeChildren {
  glossaryNode(urn: "urn:li:glossaryNode:Finance") {
    urn
    properties {
      name
      definition
    }
    children {
      count
      relationships {
        entity {
          ... on GlossaryNode {
            urn
            properties {
              name
            }
          }
          ... on GlossaryTerm {
            urn
            properties {
              name
              definition
            }
          }
        }
      }
    }
  }
}
```



### Bulk Operations


**YAML Ingestion: Create node hierarchy from Business Glossary file**

```yaml
# business_glossary.yml
version: "1"
source: MyOrganization
owners:
  users:
    - datahub
nodes:
  - name: DataGovernance
    description: Top-level governance structure
    nodes:
      - name: Classification
        description: Data classification categories
        terms:
          - name: Public
            description: Publicly available data
          - name: Internal
            description: Internal use only
          - name: Confidential
            description: Restricted access data

      - name: PersonalInformation
        description: Personal and sensitive data categories
        nodes:
          - name: DirectIdentifiers
            description: Direct personal identifiers
            terms:
              - name: Email
                description: Email addresses
              - name: SSN
                description: Social Security Numbers

          - name: IndirectIdentifiers
            description: Indirect identifiers
            terms:
              - name: IPAddress
                description: Internet Protocol addresses
              - name: DeviceID
                description: Device identifiers
# Ingest using the DataHub CLI:
# datahub ingest -c business_glossary.yml
```

See the [Business Glossary Source](../../../generated/ingestion/sources/business-glossary.md) documentation for the full YAML format specification.



## Integration Points

### Relationship with GlossaryTerm

GlossaryNodes provide organizational structure for GlossaryTerms. The relationship is established through:

- **GlossaryTerm → GlossaryNode**: A term's `glossaryTermInfo.parentNode` field references its containing node
- **Navigation**: The UI renders this as a browsable hierarchy where users can expand nodes to see contained terms
- **Search**: Users can filter by glossary node to find all terms within a category

Think of this relationship as:

- **GlossaryNode**: Folder/directory (can contain terms and other nodes)
- **GlossaryTerm**: File (the actual business definition)

### Parent-Child Relationships

GlossaryNodes form a tree structure through self-referential parent-child relationships:

- A child node references its parent via `glossaryNodeInfo.parentNode`
- A parent node can have many children (both nodes and terms)
- The DataHub UI displays this as an expandable tree in the glossary browser
- GraphQL resolvers provide specialized queries for traversing the hierarchy

**Key operations:**

- `getRootGlossaryNodes`: Fetch all top-level nodes (no parent)
- `parentNodes`: Navigate upward to find all ancestors
- `children`: Navigate downward to find immediate children
- Moving a node updates its `parentNode` reference and affects the entire subtree

### GraphQL API

The GraphQL API provides specialized operations for GlossaryNodes:

**Queries:**

- `glossaryNode(urn)`: Fetch a specific node with children
- `getRootGlossaryNodes`: Get all root-level nodes
- `search(entity: "glossaryNode")`: Search nodes by name/definition

**Mutations:**

- `createGlossaryNode`: Create a new node with optional parent
- `updateParentNode`: Move a node to a different parent
- `updateName`: Update the display name
- `updateDescription`: Update the definition

**Resolvers:**

- `children`: Fetch immediate children (nodes and terms)
- `childrenCount`: Count of children under this node
- `parentNodes`: Fetch ancestor path from node to root

See the [Business Glossary documentation](../../../glossary/business-glossary.md) for UI operations.

### Access Control and Permissions

GlossaryNodes support fine-grained access control through special glossary-specific privileges:

#### Manage Direct Glossary Children

Users with this privilege on a node can:

- Create new terms and nodes directly under this node
- Edit terms and nodes directly under this node
- Delete terms and nodes directly under this node
- Cannot affect grandchildren or deeper descendants

**Use case**: Department leads managing their immediate category structure

#### Manage All Glossary Children

Users with this privilege on a node can:

- Create, edit, and delete any term or node in the entire subtree
- Manage nested hierarchies of any depth
- Full control over the category and all descendants

**Use case**: Data governance team managing an entire domain (e.g., all PII-related terms)

#### Global Privilege: Manage Glossaries

Users with this platform-level privilege can:

- Manage any node or term across the entire glossary
- Create root-level nodes
- Full administrative control

These privileges are checked hierarchically - if you have permission on a parent node, it may grant permissions on children depending on the privilege type.

### Integration with Search and Discovery

While GlossaryNodes don't get applied to data assets directly (that's the role of GlossaryTerms), they enhance discoverability by:

1. **Faceted Navigation**: Users can browse the glossary hierarchy to find relevant terms
2. **Context**: The node structure provides semantic grouping that helps users understand term relationships
3. **Filtering**: Search interfaces can filter terms by their containing node
4. **Autocomplete**: Node structure influences term suggestions and grouping

## Notable Exceptions

### Node Name vs Display Name

Similar to GlossaryTerms, the URN identifier (`name` in `glossaryNodeKey`) is separate from the display name (`name` in `glossaryNodeInfo`):

- **URN name**: Use a stable, unchanging identifier (e.g., "finance-001", "DataGovernance")
- **Display name**: Use a human-friendly label that can be updated (e.g., "Financial Metrics", "Data Governance")

This separation allows you to rename nodes in the UI without breaking references.

### Circular References Not Allowed

The hierarchy must be a tree structure (directed acyclic graph):

- A node cannot be its own ancestor
- Moving a node under one of its descendants is prevented
- DataHub validates the hierarchy to prevent cycles

If you attempt to create a circular reference, the operation will fail with a validation error.

### Root-Level Nodes

Nodes with no parent (`parentNode` is null or not set) appear at the root level of the glossary:

- These represent top-level categories
- Creating root-level nodes may require higher privileges
- Root nodes typically represent major domains or organizational divisions

### Deleting Nodes with Children

Current behavior (subject to change):

- **DataHub may require nodes to be empty before deletion**
- You must first delete or move all child nodes and terms
- This prevents accidental loss of large glossary sections

Best practice: Always move or reassign children before deleting a node, or use bulk operations that handle the entire subtree.

### Display Properties

GlossaryNodes support the `displayProperties` aspect (added in newer versions), which provides additional UI customization:

- Custom icons or colors for the node
- Display order hints
- UI-specific rendering preferences

This is an optional enhancement for organizations that want more visual control over their glossary.

### No Direct Application to Assets

Unlike GlossaryTerms, GlossaryNodes are **not** directly applied to data assets:

- You cannot tag a dataset with a GlossaryNode
- Only GlossaryTerms can be applied to datasets, columns, dashboards, etc.
- Nodes exist solely for organizational purposes within the glossary itself

If you need to tag assets with a category, create a GlossaryTerm within that node and apply the term.

### Moving Nodes Affects All Descendants

When you move a node to a new parent:

- All child nodes and terms move with it
- The entire subtree is relocated
- References from terms to their parent node are automatically maintained
- No manual updates to individual terms are needed

This makes reorganization efficient but requires care to avoid unintended moves.



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

#### glossaryNodeKey
Key for a GlossaryNode



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ |  | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryNodeKey"
  },
  "name": "GlossaryNodeKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name"
    }
  ],
  "doc": "Key for a GlossaryNode"
}
```





#### glossaryNodeInfo
Properties associated with a GlossaryNode



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| definition | string | ✓ | Definition of business node | Searchable |
| parentNode | string |  | Parent node of the glossary term | Searchable, → IsPartOf |
| name | string |  | Display name of the node | Searchable (displayName) |
| id | string |  | Optional id for the GlossaryNode | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryNodeInfo"
  },
  "name": "GlossaryNodeInfo",
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
        "fieldType": "TEXT",
        "searchTier": 2
      },
      "type": "string",
      "name": "definition",
      "doc": "Definition of business node"
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
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldName": "displayName",
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
      "doc": "Display name of the node"
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
      "doc": "Optional id for the GlossaryNode"
    }
  ],
  "doc": "Properties associated with a GlossaryNode"
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
- IsPartOf (via `glossaryNodeInfo.parentNode`)
#### Outgoing
These are the relationships stored in this entity's aspects
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
- HasSummaryTemplate

   - DataHubPageTemplate via `assetSettings.assetSummary.templates`
- AssociatedWith

   - Domain via `domains.domains`
#### Incoming
These are the relationships stored in other entity's aspects
- IsPartOf

   - GlossaryTerm via `glossaryTermInfo.parentNode`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
