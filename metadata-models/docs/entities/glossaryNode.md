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

<details>
<summary>Python SDK: Create a root-level GlossaryNode</summary>

```python
{{ inline /metadata-ingestion/examples/library/glossary_node_create.py show_path_as_comment }}
```

</details>

<details>
<summary>Python SDK: Create a nested GlossaryNode with parent</summary>

```python
{{ inline /metadata-ingestion/examples/library/glossary_node_create_nested.py show_path_as_comment }}
```

</details>

### Managing Hierarchy

<details>
<summary>Python SDK: Build a multi-level glossary hierarchy</summary>

```python
{{ inline /metadata-ingestion/examples/library/glossary_term_create_hierarchy.py show_path_as_comment }}
```

</details>

### Adding Ownership

<details>
<summary>Python SDK: Add an owner to a GlossaryNode</summary>

```python
{{ inline /metadata-ingestion/examples/library/glossary_node_add_owner.py show_path_as_comment }}
```

</details>

### Querying GlossaryNodes

<details>
<summary>REST API: Get a GlossaryNode by URN</summary>

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

</details>

<details>
<summary>GraphQL: Query root-level GlossaryNodes</summary>

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

</details>

<details>
<summary>GraphQL: Query children of a GlossaryNode</summary>

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

</details>

### Bulk Operations

<details>
<summary>YAML Ingestion: Create node hierarchy from Business Glossary file</summary>

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

</details>

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
