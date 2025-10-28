# ER Model Relationship

Entity-Relationship (ER) Model Relationships represent the connections between entities in an entity-relationship diagram, specifically modeling how dataset fields relate to each other through foreign key constraints, joins, and other referential relationships. In DataHub, these relationships capture the semantic connections between tables, enabling users to understand data structure, enforce referential integrity, and trace data lineage at the field level.

ER Model Relationships are particularly valuable for documenting database schemas, data warehouse models, and any structured data system where understanding table relationships is critical for data governance, impact analysis, and query optimization.

## Identity

ER Model Relationships are uniquely identified by a single identifier:

- **id**: A unique string identifier for the relationship. When created programmatically, this is typically generated as an MD5 hash based on the relationship name and the two datasets involved (sorted alphabetically to ensure consistency).

The URN structure follows the pattern:

```
urn:li:erModelRelationship:<id>
```

### Example URNs

```
urn:li:erModelRelationship:employee_to_company
urn:li:erModelRelationship:a1b2c3d4e5f6g7h8i9j0
```

### ID Generation

When creating relationships through the UI or API, the ID is often generated deterministically using a hash function to ensure consistency:

1. Create a JSON string with keys in alphabetical order: `Destination`, `ERModelRelationName`, `Source`
2. Use the lower lexicographic dataset URN as "Destination" and the higher as "Source"
3. Generate an MD5 hash of this JSON string

This ensures that the same relationship between two datasets always gets the same ID, regardless of creation order.

## Important Capabilities

### Relationship Properties

ER Model Relationships capture essential metadata about how datasets connect to each other through the `erModelRelationshipProperties` aspect. This core aspect contains:

#### Core Attributes

- **name**: A human-readable name for the relationship (e.g., "Employee to Company Relationship")
- **source**: The URN of the source dataset (first entity in the relationship)
- **destination**: The URN of the destination dataset (second entity in the relationship)
- **cardinality**: Defines the relationship type between datasets

#### Cardinality Types

DataHub supports four cardinality types that describe how records in one dataset relate to records in another:

- **ONE_ONE**: One-to-one relationship. Each record in the source dataset corresponds to exactly one record in the destination dataset.

  - Example: Employee → EmployeeDetails (one employee has one detail record)

- **ONE_N**: One-to-many relationship. Each record in the source dataset can correspond to multiple records in the destination dataset.

  - Example: Department → Employee (one department has many employees)

- **N_ONE**: Many-to-one relationship. Multiple records in the source dataset can correspond to one record in the destination dataset.

  - Example: Employee → Company (many employees belong to one company)

- **N_N**: Many-to-many relationship. Records in both datasets can have multiple corresponding records in the other dataset.
  - Example: Student → Course (students take many courses, courses have many students)

#### Field Mappings

The `relationshipFieldMappings` array defines which specific fields connect the two datasets. Each mapping contains:

- **sourceField**: The field path in the source dataset (e.g., "company_id")
- **destinationField**: The field path in the destination dataset (e.g., "id")

Multiple field mappings can be specified for composite keys where the relationship depends on multiple fields.

#### Custom Properties

Like other DataHub entities, ER Model Relationships support custom properties for storing additional metadata such as:

- Constraint types (e.g., "Foreign Key", "Referential Integrity")
- Index information
- Database-specific metadata
- Business rules or validation logic

#### Timestamps

Relationships include optional timestamp information to track when they were created and last modified in the source system:

- **created**: AuditStamp with creation time and actor
- **lastModified**: AuditStamp with last modification time and actor

### Creating an ER Model Relationship

Here's a complete example showing how to create two datasets and establish a many-to-one relationship between them:

<details>
<summary>Python SDK: Create an ER Model Relationship</summary>

```python
{{ inline /metadata-ingestion/examples/library/ermodelrelationship_create_basic.py show_path_as_comment }}
```

</details>

### Editable Properties

The `editableERModelRelationshipProperties` aspect allows users to add or modify relationship metadata through the DataHub UI without overwriting information ingested from source systems. This separation follows the same pattern used across DataHub entities.

Editable properties include:

- **description**: Documentation explaining the relationship's purpose, constraints, or business logic
- **name**: An alternative display name that overrides the source system name

#### Updating Editable Properties

<details>
<summary>Python SDK: Update editable relationship properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/ermodelrelationship_update_properties.py show_path_as_comment }}
```

</details>

### Tags and Glossary Terms

ER Model Relationships support tagging and glossary term attachment just like other DataHub entities. This allows you to categorize relationships, mark them with data classification tags, or link them to business concepts.

#### Adding Tags

Tags can be used to classify relationships by type, importance, or data domain:

<details>
<summary>Python SDK: Add a tag to an ER Model Relationship</summary>

```python
{{ inline /metadata-ingestion/examples/library/ermodelrelationship_add_tag.py show_path_as_comment }}
```

</details>

#### Adding Glossary Terms

Glossary terms connect relationships to business concepts and terminology:

<details>
<summary>Python SDK: Add a glossary term to an ER Model Relationship</summary>

```python
{{ inline /metadata-ingestion/examples/library/ermodelrelationship_add_term.py show_path_as_comment }}
```

</details>

### Ownership

Ownership can be assigned to ER Model Relationships to indicate who is responsible for maintaining the relationship definition or who should be consulted about changes to the connected datasets.

<details>
<summary>Python SDK: Add an owner to an ER Model Relationship</summary>

```python
{{ inline /metadata-ingestion/examples/library/ermodelrelationship_add_owner.py show_path_as_comment }}
```

</details>

### Complex Relationships

ER Model Relationships can model sophisticated data structures including composite keys and many-to-many relationships through junction tables:

<details>
<summary>Python SDK: Create a many-to-many relationship with composite keys</summary>

```python
{{ inline /metadata-ingestion/examples/library/ermodelrelationship_complex_many_to_many.py show_path_as_comment }}
```

</details>

### Querying Relationships

ER Model Relationships can be queried using the standard DataHub REST API:

<details>
<summary>Fetch an ER Model Relationship</summary>

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AerModelRelationship%3Aemployee_to_company'
```

The response includes all aspects of the relationship:

```json
{
  "urn": "urn:li:erModelRelationship:employee_to_company",
  "aspects": {
    "erModelRelationshipKey": {
      "id": "employee_to_company"
    },
    "erModelRelationshipProperties": {
      "name": "Employee to Company Relationship",
      "source": "urn:li:dataset:(urn:li:dataPlatform:mysql,Employee,PROD)",
      "destination": "urn:li:dataset:(urn:li:dataPlatform:mysql,Company,PROD)",
      "relationshipFieldMappings": [
        {
          "sourceField": "company_id",
          "destinationField": "id"
        }
      ],
      "cardinality": "N_ONE",
      "customProperties": {
        "constraint": "Foreign Key"
      }
    }
  }
}
```

</details>

<details>
<summary>Find all relationships for a dataset</summary>

You can discover relationships connected to a specific dataset by querying the relationships API:

```bash
# Find relationships where the dataset is the source
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Amysql,Employee,PROD)&types=ermodelrelationA'

# Find relationships where the dataset is the destination
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Amysql,Company,PROD)&types=ermodelrelationB'
```

</details>

## Integration Points

ER Model Relationships integrate with several other DataHub entities and features:

### Dataset Integration

ER Model Relationships are fundamentally connected to [Dataset](./dataset.md) entities. Each relationship must reference exactly two datasets:

- Relationships are discoverable from dataset pages in the UI
- The GraphQL API automatically resolves source and destination dataset details
- Relationship information enriches dataset schema views

### Schema Field Integration

While the entity stores field paths as strings, these correspond to [SchemaField](./schemaField.md) entities within the referenced datasets. This enables:

- Visual representation of foreign key relationships in the UI
- Field-level lineage analysis
- Impact analysis when schema changes occur

### Data Lineage

ER Model Relationships complement but are distinct from DataHub's lineage features:

- **ER Model Relationships**: Model the static structure and referential constraints between datasets
- **Upstream/Downstream Lineage**: Captures how data flows through transformations and pipelines

Together, these features provide a complete picture of both data structure and data flow.

### GraphQL API

The DataHub GraphQL API provides rich querying capabilities for ER Model Relationships:

- `erModelRelationship(urn: String!)`: Fetch a specific relationship
- Create and update relationships through mutations
- Traverse from datasets to their relationships
- Bulk query capabilities for building ER diagrams

### Authorization

Creating and modifying ER Model Relationships requires appropriate permissions in DataHub's policy framework. Users must have edit permissions on both the source and destination datasets to create a relationship between them.

## Notable Exceptions

### Non-directional Relationships

While ER Model Relationships have "source" and "destination" fields, these do not necessarily imply directionality in the traditional sense of foreign keys:

- The source/destination ordering is primarily for internal consistency
- When generating IDs, datasets are ordered alphabetically to ensure the same relationship always produces the same ID
- Cardinality types (ONE_N vs N_ONE) explicitly capture the actual relationship direction

### Relationship Lifecycle

ER Model Relationships are currently separate from the datasets they connect:

- Deleting a dataset does not automatically delete its relationships
- Orphaned relationships (pointing to non-existent datasets) may exist after dataset deletion
- Applications should handle cases where relationship endpoints may not exist

### Schema Evolution

ER Model Relationships reference field paths as strings, not versioned schema references:

- If field names change in a dataset schema, the relationship may reference outdated field names
- No automatic validation ensures that referenced fields exist in current schemas
- Applications should implement field validation when creating relationships

### Platform Support

Not all data platforms have first-class support for ER Model Relationships:

- Relational databases (MySQL, PostgreSQL, Oracle) naturally map to this model
- NoSQL databases and data lakes may not have explicit relationship metadata
- Some ingestion connectors automatically extract foreign key relationships, others do not

### Future Considerations

The ER Model Relationship entity may evolve to include:

- Additional relationship types beyond cardinality (inheritance, composition)
- Versioning to track relationship changes over time
- Bidirectional field mappings for complex transformation logic
- Integration with data quality rules and constraint validation
