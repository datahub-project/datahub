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


**Python SDK: Create an ER Model Relationship**

```python
# Inlined from /metadata-ingestion/examples/library/ermodelrelationship_create_basic.py
# metadata-ingestion/examples/library/ermodelrelationship_create_basic.py
import os
import time

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ERModelRelationshipCardinalityClass,
    ERModelRelationshipKeyClass,
    ERModelRelationshipPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    RelationshipFieldMappingClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

GMS_ENDPOINT = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
GMS_TOKEN = os.getenv("DATAHUB_GMS_TOKEN")
PLATFORM = "mysql"
ENV = "PROD"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, token=GMS_TOKEN)


def create_dataset_with_schema(
    dataset_name: str, fields: list[SchemaFieldClass]
) -> str:
    """Helper function to create a dataset with schema."""
    dataset_urn = make_dataset_urn(PLATFORM, dataset_name, ENV)

    schema_metadata = SchemaMetadataClass(
        schemaName=dataset_name,
        platform=make_data_platform_urn(PLATFORM),
        fields=fields,
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        )
    )

    return dataset_urn


def create_schema_field(
    field_path: str, native_type: str, data_type: SchemaFieldDataTypeClass
) -> SchemaFieldClass:
    """Helper function to create a schema field."""
    return SchemaFieldClass(
        fieldPath=field_path,
        type=data_type,
        nativeDataType=native_type,
        description=f"Field: {field_path}",
        lastModified=AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    )


# Create Employee table
employee_fields = [
    create_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    create_schema_field(
        "name", "varchar(100)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "email", "varchar(255)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "company_id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())
    ),
]
employee_urn = create_dataset_with_schema("Employee", employee_fields)
print(f"Created Employee dataset: {employee_urn}")

# Create Company table
company_fields = [
    create_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    create_schema_field(
        "name", "varchar(200)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "industry", "varchar(100)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]
company_urn = create_dataset_with_schema("Company", company_fields)
print(f"Created Company dataset: {company_urn}")

# Create ER Model Relationship
relationship_id = "employee_to_company"
relationship_urn = f"urn:li:erModelRelationship:{relationship_id}"

# Emit the key aspect
relationship_key = ERModelRelationshipKeyClass(id=relationship_id)
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=relationship_urn,
        aspect=relationship_key,
    )
)

# Emit the properties aspect
relationship_properties = ERModelRelationshipPropertiesClass(
    name="Employee to Company Relationship",
    source=employee_urn,
    destination=company_urn,
    relationshipFieldMappings=[
        RelationshipFieldMappingClass(
            sourceField="company_id",
            destinationField="id",
        )
    ],
    cardinality=ERModelRelationshipCardinalityClass.N_ONE,
    customProperties={
        "constraint_type": "FOREIGN_KEY",
        "on_delete": "CASCADE",
        "on_update": "CASCADE",
    },
    created=AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    ),
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=relationship_urn,
        aspect=relationship_properties,
    )
)

print(f"Created ER Model Relationship: {relationship_urn}")
print(
    "This N:1 relationship connects Employee.company_id to Company.id, "
    "representing that many employees belong to one company."
)

```



### Editable Properties

The `editableERModelRelationshipProperties` aspect allows users to add or modify relationship metadata through the DataHub UI without overwriting information ingested from source systems. This separation follows the same pattern used across DataHub entities.

Editable properties include:

- **description**: Documentation explaining the relationship's purpose, constraints, or business logic
- **name**: An alternative display name that overrides the source system name

#### Updating Editable Properties


**Python SDK: Update editable relationship properties**

```python
# Inlined from /metadata-ingestion/examples/library/ermodelrelationship_update_properties.py
# metadata-ingestion/examples/library/ermodelrelationship_update_properties.py
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableERModelRelationshipPropertiesClass,
)

GMS_ENDPOINT = "http://localhost:8080"
relationship_urn = "urn:li:erModelRelationship:employee_to_company"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})

# Create or update editable properties
audit_stamp = AuditStampClass(
    time=int(time.time() * 1000), actor="urn:li:corpuser:datahub"
)

editable_properties = EditableERModelRelationshipPropertiesClass(
    name="Employee-Company Foreign Key",
    description=(
        "This relationship establishes referential integrity between the Employee "
        "and Company tables. Each employee record must reference a valid company. "
        "The relationship enforces CASCADE on both UPDATE and DELETE operations, "
        "meaning changes to company IDs will propagate to employee records, and "
        "deleting a company will delete all associated employees."
    ),
    created=audit_stamp,
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=relationship_urn,
        aspect=editable_properties,
    )
)

print(f"Updated editable properties for ER Model Relationship {relationship_urn}")
print(f"Name: {editable_properties.name}")
print(f"Description: {editable_properties.description}")

```



### Tags and Glossary Terms

ER Model Relationships support tagging and glossary term attachment just like other DataHub entities. This allows you to categorize relationships, mark them with data classification tags, or link them to business concepts.

#### Adding Tags

Tags can be used to classify relationships by type, importance, or data domain:


**Python SDK: Add a tag to an ER Model Relationship**

```python
# Inlined from /metadata-ingestion/examples/library/ermodelrelationship_add_tag.py
# metadata-ingestion/examples/library/ermodelrelationship_add_tag.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)

GMS_ENDPOINT = "http://localhost:8080"
relationship_urn = "urn:li:erModelRelationship:employee_to_company"
tag_urn = "urn:li:tag:ForeignKey"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})

# Read current tags
# FIXME: emitter.get not available
# gms_response = emitter.get(relationship_urn, aspects=["globalTags"])
current_tags: dict[
    str, object
] = {}  # gms_response.get("globalTags", {}) if gms_response else {}

# Build new tags list
existing_tags = []
if isinstance(current_tags, dict) and "tags" in current_tags:
    tags_list = current_tags["tags"]
    if isinstance(tags_list, list):
        existing_tags = [tag["tag"] for tag in tags_list]

# Add new tag if not already present
if tag_urn not in existing_tags:
    tag_associations = [
        TagAssociationClass(tag=existing_tag) for existing_tag in existing_tags
    ]
    tag_associations.append(TagAssociationClass(tag=tag_urn))

    global_tags = GlobalTagsClass(tags=tag_associations)

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=relationship_urn,
            aspect=global_tags,
        )
    )

    print(f"Added tag {tag_urn} to ER Model Relationship {relationship_urn}")
else:
    print(f"Tag {tag_urn} already exists on {relationship_urn}")

```



#### Adding Glossary Terms

Glossary terms connect relationships to business concepts and terminology:


**Python SDK: Add a glossary term to an ER Model Relationship**

```python
# Inlined from /metadata-ingestion/examples/library/ermodelrelationship_add_term.py
# metadata-ingestion/examples/library/ermodelrelationship_add_term.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)

GMS_ENDPOINT = "http://localhost:8080"
relationship_urn = "urn:li:erModelRelationship:employee_to_company"
term_urn = "urn:li:glossaryTerm:ReferentialIntegrity"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})

# Read current glossary terms
# FIXME: emitter.get not available
# gms_response = emitter.get(relationship_urn, aspects=["glossaryTerms"])
current_terms: dict[
    str, object
] = {}  # gms_response.get("glossaryTerms", {}) if gms_response else {}

# Build new terms list
existing_terms = []
if isinstance(current_terms, dict) and "terms" in current_terms:
    terms_list = current_terms["terms"]
    if isinstance(terms_list, list):
        existing_terms = [term["urn"] for term in terms_list]

# Add new term if not already present
if term_urn not in existing_terms:
    term_associations = [
        GlossaryTermAssociationClass(urn=existing_term)
        for existing_term in existing_terms
    ]
    term_associations.append(GlossaryTermAssociationClass(urn=term_urn))

    glossary_terms = GlossaryTermsClass(
        terms=term_associations,
        auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=relationship_urn,
            aspect=glossary_terms,
        )
    )

    print(f"Added glossary term {term_urn} to ER Model Relationship {relationship_urn}")
else:
    print(f"Glossary term {term_urn} already exists on {relationship_urn}")

```



### Ownership

Ownership can be assigned to ER Model Relationships to indicate who is responsible for maintaining the relationship definition or who should be consulted about changes to the connected datasets.


**Python SDK: Add an owner to an ER Model Relationship**

```python
# Inlined from /metadata-ingestion/examples/library/ermodelrelationship_add_owner.py
# metadata-ingestion/examples/library/ermodelrelationship_add_owner.py
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

GMS_ENDPOINT = "http://localhost:8080"
relationship_urn = "urn:li:erModelRelationship:employee_to_company"
owner_urn = "urn:li:corpuser:jdoe"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})

# Read current ownership
# FIXME: emitter.get not available
# gms_response = emitter.get(relationship_urn, aspects=["ownership"])
current_ownership: dict[
    str, object
] = {}  # gms_response.get("ownership", {}) if gms_response else {}

# Build new owners list
existing_owners = []
if isinstance(current_ownership, dict) and "owners" in current_ownership:
    owners_list = current_ownership["owners"]
    if isinstance(owners_list, list):
        existing_owners = [owner["owner"] for owner in owners_list]

# Add new owner if not already present
if owner_urn not in existing_owners:
    owner_list = [
        OwnerClass(owner=existing_owner, type=OwnershipTypeClass.DATAOWNER)
        for existing_owner in existing_owners
    ]
    owner_list.append(
        OwnerClass(
            owner=owner_urn,
            type=OwnershipTypeClass.DATAOWNER,
        )
    )

    ownership = OwnershipClass(
        owners=owner_list,
        lastModified=AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=relationship_urn,
            aspect=ownership,
        )
    )

    print(f"Added owner {owner_urn} to ER Model Relationship {relationship_urn}")
else:
    print(f"Owner {owner_urn} already exists on {relationship_urn}")

```



### Complex Relationships

ER Model Relationships can model sophisticated data structures including composite keys and many-to-many relationships through junction tables:


**Python SDK: Create a many-to-many relationship with composite keys**

```python
# Inlined from /metadata-ingestion/examples/library/ermodelrelationship_complex_many_to_many.py
# metadata-ingestion/examples/library/ermodelrelationship_complex_many_to_many.py
import time

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ERModelRelationshipCardinalityClass,
    ERModelRelationshipKeyClass,
    ERModelRelationshipPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    RelationshipFieldMappingClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

GMS_ENDPOINT = "http://localhost:8080"
PLATFORM = "postgres"
ENV = "PROD"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})


def create_dataset_with_schema(
    dataset_name: str, fields: list[SchemaFieldClass]
) -> str:
    """Helper function to create a dataset with schema."""
    dataset_urn = make_dataset_urn(PLATFORM, dataset_name, ENV)

    schema_metadata = SchemaMetadataClass(
        schemaName=dataset_name,
        platform=make_data_platform_urn(PLATFORM),
        fields=fields,
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        )
    )

    return dataset_urn


def create_schema_field(
    field_path: str, native_type: str, data_type: SchemaFieldDataTypeClass
) -> SchemaFieldClass:
    """Helper function to create a schema field."""
    return SchemaFieldClass(
        fieldPath=field_path,
        type=data_type,
        nativeDataType=native_type,
        description=f"Field: {field_path}",
        lastModified=AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    )


# Create Student table
student_fields = [
    create_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    create_schema_field(
        "name", "varchar(100)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "email", "varchar(255)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]
student_urn = create_dataset_with_schema("Student", student_fields)
print(f"Created Student dataset: {student_urn}")

# Create Course table
course_fields = [
    create_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    create_schema_field(
        "code", "varchar(20)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "title", "varchar(200)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]
course_urn = create_dataset_with_schema("Course", course_fields)
print(f"Created Course dataset: {course_urn}")

# Create StudentCourse junction table with composite key
student_course_fields = [
    create_schema_field(
        "student_id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())
    ),
    create_schema_field(
        "course_id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())
    ),
    create_schema_field(
        "enrollment_date", "date", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "grade", "varchar(2)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]
student_course_urn = create_dataset_with_schema("StudentCourse", student_course_fields)
print(f"Created StudentCourse junction table: {student_course_urn}")

# Create relationship: StudentCourse -> Student (many-to-one)
student_relationship_id = "student_course_to_student"
student_relationship_urn = f"urn:li:erModelRelationship:{student_relationship_id}"

student_relationship_key = ERModelRelationshipKeyClass(id=student_relationship_id)
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=student_relationship_urn,
        aspect=student_relationship_key,
    )
)

student_relationship_properties = ERModelRelationshipPropertiesClass(
    name="StudentCourse to Student Relationship",
    source=student_course_urn,
    destination=student_urn,
    relationshipFieldMappings=[
        RelationshipFieldMappingClass(
            sourceField="student_id",
            destinationField="id",
        )
    ],
    cardinality=ERModelRelationshipCardinalityClass.N_ONE,
    customProperties={
        "constraint_type": "FOREIGN_KEY",
        "part_of_composite_key": "true",
    },
    created=AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    ),
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=student_relationship_urn,
        aspect=student_relationship_properties,
    )
)

print(f"Created relationship: {student_relationship_urn}")

# Create relationship: StudentCourse -> Course (many-to-one)
course_relationship_id = "student_course_to_course"
course_relationship_urn = f"urn:li:erModelRelationship:{course_relationship_id}"

course_relationship_key = ERModelRelationshipKeyClass(id=course_relationship_id)
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=course_relationship_urn,
        aspect=course_relationship_key,
    )
)

course_relationship_properties = ERModelRelationshipPropertiesClass(
    name="StudentCourse to Course Relationship",
    source=student_course_urn,
    destination=course_urn,
    relationshipFieldMappings=[
        RelationshipFieldMappingClass(
            sourceField="course_id",
            destinationField="id",
        )
    ],
    cardinality=ERModelRelationshipCardinalityClass.N_ONE,
    customProperties={
        "constraint_type": "FOREIGN_KEY",
        "part_of_composite_key": "true",
    },
    created=AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    ),
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=course_relationship_urn,
        aspect=course_relationship_properties,
    )
)

print(f"Created relationship: {course_relationship_urn}")

print("\nMany-to-many relationship established through junction table:")
print("- Student N:N Course (via StudentCourse junction table)")
print("- StudentCourse has composite primary key (student_id, course_id)")
print("- Each component of the composite key is a foreign key to its respective table")

```



### Querying Relationships

ER Model Relationships can be queried using the standard DataHub REST API:


**Fetch an ER Model Relationship**

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




**Find all relationships for a dataset**

You can discover relationships connected to a specific dataset by querying the relationships API:

```bash
# Find relationships where the dataset is the source
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Amysql,Employee,PROD)&types=ermodelrelationA'

# Find relationships where the dataset is the destination
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Amysql,Company,PROD)&types=ermodelrelationB'
```



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

#### erModelRelationshipProperties
Properties associated with a ERModelRelationship



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| name | string | ✓ | Name of the ERModelRelation | Searchable |
| source | string | ✓ | First dataset in the erModelRelationship (no directionality) | Searchable, → ermodelrelationA |
| destination | string | ✓ | Second dataset in the erModelRelationship (no directionality) | Searchable, → ermodelrelationB |
| relationshipFieldMappings | RelationshipFieldMapping[] | ✓ | ERModelRelationFieldMapping (in future we can make it an array) |  |
| created | [AuditStamp](#auditstamp) |  | A timestamp documenting when the asset was created in the source Data Platform (not on DataHub) | Searchable |
| lastModified | [AuditStamp](#auditstamp) |  | A timestamp documenting when the asset was last modified in the source Data Platform (not on Data... | Searchable |
| cardinality | ERModelRelationshipCardinality | ✓ | Cardinality of the relationship |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "erModelRelationshipProperties",
    "schemaVersion": 2
  },
  "name": "ERModelRelationshipProperties",
  "namespace": "com.linkedin.ermodelrelation",
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
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "Name of the ERModelRelation"
    },
    {
      "Relationship": {
        "entityTypes": [
          "dataset"
        ],
        "name": "ermodelrelationA"
      },
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "source",
      "doc": "First dataset in the erModelRelationship (no directionality)"
    },
    {
      "Relationship": {
        "entityTypes": [
          "dataset"
        ],
        "name": "ermodelrelationB"
      },
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "destination",
      "doc": "Second dataset in the erModelRelationship (no directionality)"
    },
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "RelationshipFieldMapping",
          "namespace": "com.linkedin.ermodelrelation",
          "fields": [
            {
              "type": "string",
              "name": "sourceField",
              "doc": "All fields from dataset A that are required for the join, maps to bFields 1:1"
            },
            {
              "type": "string",
              "name": "destinationField",
              "doc": "All fields from dataset B that are required for the join, maps to aFields 1:1"
            }
          ],
          "doc": "Individual Field Mapping of a relationship- one of several"
        }
      },
      "name": "relationshipFieldMappings",
      "doc": "ERModelRelationFieldMapping (in future we can make it an array)"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME"
        }
      },
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
      "doc": "A timestamp documenting when the asset was created in the source Data Platform (not on DataHub)"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME"
        }
      },
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "A timestamp documenting when the asset was last modified in the source Data Platform (not on DataHub)"
    },
    {
      "type": {
        "type": "enum",
        "name": "ERModelRelationshipCardinality",
        "namespace": "com.linkedin.ermodelrelation",
        "symbols": [
          "ONE_ONE",
          "ONE_N",
          "N_ONE",
          "N_N"
        ],
        "doc": "Cardinality of a relationship between two datasets or logical datasets.\nUsed by both ERModelRelationship (physical) and SemanticModelRelationship\n(semantic-layer join paths). Directionality is carried by the source/from\nand destination/to fields on those records, not by this enum."
      },
      "name": "cardinality",
      "default": "N_N",
      "doc": "Cardinality of the relationship"
    }
  ],
  "doc": "Properties associated with a ERModelRelationship"
}
```





#### editableERModelRelationshipProperties
EditableERModelRelationProperties stores editable changes made to erModelRelationship properties. This separates changes made from
ingestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| created | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of... |  |
| lastModified | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the last modification of this resource/association/sub-resource. I... |  |
| deleted | [AuditStamp](#auditstamp) |  | An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically,... |  |
| description | string |  | Documentation of the erModelRelationship | Searchable (editedDescription) |
| name | string |  | Display name of the ERModelRelation | Searchable (editedName) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableERModelRelationshipProperties"
  },
  "name": "EditableERModelRelationshipProperties",
  "namespace": "com.linkedin.ermodelrelation",
  "fields": [
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
      "Searchable": {
        "fieldName": "editedDescription",
        "fieldType": "TEXT",
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Documentation of the erModelRelationship"
    },
    {
      "Searchable": {
        "fieldName": "editedName",
        "fieldType": "TEXT_PARTIAL"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "name",
      "default": null,
      "doc": "Display name of the ERModelRelation"
    }
  ],
  "doc": "EditableERModelRelationProperties stores editable changes made to erModelRelationship properties. This separates changes made from\ningestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines"
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





#### globalTags
Tag aspect used for applying tags to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| tags | TagAssociation[] | ✓ | Tags associated with a given entity | Searchable, → TaggedWith |



#### Raw Schema


```javascript
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
```





#### glossaryTerms
Related business terms information



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| terms | GlossaryTermAssociation[] | ✓ | The related business terms |  |
| auditStamp | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who reported the related business term |  |



#### Raw Schema


```javascript
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
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
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
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
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


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- ermodelrelationA

   - Dataset via `erModelRelationshipProperties.source`
- ermodelrelationB

   - Dataset via `erModelRelationshipProperties.destination`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- TaggedWith

   - Tag via `globalTags.tags`
- TermedWith

   - GlossaryTerm via `glossaryTerms.terms.urn`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
