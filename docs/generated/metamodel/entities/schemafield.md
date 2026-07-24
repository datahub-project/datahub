# SchemaField

The schemaField entity represents an individual column or field within a dataset's schema. While schema information is typically ingested as part of a dataset's `schemaMetadata` aspect, schemaField entities exist as first-class entities to enable direct attachment of metadata like tags, glossary terms, documentation, and structured properties at the field level.

SchemaField entities are automatically created by DataHub when datasets with schemas are ingested. They serve as the link between dataset-level metadata and column-level metadata, enabling fine-grained data governance and lineage tracking at the field level.

## Identity

SchemaField entities are uniquely identified by two components:

- **Parent URN**: The URN of the dataset that contains this field
- **Field Path**: The path identifying the field within the schema (e.g., `user_id`, `address.zipcode` for nested fields)

The URN structure for a schemaField follows this pattern:

```
urn:li:schemaField:(<parent_dataset_urn>,<encoded_field_path>)
```

### Examples

**Simple field:**

```
urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD),user_id)
```

**Nested field:**

```
urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD),address.zipcode)
```

**Field with special characters (URL encoded):**

```
urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),first%20name)
```

Note that the field path component may be URL-encoded if it contains special characters. The v1 field path uses `.` notation for nested structures, while v2 field paths include type information (e.g., `[version=2.0].[type=struct].address.[type=string].zipcode`).

## Important Capabilities

### Field Information (schemafieldInfo)

The `schemafieldInfo` aspect contains basic identifying information about the schema field:

- **name**: The display name of the field
- **schemaFieldAliases**: Alternative URNs for this field, used to store field path variations

This aspect is primarily used internally by DataHub to support field path variations and search functionality.

### Documentation

The `documentation` aspect stores field-level documentation from multiple sources. Unlike the dataset-level description pattern which uses separate aspects (`datasetProperties` and `editableDatasetProperties`), field-level documentation uses a single unified aspect that can contain multiple documentation entries from different sources.

Each documentation entry includes:

- The documentation text/description
- The source system or attribution information


**Python SDK: Add or update documentation for a schema field**

```python
# Inlined from /metadata-ingestion/examples/library/schemafield_add_documentation.py
import time

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="bigquery", name="project.dataset.transactions", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="transaction_amount"
)

current_docs = graph.get_aspect(
    entity_urn=field_urn, aspect_type=models.DocumentationClass
)

documentation_text = (
    "The monetary value of the transaction in USD. "
    "This field is calculated from the base currency amount "
    "using the exchange rate at transaction time."
)

attribution = models.MetadataAttributionClass(
    time=int(time.time() * 1000),
    actor=builder.make_user_urn("data_steward"),
    source=builder.make_data_platform_urn("manual"),
)

new_doc = models.DocumentationAssociationClass(
    documentation=documentation_text,
    attribution=attribution,
)

if current_docs and current_docs.documentations:
    source_exists = False
    for i, doc in enumerate(current_docs.documentations):
        if doc.attribution and doc.attribution.source == attribution.source:
            current_docs.documentations[i] = new_doc
            source_exists = True
            break
    if not source_exists:
        current_docs.documentations.append(new_doc)
else:
    current_docs = models.DocumentationClass(documentations=[new_doc])

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=field_urn,
        aspect=current_docs,
    )
)

```



### Tags

Tags can be added directly to schema fields using the `globalTags` aspect. This is separate from tags added at the dataset level, allowing for fine-grained classification of individual columns.

Tags on fields are commonly used to:

- Mark sensitive data (PII, PHI, confidential)
- Indicate data quality issues
- Flag deprecated fields
- Classify data by security level or compliance requirements


**Python SDK: Add a tag to a schema field**

```python
# Inlined from /metadata-ingestion/examples/library/schemafield_add_tag.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="postgres", name="public.users", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="email_address"
)

current_tags = graph.get_aspect(
    entity_urn=field_urn, aspect_type=models.GlobalTagsClass
)

tag_to_add = builder.make_tag_urn("PII")
tag_association = models.TagAssociationClass(tag=tag_to_add)

if current_tags and current_tags.tags:
    if tag_to_add not in [tag.tag for tag in current_tags.tags]:
        current_tags.tags.append(tag_association)
else:
    current_tags = models.GlobalTagsClass(tags=[tag_association])

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=field_urn,
        aspect=current_tags,
    )
)

```



### Glossary Terms

Glossary terms can be attached to schema fields via the `glossaryTerms` aspect, enabling semantic annotation at the column level. This helps users understand the business meaning of individual fields.


**Python SDK: Add a glossary term to a schema field**

```python
# Inlined from /metadata-ingestion/examples/library/schemafield_add_term.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="snowflake", name="analytics.public.orders", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="customer_id"
)

current_terms = graph.get_aspect(
    entity_urn=field_urn, aspect_type=models.GlossaryTermsClass
)

term_to_add = builder.make_term_urn("CustomerIdentifier")
term_association = models.GlossaryTermAssociationClass(urn=term_to_add)

if current_terms and current_terms.terms:
    if term_to_add not in [term.urn for term in current_terms.terms]:
        current_terms.terms.append(term_association)
else:
    current_terms = models.GlossaryTermsClass(
        terms=[term_association],
        auditStamp=models.AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
    )

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=field_urn,
        aspect=current_terms,
    )
)

```



### Business Attributes

The `businessAttributes` aspect allows association of business attribute definitions with schema fields. Business attributes provide a way to attach enterprise-specific metadata dimensions (like data classification, retention policies, or business rules) directly to fields.

This is particularly useful for organizations that need to track custom governance metadata at the field level that isn't covered by standard aspects.

### Structured Properties

Schema fields support structured properties via the `structuredProperties` aspect, allowing organizations to extend the metadata model with custom typed properties. This is useful for tracking field-level metadata like:

- Data quality scores
- Business criticality ratings
- Custom classification schemes
- Regulatory compliance markers


**Python SDK: Add structured properties to a schema field**

```python
# Inlined from /metadata-ingestion/examples/library/schemafield_add_structured_properties.py
import time

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="hive", name="logging.events.clickstream", env="PROD"
)

field_urn = builder.make_schema_field_urn(parent_urn=dataset_urn, field_path="user_id")

current_properties = graph.get_aspect(
    entity_urn=field_urn, aspect_type=models.StructuredPropertiesClass
)

property_urn = "urn:li:structuredProperty:io.acryl.dataQuality.score"
property_value = "0.95"

new_assignment = models.StructuredPropertyValueAssignmentClass(
    propertyUrn=property_urn,
    values=[property_value],
    created=models.AuditStampClass(
        time=int(time.time() * 1000), actor=builder.make_user_urn("datahub")
    ),
)

if current_properties and current_properties.properties:
    property_exists = False
    for i, prop in enumerate(current_properties.properties):
        if prop.propertyUrn == property_urn:
            current_properties.properties[i] = new_assignment
            property_exists = True
            break
    if not property_exists:
        current_properties.properties.append(new_assignment)
else:
    current_properties = models.StructuredPropertiesClass(properties=[new_assignment])

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=field_urn,
        aspect=current_properties,
    )
)

```



### Field Aliases (schemaFieldAliases)

The `schemaFieldAliases` aspect stores alternative URNs for a schema field. This is useful when:

- Field paths change due to schema evolution
- Multiple field path formats are used (v1 vs v2)
- Cross-platform field references need to be maintained

### Deprecation

Fields can be marked as deprecated using the `deprecation` aspect, indicating they should not be used in new applications or analyses. The deprecation aspect includes:

- Deprecation timestamp
- Optional note explaining the deprecation
- Optional actor who deprecated the field

### Logical Parent

The `logicalParent` aspect can associate a schema field with a logical parent entity (like a container or domain), enabling organizational hierarchies that differ from the physical dataset structure.

### Forms

Forms can be attached to schema fields via the `forms` aspect, enabling structured data collection and validation at the field level. This is useful for capturing field-level certifications, approvals, or additional metadata.

### Status

The `status` aspect indicates whether a schema field is active or has been soft-deleted.

### Test Results

The `testResults` aspect can store results of data quality tests run on specific fields, linking test outcomes directly to the columns they validate.

### SubTypes

The `subTypes` aspect allows categorization of schema fields beyond their data type, enabling custom classification schemes.

## Code Examples

### Querying a Schema Field via REST API

The standard GET API can be used to retrieve schema field entities and their aspects:


**Fetch a schemaField entity**

```python
# Inlined from /metadata-ingestion/examples/library/schemafield_query_entity.py
from typing import Any, cast

import datahub.emitter.mce_builder as builder
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="postgres", name="public.customers", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="email_address"
)

entity = graph.get_entity_semityped(entity_urn=field_urn)

if entity:
    print(f"Schema Field URN: {field_urn}")
    print(f"Entity Type: {entity.get('entityType')}")

    aspects = cast(dict[str, Any], entity.get("aspects", {}))

    if "globalTags" in aspects:
        tags = aspects["globalTags"]["tags"]
        print(f"Tags: {[tag['tag'] for tag in tags]}")

    if "glossaryTerms" in aspects:
        terms = aspects["glossaryTerms"]["terms"]
        print(f"Glossary Terms: {[term['urn'] for term in terms]}")

    if "documentation" in aspects:
        docs = aspects["documentation"]["documentations"]
        for doc in docs:
            print(f"Documentation: {doc['documentation'][:100]}...")

    if "structuredProperties" in aspects:
        props = aspects["structuredProperties"]["properties"]
        for prop in props:
            print(f"Property {prop['propertyUrn']}: {prop['values']}")
else:
    print(f"Schema field {field_urn} not found")

```

Example API call:

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AschemaField%3A(urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Apostgres%2Cpublic.users%2CPROD)%2Cuser_id)'
```

This returns all aspects associated with the schema field, including tags, terms, documentation, and structured properties.



### Working with Fine-Grained Lineage

Schema fields are central to fine-grained (column-level) lineage. When defining lineage between datasets, you can specify which fields flow from upstream to downstream:


**Example lineage query showing field-level relationships**

```bash
# Find upstream fields of a specific schema field
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3AschemaField%3A(urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Apostgres%2Cpublic.orders%2CPROD)%2Cuser_id)&types=DownstreamOf'
```

This shows which upstream fields contribute to this field's values, enabling impact analysis at the column level.



## Integration Points

### Relationship with Datasets

Schema fields have a parent-child relationship with datasets. The dataset's `schemaMetadata` aspect defines the structure and metadata of fields, while individual schemaField entities allow direct metadata attachment at the field level.

Key integration points:

- Fields are referenced in `schemaMetadata` and `editableSchemaMetadata` aspects of datasets
- Field-level tags and terms can be set via dataset aspects (`schemaMetadata`) or directly on schemaField entities
- The UI typically modifies `editableSchemaMetadata` on the dataset, while ingestion connectors set `schemaMetadata`

### Fine-Grained Lineage

Schema fields are essential for column-level lineage:

- **DataJob entities**: The `dataJobInputOutput` aspect can specify `inputDatasetFields` and `outputDatasetFields`
- **Dataset lineage**: The `upstreamLineage` aspect on datasets can include `fineGrainedLineages` that map specific fields
- **Lineage queries**: Field-level lineage appears as relationships between schemaField entities

### GraphQL API

The GraphQL API exposes schema field entities as first-class entities with the `SchemaFieldEntity` type. Key resolvers include:

- Fetching field metadata (tags, terms, documentation)
- Querying field lineage relationships
- Searching for fields across datasets

Note: Field fetching via GraphQL is controlled by the `schemaFieldEntityFetchEnabled` feature flag. When disabled, schema field metadata is accessed only through the parent dataset's schema aspects.

### Search and Discovery

Schema fields are indexed for search, enabling users to:

- Find datasets by column names
- Search for fields with specific tags or terms
- Discover fields by description content
- Filter by field-level classifications

## Notable Exceptions

### Dual Access Patterns

Schema field metadata can be accessed and modified in two ways:

1. **Via the parent dataset**: Using `schemaMetadata` or `editableSchemaMetadata` aspects on the dataset
2. **Directly on schemaField entities**: Using aspects like `globalTags`, `glossaryTerms`, `documentation` on the schemaField URN

Best practices:

- Ingestion connectors should use dataset-level aspects (`schemaMetadata`)
- UI edits typically use dataset-level aspects (`editableSchemaMetadata`)
- Direct schemaField entity updates are useful for programmatic bulk operations or when working with field-level lineage

### Feature Flag Dependency

The ability to fetch schemaField entities via GraphQL depends on the `schemaFieldEntityFetchEnabled` feature flag. When disabled:

- Schema field entities are not directly queryable
- Field metadata must be accessed through parent datasets
- Field-level operations may have limited functionality

This flag exists for performance reasons, as materializing individual field entities can be expensive for datasets with hundreds of columns.

### Field Path Encoding

Field paths in schemaField URNs must be URL-encoded if they contain special characters (spaces, special symbols, etc.). Always use the `make_schema_field_urn` utility function from `datahub.emitter.mce_builder` to construct URNs correctly:

```python
from datahub.emitter.mce_builder import make_schema_field_urn

# Automatically handles encoding
field_urn = make_schema_field_urn(
    parent_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
    field_path="first name"  # Will be encoded as "first%20name"
)
```

### V1 vs V2 Field Paths

DataHub supports two field path formats:

- **V1**: Simple dot notation (e.g., `address.zipcode`)
- **V2**: Type-aware notation (e.g., `[version=2.0].[type=struct].address.[type=string].zipcode`)

V2 field paths are required for:

- Union types where field names alone are ambiguous
- Complex nested structures with type information
- Precise field path disambiguation

Most simple schemas can use v1 field paths. Use v2 when dealing with complex types or when ingestion connectors generate them.



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

#### schemaFieldKey
Key for a SchemaField



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| parent | string | ✓ | Parent associated with the schema field | Searchable |
| fieldPath | string | ✓ | fieldPath identifying the schema field | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "schemaFieldKey"
  },
  "name": "SchemaFieldKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "fieldType": "URN"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "parent",
      "doc": "Parent associated with the schema field"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "fieldPath",
      "doc": "fieldPath identifying the schema field"
    }
  ],
  "doc": "Key for a SchemaField"
}
```





#### schemafieldInfo
None



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string |  |  | Searchable |
| schemaFieldAliases | string[] |  | Used to store field path variations for the schemaField urn. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "schemafieldInfo"
  },
  "name": "SchemaFieldInfo",
  "namespace": "com.linkedin.schemafield",
  "fields": [
    {
      "Searchable": {
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "KEYWORD",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": [
        "null",
        "string"
      ],
      "name": "name",
      "default": null
    },
    {
      "Searchable": {
        "/*": {
          "fieldType": "URN",
          "queryByDefault": true
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "schemaFieldAliases",
      "default": null,
      "doc": "Used to store field path variations for the schemaField urn."
    }
  ]
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





#### businessAttributes
BusinessAttribute aspect used for applying it to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| businessAttribute | BusinessAttributeAssociation |  | Business Attribute for this field. | → BusinessAttributeOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "businessAttributes"
  },
  "name": "BusinessAttributes",
  "namespace": "com.linkedin.businessattribute",
  "fields": [
    {
      "Relationship": {
        "/businessAttributeUrn": {
          "entityTypes": [
            "businessAttribute"
          ],
          "name": "BusinessAttributeOf"
        }
      },
      "SearchableRef": {
        "/businessAttributeUrn": {
          "boostScore": 0.5,
          "fieldName": "businessAttributeRef",
          "fieldType": "URN",
          "refType": "businessAttribute"
        }
      },
      "type": [
        "null",
        {
          "type": "record",
          "name": "BusinessAttributeAssociation",
          "namespace": "com.linkedin.businessattribute",
          "fields": [
            {
              "Searchable": {
                "fieldName": "schemaFieldBusinessAttribute",
                "includeSystemModifiedAt": true,
                "queryByDefault": false,
                "systemModifiedAtFieldName": "schemaFieldBusinessAttributeModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.BusinessAttributeUrn"
              },
              "type": "string",
              "name": "businessAttributeUrn",
              "doc": "Urn of the applied businessAttribute"
            }
          ]
        }
      ],
      "name": "businessAttribute",
      "default": null,
      "doc": "Business Attribute for this field."
    }
  ],
  "doc": "BusinessAttribute aspect used for applying it to an entity"
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





#### schemaFieldAliases
None



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| aliases | string[] |  | Used to store aliases | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "schemaFieldAliases"
  },
  "name": "SchemaFieldAliases",
  "namespace": "com.linkedin.schemafield",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "schemaFieldAliases",
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
      "name": "aliases",
      "default": null,
      "doc": "Used to store aliases"
    }
  ]
}
```





#### documentation
Aspect used for storing all applicable documentations on assets.
This aspect supports multiple documentations from different sources.
There is an implicit assumption that there is only one documentation per
   source.
For example, if there are two documentations from the same source, the
   latest one will overwrite the previous one.
If there are two documentations from different sources, both will be
   stored.
Future evolution considerations:
The first entity that uses this aspect is Schema Field. We will expand this
    aspect to other entities eventually.
The values of the documentation are not currently searchable. This will be
    changed once this aspect develops opinion on which documentation entry is
    the authoritative one.
Ensuring that there is only one documentation per source is a business
    rule that is not enforced by the aspect yet. This will currently be enforced by the
    application that uses this aspect. We will eventually enforce this rule in
    the aspect using AspectMutators.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| documentations | DocumentationAssociation[] | ✓ | Documentations associated with this asset. We could be receiving docs from different sources |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "documentation"
  },
  "name": "Documentation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DocumentationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "documentation",
              "doc": "Description of this asset"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "documentationAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "documentationAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "documentationAttributionDates",
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
          "doc": "Properties of applied documentation including the attribution of the doc"
        }
      },
      "name": "documentations",
      "doc": "Documentations associated with this asset. We could be receiving docs from different sources"
    }
  ],
  "doc": "Aspect used for storing all applicable documentations on assets.\nThis aspect supports multiple documentations from different sources.\nThere is an implicit assumption that there is only one documentation per\n   source.\nFor example, if there are two documentations from the same source, the\n   latest one will overwrite the previous one.\nIf there are two documentations from different sources, both will be\n   stored.\nFuture evolution considerations:\nThe first entity that uses this aspect is Schema Field. We will expand this\n    aspect to other entities eventually.\nThe values of the documentation are not currently searchable. This will be\n    changed once this aspect develops opinion on which documentation entry is\n    the authoritative one.\nEnsuring that there is only one documentation per source is a business\n    rule that is not enforced by the aspect yet. This will currently be enforced by the\n    application that uses this aspect. We will eventually enforce this rule in\n    the aspect using AspectMutators."
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





#### logicalParent
Relates a physical asset to a logical model.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| parent | [Edge](#edge) |  |  | Searchable, → PhysicalInstanceOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "logicalParent"
  },
  "name": "LogicalParent",
  "namespace": "com.linkedin.logical",
  "fields": [
    {
      "Relationship": {
        "/destinationUrn": {
          "createdActor": "parent/created/actor",
          "createdOn": "parent/created/time",
          "entityTypes": [
            "dataset",
            "schemaField"
          ],
          "name": "PhysicalInstanceOf",
          "properties": "parent/properties",
          "updatedActor": "parent/lastModified/actor",
          "updatedOn": "parent/lastModified/time"
        }
      },
      "Searchable": {
        "/destinationUrn": {
          "addToFilters": true,
          "fieldName": "logicalParent",
          "fieldType": "URN",
          "filterNameOverride": "Physical Instance Of",
          "hasValuesFieldName": "hasLogicalParent",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "record",
          "name": "Edge",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "sourceUrn",
              "default": null,
              "doc": "Urn of the source of this relationship edge.\nIf not specified, assumed to be the entity that this aspect belongs to."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "destinationUrn",
              "doc": "Urn of the destination of this relationship edge."
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
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "properties",
              "default": null,
              "doc": "A generic properties bag that allows us to store specific information on this graph edge."
            }
          ],
          "doc": "A common structure to represent all edges to entities when used inside aspects as collections\nThis ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically."
        }
      ],
      "name": "parent",
      "default": null
    }
  ],
  "doc": "Relates a physical asset to a logical model."
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

#### Edge

A common structure to represent all edges to entities when used inside aspects as collections
This ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically.

**Fields:**

- `sourceUrn` (string?): Urn of the source of this relationship edge. If not specified, assumed to be ...
- `destinationUrn` (string): Urn of the destination of this relationship edge.
- `created` (AuditStamp?): Audit stamp containing who created this relationship edge and when
- `lastModified` (AuditStamp?): Audit stamp containing who last modified this relationship edge and when
- `properties` (map?): A generic properties bag that allows us to store specific information on this...

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
- PhysicalInstanceOf (via `logicalParent.parent`)
#### Outgoing
These are the relationships stored in this entity's aspects
- BusinessAttributeOf

   - BusinessAttribute via `businessAttributes.businessAttribute`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
- PhysicalInstanceOf

   - Dataset via `logicalParent.parent`
- TaggedWith

   - Tag via `globalTags.tags`
- TermedWith

   - GlossaryTerm via `glossaryTerms.terms.urn`
#### Incoming
These are the relationships stored in other entity's aspects
- DownstreamOf

   - Dataset via `upstreamLineage.fineGrainedLineages`
- ForeignKeyTo

   - Dataset via `schemaMetadata.foreignKeys.foreignFields`
   - GlossaryTerm via `schemaMetadata.foreignKeys.foreignFields`
   - Assertion via `assertionInfo.schemaAssertion.schema.foreignKeys.foreignFields`
- PhysicalInstanceOf

   - Dataset via `logicalParent.parent`
- Consumes

   - DataJob via `dataJobInputOutput.inputDatasetFields`
- Produces

   - DataJob via `dataJobInputOutput.outputDatasetFields`
- consumesField

   - Chart via `inputFields.fields.schemaFieldUrn`
   - Dashboard via `inputFields.fields.schemaFieldUrn`
- Asserts

   - Assertion via `assertionInfo.datasetAssertion.fields`
   - Assertion via `assertionInfo.customAssertion.field`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
