# Form

The form entity is a core entity in DataHub's metadata model that enables structured metadata collection and compliance initiatives. Forms provide a centralized, template-based approach for capturing essential metadata across data assets through a collaborative, crowdsourced workflow.

## Identity

Forms are identified by a unique string identifier that is chosen by the creator.

A form URN is structured as: `urn:li:form:<form-id>`

For example: `urn:li:form:metadataInitiative2024`

The form identifier should be meaningful and descriptive, typically representing the initiative or purpose of the form (e.g., `piiClassification2024`, `dataQualityCompliance`, `retentionPolicyForm`).

## Important Capabilities

### Form Metadata and Configuration

Forms are defined using the `formInfo` aspect, which contains all the core information about a form:

- **Name and Description**: Display name and detailed description of the form's purpose
- **Type**: Two types are supported:
  - `COMPLETION`: Forms designed purely for collecting metadata fields on entities
  - `VERIFICATION`: Forms used to verify that entities comply with a policy via presence of specific metadata fields
- **Prompts**: A list of prompts (questions) that users need to respond to. Each prompt represents a requirement to fill out specific metadata. Prompts currently support:
  - `STRUCTURED_PROPERTY`: Prompts to apply structured properties to entities
  - `FIELDS_STRUCTURED_PROPERTY`: Prompts to apply structured properties to schema fields (columns) of datasets
- **Actor Assignment**: Defines who should complete the form. Forms can be assigned to:
  - Asset owners (default)
  - Specific users
  - Specific groups

Here's an example of form metadata:


**Python SDK: Create a form**

```python
# Inlined from /metadata-ingestion/examples/library/form_create.py
import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    FormActorAssignmentClass,
    FormInfoClass,
    FormPromptClass,
    FormPromptTypeClass,
    FormTypeClass,
    StructuredPropertyParamsClass,
)
from datahub.metadata.urns import FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Use the structured property created by structured_property_create_basic.py
retention_property_urn = "urn:li:structuredProperty:io.acryl.privacy.retentionTime"

# define the prompts for our form
prompt_1 = FormPromptClass(
    id="1",  # ensure IDs are globally unique
    title="Data Retention Policy",
    type=FormPromptTypeClass.STRUCTURED_PROPERTY,  # structured property type prompt
    structuredPropertyParams=StructuredPropertyParamsClass(urn=retention_property_urn),
    required=True,
)
prompt_2 = FormPromptClass(
    id="2",  # ensure IDs are globally unique
    title="Field-Level Retention",
    type=FormPromptTypeClass.FIELDS_STRUCTURED_PROPERTY,  # structured property prompt on dataset schema fields
    structuredPropertyParams=StructuredPropertyParamsClass(urn=retention_property_urn),
    required=False,  # dataset schema fields prompts should not be required
)

form_urn = FormUrn("metadata_initiative_1")
form_info_aspect = FormInfoClass(
    name="Metadata Initiative 2024",
    description="Please respond to this form for metadata compliance purposes",
    type=FormTypeClass.VERIFICATION,
    actors=FormActorAssignmentClass(owners=True),
    prompts=[prompt_1, prompt_2],
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=form_info_aspect,
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)
rest_emitter.emit_mcp(event)
print(f"Created form: {form_urn}")

```



### Dynamic Form Assignment

Forms can be assigned to entities in two ways:

1. **Explicit Assignment**: Directly assign a form to specific entity URNs
2. **Dynamic Assignment**: Use the `dynamicFormAssignment` aspect to automatically assign forms to entities matching certain criteria

The `dynamicFormAssignment` aspect enables rule-based form assignment by specifying filters. Entities matching the filter criteria will automatically have the form applied. The filter supports:

- Entity type (e.g., datasets, dashboards, charts)
- Platform (e.g., snowflake, bigquery, postgres)
- Domain membership
- Container membership

This dynamic approach is particularly powerful for large-scale compliance initiatives where you want to apply forms to all assets of a certain type or within a specific domain.


**Python SDK: Assign forms dynamically with filters**

```python
# Inlined from /metadata-ingestion/examples/library/form_create_with_dynamic_assignment.py
import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    CriterionClass,
    DynamicFormAssignmentClass,
    FilterClass,
    FormActorAssignmentClass,
    FormInfoClass,
    FormPromptClass,
    FormPromptTypeClass,
    FormTypeClass,
    StructuredPropertyParamsClass,
)
from datahub.metadata.urns import FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a form that will be dynamically assigned to all Snowflake datasets
# in the "Finance" domain

# Define the form metadata
form_urn = FormUrn("snowflake_finance_compliance")
form_info = FormInfoClass(
    name="Snowflake Finance Data Compliance",
    description="Compliance form for all Snowflake datasets in the Finance domain",
    type=FormTypeClass.VERIFICATION,
    actors=FormActorAssignmentClass(owners=True),
    prompts=[
        FormPromptClass(
            id="retention_time_prompt",
            title="Data Retention Period",
            description="Specify how long this data should be retained",
            type=FormPromptTypeClass.STRUCTURED_PROPERTY,
            structuredPropertyParams=StructuredPropertyParamsClass(
                urn="urn:li:structuredProperty:io.acryl.dataRetentionTime"
            ),
            required=True,
        ),
        FormPromptClass(
            id="pii_classification_prompt",
            title="PII Classification",
            description="Classify whether this dataset contains PII",
            type=FormPromptTypeClass.STRUCTURED_PROPERTY,
            structuredPropertyParams=StructuredPropertyParamsClass(
                urn="urn:li:structuredProperty:io.acryl.piiClassification"
            ),
            required=True,
        ),
    ],
)

# Define dynamic assignment filter
# This form will be assigned to all entities matching these criteria
dynamic_assignment = DynamicFormAssignmentClass(
    filter=FilterClass(
        criteria=[
            CriterionClass(
                field="platform",
                values=["urn:li:dataPlatform:snowflake"],
                condition="EQUAL",
            ),
            CriterionClass(
                field="domains",
                values=["urn:li:domain:finance"],
                condition="EQUAL",
            ),
            CriterionClass(
                field="_entityType",
                values=["urn:li:entityType:dataset"],
                condition="EQUAL",
            ),
        ]
    )
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

# Emit the form info
form_info_event = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=form_info,
)
rest_emitter.emit(form_info_event)

# Emit the dynamic assignment
dynamic_assignment_event = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=dynamic_assignment,
)
rest_emitter.emit(dynamic_assignment_event)

log.info(
    f"Created form {form_urn} with dynamic assignment to Snowflake datasets in Finance domain"
)

```



### Form Assignment on Entities

When forms are assigned to entities (either explicitly or dynamically), the assignments are tracked via the `forms` aspect on the target entity. This aspect maintains:

- **Incomplete Forms**: Forms that have outstanding prompts to be completed
- **Complete Forms**: Forms where all required prompts have been responded to
- **Verifications**: For verification-type forms, tracks which forms have been successfully verified
- **Prompt Status**: For each form, tracks which prompts are complete vs. incomplete, along with timestamps

This allows DataHub to provide progress tracking, notifications, and analytics about form completion across your data catalog.

### Ownership

Forms can have owners assigned through the standard `ownership` aspect. Owners of forms are typically the governance or compliance team members who are responsible for managing and maintaining the form.


**Python SDK: Add owners to a form**

```python
# Inlined from /metadata-ingestion/examples/library/form_add_owner.py
import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import CorpUserUrn, FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Form URN to add owner to
form_urn = FormUrn("metadata_initiative_2024")

# Create ownership aspect
ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner=str(CorpUserUrn("governance_team")),
            type=OwnershipTypeClass.TECHNICAL_OWNER,
        )
    ],
    lastModified=AuditStampClass(
        time=0, actor="urn:li:corpuser:datahub", impersonator=None
    ),
)

# Create and emit metadata change proposal
event = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=ownership,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Added owner to form {form_urn}")

```



## Code Examples

### Creating and Managing Forms


**Python SDK: Create a verification form with multiple prompts**

```python
# Inlined from /metadata-ingestion/examples/library/form_create.py
import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    FormActorAssignmentClass,
    FormInfoClass,
    FormPromptClass,
    FormPromptTypeClass,
    FormTypeClass,
    StructuredPropertyParamsClass,
)
from datahub.metadata.urns import FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Use the structured property created by structured_property_create_basic.py
retention_property_urn = "urn:li:structuredProperty:io.acryl.privacy.retentionTime"

# define the prompts for our form
prompt_1 = FormPromptClass(
    id="1",  # ensure IDs are globally unique
    title="Data Retention Policy",
    type=FormPromptTypeClass.STRUCTURED_PROPERTY,  # structured property type prompt
    structuredPropertyParams=StructuredPropertyParamsClass(urn=retention_property_urn),
    required=True,
)
prompt_2 = FormPromptClass(
    id="2",  # ensure IDs are globally unique
    title="Field-Level Retention",
    type=FormPromptTypeClass.FIELDS_STRUCTURED_PROPERTY,  # structured property prompt on dataset schema fields
    structuredPropertyParams=StructuredPropertyParamsClass(urn=retention_property_urn),
    required=False,  # dataset schema fields prompts should not be required
)

form_urn = FormUrn("metadata_initiative_1")
form_info_aspect = FormInfoClass(
    name="Metadata Initiative 2024",
    description="Please respond to this form for metadata compliance purposes",
    type=FormTypeClass.VERIFICATION,
    actors=FormActorAssignmentClass(owners=True),
    prompts=[prompt_1, prompt_2],
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=form_info_aspect,
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)
rest_emitter.emit_mcp(event)
print(f"Created form: {form_urn}")

```




**Python SDK: Create a form with dynamic assignment**

```python
# Inlined from /metadata-ingestion/examples/library/form_create_with_dynamic_assignment.py
import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    CriterionClass,
    DynamicFormAssignmentClass,
    FilterClass,
    FormActorAssignmentClass,
    FormInfoClass,
    FormPromptClass,
    FormPromptTypeClass,
    FormTypeClass,
    StructuredPropertyParamsClass,
)
from datahub.metadata.urns import FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a form that will be dynamically assigned to all Snowflake datasets
# in the "Finance" domain

# Define the form metadata
form_urn = FormUrn("snowflake_finance_compliance")
form_info = FormInfoClass(
    name="Snowflake Finance Data Compliance",
    description="Compliance form for all Snowflake datasets in the Finance domain",
    type=FormTypeClass.VERIFICATION,
    actors=FormActorAssignmentClass(owners=True),
    prompts=[
        FormPromptClass(
            id="retention_time_prompt",
            title="Data Retention Period",
            description="Specify how long this data should be retained",
            type=FormPromptTypeClass.STRUCTURED_PROPERTY,
            structuredPropertyParams=StructuredPropertyParamsClass(
                urn="urn:li:structuredProperty:io.acryl.dataRetentionTime"
            ),
            required=True,
        ),
        FormPromptClass(
            id="pii_classification_prompt",
            title="PII Classification",
            description="Classify whether this dataset contains PII",
            type=FormPromptTypeClass.STRUCTURED_PROPERTY,
            structuredPropertyParams=StructuredPropertyParamsClass(
                urn="urn:li:structuredProperty:io.acryl.piiClassification"
            ),
            required=True,
        ),
    ],
)

# Define dynamic assignment filter
# This form will be assigned to all entities matching these criteria
dynamic_assignment = DynamicFormAssignmentClass(
    filter=FilterClass(
        criteria=[
            CriterionClass(
                field="platform",
                values=["urn:li:dataPlatform:snowflake"],
                condition="EQUAL",
            ),
            CriterionClass(
                field="domains",
                values=["urn:li:domain:finance"],
                condition="EQUAL",
            ),
            CriterionClass(
                field="_entityType",
                values=["urn:li:entityType:dataset"],
                condition="EQUAL",
            ),
        ]
    )
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

# Emit the form info
form_info_event = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=form_info,
)
rest_emitter.emit(form_info_event)

# Emit the dynamic assignment
dynamic_assignment_event = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=dynamic_assignment,
)
rest_emitter.emit(dynamic_assignment_event)

log.info(
    f"Created form {form_urn} with dynamic assignment to Snowflake datasets in Finance domain"
)

```




**Python SDK: Assign a form to specific entities**

```python
# Inlined from /metadata-ingestion/examples/library/form_assign_to_entities.py
import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import FormAssociationClass, FormsClass
from datahub.metadata.urns import DatasetUrn, FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Form to assign
form_urn = FormUrn("metadata_initiative_2024")

# Entities to assign the form to
dataset_urns = [
    DatasetUrn(platform="snowflake", name="prod.analytics.customer_data", env="PROD"),
    DatasetUrn(platform="snowflake", name="prod.analytics.sales_data", env="PROD"),
]

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Assign the form to each entity by updating the forms aspect
for dataset_urn in dataset_urns:
    # Create a forms aspect with the form marked as incomplete
    forms_aspect = FormsClass(
        incompleteForms=[FormAssociationClass(urn=str(form_urn))],
        completedForms=[],
        verifications=[],
    )

    # Emit the forms aspect for the entity
    event = MetadataChangeProposalWrapper(
        entityUrn=str(dataset_urn),
        aspect=forms_aspect,
    )
    rest_emitter.emit(event)
    log.info(f"Assigned form {form_urn} to entity {dataset_urn}")

log.info(f"Successfully assigned form to {len(dataset_urns)} entities")

```




**Python SDK: Remove a form from entities**

```python
# Inlined from /metadata-ingestion/examples/library/form_remove_from_entities.py
import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import FormsClass
from datahub.metadata.urns import DatasetUrn, FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Form to remove
form_urn = FormUrn("metadata_initiative_2024")

# Entities to remove the form from
dataset_urns = [
    DatasetUrn(platform="snowflake", name="prod.analytics.customer_data", env="PROD"),
    DatasetUrn(platform="snowflake", name="prod.analytics.sales_data", env="PROD"),
]

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Remove the form from each entity by setting empty forms aspect
for dataset_urn in dataset_urns:
    # Create an empty forms aspect
    # This will remove all form assignments from the entity
    forms_aspect = FormsClass(
        incompleteForms=[],
        completedForms=[],
        verifications=[],
    )

    # Emit the forms aspect for the entity
    event = MetadataChangeProposalWrapper(
        entityUrn=str(dataset_urn),
        aspect=forms_aspect,
    )
    rest_emitter.emit(event)
    log.info(f"Removed forms from entity {dataset_urn}")

log.info(f"Successfully removed forms from {len(dataset_urns)} entities")

```



### Querying Forms

Forms can be queried via the REST API to retrieve form definitions and check their assignment status.


**Query a form via REST API**

```shell
curl 'http://localhost:8080/entities/urn%3Ali%3Aform%3AmetadataInitiative2024'
```

This will return the form entity with all its aspects including `formInfo`, `dynamicFormAssignment` (if configured), and `ownership`.




**Check form assignments on an entity**

```shell
curl 'http://localhost:8080/entities/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Asnowflake,mydb.schema.table,PROD)?aspects=forms'
```

This returns the `forms` aspect showing all incomplete and completed forms for the dataset.



## Integration Points

### Relationship with Other Entities

Forms have several key integration points in the DataHub ecosystem:

1. **Structured Properties**: Forms primarily interact with structured properties through prompts. Each prompt can require users to fill out specific structured properties on entities or schema fields.

2. **Assets (Datasets, Dashboards, Charts, etc.)**: Forms are assigned to data assets to collect metadata. The relationship is tracked through:

   - The `forms` aspect on the target entity
   - The `dynamicFormAssignment` filter criteria on the form

3. **Users and Groups**: Forms are assigned to specific actors (users or groups) who are responsible for completing them. This creates a workflow where:

   - Forms appear in assignees' task lists
   - Assignees receive notifications about pending forms
   - Completion tracking is maintained per user

4. **Domains and Containers**: Forms can be automatically assigned based on domain or container membership, enabling governance teams to apply compliance requirements at the domain or container level.

### Typical Usage Patterns

1. **Compliance Initiatives**: Organizations create verification forms to ensure critical assets meet compliance requirements (e.g., PII classification, data retention policies)

2. **Metadata Enrichment**: Completion forms help gather missing documentation, ownership, and domain assignments for high-value assets

3. **Governance Workflows**: Forms enable systematic metadata collection by routing tasks to domain experts who are best positioned to provide accurate information

4. **Quality Improvement**: Forms can be used to incrementally improve metadata quality by focusing on the most important assets first

## Notable Exceptions

### Form Prompt Types

Currently, forms only support structured property prompts. Each prompt requires users to set values for structured properties on entities or their schema fields. Future extensions may include prompts for other metadata types (e.g., documentation requirements, tag requirements).

### Schema Field Prompts

When using `FIELDS_STRUCTURED_PROPERTY` prompt types, these prompts should not be marked as required, as they apply to an indeterminate number of schema fields. The form is considered complete when users have appropriately responded to the field-level prompts.

### Assignment Logic

Forms are assigned to entities through two independent mechanisms:

- **Direct assignment**: Explicitly specified entity URNs
- **Dynamic assignment**: Filter-based automatic assignment

An entity can have the same form assigned through both mechanisms. The `forms` aspect on the entity consolidates all assignments regardless of how they were made.

### Form Lifecycle

Once a form is created and published, assignees will see it in their task queue. Deleting a form requires:

1. Hard deleting the form entity itself
2. Removing all references to the form from entities that have it assigned

This two-step process ensures referential integrity across the metadata graph.



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

#### formInfo
Information about a form to help with filling out metadata on entities.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Display name of the form | Searchable |
| description | string |  | Description of the form |  |
| type | FormType | ✓ | The type of this form | Searchable |
| prompts | FormPrompt[] | ✓ | List of prompts to present to the user to encourage filling out metadata |  |
| actors | FormActorAssignment | ✓ | Who the form is assigned to, e.g. who should see the form when visiting the entity page or govern... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "formInfo"
  },
  "name": "FormInfo",
  "namespace": "com.linkedin.form",
  "fields": [
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the form"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of the form"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "COMPLETION": "A form simply used for collecting metadata fields for an entity.",
          "VERIFICATION": "This form is used for \"verifying\" that entities comply with a policy via presence of a specific set of metadata fields."
        },
        "name": "FormType",
        "namespace": "com.linkedin.form",
        "symbols": [
          "COMPLETION",
          "VERIFICATION"
        ]
      },
      "name": "type",
      "default": "COMPLETION",
      "doc": "The type of this form"
    },
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormPrompt",
          "namespace": "com.linkedin.form",
          "fields": [
            {
              "Searchable": {
                "fieldName": "promptId",
                "fieldType": "KEYWORD",
                "queryByDefault": false
              },
              "type": "string",
              "name": "id",
              "doc": "The unique id for this prompt. This must be GLOBALLY unique."
            },
            {
              "type": "string",
              "name": "title",
              "doc": "The title of this prompt"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "description",
              "default": null,
              "doc": "The description of this prompt"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FIELDS_STRUCTURED_PROPERTY": "This prompt is meant to apply a structured property to a schema fields entity",
                  "STRUCTURED_PROPERTY": "This prompt is meant to apply a structured property to an entity"
                },
                "name": "FormPromptType",
                "namespace": "com.linkedin.form",
                "symbols": [
                  "STRUCTURED_PROPERTY",
                  "FIELDS_STRUCTURED_PROPERTY"
                ]
              },
              "name": "type",
              "doc": "The type of prompt"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "StructuredPropertyParams",
                  "namespace": "com.linkedin.form",
                  "fields": [
                    {
                      "Searchable": {
                        "fieldName": "structuredPropertyPromptUrns",
                        "fieldType": "URN"
                      },
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "urn",
                      "doc": "The structured property that is required on this entity"
                    }
                  ]
                }
              ],
              "name": "structuredPropertyParams",
              "default": null,
              "doc": "An optional set of information specific to structured properties prompts.\nThis should be filled out if the prompt is type STRUCTURED_PROPERTY or FIELDS_STRUCTURED_PROPERTY."
            },
            {
              "type": "boolean",
              "name": "required",
              "default": false,
              "doc": "Whether the prompt is required to be completed, in order for the form to be marked as complete."
            }
          ],
          "doc": "A prompt to present to the user to encourage filling out metadata"
        }
      },
      "name": "prompts",
      "default": [],
      "doc": "List of prompts to present to the user to encourage filling out metadata"
    },
    {
      "type": {
        "type": "record",
        "name": "FormActorAssignment",
        "namespace": "com.linkedin.form",
        "fields": [
          {
            "Searchable": {
              "fieldName": "isOwnershipForm",
              "fieldType": "BOOLEAN"
            },
            "type": "boolean",
            "name": "owners",
            "default": true,
            "doc": "Whether the form should be assigned to the owners of assets that it is applied to.\nThis is the default."
          },
          {
            "Searchable": {
              "/*": {
                "fieldName": "assignedGroups",
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
            "name": "groups",
            "default": null,
            "doc": "Optional: Specific set of groups that are targeted by this form assignment."
          },
          {
            "Searchable": {
              "/*": {
                "fieldName": "assignedUsers",
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
            "name": "users",
            "default": null,
            "doc": "Optional: Specific set of users that are targeted by this form assignment."
          }
        ]
      },
      "name": "actors",
      "default": {
        "groups": null,
        "owners": true,
        "users": null
      },
      "doc": "Who the form is assigned to, e.g. who should see the form when visiting the entity page or governance center"
    }
  ],
  "doc": "Information about a form to help with filling out metadata on entities."
}
```





#### dynamicFormAssignment
Information about how a form is assigned to entities dynamically. Provide a filter to
match a set of entities instead of explicitly applying a form to specific entities.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| filter | Filter | ✓ | The filter applied when assigning this form to entities. Entities that match this filter will hav... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dynamicFormAssignment",
    "schemaVersion": 2
  },
  "name": "DynamicFormAssignment",
  "namespace": "com.linkedin.form",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "Filter",
        "namespace": "com.linkedin.metadata.query.filter",
        "fields": [
          {
            "type": [
              "null",
              {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "ConjunctiveCriterion",
                  "namespace": "com.linkedin.metadata.query.filter",
                  "fields": [
                    {
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "Criterion",
                          "namespace": "com.linkedin.metadata.query.filter",
                          "fields": [
                            {
                              "type": "string",
                              "name": "field",
                              "doc": "The name of the field that the criterion refers to"
                            },
                            {
                              "type": {
                                "type": "array",
                                "items": "string"
                              },
                              "name": "values",
                              "default": [],
                              "doc": "Values to match against the field (one match suffices unless otherwise documented)."
                            },
                            {
                              "type": {
                                "type": "enum",
                                "symbolDocs": {
                                  "ANCESTORS_INCL": "Represent the relation: URN field matches any nested parent in addition to the given URN",
                                  "CONTAIN": "Represent the relation: String field contains value, e.g. name contains Profile",
                                  "DESCENDANTS_INCL": "Represent the relation: URN field any nested children in addition to the given URN",
                                  "END_WITH": "Represent the relation: String field ends with value, e.g. name ends with Event",
                                  "EQUAL": "Represent the relation: field = value, e.g. platform = hdfs",
                                  "EXISTS": "Represents the relation: field exists and is non-empty, e.g. owners is not null and != [] (empty)",
                                  "GREATER_THAN": "Represent the relation greater than, e.g. ownerCount > 5",
                                  "GREATER_THAN_OR_EQUAL_TO": "Represent the relation greater than or equal to, e.g. ownerCount >= 5",
                                  "IEQUAL": "Represent the relation: field = value and support case insensitive values, e.g. platform = hdfs",
                                  "IN": "Represent the relation: String field is one of the array values to, e.g. name in [\"Profile\", \"Event\"]",
                                  "IS_NULL": "Represent the relation: field is null, e.g. platform is null",
                                  "LESS_THAN": "Represent the relation less than, e.g. ownerCount < 3",
                                  "LESS_THAN_OR_EQUAL_TO": "Represent the relation less than or equal to, e.g. ownerCount <= 3",
                                  "RELATED_INCL": "Represent the relation: URN field matches any nested child or parent in addition to the given URN",
                                  "START_WITH": "Represent the relation: String field starts with value, e.g. name starts with PageView"
                                },
                                "name": "Condition",
                                "namespace": "com.linkedin.metadata.query.filter",
                                "symbols": [
                                  "CONTAIN",
                                  "END_WITH",
                                  "EQUAL",
                                  "IEQUAL",
                                  "IS_NULL",
                                  "EXISTS",
                                  "GREATER_THAN",
                                  "GREATER_THAN_OR_EQUAL_TO",
                                  "IN",
                                  "LESS_THAN",
                                  "LESS_THAN_OR_EQUAL_TO",
                                  "START_WITH",
                                  "DESCENDANTS_INCL",
                                  "ANCESTORS_INCL",
                                  "RELATED_INCL"
                                ],
                                "doc": "The matching condition in a filter criterion"
                              },
                              "name": "condition",
                              "default": "EQUAL",
                              "doc": "The condition for the criterion, e.g. EQUAL, START_WITH"
                            },
                            {
                              "type": "boolean",
                              "name": "negated",
                              "default": false,
                              "doc": "Whether the condition should be negated"
                            }
                          ],
                          "doc": "A criterion for matching a field with given values"
                        }
                      },
                      "name": "and",
                      "doc": "A list of and criteria the filter applies to the query"
                    }
                  ],
                  "doc": "A list of criterion and'd together."
                }
              }
            ],
            "name": "or",
            "default": null,
            "doc": "A list of disjunctive criterion for the filter. (or operation to combine filters)"
          },
          {
            "type": [
              "null",
              {
                "type": "array",
                "items": "com.linkedin.metadata.query.filter.Criterion"
              }
            ],
            "name": "criteria",
            "default": null,
            "doc": "Deprecated! A list of conjunctive criterion for the filter. If \"or\" field is provided, then this field is ignored."
          }
        ],
        "doc": "The filter for finding a record or a collection of records"
      },
      "name": "filter",
      "doc": "The filter applied when assigning this form to entities. Entities that match this filter\nwill have this form applied to them. Right now this filter only supports filtering by\nplatform, entity type, container, and domain through the UI."
    }
  ],
  "doc": "Information about how a form is assigned to entities dynamically. Provide a filter to\nmatch a set of entities instead of explicitly applying a form to specific entities."
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
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
