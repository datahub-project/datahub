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

<details>
<summary>Python SDK: Create a form</summary>

```python
{{ inline /metadata-ingestion/examples/library/form_create.py show_path_as_comment }}
```

</details>

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

<details>
<summary>Python SDK: Assign forms dynamically with filters</summary>

```python
{{ inline /metadata-ingestion/examples/library/form_create_with_dynamic_assignment.py show_path_as_comment }}
```

</details>

### Form Assignment on Entities

When forms are assigned to entities (either explicitly or dynamically), the assignments are tracked via the `forms` aspect on the target entity. This aspect maintains:

- **Incomplete Forms**: Forms that have outstanding prompts to be completed
- **Complete Forms**: Forms where all required prompts have been responded to
- **Verifications**: For verification-type forms, tracks which forms have been successfully verified
- **Prompt Status**: For each form, tracks which prompts are complete vs. incomplete, along with timestamps

This allows DataHub to provide progress tracking, notifications, and analytics about form completion across your data catalog.

### Ownership

Forms can have owners assigned through the standard `ownership` aspect. Owners of forms are typically the governance or compliance team members who are responsible for managing and maintaining the form.

<details>
<summary>Python SDK: Add owners to a form</summary>

```python
{{ inline /metadata-ingestion/examples/library/form_add_owner.py show_path_as_comment }}
```

</details>

## Code Examples

### Creating and Managing Forms

<details>
<summary>Python SDK: Create a verification form with multiple prompts</summary>

```python
{{ inline /metadata-ingestion/examples/library/form_create.py show_path_as_comment }}
```

</details>

<details>
<summary>Python SDK: Create a form with dynamic assignment</summary>

```python
{{ inline /metadata-ingestion/examples/library/form_create_with_dynamic_assignment.py show_path_as_comment }}
```

</details>

<details>
<summary>Python SDK: Assign a form to specific entities</summary>

```python
{{ inline /metadata-ingestion/examples/library/form_assign_to_entities.py show_path_as_comment }}
```

</details>

<details>
<summary>Python SDK: Remove a form from entities</summary>

```python
{{ inline /metadata-ingestion/examples/library/form_remove_from_entities.py show_path_as_comment }}
```

</details>

### Querying Forms

Forms can be queried via the REST API to retrieve form definitions and check their assignment status.

<details>
<summary>Query a form via REST API</summary>

```shell
curl 'http://localhost:8080/entities/urn%3Ali%3Aform%3AmetadataInitiative2024'
```

This will return the form entity with all its aspects including `formInfo`, `dynamicFormAssignment` (if configured), and `ownership`.

</details>

<details>
<summary>Check form assignments on an entity</summary>

```shell
curl 'http://localhost:8080/entities/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Asnowflake,mydb.schema.table,PROD)?aspects=forms'
```

This returns the `forms` aspect showing all incomplete and completed forms for the dataset.

</details>

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
