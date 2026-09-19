

# Workflow Reference

> **Availability:** DataHub Cloud only

> **Note**: Action Workflows is currently in **Private Beta**. To enable this feature, please reach out to the DataHub team.

This page is the exhaustive JSON syntax reference for an Action Workflow definition — the JSON document you submit as the `input` argument to the `upsertActionWorkflow` GraphQL mutation. For the conceptual introduction, see [Action Workflows](action-workflows.md). For a worked end-to-end example, see the [Workflow Tutorial](action-workflows-tutorial.md).

The reference is organised top-down: the top-level definition first, then each primitive it composes, then the shared filter dialect, then the resolver catalogue, then the enum tables, then the GraphQL surface.

> **The launching entity.** Throughout this reference, the _launching entity_ is the entity from whose profile page the user launched the workflow — for example, the dataset on which the requester clicked the workflow's launch icon. Resolvers and filters that specify `"source": "launching"` begin their traversal from this entity.

## How to Apply a Workflow Definition

A workflow definition is a JSON object submitted as the `input` argument to the `upsertActionWorkflow` GraphQL mutation. There are three common ways to apply one:

### DataHub Python SDK

```python
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

graph = DataHubGraph(DatahubClientConfig(
    server="https://your-datahub-instance.acryl.io/gms",
    token="YOUR_ACCESS_TOKEN"
))

UPSERT = """
mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
  upsertActionWorkflow(input: $input) { urn }
}
"""

with open("dataset-promotion-workflow.json") as f:
    workflow_input = json.load(f)

result = graph.execute_graphql(UPSERT, variables={"input": workflow_input})
print(result["upsertActionWorkflow"]["urn"])
```

### Raw GraphQL

```bash
curl -X POST https://your-datahub-instance.acryl.io/api/graphql \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "query": "mutation upsertActionWorkflow(\$input: UpsertActionWorkflowInput!) { upsertActionWorkflow(input: \$input) { urn } }",
  "variables": { "input": $(cat dataset-promotion-workflow.json) }
}
EOF
```

### datahub CLI

There is no dedicated CLI subcommand for workflow upsert today. Wrap the JSON in a small Python or shell script that calls the Python SDK shown above. For most customers, GitOps-managed workflow JSONs are applied through a CI job that imports the SDK and calls `upsertActionWorkflow` directly.

## Workflow Definition

The top-level shape of a workflow definition.

```json
{
  "urn": "urn:li:actionWorkflow:my-workflow",
  "name": "My Workflow",
  "description": "Free-form description shown to users.",
  "category": "CUSTOM",
  "customCategory": "GOVERNANCE",
  "trigger": { ... },
  "steps": [ ... ]
}
```

| Field            | Type                   | Required             | Description                                                                                                                             |
| ---------------- | ---------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `urn`            | string                 | No                   | URN of an existing workflow to update. Omit to create a new workflow — DataHub generates a UUID-based URN.                              |
| `name`           | string                 | Yes                  | Display name of the workflow, shown in the entrypoint launch icon and the Task Center.                                                  |
| `description`    | string                 | No                   | Description shown to requesters before they launch the workflow.                                                                        |
| `category`       | enum                   | Yes                  | `ACCESS` for access-request workflows, `CUSTOM` for everything else. See [Categories](#categories).                                     |
| `customCategory` | string                 | If `category=CUSTOM` | Free-form string to group related custom workflows (e.g., `GOVERNANCE`, `LIFECYCLE`, `PROPOSAL`). Required when `category` is `CUSTOM`. |
| `trigger`        | [Trigger](#trigger)    | Yes                  | How and where requests are launched.                                                                                                    |
| `steps`          | array of [Step](#step) | Yes                  | Ordered approval steps. Must contain at least one step.                                                                                 |

## Trigger

Describes how requests against this workflow are launched.

```json
{
  "trigger": {
    "type": "FORM_SUBMITTED",
    "form": { ... }
  }
}
```

| Field  | Type          | Required | Description                                                                                     |
| ------ | ------------- | -------- | ----------------------------------------------------------------------------------------------- |
| `type` | enum          | Yes      | Only `FORM_SUBMITTED` is supported today — the workflow is launched when a user submits a form. |
| `form` | [Form](#form) | Yes      | The form the user fills in to start a request.                                                  |

## Form

Defines the entry points where the workflow's launch icon appears and the fields the requester fills in.

```json
{
  "form": {
    "entityTypes": ["DATASET"],
    "entrypoints": [ ... ],
    "fields": [ ... ],
    "excludedDefaultFields": ["EXPIRATION_DATE"]
  }
}
```

| Field                   | Type                                     | Required | Description                                                                                                                                    |
| ----------------------- | ---------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `entityTypes`           | array of [EntityType](#entity-types)     | No       | Entity types whose profile pages can launch this workflow. Omit for context-free workflows that only launch from the `HOME` entrypoint.        |
| `entrypoints`           | array of [Entrypoint](#entrypoint)       | Yes      | UI surfaces where the workflow's launch icon appears.                                                                                          |
| `fields`                | array of [Field](#field)                 | Yes      | The form fields the requester fills in. Submitted in order; fields with a `filterCondition` reveal themselves only when their condition holds. |
| `excludedDefaultFields` | array of [DefaultField](#default-fields) | No       | Default fields that DataHub adds to every form. List the ones you want hidden — supported values: `EXPIRATION_DATE`, `ADDITIONAL_NOTES`.       |

## Entrypoint

Where the workflow's launch icon appears in the UI.

```json
{
  "type": "ENTITY_PROFILE",
  "label": "Promote to Certified",
  "filter": {
    "operator": "AND",
    "filters": [
      {
        "field": "platform",
        "values": ["urn:li:dataPlatform:snowflake"],
        "condition": "EQUAL"
      }
    ]
  }
}
```

| Field    | Type                      | Required | Description                                                                                                                                                                                                                         |
| -------- | ------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`   | enum                      | Yes      | `HOME` (icon on the catalog home page) or `ENTITY_PROFILE` (icon on the profile pages of the entity types listed in the parent form's `entityTypes`).                                                                               |
| `label`  | string                    | Yes      | User-facing label on the launch icon.                                                                                                                                                                                               |
| `filter` | [Filter](#filter-dialect) | No       | Visibility predicate evaluated against the launching entity's attributes. Only entities matching the filter see the launch icon. Only meaningful when `type` is `ENTITY_PROFILE`. Omit to show on every entity of the listed types. |

## Field

A single form field the requester fills in.

```json
{
  "id": "justification",
  "name": "Reason for certification",
  "description": "Minimum 100 characters.",
  "valueType": "RICH_TEXT",
  "allowedEntityTypes": ["CORP_USER"],
  "allowedValues": [{ "stringValue": "30_DAYS" }],
  "cardinality": "SINGLE",
  "required": true,
  "validation": { "pattern": "[\\s\\S]{100,}", "errorMessage": "..." },
  "filterCondition": { ... },
  "dynamicSource": { ... },
  "valuesSource": { ... }
}
```

| Field                | Type                                           | Required | Description                                                                                                                                                                                                                                                    |
| -------------------- | ---------------------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                 | string                                         | Yes      | Workflow-local identifier. Other primitives reference this field via `formField:<id>` in filter expressions and `field:<id>` in resolver sources.                                                                                                              |
| `name`               | string                                         | Yes      | Display label shown above the input control.                                                                                                                                                                                                                   |
| `description`        | string                                         | No       | Helper text shown under the field.                                                                                                                                                                                                                             |
| `valueType`          | enum                                           | Yes      | One of `STRING`, `RICH_TEXT`, `URN`, `DATE`, `NUMBER`, `BOOLEAN`. See [Value Types](#value-types).                                                                                                                                                             |
| `allowedEntityTypes` | array of [EntityType](#entity-types)           | No       | Required when `valueType` is `URN`. Restricts the entity-picker to these types (e.g., `["CORP_USER"]`, `["GLOSSARY_TERM"]`).                                                                                                                                   |
| `allowedValues`      | array of [PropertyValue](#property-value)      | No       | Fixed enumeration of allowed values. Renders as a dropdown.                                                                                                                                                                                                    |
| `cardinality`        | enum                                           | Yes      | `SINGLE` (one value) or `MULTIPLE` (a list of values).                                                                                                                                                                                                         |
| `required`           | boolean                                        | No       | Defaults to `false`. Required fields must be populated before the request can be submitted.                                                                                                                                                                    |
| `validation`         | [FieldValidation](#field-validation)           | No       | Submit-time regex + length validation with a custom error message.                                                                                                                                                                                             |
| `filterCondition`    | [Filter](#filter-dialect)                      | No       | Visibility predicate. The field renders only when the filter evaluates to true. The filter can reference launching-entity attributes (`tags`, `platform`, etc.) and prior form-field values (via `formField:<id>`). See [Field Visibility](#field-visibility). |
| `dynamicSource`      | [DynamicSource](#dynamicsource)                | No       | Resolves the field's selectable options by traversing the catalog graph. URN-valued fields only.                                                                                                                                                               |
| `valuesSource`       | [ValuesSource](#valuessource)                  | No       | Simpler alternative to `dynamicSource` for the special-case of "members of the current user's groups". URN-valued fields only.                                                                                                                                 |
| `condition`          | [LegacyFieldCondition](#legacy-fieldcondition) | No       | **Legacy v1.** Single-field-value condition for field visibility. Prefer `filterCondition` for new workflows.                                                                                                                                                  |

### Field Validation

Submit-time validation rule applied to a field's value.

```json
{
  "validation": {
    "pattern": "[\\s\\S]{100,}",
    "errorMessage": "Please describe at least 100 characters."
  }
}
```

| Field          | Type   | Required | Description                                                                                                           |
| -------------- | ------ | -------- | --------------------------------------------------------------------------------------------------------------------- |
| `pattern`      | string | Yes      | Java-flavoured regex pattern that the submitted value must match. Use length bounds (`{100,}`) for length validation. |
| `errorMessage` | string | No       | Message rendered inline when validation fails. If omitted, a generic error message is shown.                          |

### Field Visibility

Field visibility is expressed with a `filterCondition` ([Filter](#filter-dialect) shape) that can mix:

- **Launching-entity attributes** — `tags`, `glossaryTerms`, `domain`, `platform`, `owners`, `subTypes`, `name`, `description`, etc. See [Filter Dialect](#filter-dialect) for the full list.
- **Prior form-field values** — referenced as `formField:<field-id>`. The value the requester has entered in the named field at the moment the form is being rendered.

```json
{
  "filterCondition": {
    "operator": "AND",
    "filters": [
      {
        "field": "glossaryTerms",
        "values": ["urn:li:glossaryTerm:certification.gold"],
        "condition": "CONTAIN"
      },
      {
        "field": "formField:target_tier",
        "values": ["urn:li:glossaryTerm:certification.gold"],
        "condition": "EQUAL",
        "negated": true
      }
    ]
  }
}
```

The field renders only when every filter predicate evaluates to true (`operator: AND`); use `operator: OR` for any-of semantics.

### Legacy FieldCondition

The v1 single-field-value condition is still accepted by `upsertActionWorkflow` for back-compat but is deprecated. Prefer `filterCondition`.

```json
{
  "condition": {
    "type": "SINGLE_FIELD_VALUE",
    "singleFieldValueCondition": {
      "field": "access_duration",
      "values": ["PERMANENT"],
      "condition": "EQUAL"
    }
  }
}
```

| Field                                 | Type    | Required | Description                                              |
| ------------------------------------- | ------- | -------- | -------------------------------------------------------- |
| `type`                                | enum    | Yes      | Only `SINGLE_FIELD_VALUE` is supported.                  |
| `singleFieldValueCondition.field`     | string  | Yes      | Workflow-local field id whose value is checked.          |
| `singleFieldValueCondition.values`    | array   | Yes      | Values the field is compared against.                    |
| `singleFieldValueCondition.condition` | enum    | No       | `EQUAL`, `CONTAIN`, `START_WITH`, etc. Default: `EQUAL`. |
| `singleFieldValueCondition.negated`   | boolean | No       | Invert the predicate. Default: `false`.                  |

## DynamicSource

Resolves URN-valued field options or step actors by traversing the catalog graph at form-render time / step-open time.

```json
{
  "dynamicSource": {
    "resolvers": [
      { "resolver": "OWNERS_OF", "source": "launching" },
      { "resolver": "DOMAIN_OWNERS_OF", "source": "field:co_signer_steward" }
    ],
    "filter": {
      "operator": "AND",
      "filters": [ ... ]
    }
  }
}
```

| Field       | Type                            | Required | Description                                                                                                                                                                                                                   |
| ----------- | ------------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `resolvers` | array of [Resolver](#resolvers) | Yes      | One or more resolvers. The engine evaluates each, then unions the returned URN sets. At least one resolver required.                                                                                                          |
| `filter`    | [Filter](#filter-dialect)       | No       | Destination-boundary predicate applied to the resolved URN set. Useful for narrowing the resolver output by platform, tag, or any indexed attribute. Multi-value `EQUAL` is treated as OR — only URNs in the value list pass. |

### Resolvers

Each resolver entry traverses one documented relationship in the catalog graph.

```json
{ "resolver": "OWNERS_OF", "source": "launching" }
```

| Field      | Type   | Required | Description                                                                                                                                                                                                                                                                                              |
| ---------- | ------ | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `resolver` | string | Yes      | Resolver identifier from the [Resolver Catalogue](#resolver-catalogue) below.                                                                                                                                                                                                                            |
| `source`   | string | No       | Where to start the traversal. `"launching"` (default) starts at the launching entity. `"field:<form-field-id>"` starts at the URN the requester picked in a prior form field (the field must be URN-valued and submitted before this resolver evaluates). Required for chained / cross-field resolution. |

### Resolver Catalogue

The full set of built-in resolvers.

**Actor-context resolvers** — return user, group, and role URNs suitable for step actor resolution.

| Resolver                  | Returns       | Purpose                                                |
| ------------------------- | ------------- | ------------------------------------------------------ |
| `OWNERS_OF`               | users, groups | Owners of the source entity.                           |
| `DOMAIN_OWNERS_OF`        | users, groups | Owners of the source entity's domain.                  |
| `DATA_PRODUCT_OWNERS_OF`  | users, groups | Owners of the source entity's data product.            |
| `APPLICATION_OWNERS_OF`   | users, groups | Owners of the source entity's application.             |
| `GLOSSARY_TERM_OWNERS_OF` | users, groups | Owners of a glossary term linked to the source entity. |
| `TAG_OWNERS_OF`           | users, groups | Owners of a tag linked to the source entity.           |
| `CONTAINER_OWNERS_OF`     | users, groups | Owners of the source entity's container.               |

**Field-context resolvers (forward 1-hop)** — return entity URNs reachable from the source entity along one documented edge.

| Resolver            | Returns             | Purpose                                         |
| ------------------- | ------------------- | ----------------------------------------------- |
| `TAGS_OF`           | tag URNs            | Tags attached to the source entity.             |
| `GLOSSARY_TERMS_OF` | glossary term URNs  | Glossary terms attached to the source entity.   |
| `DOMAIN_OF`         | domain URN          | Domain of the source entity.                    |
| `DATA_PRODUCT_OF`   | data product URN(s) | Data products that contain the source entity.   |
| `APPLICATIONS_OF`   | application URN(s)  | Applications associated with the source entity. |
| `CONTAINER_OF`      | container URN       | Container of the source entity.                 |

**Field-context resolvers (reverse 2-hop)** — return entities that share a structural relationship with the source entity through an intermediate node.

| Resolver                   | Returns     | Purpose                                                       |
| -------------------------- | ----------- | ------------------------------------------------------------- |
| `SIBLINGS_IN_DOMAIN`       | entity URNs | Other entities in the same domain as the source entity.       |
| `SIBLINGS_IN_DATA_PRODUCT` | entity URNs | Other entities in the same data product as the source entity. |
| `SIBLINGS_IN_APPLICATION`  | entity URNs | Other entities in the same application as the source entity.  |

**Catalogue-wide resolver** — returns every entity matching the parent field's `allowedEntityTypes`, useful when paired with a `filter` for fixed-URN allow-lists.

| Resolver       | Returns     | Purpose                                                                                                                         |
| -------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `ALL_ENTITIES` | entity URNs | All entities matching the parent field's `allowedEntityTypes`. Use with a `filter` (e.g., `urn`-EQUAL) for curated allow-lists. |

## ValuesSource

Simpler alternative to `dynamicSource` for the special case where a URN-valued field should be restricted to entities the current user already has a context-defined relationship with.

```json
{
  "valuesSource": {
    "type": "CURRENT_USER_GROUPS",
    "nameRegex": "^data-eng-.*"
  }
}
```

| Field       | Type   | Required | Description                                                                                                        |
| ----------- | ------ | -------- | ------------------------------------------------------------------------------------------------------------------ |
| `type`      | enum   | Yes      | Only `CURRENT_USER_GROUPS` is supported today — the picker is restricted to groups the requesting user belongs to. |
| `nameRegex` | string | No       | Java-flavoured regex applied to the returned entities' `name` attribute as an additional narrowing filter.         |

## Step

A single approval step in the decision graph.

```json
{
  "id": "data-owner-approval",
  "type": "APPROVAL",
  "description": "ALL_OF: every dataset owner must approve.",
  "actors": { ... },
  "condition": { ... },
  "quorum": { "allOf": true }
}
```

| Field         | Type                      | Required | Description                                                                                                                                                                              |
| ------------- | ------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`          | string                    | Yes      | Workflow-local step identifier. Referenced in audit history's `stepId`.                                                                                                                  |
| `type`        | enum                      | Yes      | Only `APPROVAL` is supported today.                                                                                                                                                      |
| `description` | string                    | No       | Description shown to reviewers when the step opens.                                                                                                                                      |
| `actors`      | [StepActors](#stepactors) | Yes      | Who can decide on this step.                                                                                                                                                             |
| `condition`   | [Filter](#filter-dialect) | No       | Step skip condition. If the condition evaluates to false when the step is about to open, the engine records a `SKIPPED` decision in the audit log and opens the next step automatically. |
| `quorum`      | [Quorum](#quorum)         | No       | How many actors must approve for the step to advance. Default: `{ "anyOf": true }`.                                                                                                      |

### StepActors

Who can decide on a step.

```json
{
  "actors": {
    "userUrns": ["urn:li:corpuser:alice"],
    "groupUrns": ["urn:li:corpGroup:data-stewards"],
    "roleUrns": ["urn:li:dataHubRole:Editor"],
    "dynamicSource": {
      "resolvers": [{ "resolver": "OWNERS_OF", "source": "launching" }]
    },
    "dynamicAssignment": { "type": "ENTITY_OWNERS" }
  }
}
```

| Field               | Type                                                 | Required | Description                                                                                                                                                         |
| ------------------- | ---------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `userUrns`          | array of string                                      | No       | Static user URNs. Defaults to an empty array when omitted.                                                                                                          |
| `groupUrns`         | array of string                                      | No       | Static group URNs. Any member of an assigned group can satisfy the slot. Defaults to an empty array when omitted.                                                   |
| `roleUrns`          | array of string                                      | No       | Static role URNs. Any holder of an assigned role can satisfy the slot. Defaults to an empty array when omitted.                                                     |
| `dynamicSource`     | [DynamicSource](#dynamicsource)                      | No       | Graph-traversal resolver evaluated when the step opens. Returned URNs are added to the actor pool. Preferred over the legacy `dynamicAssignment` for new workflows. |
| `dynamicAssignment` | [LegacyDynamicAssignment](#legacy-dynamicassignment) | No       | **Legacy v1.** Named-resolver shortcut for the common owner-based assignments. Prefer `dynamicSource`.                                                              |

At least one of `userUrns`, `groupUrns`, `roleUrns`, `dynamicSource`, or `dynamicAssignment` must produce at least one actor when the step opens. A step with no resolvable actors fails fast with `QUORUM_UNREACHABLE`. When a step uses only dynamic actors, omit `userUrns`, `groupUrns`, and `roleUrns` entirely — they default to empty arrays.

### Legacy DynamicAssignment

The v1 named-resolver shortcut for owner-based assignments. Still accepted for back-compat. Prefer `dynamicSource` with the equivalent resolver name (`OWNERS_OF`, `DOMAIN_OWNERS_OF`, `DATA_PRODUCT_OWNERS_OF`) for new workflows — the `dynamicSource` form supports cross-field resolution and chained filters that this legacy shape does not.

```json
{
  "dynamicAssignment": {
    "type": "ENTITY_OWNERS",
    "ownershipTypeUrns": ["urn:li:ownershipType:__system__technical_owner"]
  }
}
```

| Field               | Type            | Required | Description                                                                                                                                                          |
| ------------------- | --------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`              | enum            | Yes      | One of `ENTITY_OWNERS`, `ENTITY_DOMAIN_OWNERS`, `ENTITY_DATA_PRODUCT_OWNERS`. Maps to `OWNERS_OF` / `DOMAIN_OWNERS_OF` / `DATA_PRODUCT_OWNERS_OF` in the v2 dialect. |
| `ownershipTypeUrns` | array of string | No       | Filter the resolved owners by these ownership type URNs (e.g., Technical Owner only). Omit to resolve owners of any ownership type.                                  |

### Quorum

How many actors must approve for the step to advance.

The quorum field uses a union encoding — set exactly one of `anyOf`, `allOf`, or `nofM`.

```json
{ "quorum": { "anyOf": true } }
```

```json
{ "quorum": { "allOf": true } }
```

```json
{ "quorum": { "nofM": { "n": 2 } } }
```

| Field   | Type    | Required | Description                                                                                                                                                           |
| ------- | ------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `anyOf` | boolean | No       | When `true`, any single approval advances the step. This is the default if no `quorum` is specified.                                                                  |
| `allOf` | boolean | No       | When `true`, every actor or slot resolved at step-open time must approve. Group and role slots are expanded — any group member can satisfy that slot.                 |
| `nofM`  | object  | No       | Threshold quorum. `{ "n": <count> }` requires `n` distinct approvals out of the resolved actor pool. The `n` value must be ≥ 1 and ≤ the pool size at step-open time. |

## Filter Dialect

The shared filter shape used by entrypoint visibility, field visibility (`filterCondition`), step skip conditions (`condition`), and `dynamicSource.filter`.

```json
{
  "operator": "AND",
  "filters": [
    {
      "field": "platform",
      "values": ["urn:li:dataPlatform:snowflake"],
      "condition": "EQUAL"
    },
    {
      "field": "tags",
      "values": ["urn:li:tag:pii"],
      "condition": "CONTAIN",
      "negated": true
    }
  ]
}
```

| Field      | Type                             | Required | Description                                                      |
| ---------- | -------------------------------- | -------- | ---------------------------------------------------------------- |
| `operator` | enum                             | Yes      | `AND`, `OR`, or `NOT`. Defines how the child predicates combine. |
| `filters`  | array of [Predicate](#predicate) | Yes      | One or more atomic predicates.                                   |

### Predicate

A single field/value/operator/negation tuple.

| Field       | Type            | Required | Description                                                                                                                                                                            |
| ----------- | --------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `field`     | string          | Yes      | The attribute to compare. See [Field References](#field-references) for the supported attributes.                                                                                      |
| `values`    | array of string | Yes      | Values to compare against. Multi-value `EQUAL` / `CONTAIN` is treated as OR — a single match passes.                                                                                   |
| `condition` | enum            | No       | Comparison operator: `EQUAL`, `CONTAIN`, `START_WITH`, `END_WITH`, `IN`, `EXISTS`, `GREATER_THAN`, `LESS_THAN`, `GREATER_THAN_OR_EQUAL_TO`, `LESS_THAN_OR_EQUAL_TO`. Default: `EQUAL`. |
| `negated`   | boolean         | No       | Invert the predicate. Default: `false`.                                                                                                                                                |

### Field References

The `field` slot accepts two kinds of references:

**Launching-entity attributes** — properties of the entity from which the workflow was launched. Used in entrypoint visibility, step skip conditions, and the launching-entity half of field visibility conditions.

| Field           | Description                                                        |
| --------------- | ------------------------------------------------------------------ |
| `urn`           | The entity's URN.                                                  |
| `platform`      | The entity's platform URN (e.g., `urn:li:dataPlatform:snowflake`). |
| `entityType`    | The entity's type (`DATASET`, `DASHBOARD`, etc.).                  |
| `subTypes`      | The entity's sub-type values.                                      |
| `name`          | The entity's display name.                                         |
| `description`   | The entity's description.                                          |
| `tags`          | URNs of tags attached to the entity.                               |
| `glossaryTerms` | URNs of glossary terms attached to the entity.                     |
| `domain`        | URN of the entity's domain.                                        |
| `dataProduct`   | URN(s) of data products that contain the entity.                   |
| `application`   | URN of the entity's application.                                   |
| `container`     | URN of the entity's container.                                     |
| `owners`        | URNs of users / groups that own the entity.                        |

**Form-field references** — values the requester has entered in a prior form field. Used in field visibility conditions and step skip conditions. The referenced field must be defined earlier in the form's `fields` array.

| Field reference        | Description                                                                                                                        |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `formField:<field-id>` | The current value of the named form field. For `MULTIPLE`-cardinality fields, the predicate evaluates against the array of values. |

## Categories

The high-level category of a workflow, used for grouping in the UI and for governance reporting.

| Value    | Purpose                                                                                                                      |
| -------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `ACCESS` | Access-request workflows (the classical use case). Requests appear under the "Access" filter in the Task Center.             |
| `CUSTOM` | Everything else. Requires populating `customCategory` with a free-form string to identify the category (e.g., `GOVERNANCE`). |

## Value Types

Supported `valueType` values for form fields.

| Value       | Cardinality       | Description                                                                                              |
| ----------- | ----------------- | -------------------------------------------------------------------------------------------------------- |
| `STRING`    | SINGLE / MULTIPLE | Single-line text input. Pair with `allowedValues` for enum-style dropdowns.                              |
| `RICH_TEXT` | SINGLE            | Multi-line rich-text input with formatting (bold, italic, links, etc.).                                  |
| `URN`       | SINGLE / MULTIPLE | Entity reference. Requires `allowedEntityTypes`. Optionally configure `dynamicSource` or `valuesSource`. |
| `DATE`      | SINGLE            | Date/time, stored as epoch milliseconds.                                                                 |
| `NUMBER`    | SINGLE / MULTIPLE | Integer or floating-point number.                                                                        |
| `BOOLEAN`   | SINGLE            | True/false toggle.                                                                                       |

## Entity Types

Entity-type identifiers used in `entityTypes` and `allowedEntityTypes`. Common values:

`DATASET`, `DASHBOARD`, `CHART`, `DATA_PRODUCT`, `DATA_FLOW`, `DATA_JOB`, `DOMAIN`, `GLOSSARY_TERM`, `GLOSSARY_NODE`, `CORP_USER`, `CORP_GROUP`, `TAG`, `CONTAINER`, `ML_MODEL`, `ML_MODEL_GROUP`, `ML_FEATURE`, `ML_FEATURE_TABLE`, `APPLICATION`.

The full set of entity types is defined by the catalog's entity registry — see the `entityType` aspect on the [DataHub data model](../../what-is-datahub/datahub-concepts.md) page for the canonical list.

## Default Fields

Values for `excludedDefaultFields`. These are fields DataHub adds to every form by default; list them here to hide them on a specific workflow.

| Value              | Description                                              |
| ------------------ | -------------------------------------------------------- |
| `EXPIRATION_DATE`  | A date picker labelled "Expires on" added to every form. |
| `ADDITIONAL_NOTES` | A free-form rich-text notes field added to every form.   |

## Property Value

A single value in a field's `allowedValues` enumeration. Use the variant that matches the field's `valueType`.

```json
{ "stringValue": "30_DAYS" }
```

```json
{ "numberValue": 90 }
```

```json
{ "urnValue": "urn:li:corpuser:alice" }
```

```json
{ "booleanValue": true }
```

| Field          | Type    | Use when                                            |
| -------------- | ------- | --------------------------------------------------- |
| `stringValue`  | string  | `valueType` is `STRING` or `RICH_TEXT`.             |
| `numberValue`  | number  | `valueType` is `NUMBER`.                            |
| `urnValue`     | string  | `valueType` is `URN`. The URN of an allowed entity. |
| `booleanValue` | boolean | `valueType` is `BOOLEAN`.                           |

## GraphQL Surface

The mutations and queries used to manage workflow definitions and requests. Visit GraphiQL at `https://your-datahub-instance.acryl.io/api/graphiql` for the live, fully-typed schema.

### Mutations

| Mutation                                 | Purpose                                                                                                                                                                                                  |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `upsertActionWorkflow(input)`            | Create or update a workflow definition. Returns the persisted workflow's URN. Every upsert produces a new immutable revision; in-flight requests continue against the revision they were launched under. |
| `deleteActionWorkflow(urn)`              | Delete a workflow definition. Returns an error if any requests referencing the workflow are still in `PENDING` state — resolve or cancel open requests first.                                            |
| `createActionWorkflowFormRequest(input)` | Submit a new workflow request. Returns the new request's URN.                                                                                                                                            |
| `reviewActionWorkflowFormRequest(input)` | Record an approval or rejection on a pending step. Returns an error if the step is already in a terminal state.                                                                                          |
| `cancelActionWorkflowFormRequest(urn)`   | Cancel an in-flight workflow request. Callable by the original requester on their own requests, or by an administrator with `Manage Global Settings` on any request.                                     |

### Queries

| Query                                            | Purpose                                                                                                                  |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `actionWorkflow(urn)`                            | Fetch a single workflow definition by URN, including its current revision and all aspects.                               |
| `listActionWorkflows(input)`                     | Page and filter the catalogue of workflow definitions by name, category, custom category, or launching entity type.      |
| `actionWorkflowFormRequest(urn)`                 | Fetch a single workflow request by URN, including its decisions, current step state, and audit history.                  |
| `listActionWorkflowFormRequests(input)`          | Page and filter workflow requests by status (`PENDING`, `COMPLETED`, `CANCELLED`), workflow URN, requester, or assignee. |
| `listActionWorkflowsForEntityProfile(entityUrn)` | List the workflows whose entrypoints are visible for a given entity, accounting for entrypoint filters.                  |
