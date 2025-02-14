---
description: >-
  This guide details the Entity Events API, which allows you to take action when
  things change on DataHub.
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Entity Events API
<FeatureAvailability saasOnly />

## Introduction

The Events API allows you to integrate changes happening on the DataHub Metadata Graph in real time into a broader event-based architecture.

### Supported Integrations

* [AWS EventBridge](docs/managed-datahub/operator-guide/setting-up-events-api-on-aws-eventbridge.md)
* [DataHub Cloud Event Source](docs/actions/sources/datahub-cloud-event-source.md)

### Use Cases

Real-time use cases broadly fall into the following categories:

* **Workflow Integration:** Integrate DataHub flows into your organization's internal workflow management system. For example, create a Jira ticket when specific Tags or Terms are proposed on a Dataset.
* **Notifications**: Generate organization-specific notifications when a change is made on DataHub. For example, send an email to the governance team when a "PII" tag is added to any data asset.
* **Metadata Enrichment**: Trigger downstream metadata changes when an upstream change occurs. For example, propagating glossary terms or tags to downstream entities.
* **Synchronization**: Syncing changes made in DataHub into a 3rd party system. For example, reflecting Tag additions in DataHub into Snowflake.
* **Auditing:** Audit **** _who_ is making _what changes_ on DataHub through time.

## Event Structure

Each entity event is serialized to JSON & follows a common base structure.

**Common Fields**

| Name                 | Type   | Description                                                                                                                                                                                             | Optional  |
| -------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| **entityUrn**        | String | The unique identifier for the Entity being changed. For example, a Dataset's urn.                                                                                                                       | Fals**e** |
| **entityType**       | String | The type of the entity being changed. Supported values include `dataset`, `chart`, `dashboard`, `dataFlow (Pipeline)`, `dataJob` (Task), `domain`, `tag`, `glossaryTerm`, `corpGroup`, & `corpUser.`    | False     |
| **category**         | String | The category of the change, related to the kind of operation that was performed. Examples include `TAG`, `GLOSSARY_TERM`, `DOMAIN`, `LIFECYCLE`, and more.                                              | False     |
| **operation**        | String | The operation being performed on the entity given the category. For example, `ADD` ,`REMOVE`, `MODIFY`. For the set of valid operations, see the full catalog below.                                    | False     |
| **modifier**         | String | The modifier that has been applied to the entity. The value depends on the category. An example includes the URN of a tag being applied to a Dataset or Schema Field.                                   | True      |
| **parameters**       | Dict   | Additional key-value parameters used to provide specific context. The precise contents depends on the category + operation of the event. See the catalog below for a full summary of the combinations.  | True      |
| **auditStamp.actor** | String | The urn of the actor who triggered the change.                                                                                                                                                          | False     |
| **auditStamp.time**  | Number | The timestamp in milliseconds corresponding to the event.                                                                                                                                               | False     |  

For example, an event indicating that a Tag has been added to a particular Dataset would populate each of these fields:

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TAG",
  "operation": "ADD",
  "modifier": "urn:li:tag:PII",
  "parameters": {
    "tagUrn": "urn:li:tag:PII"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

In the following sections, we'll take a closer look at the purpose and structure of each supported event type.

## Event Types

Below, we will review the catalog of events available for consumption.

### Add Tag Event

This event is emitted when a Tag has been added to an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>TAG</td><td>ADD</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>container</code>, <code>dataFlow</code> , <code>schemaField</code></td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type   | Description                                                                                                                                                  | Optional |
| --------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| tagUrn    | String | The urn of the tag that has been added.                                                                                                                      | False    |
| fieldPath | String | The path of the schema field which the tag is being added to. This field is **only** present if the entity type is `schemaField`.                            | True     |
| parentUrn | String | The urn of a parent entity. This field is only present if the entity type is `schemaField`, and will contain the parent Dataset to which the field belongs.  | True     |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TAG",
  "operation": "ADD",
  "modifier": "urn:li:tag:PII"
  "parameters": {
    "tagUrn": "urn:li:tag:PII"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```



### Remove Tag Event

This event is emitted when a Tag has been removed from an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>TAG</td><td>REMOVE</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>container</code>, <code>dataFlow</code>, <code>schemaField</code></td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type   | Description                                                                                                                                                  | Optional |
| --------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| tagUrn    | String | The urn of the tag that has been removed.                                                                                                                    | False    |
| fieldPath | String | The path of the schema field which the tag is being removed from. This field is **only** present if the entity type is `schemaField`.                        | True     |
| parentUrn | String | The urn of a parent entity. This field is only present if the entity type is `schemaField`, and will contain the parent Dataset to which the field belongs.  | True     |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TAG",
  "operation": "REMOVE",
  "modifier": "urn:li:tag:PII",
  "parameters": {
    "tagUrn": "urn:li:tag:PII"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Add Glossary Term Event

This event is emitted when a Glossary Term has been added to an entity on DataHub.

**Header**

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>GLOSSARY_TERM</td><td>ADD</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>container</code>, <code>dataFlow</code> , <code>schemaField</code></td><td></td></tr></tbody></table>

#### Parameters

| Name      |   | Type   | Description                                                                                                                                                  | Optional |
| --------- | - | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| termUrn   |   | String | The urn of the glossary term that has been added.                                                                                                            | False    |
| fieldPath |   | String | The path of the schema field to which the term is being added. This field is **only** present if the entity type is `schemaField`.                           | True     |
| parentUrn |   | String | The urn of a parent entity. This field is only present if the entity type is `schemaField`, and will contain the parent Dataset to which the field belongs.  | True     |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "GLOSSARY_TERM",
  "operation": "ADD",
  "modifier": "urn:li:glossaryTerm:ExampleNode.ExampleTerm",
  "parameters": {
    "termUrn": "urn:li:glossaryTerm:ExampleNode.ExampleTerm"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Remove Glossary Term Event

This event is emitted when a Glossary Term has been removed from an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>GLOSSARY_TERM</td><td>REMOVE</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>container</code>, <code>dataFlow</code> , <code>schemaField</code></td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type   | Description                                                                                                                                                  | Optional |
| --------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| termUrn   | String | The urn of the glossary term that has been removed.                                                                                                          | False    |
| fieldPath | String | The path of the schema field from which the term is being removed. This field is **only** present if the entity type is `schemaField`.                       | True     |
| parentUrn | String | The urn of a parent entity. This field is only present if the entity type is `schemaField`, and will contain the parent Dataset to which the field belongs.  | True     |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "GLOSSARY_TERM",
  "operation": "REMOVE",
  "modifier": "urn:li:glossaryTerm:ExampleNode.ExampleTerm",
  "parameters": {
    "termUrn": "urn:li:glossaryTerm:ExampleNode.ExampleTerm"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Add Domain Event

This event is emitted when Domain has been added to an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>DOMAIN</td><td>ADD</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>container</code>, <code>dataFlow</code> </td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type   | Description                                 | Optional |
| --------- | ------ | ------------------------------------------- | -------- |
| domainUrn | String | The urn of the domain that has been added.  | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DOMAIN",
  "operation": "ADD",
  "modifier": "urn:li:domain:ExampleDomain",
  "parameters": {
    "domainUrn": "urn:li:domain:ExampleDomain"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Remove Domain Event

This event is emitted when Domain has been removed from an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>DOMAIN</td><td>REMOVE</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>container</code> ,<code>dataFlow</code> </td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type   | Description                                   | Optional |
| --------- | ------ | --------------------------------------------- | -------- |
| domainUrn | String | The urn of the domain that has been removed.  | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DOMAIN",
  "operation": "REMOVE",
  "modifier": "urn:li:domain:ExampleDomain",
  "parameters": {
     "domainUrn": "urn:li:domain:ExampleDomain"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Add Owner Event

This event is emitted when a new owner has been assigned to an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>OWNER</td><td>ADD</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>dataFlow</code> , <code>container</code>, <code>glossaryTerm</code>, <code>domain</code>, <code>tag</code></td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type   | Description                                                                                                   | Optional |
| --------- | ------ | ------------------------------------------------------------------------------------------------------------- | -------- |
| ownerUrn  | String | The urn of the owner that has been added.                                                                     | False    |
| ownerType | String | The type of the owner that has been added. `TECHNICAL_OWNER`, `BUSINESS_OWNER`, `DATA_STEWARD`, `NONE`, etc.  | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "OWNER",
  "operation": "ADD",
  "modifier": "urn:li:corpuser:jdoe",
  "parameters": {
     "ownerUrn": "urn:li:corpuser:jdoe",
     "ownerType": "BUSINESS_OWNER"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Remove Owner Event

This event is emitted when an existing owner has been removed from an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>OWNER</td><td>REMOVE</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>container</code> ,<code>dataFlow</code> , <code>glossaryTerm</code>, <code>domain</code>, <code>tag</code></td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type   | Description                                                                                                     | Optional |
| --------- | ------ | --------------------------------------------------------------------------------------------------------------- | -------- |
| ownerUrn  | String | The urn of the owner that has been removed.                                                                     | False    |
| ownerType | String | The type of the owner that has been removed. `TECHNICAL_OWNER`, `BUSINESS_OWNER`, `DATA_STEWARD`, `NONE`, etc.  | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "OWNER",
  "operation": "REMOVE",
  "modifier": "urn:li:corpuser:jdoe",
  "parameters": {
    "ownerUrn": "urn:li:corpuser:jdoe",
    "ownerType": "BUSINESS_OWNER"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Add Description Event

This event is emitted when a description has been added to an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>DOCUMENTATION</td><td>ADD</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>dataFlow</code> , <code>container</code>, <code>glossaryTerm</code>, <code>domain</code>, <code>tag</code>, <code>schemaField</code></td><td></td></tr></tbody></table>

#### Parameters

| Name        | Type   | Description                                                                                                  | Optional |
|-------------| ------ |--------------------------------------------------------------------------------------------------------------| -------- |
| description | String | The description that has been added.                                                                         | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DOCUMENTATION",
  "operation": "ADD",
  "parameters": {
    "description": "This is a new description"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1706646452982
  }
}
```

### Remove Description Event

This event is emitted when an existing description has been removed from an entity on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>DOCUMENTATION</td><td>REMOVE</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>container</code> ,<code>dataFlow</code> , <code>glossaryTerm</code>, <code>domain</code>, <code>tag</code>, <code>schemaField</code></td><td></td></tr></tbody></table>

#### Parameters

| Name        | Type   | Description                            | Optional |
|-------------| ------ |----------------------------------------| -------- |
| description | String | The description that has been removed. | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DOCUMENTATION",
  "operation": "REMOVE",
  "parameters": {
    "description": "This is the removed description"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1706646452982
  }
}
```

### Modify Deprecation Event

This event is emitted when the deprecation status of an entity has been modified on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>DEPRECATION</td><td>MODIFY</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>dataFlow</code> , <code>container</code></td><td></td></tr></tbody></table>

#### Parameters

| Name   | Type   | Description                                                                 | Optional |
| ------ | ------ | --------------------------------------------------------------------------- | -------- |
| status | String | The new deprecation status of the entity, either `DEPRECATED` or `ACTIVE`.  | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DEPRECATION",
  "operation": "MODIFY",
  "modifier": "DEPRECATED",
  "parameters": {
    "status": "DEPRECATED"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Add Dataset Schema Field Event

This event is emitted when a new field has been added to a **Dataset** **Schema**.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>TECHNICAL_SCHEMA</td><td>ADD</td><td><code>dataset</code></td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type    | Description                                                                                                                                                                | Optional |
| --------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| fieldUrn  | String  | The urn of the new schema field.                                                                                                                                           | False    |
| fieldPath | String  | The path of the new field. For more information about field paths, check out [Dataset Field Paths Explained](docs/generated/metamodel/entities/dataset.md#field-paths-explained) | False    |
| nullable  | Boolean | Whether the new field is nullable.                                                                                                                                         | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TECHNICAL_SCHEMA",
  "operation": "ADD",
  "modifier": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
  "parameters": {
    "fieldUrn": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
    "fieldPath": "newFieldName",
    "nullable": false
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Remove Dataset Schema Field Event

This event is emitted when a new field has been remove from a **Dataset** **Schema**.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>TECHNICAL_SCHEMA</td><td>REMOVE</td><td><code>dataset</code></td><td></td></tr></tbody></table>

#### Parameters

| Name      | Type    | Description                                                                                                                                                                    | Optional |
| --------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| fieldUrn  | String  | The urn of the removed schema field.                                                                                                                                           | False    |
| fieldPath | String  | The path of the removed field. For more information about field paths, check out [Dataset Field Paths Explained](docs/generated/metamodel/entities/dataset.md#field-paths-explained) | False    |
| nullable  | Boolean | Whether the removed field is nullable.                                                                                                                                         | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TECHNICAL_SCHEMA",
  "operation": "REMOVE",
  "modifier": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
  "parameters": {
    "fieldUrn": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
    "fieldPath": "newFieldName",
    "nullable": false
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Entity Create Event

This event is emitted when a new entity has been created on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>LIFECYCLE</td><td>CREATE</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>dataFlow</code> , <code>glossaryTerm</code>, <code>domain</code>, <code>tag</code>, <code>container</code></td><td></td></tr></tbody></table>

#### Parameters

_None_

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "CREATE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Entity Soft-Delete Event

This event is emitted when a new entity has been soft-deleted on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>LIFECYCLE</td><td>SOFT_DELETE</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>dataFlow</code> , <code>glossaryTerm</code>, <code>domain</code>, <code>tag</code>, <code>container</code></td><td></td></tr></tbody></table>

#### Parameters

_None_

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "SOFT_DELETE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Entity Hard-Delete Event

This event is emitted when a new entity has been hard-deleted on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>LIFECYCLE</td><td>HARD_DELETE</td><td><code>dataset</code>, <code>dashboard</code>, <code>chart</code>, <code>dataJob</code>, <code>dataFlow</code> , <code>glossaryTerm</code>, <code>domain</code>, <code>tag</code>, <code>container</code></td><td></td></tr></tbody></table>

#### Parameters

_None_

#### Sample Event

```
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "HARD_DELETE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Completed Assertion Run Event

This event is emitted when an Assertion has been run has succeeded on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>RUN</td><td>COMPLETED</td><td><code>assertion</code></td><td></td></tr></tbody></table>

#### Parameters

| Name       | Type   | Description                                           | Optional |
| ---------- | ------ | ----------------------------------------------------- | -------- |
| runResult  | String | The result of the run, either `SUCCESS` or `FAILURE`. | False    |
| runId      | String | Native (platform-specific) identifier for this run.   | False    |
| aserteeUrn | String | Urn of entity on which the assertion is applicable.   | False    |

####

#### Sample Event

```
{
  "entityUrn": "urn:li:assertion:abc",
  "entityType": "assertion",
  "category": "RUN",
  "operation": "COMPLETED",
  "parameters": {
    "runResult": "SUCCESS",
    "runId": "123",
    "asserteeUrn": "urn:li:dataset:def"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Started Data Process Instance Run Event

This event is emitted when a Data Process Instance Run has STARTED on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>RUN</td><td>STARTED</td><td><code>dataProcessInstance</code></td><td></td></tr></tbody></table>

#### Parameters

| Name              | Type    | Description                                                                                     | Optional |
| ----------------- | ------- | ----------------------------------------------------------------------------------------------- | -------- |
| attempt           | Integer | The number of attempts that have been made.                                                     | True     |
| dataFlowUrn       | String  | The urn of the associated Data Flow. Only filled in if this run is associated with a Data Flow. | True     |
| dataJobUrn        | String  | The urn of the associated Data Flow. Only filled in if this run is associated with a Data Job.  | True     |
| parentInstanceUrn | String  | Urn of the parent DataProcessInstance (if there is one).                                        | True     |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataProcessInstance:abc",
  "entityType": "dataProcessInstance",
  "category": "RUN",
  "operation": "STARTED",
  "parameters": {
    "dataFlowUrn": "urn:li:dataFlow:def",
    "attempt": "1",
    "parentInstanceUrn": ""urn:li:dataProcessInstance:ghi"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Completed Data Process Instance Run Event

This event is emitted when a Data Process Instance Run has been COMPLETED on DataHub.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>RUN</td><td>COMPLETED</td><td><code>dataProcessInstance</code></td><td></td></tr></tbody></table>

#### Parameters

| Name              | Type    | Description                                                                                     | Optional |
| ----------------- | ------- | ----------------------------------------------------------------------------------------------- | -------- |
| runResult         | String  | The result of the run, one of  `SUCCESS` , `FAILURE`, `SKIPPED`, or `UP_FOR_RETRY` .            | False    |
| attempt           | Integer | The number of attempts that have been made.                                                     | True     |
| dataFlowUrn       | String  | The urn of the associated Data Flow. Only filled in if this run is associated with a Data Flow. | True     |
| dataJobUrn        | String  | The urn of the associated Data Flow. Only filled in if this run is associated with a Data Job.  | True     |
| parentInstanceUrn | String  | Urn of the parent DataProcessInstance.                                                          | True     |

#### Sample Event

```
{
  "entityUrn": "urn:li:dataProcessInstance:abc",
  "entityType": "dataProcessInstance",
  "category": "RUN",
  "operation": "COMPLETED",
  "parameters": {
    "runResult": "SUCCESS"
    "attempt": "2",
    "dataFlowUrn": "urn:li:dataFlow:def",
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```



### Action Request Created Event

This event is emitted when a new Action Request (Metadata Proposal) has been created.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>LIFECYCLE</td><td><code>CREATED</code></td><td><code>actionRequest</code></td><td></td></tr></tbody></table>

#### Parameters

These are the common parameters for all Action Request create events.

| Name              | Type   | Description                                                                                                                                        | Optional |
| ----------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| actionRequestType | String | The type of Action Request. One of `TAG_ASSOCIATION`, `TERM_ASSOCIATION`, `CREATE_GLOSSARY_NODE`, `CREATE_GLOSSARY_TERM`, or `UPDATE_DESCRIPTION.` | False    |
| resourceType      | String | The type of entity this Action Request is applied on, such as `dataset`.                                                                           | True     |
| resourceUrn       | String | The entity this Action Request is applied on.                                                                                                      | True     |
| subResourceType   | String | Filled if this Action Request is applied on a sub-resource, such as a `schemaField`.                                                               | True     |
| subResource       | String | Identifier of the sub-resource if this proposal is applied on one.                                                                                 | True     |

Parameters specific to different proposal types are listed below.

#### Tag Association Proposal Specific Parameters and Sample Event

| Name   | Type   | Description                               | Optional |
| ------ | ------ | ----------------------------------------- | -------- |
| tagUrn | String | The urn of the Tag that would be applied. | False    |

```
{
  "entityUrn": "urn:li:actionRequest:abc",
  "entityType": "actionRequest",
  "category": "LIFECYCLE",
  "operation": "CREATED",
  "parameters": {
    "actionRequestType": "TAG_ASSOCIATION",
    "resourceType": "dataset",
    "resourceUrn": "urn:li:dataset:snowflakeDataset,
    "tagUrn": "urn:li:tag:Classification"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

#### Term Association Proposal Specific Parameters and Sample Event

| Name    | Type   | Description                                         | Optional |
| ------- | ------ | --------------------------------------------------- | -------- |
| termUrn | String | The urn of the Glossary Term that would be applied. | False    |

```
{
  "entityUrn": "urn:li:actionRequest:abc",
  "entityType": "actionRequest",
  "category": "LIFECYCLE",
  "operation": "CREATED",
  "parameters": {
    "actionRequestType": "TERM_ASSOCIATION",
    "resourceType": "dataset",
    "resourceUrn": "urn:li:dataset:snowflakeDataset,
    "termUrn": "urn:li:glossaryTerm:Classification"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

#### Create Glossary Node/Term Proposal Specific Parameters and Sample Event

| Name               | Type   | Description                                                                       | Optional |
| ------------------ | ------ | --------------------------------------------------------------------------------- | -------- |
| glossaryEntityName | String | The name of the Glossary Entity that would be created.                            | False    |
| parentNodeUrn      | String | The urn of the Parent Node that would be associated with the new Glossary Entity. | True     |
| description        | String | The description of the new Glossary Entity.                                       | True     |

```
{
  "entityUrn": "urn:li:actionRequest:abc",
  "entityType": "actionRequest",
  "category": "LIFECYCLE",
  "operation": "CREATED",
  "parameters": {
    "actionRequestType": "CREATE_GLOSSARY_TERM",
    "resourceType": "glossaryNode",
    "glossaryEntityName": "PII",
    "parentNodeUrn": "urn:li:glossaryNode:Classification",
    "description": "Personally Identifiable Information"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

#### Update Description Proposal Specific Parameters

| Name        | Type   | Description                       | Optional |
| ----------- | ------ | --------------------------------- | -------- |
| description | String | The proposed updated description. | False    |

```
{
  "entityUrn": "urn:li:actionRequest:abc",
  "entityType": "actionRequest",
  "category": "LIFECYCLE",
  "operation": "CREATED",
  "parameters": {
    "actionRequestType": "UPDATE_DESCRIPTION",
    "resourceType": "glossaryNode",
    "description": "Personally Identifiable Information"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Action Request Status Change Event

This event is emitted when an existing Action Request (proposal) changes status. For example, this event will be emitted when an Action Request transitions from pending to completed.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>LIFECYCLE</td><td><code>PENDING,</code> <code>COMPLETED</code></td><td><code>actionRequest</code></td><td></td></tr></tbody></table>

#### Parameters

These are the common parameters for all parameters.


| Name                | Type   | Description                                                                               | Optional |
| ------------------- | ------ | ----------------------------------------------------------------------------------------- | -------- |
| actionRequestStatus | String | The status of the Action Request.                                                         | False    |
| actionRequestResult | String | Only filled if the `actionRequestStatus` is `COMPLETED`. Either `ACCEPTED` or `REJECTED`. | True     |

#### Sample Event

```
{
  "entityUrn": "urn:li:actionRequest:abc",
  "entityType": "actionRequest",
  "category": "LIFECYCLE",
  "operation": "COMPLETED",
  "parameters": {
    "actionRequestStatus": "COMPLETED",
    "actionRequestResult": "ACCEPTED"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Incident Change Event

This event is emitted when an Incident has been created or it's status changes.

#### Header

<table><thead><tr><th>Category</th><th>Operation</th><th>Entity Types</th><th data-hidden></th></tr></thead><tbody><tr><td>INCIDENT</td><td><code>ACTIVE, </code><code>RESOLVED</code></td><td><code>incident</code></td><td></td></tr></tbody></table>

#### Parameters

| Name         | Type   | Description                                       | Optional |
|--------------| ------ |---------------------------------------------------| -------- |
| entities     | String | The list of entities associated with the incident | False    |

#### Sample Event

```
{
  "entityUrn": "urn:li:incident:16ff200a-0ac5-4a7d-bbab-d4bdb4f831f9",
  "entityType": "incident",
  "category": "INCIDENT",
  "operation": "ACTIVE",
  "parameters": {
    "entities": "[urn:li:dataset:abc, urn:li:dataset:abc2]",
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```
