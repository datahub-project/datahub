# DataHubExecutionRequest


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

#### dataHubExecutionRequestInput
An request to execution some remote logic or action.
TODO: Determine who is responsible for emitting execution request success or failure. Executor?



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| task | string | ✓ | The name of the task to execute, for example RUN_INGEST |  |
| args | map | ✓ | Arguments provided to the task |  |
| executorId | string | ✓ | Advanced: specify a specific executor to route the request to. If none is provided, a "default" e... | Searchable |
| source | ExecutionRequestSource | ✓ | Source which created the execution request |  |
| requestedAt | long | ✓ | Time at which the execution request input was created | Searchable (requestTimeMs) |
| actorUrn | string |  | Urn of the actor who created this execution request. | Searchable |
| cliVersionAudit | CliVersionAudit |  | Audit metadata for the CLI version chosen for this execution — which tier of the resolution chain... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubExecutionRequestInput",
    "schemaVersion": 2
  },
  "name": "ExecutionRequestInput",
  "namespace": "com.linkedin.execution",
  "fields": [
    {
      "type": "string",
      "name": "task",
      "doc": "The name of the task to execute, for example RUN_INGEST"
    },
    {
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "args",
      "doc": "Arguments provided to the task"
    },
    {
      "Searchable": {
        "fieldName": "executorId",
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "type": "string",
      "name": "executorId",
      "doc": "Advanced: specify a specific executor to route the request to. If none is provided, a \"default\" executor is used."
    },
    {
      "type": {
        "type": "record",
        "name": "ExecutionRequestSource",
        "namespace": "com.linkedin.execution",
        "fields": [
          {
            "type": "string",
            "name": "type",
            "doc": "The type of the execution request source, e.g. INGESTION_SOURCE"
          },
          {
            "Relationship": {
              "entityTypes": [
                "dataHubIngestionSource"
              ],
              "name": "ingestionSource"
            },
            "Searchable": {
              "fieldName": "ingestionSource",
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
            "name": "ingestionSource",
            "default": null,
            "doc": "The urn of the ingestion source associated with the ingestion request. Present if type is INGESTION_SOURCE"
          }
        ]
      },
      "name": "source",
      "doc": "Source which created the execution request"
    },
    {
      "Searchable": {
        "fieldName": "requestTimeMs",
        "fieldType": "COUNT",
        "queryByDefault": false
      },
      "type": "long",
      "name": "requestedAt",
      "doc": "Time at which the execution request input was created"
    },
    {
      "Searchable": {
        "fieldType": "URN"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "actorUrn",
      "default": null,
      "doc": "Urn of the actor who created this execution request."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "CliVersionAudit",
          "namespace": "com.linkedin.execution",
          "fields": [
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "APPLICATION_DEFAULT": " Step 4 \u2014 fell through to defaultCliVersion from application.yaml. ",
                  "MATRIX_COHORT": " Step 2 \u2014 matched a cohort whose deployments list contains this deployment's id. ",
                  "MATRIX_CONNECTOR_DEFAULT": " Step 3 \u2014 fell through to the connector's _default in the matrix. ",
                  "SOURCE_CONFIG_OVERRIDE": " Step 1 \u2014 explicit cli_version on the ingestion source's recipe config. "
                },
                "name": "CliVersionSource",
                "namespace": "com.linkedin.execution",
                "symbols": [
                  "SOURCE_CONFIG_OVERRIDE",
                  "MATRIX_COHORT",
                  "MATRIX_CONNECTOR_DEFAULT",
                  "APPLICATION_DEFAULT"
                ]
              },
              "name": "source",
              "doc": "Which level of the resolution priority hit."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "serverVersion",
              "default": null,
              "doc": "GMS server version that performed the resolution. Populated regardless of which tier hit.\nEquals `GitVersion.getVersion()` on the pod that wrote this aspect."
            }
          ],
          "doc": "Audit record for the CLI version chosen for an ingestion execution.\n\nStamped on each ingestion or test-connection ExecutionRequestInput. Captures only metadata\nabout the resolution (which tier fired + which GMS performed it) \u2014 the resolved CLI version\nitself lives in `args.version` on the same aspect (the wire-format field consumed by the\nexecutor). Splitting these avoids storing the version string twice on the aspect; this record\nexists so post-hoc forensics can answer \"which tier produced the version, and which GMS wrote\nthis?\" from a single SQL query without log archaeology.\n\nNOTE: this stamps the version GMS *resolved*, not the version the executor actually\ninstalled. The executor may run a different effective version when (a) the recipe's\n`extra_pip` requirements transitively pull in `acryl-datahub`, (b) the customer opts out\nof installing `acryl-datahub` (e.g. `version=\"no-acryl-datahub\"`), or (c) a bundled image\nshort-circuits the install step. Treat this aspect as GMS-side intent, not proof-of-install.\n\nThe resolution chain is, in priority order:\n  1. Per-source `config.version` explicit override (SOURCE_CONFIG_OVERRIDE)\n  2. Cohort whose `deployments` list contains this deployment's id (MATRIX_COHORT)\n  3. Connector's `_default` from the matrix (MATRIX_CONNECTOR_DEFAULT)\n  4. `defaultCliVersion` from application.yaml (APPLICATION_DEFAULT)"
        }
      ],
      "name": "cliVersionAudit",
      "default": null,
      "doc": "Audit metadata for the CLI version chosen for this execution \u2014 which tier of the\nresolution chain produced the version (source config override / matrix cohort / matrix\nconnector default / application default) and which GMS performed the resolution. Stamped at\nrequest time so post-hoc forensics does not require iterating the generic args map. The\nresolved CLI version string itself lives in `args.version` on this same aspect; this record\ndeliberately does not duplicate it. Optional for backward compatibility \u2014 older execution\nrequests will not have this set."
    }
  ],
  "doc": "An request to execution some remote logic or action.\nTODO: Determine who is responsible for emitting execution request success or failure. Executor?"
}
```





#### dataHubExecutionRequestSignal
An signal sent to a running execution request



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| signal | string | ✓ | The signal to issue, e.g. KILL |  |
| executorId | string |  | Advanced: specify a specific executor to route the request to. If none is provided, a "default" e... |  |
| createdAt | [AuditStamp](#auditstamp) | ✓ | Audit Stamp |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubExecutionRequestSignal"
  },
  "name": "ExecutionRequestSignal",
  "namespace": "com.linkedin.execution",
  "fields": [
    {
      "type": "string",
      "name": "signal",
      "doc": "The signal to issue, e.g. KILL"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "executorId",
      "default": null,
      "doc": "Advanced: specify a specific executor to route the request to. If none is provided, a \"default\" executor is used."
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
      "name": "createdAt",
      "doc": "Audit Stamp"
    }
  ],
  "doc": "An signal sent to a running execution request"
}
```





#### dataHubExecutionRequestResult
The result of an execution request



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| status | string | ✓ | The status of the execution request | Searchable (executionResultStatus) |
| report | string |  | The pretty-printed execution report. |  |
| structuredReport | StructuredExecutionReport |  | A structured report if available. |  |
| startTimeMs | long |  | Time at which the request was created | Searchable |
| durationMs | long |  | Duration in milliseconds |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubExecutionRequestResult"
  },
  "name": "ExecutionRequestResult",
  "namespace": "com.linkedin.execution",
  "fields": [
    {
      "Searchable": {
        "fieldName": "executionResultStatus",
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "status",
      "doc": "The status of the execution request"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "report",
      "default": null,
      "doc": "The pretty-printed execution report."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "StructuredExecutionReport",
          "namespace": "com.linkedin.execution",
          "fields": [
            {
              "type": "string",
              "name": "type",
              "doc": "The type of the structured report. (e.g. INGESTION_REPORT, TEST_CONNECTION_REPORT, etc.)"
            },
            {
              "type": "string",
              "name": "serializedValue",
              "doc": "The serialized value of the structured report"
            },
            {
              "type": "string",
              "name": "contentType",
              "doc": "The content-type of the serialized value (e.g. application/json, application/json;gzip etc.)"
            }
          ],
          "doc": "A flexible carrier for structured results of an execution request.\nThe goal is to allow for free flow of structured responses from execution tasks to the orchestrator or observer.\nThe full spectrum of different execution report types is not intended to be modeled by this object."
        }
      ],
      "name": "structuredReport",
      "default": null,
      "doc": "A structured report if available."
    },
    {
      "Searchable": {
        "fieldName": "startTimeMs",
        "fieldType": "COUNT",
        "queryByDefault": false
      },
      "type": [
        "null",
        "long"
      ],
      "name": "startTimeMs",
      "default": null,
      "doc": "Time at which the request was created"
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "durationMs",
      "default": null,
      "doc": "Duration in milliseconds"
    }
  ],
  "doc": "The result of an execution request"
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
- ingestionSource

   - DataHubIngestionSource via `dataHubExecutionRequestInput.source.ingestionSource`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
