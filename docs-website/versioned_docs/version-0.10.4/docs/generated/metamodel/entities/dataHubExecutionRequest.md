---
sidebar_position: 37
title: DataHubExecutionRequest
slug: /generated/metamodel/entities/datahubexecutionrequest
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataHubExecutionRequest.md
---

# DataHubExecutionRequest

## Aspects

### dataHubExecutionRequestInput

An request to execution some remote logic or action.
TODO: Determine who is responsible for emitting execution request success or failure. Executor?

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubExecutionRequestInput"
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
    }
  ],
  "doc": "An request to execution some remote logic or action.\nTODO: Determine who is responsible for emitting execution request success or failure. Executor?"
}
```

</details>

### dataHubExecutionRequestSignal

An signal sent to a running execution request

<details>
<summary>Schema</summary>

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

</details>

### dataHubExecutionRequestResult

The result of an execution request

<details>
<summary>Schema</summary>

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

</details>

## Relationships

### Outgoing

These are the relationships stored in this entity's aspects

- ingestionSource

  - DataHubIngestionSource via `dataHubExecutionRequestInput.source.ingestionSource`

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
