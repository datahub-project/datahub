---
sidebar_position: 6
title: DataProcessInstance
slug: /generated/metamodel/entities/dataprocessinstance
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataProcessInstance.md
---

# DataProcessInstance

DataProcessInstance represents an instance of a datajob/jobflow run

## Aspects

### dataProcessInstanceInput

Information about the inputs datasets of a Data process

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceInput"
  },
  "name": "DataProcessInstanceInput",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "name": "Consumes"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "inputs",
          "fieldType": "URN",
          "numValuesFieldName": "numInputs",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "inputs",
      "doc": "Input datasets to be consumed"
    }
  ],
  "doc": "Information about the inputs datasets of a Data process"
}
```

</details>

### dataProcessInstanceOutput

Information about the outputs of a Data process

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceOutput"
  },
  "name": "DataProcessInstanceOutput",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "name": "Produces"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "outputs",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputs",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "outputs",
      "doc": "Output datasets to be produced"
    }
  ],
  "doc": "Information about the outputs of a Data process"
}
```

</details>

### dataProcessInstanceProperties

The inputs and outputs of this data process

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceProperties"
  },
  "name": "DataProcessInstanceProperties",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Searchable": {
        "/*": {
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
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "Process name"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "Process Type"
      },
      "type": [
        "null",
        {
          "type": "enum",
          "name": "DataProcessType",
          "namespace": "com.linkedin.dataprocess",
          "symbols": [
            "BATCH_SCHEDULED",
            "BATCH_AD_HOC",
            "STREAMING"
          ]
        }
      ],
      "name": "type",
      "default": null,
      "doc": "Process type"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "created",
          "fieldType": "COUNT",
          "queryByDefault": false
        }
      },
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
      "doc": "Audit stamp containing who reported the lineage and when"
    }
  ],
  "doc": "The inputs and outputs of this data process"
}
```

</details>

### dataProcessInstanceRelationships

Information about Data process relationships

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceRelationships"
  },
  "name": "DataProcessInstanceRelationships",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "dataJob",
          "dataFlow"
        ],
        "name": "InstanceOf"
      },
      "Searchable": {
        "/*": {
          "fieldName": "parentTemplate",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parentTemplate",
      "default": null,
      "doc": "The parent entity whose run instance it is"
    },
    {
      "Relationship": {
        "entityTypes": [
          "dataProcessInstance"
        ],
        "name": "ChildOf"
      },
      "Searchable": {
        "/*": {
          "fieldName": "parentInstance",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parentInstance",
      "default": null,
      "doc": "The parent DataProcessInstance where it belongs to.\nIf it is a Airflow Task then it should belong to an Airflow Dag run as well\nwhich will be another DataProcessInstance"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataProcessInstance"
          ],
          "name": "UpstreamOf"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "upstream",
          "fieldType": "URN",
          "numValuesFieldName": "numUpstreams",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "upstreamInstances",
      "doc": "Input DataProcessInstance which triggered this dataprocess instance"
    }
  ],
  "doc": "Information about Data process relationships"
}
```

</details>

### dataProcessInstanceRunEvent (Timeseries)

An event representing the current status of data process run.
DataProcessRunEvent should be used for reporting the status of a dataProcess' run.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceRunEvent",
    "type": "timeseries"
  },
  "name": "DataProcessInstanceRunEvent",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION"
            },
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "String representation of the partition"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition if applicable"
            }
          ],
          "doc": "Defines how the data is partitioned"
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "TimeseriesField": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "STARTED": "The status where the Data processing run is in."
        },
        "name": "DataProcessRunStatus",
        "namespace": "com.linkedin.dataprocess",
        "symbols": [
          "STARTED",
          "COMPLETE"
        ]
      },
      "name": "status"
    },
    {
      "type": [
        "null",
        "int"
      ],
      "name": "attempt",
      "default": null,
      "doc": "Return the try number that this Instance Run is in"
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        {
          "type": "record",
          "name": "DataProcessInstanceRunResult",
          "namespace": "com.linkedin.dataprocess",
          "fields": [
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Run Failed",
                  "SKIPPED": " The Run Skipped",
                  "SUCCESS": " The Run Succeeded",
                  "UP_FOR_RETRY": " The Run Failed and will Retry"
                },
                "name": "RunResultType",
                "namespace": "com.linkedin.dataprocess",
                "symbols": [
                  "SUCCESS",
                  "FAILURE",
                  "SKIPPED",
                  "UP_FOR_RETRY"
                ]
              },
              "name": "type",
              "doc": " The final result, e.g. SUCCESS, FAILURE, SKIPPED, or UP_FOR_RETRY."
            },
            {
              "type": "string",
              "name": "nativeResultType",
              "doc": "It identifies the system where the native result comes from like Airflow, Azkaban, etc.."
            }
          ]
        }
      ],
      "name": "result",
      "default": null,
      "doc": "The final result of the Data Processing run."
    }
  ],
  "doc": "An event representing the current status of data process run.\nDataProcessRunEvent should be used for reporting the status of a dataProcess' run."
}
```

</details>

## Relationships

### Self

These are the relationships to itself, stored in this entity's aspects

- ChildOf (via `dataProcessInstanceRelationships.parentInstance`)
- UpstreamOf (via `dataProcessInstanceRelationships.upstreamInstances`)

### Outgoing

These are the relationships stored in this entity's aspects

- Consumes

  - Dataset via `dataProcessInstanceInput.inputs`

- Produces

  - Dataset via `dataProcessInstanceOutput.outputs`

- InstanceOf

  - DataJob via `dataProcessInstanceRelationships.parentTemplate`
  - DataFlow via `dataProcessInstanceRelationships.parentTemplate`

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
