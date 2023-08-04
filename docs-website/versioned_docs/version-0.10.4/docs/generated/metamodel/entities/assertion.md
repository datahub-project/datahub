---
sidebar_position: 17
title: Assertion
slug: /generated/metamodel/entities/assertion
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/assertion.md
---

# Assertion

Assertion entity represents a data quality rule applied on dataset.  
In future, it can evolve to span across Datasets, Flows (Pipelines), Models, Features etc.

## Identity

An **Assertion** is identified by globally unique identifier which remains constant between runs of the assertion. For each source of assertion information, it is expected that the logic required to generate the stable guid will differ. For example, a unique GUID is generated from each assertion from Great Expectations based on a combination of the assertion name along with its parameters.

## Important Capabilities

### Assertion Info

Type and Details of assertions set on a Dataset (Table).

**Scope**: Column, Rows, Schema
**Inputs**: Column(s)
**Aggregation**: Max, Min, etc
**Operator**: Greater Than, Not null, etc
**Parameters**: Value, Min Value, Max Value

### Assertion Run Events

Evaluation status and results for an assertion tracked over time.

<details>
<summary>Python SDK: Emit assertion info and results for dataset </summary>

```python
# Inlined from /metadata-ingestion/examples/library/data_quality_mcpw_rest.py
import json
import time

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    DatasetAssertionInfo,
    DatasetAssertionScope,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import PartitionSpec


def datasetUrn(tbl: str) -> str:
    return builder.make_dataset_urn("postgres", tbl)


def fldUrn(tbl: str, fld: str) -> str:
    return f"urn:li:schemaField:({datasetUrn(tbl)}, {fld})"


def assertionUrn(info: AssertionInfo) -> str:
    return "urn:li:assertion:432475190cc846f2894b5b3aa4d55af2"


def emitAssertionResult(assertionResult: AssertionRunEvent) -> None:
    dataset_assertionRunEvent_mcp = MetadataChangeProposalWrapper(
        entityUrn=assertionResult.assertionUrn,
        aspect=assertionResult,
    )

    # Emit BatchAssertion Result! (timeseries aspect)
    emitter.emit_mcp(dataset_assertionRunEvent_mcp)


# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

datasetProperties = DatasetProperties(
    name="bazTable",
)
# Construct a MetadataChangeProposalWrapper object for dataset
dataset_mcp = MetadataChangeProposalWrapper(
    entityUrn=datasetUrn("bazTable"),
    aspect=datasetProperties,
)

# Emit Dataset entity properties aspect! (Skip if dataset is already present)
emitter.emit_mcp(dataset_mcp)

# Construct an assertion object.
assertion_maxVal = AssertionInfo(
    type=AssertionType.DATASET,
    datasetAssertion=DatasetAssertionInfo(
        scope=DatasetAssertionScope.DATASET_COLUMN,
        operator=AssertionStdOperator.BETWEEN,
        nativeType="expect_column_max_to_be_between",
        aggregation=AssertionStdAggregation.MAX,
        fields=[fldUrn("bazTable", "col1")],
        dataset=datasetUrn("bazTable"),
        nativeParameters={"max_value": "99", "min_value": "89"},
        parameters=AssertionStdParameters(
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="89"
            ),
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="99"
            ),
        ),
    ),
    customProperties={"suite_name": "demo_suite"},
)

# Construct a MetadataChangeProposalWrapper object.
assertion_maxVal_mcp = MetadataChangeProposalWrapper(
    entityUrn=assertionUrn(assertion_maxVal),
    aspect=assertion_maxVal,
)

# Emit Assertion entity info aspect!
emitter.emit_mcp(assertion_maxVal_mcp)

# Construct an assertion platform object.
assertion_dataPlatformInstance = DataPlatformInstance(
    platform=builder.make_data_platform_urn("great-expectations")
)

# Construct a MetadataChangeProposalWrapper object for assertion platform
assertion_dataPlatformInstance_mcp = MetadataChangeProposalWrapper(
    entityUrn=assertionUrn(assertion_maxVal),
    aspect=assertion_dataPlatformInstance,
)
# Emit Assertion entity platform aspect!
emitter.emit(assertion_dataPlatformInstance_mcp)


# Construct batch assertion result object for partition 1 batch
assertionResult_maxVal_batch_partition1 = AssertionRunEvent(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("bazTable"),
    partitionSpec=PartitionSpec(partition=json.dumps([{"country": "IN"}])),
    runId="uuid1",
    status=AssertionRunStatus.COMPLETE,
    result=AssertionResult(
        type=AssertionResultType.SUCCESS,
        externalUrl="http://example.com/uuid1",
        actualAggValue=90,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_partition1,
)

# Construct batch assertion result object for partition 2 batch
assertionResult_maxVal_batch_partition2 = AssertionRunEvent(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("bazTable"),
    partitionSpec=PartitionSpec(partition=json.dumps([{"country": "US"}])),
    runId="uuid1",
    status=AssertionRunStatus.COMPLETE,
    result=AssertionResult(
        type=AssertionResultType.FAILURE,
        externalUrl="http://example.com/uuid1",
        actualAggValue=101,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_partition2,
)

# Construct batch assertion result object for full table batch.
assertionResult_maxVal_batch_fulltable = AssertionRunEvent(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("bazTable"),
    runId="uuid1",
    status=AssertionRunStatus.COMPLETE,
    result=AssertionResult(
        type=AssertionResultType.SUCCESS,
        externalUrl="http://example.com/uuid1",
        actualAggValue=93,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_fulltable,
)

```

</details>

## Aspects

### assertionInfo

Information about an assertion

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "assertionInfo"
  },
  "name": "AssertionInfo",
  "namespace": "com.linkedin.assertion",
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
      "type": {
        "type": "enum",
        "name": "AssertionType",
        "namespace": "com.linkedin.assertion",
        "symbols": [
          "DATASET"
        ]
      },
      "name": "type",
      "doc": "Type of assertion. Assertion types can evolve to span Datasets, Flows (Pipelines), Models, Features etc."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "DatasetAssertionInfo",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "dataset"
                ],
                "name": "Asserts"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "dataset",
              "doc": "The dataset targeted by this assertion."
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "DATASET_COLUMN": "This assertion applies to dataset columns",
                  "DATASET_ROWS": "This assertion applies to entire rows of the dataset",
                  "DATASET_SCHEMA": "This assertion applies to the schema of the dataset",
                  "UNKNOWN": "The scope of the assertion is unknown"
                },
                "name": "DatasetAssertionScope",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "DATASET_COLUMN",
                  "DATASET_ROWS",
                  "DATASET_SCHEMA",
                  "UNKNOWN"
                ]
              },
              "name": "scope",
              "doc": "Scope of the Assertion. What part of the dataset does this assertion apply to?"
            },
            {
              "Relationship": {
                "/*": {
                  "entityTypes": [
                    "schemaField"
                  ],
                  "name": "Asserts"
                }
              },
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "fields",
              "default": null,
              "doc": "One or more dataset schema fields that are targeted by this assertion"
            },
            {
              "type": [
                "null",
                {
                  "type": "enum",
                  "symbolDocs": {
                    "COLUMNS": "Assertion is applied on all columns.",
                    "COLUMN_COUNT": "Assertion is applied on number of columns.",
                    "IDENTITY": "Assertion is applied on individual column value.",
                    "MAX": "Assertion is applied on column std deviation",
                    "MEAN": "Assertion is applied on column mean",
                    "MEDIAN": "Assertion is applied on column median",
                    "MIN": "Assertion is applied on column min",
                    "NULL_COUNT": "Assertion is applied on number of null values in column",
                    "NULL_PROPORTION": "Assertion is applied on proportion of null values in column",
                    "ROW_COUNT": "Assertion is applied on number of rows.",
                    "STDDEV": "Assertion is applied on column std deviation",
                    "SUM": "Assertion is applied on column sum",
                    "UNIQUE_COUNT": "Assertion is applied on number of distinct values in column",
                    "UNIQUE_PROPOTION": "Assertion is applied on proportion of distinct values in column",
                    "_NATIVE_": "Other"
                  },
                  "name": "AssertionStdAggregation",
                  "namespace": "com.linkedin.assertion",
                  "symbols": [
                    "ROW_COUNT",
                    "COLUMNS",
                    "COLUMN_COUNT",
                    "IDENTITY",
                    "MEAN",
                    "MEDIAN",
                    "UNIQUE_COUNT",
                    "UNIQUE_PROPOTION",
                    "NULL_COUNT",
                    "NULL_PROPORTION",
                    "STDDEV",
                    "MIN",
                    "MAX",
                    "SUM",
                    "_NATIVE_"
                  ],
                  "doc": "The function that is applied to the aggregation input (schema, rows, column values) before evaluating an operator."
                }
              ],
              "name": "aggregation",
              "default": null,
              "doc": "Standardized assertion operator"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BETWEEN": "Value being asserted is between min_value and max_value.  Requires 'minValue' & 'maxValue' parameters.",
                  "CONTAIN": "Value being asserted contains value. Requires 'value' parameter.",
                  "END_WITH": "Value being asserted ends with value. Requires 'value' parameter.",
                  "EQUAL_TO": "Value being asserted is equal to value. Requires 'value' parameter.",
                  "GREATER_THAN": "Value being asserted is greater than some value. Requires 'value' parameter.",
                  "GREATER_THAN_OR_EQUAL_TO": "Value being asserted is greater than or equal to some value. Requires 'value' parameter.",
                  "IN": "Value being asserted is one of the array values. Requires 'value' parameter.",
                  "LESS_THAN": "Value being asserted is less than a max value. Requires 'value' parameter.",
                  "LESS_THAN_OR_EQUAL_TO": "Value being asserted is less than or equal to some value. Requires 'value' parameter.",
                  "NOT_IN": "Value being asserted is not in one of the array values. Requires 'value' parameter.",
                  "NOT_NULL": "Value being asserted is not null. Requires no parameters.",
                  "REGEX_MATCH": "Value being asserted matches the regex value. Requires 'value' parameter.",
                  "START_WITH": "Value being asserted starts with value. Requires 'value' parameter.",
                  "_NATIVE_": "Other"
                },
                "name": "AssertionStdOperator",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "BETWEEN",
                  "LESS_THAN",
                  "LESS_THAN_OR_EQUAL_TO",
                  "GREATER_THAN",
                  "GREATER_THAN_OR_EQUAL_TO",
                  "EQUAL_TO",
                  "NOT_NULL",
                  "CONTAIN",
                  "END_WITH",
                  "START_WITH",
                  "REGEX_MATCH",
                  "IN",
                  "NOT_IN",
                  "_NATIVE_"
                ],
                "doc": "A boolean operator that is applied on the input to an assertion, after an aggregation function has been applied."
              },
              "name": "operator",
              "doc": "Standardized assertion operator"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AssertionStdParameters",
                  "namespace": "com.linkedin.assertion",
                  "fields": [
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "AssertionStdParameter",
                          "namespace": "com.linkedin.assertion",
                          "fields": [
                            {
                              "type": "string",
                              "name": "value",
                              "doc": "The parameter value"
                            },
                            {
                              "type": {
                                "type": "enum",
                                "name": "AssertionStdParameterType",
                                "namespace": "com.linkedin.assertion",
                                "symbols": [
                                  "STRING",
                                  "NUMBER",
                                  "LIST",
                                  "SET",
                                  "UNKNOWN"
                                ]
                              },
                              "name": "type",
                              "doc": "The type of the parameter"
                            }
                          ],
                          "doc": "Single parameter for AssertionStdOperators."
                        }
                      ],
                      "name": "value",
                      "default": null,
                      "doc": "The value parameter of an assertion"
                    },
                    {
                      "type": [
                        "null",
                        "com.linkedin.assertion.AssertionStdParameter"
                      ],
                      "name": "maxValue",
                      "default": null,
                      "doc": "The maxValue parameter of an assertion"
                    },
                    {
                      "type": [
                        "null",
                        "com.linkedin.assertion.AssertionStdParameter"
                      ],
                      "name": "minValue",
                      "default": null,
                      "doc": "The minValue parameter of an assertion"
                    }
                  ],
                  "doc": "Parameters for AssertionStdOperators."
                }
              ],
              "name": "parameters",
              "default": null,
              "doc": "Standard parameters required for the assertion. e.g. min_value, max_value, value, columns"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "nativeType",
              "default": null,
              "doc": "Native assertion type"
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "nativeParameters",
              "default": null,
              "doc": "Native parameters required for the assertion."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "logic",
              "default": null
            }
          ],
          "doc": "Attributes that are applicable to single-Dataset Assertions"
        }
      ],
      "name": "datasetAssertion",
      "default": null,
      "doc": "Dataset Assertion information when type is DATASET"
    }
  ],
  "doc": "Information about an assertion"
}
```

</details>

### dataPlatformInstance

The specific instance of the data platform that this entity belongs to

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```

</details>

### status

The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.

<details>
<summary>Schema</summary>

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
      "doc": "Whether the entity has been removed (soft-deleted)."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```

</details>

### assertionRunEvent (Timeseries)

An event representing the current status of evaluating an assertion on a batch.
AssertionRunEvent should be used for reporting the status of a run as an assertion evaluation progresses.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "assertionRunEvent",
    "type": "timeseries"
  },
  "name": "AssertionRunEvent",
  "namespace": "com.linkedin.assertion",
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
      "type": "string",
      "name": "runId",
      "doc": " Native (platform-specific) identifier for this run"
    },
    {
      "TimeseriesField": {},
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "assertionUrn"
    },
    {
      "TimeseriesField": {},
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "asserteeUrn"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "BatchSpec",
          "namespace": "com.linkedin.assertion",
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
              "type": [
                "null",
                "string"
              ],
              "name": "nativeBatchId",
              "default": null,
              "doc": "The native identifier as specified by the system operating on the batch."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "query",
              "default": null,
              "doc": "A query that identifies a batch of data"
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "limit",
              "default": null,
              "doc": "Any limit to the number of rows in the batch, if applied"
            }
          ],
          "doc": "A batch on which certain operations, e.g. data quality evaluation, is done."
        }
      ],
      "name": "batchSpec",
      "default": null,
      "doc": "Specification of the batch which this run is evaluating"
    },
    {
      "TimeseriesField": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "COMPLETE": "The Assertion Run has completed"
        },
        "name": "AssertionRunStatus",
        "namespace": "com.linkedin.assertion",
        "symbols": [
          "COMPLETE"
        ]
      },
      "name": "status",
      "doc": "The status of the assertion run as per this timeseries event."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AssertionResult",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "TimeseriesField": {},
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Assertion Failed",
                  "SUCCESS": " The Assertion Succeeded"
                },
                "name": "AssertionResultType",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": " The final result, e.g. either SUCCESS or FAILURE."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "rowCount",
              "default": null,
              "doc": "Number of rows for evaluated batch"
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "missingCount",
              "default": null,
              "doc": "Number of rows with missing value for evaluated batch"
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "unexpectedCount",
              "default": null,
              "doc": "Number of rows with unexpected value for evaluated batch"
            },
            {
              "type": [
                "null",
                "float"
              ],
              "name": "actualAggValue",
              "default": null,
              "doc": "Observed aggregate value for evaluated batch"
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "nativeResults",
              "default": null,
              "doc": "Other results of evaluation"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "externalUrl",
              "default": null,
              "doc": "URL where full results are available"
            }
          ],
          "doc": "The result of running an assertion"
        }
      ],
      "name": "result",
      "default": null,
      "doc": "Results of assertion, present if the status is COMPLETE"
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "name": "runtimeContext",
      "default": null,
      "doc": "Runtime parameters of evaluation"
    }
  ],
  "doc": "An event representing the current status of evaluating an assertion on a batch.\nAssertionRunEvent should be used for reporting the status of a run as an assertion evaluation progresses."
}
```

</details>

## Relationships

### Outgoing

These are the relationships stored in this entity's aspects

- Asserts

  - Dataset via `assertionInfo.datasetAssertion.dataset`
  - SchemaField via `assertionInfo.datasetAssertion.fields`

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
