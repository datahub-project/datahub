---
sidebar_position: 16
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
      "Searchable": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "CUSTOM": "A custom assertion. \nWhen this is the value, the customAssertion field will be populated.\nUse this assertion type when the exact type of assertion is not modeled in DataHub or\nas a starting point when integrating third-party data quality tools.",
          "DATASET": "A single-dataset assertion.\nWhen this is the value, the datasetAssertion field will be populated.",
          "DATA_SCHEMA": "A schema or structural assertion.\n\nWould have named this SCHEMA but the codegen for PDL does not allow this (reserved word).",
          "FIELD": "A structured assertion targeting a specific column or field of the Dataset.",
          "FRESHNESS": "A freshness assertion, or an assertion which indicates when a particular operation should occur\nto an asset.",
          "SQL": "A raw SQL-statement based assertion",
          "VOLUME": "A volume assertion, or an assertion which indicates how much data should be available for a\nparticular asset."
        },
        "name": "AssertionType",
        "namespace": "com.linkedin.assertion",
        "symbols": [
          "DATASET",
          "FRESHNESS",
          "VOLUME",
          "SQL",
          "FIELD",
          "DATA_SCHEMA",
          "CUSTOM"
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
              "Searchable": {},
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "DATASET_COLUMN": "This assertion applies to dataset column(s)",
                  "DATASET_ROWS": "This assertion applies to entire rows of the dataset",
                  "DATASET_SCHEMA": "This assertion applies to the schema of the dataset",
                  "DATASET_STORAGE_SIZE": "This assertion applies to the storage size of the dataset",
                  "UNKNOWN": "The scope of the assertion is unknown"
                },
                "name": "DatasetAssertionScope",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "DATASET_COLUMN",
                  "DATASET_ROWS",
                  "DATASET_STORAGE_SIZE",
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
              "Searchable": {
                "/*": {
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
              "name": "fields",
              "default": null,
              "doc": "One or more dataset schema fields that are targeted by this assertion.\n\nThis field is expected to be provided if the assertion scope is DATASET_COLUMN."
            },
            {
              "Searchable": {},
              "type": [
                "null",
                {
                  "type": "enum",
                  "symbolDocs": {
                    "COLUMNS": "Assertion is applied on all columns.",
                    "COLUMN_COUNT": "Assertion is applied on number of columns.",
                    "IDENTITY": "Assertion is applied on individual column value. (No aggregation)",
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
                    "UNIQUE_PROPORTION": "Assertion is applied on proportion of distinct values in column",
                    "UNIQUE_PROPOTION": "Assertion is applied on proportion of distinct values in column\n\nDeprecated! Use UNIQUE_PROPORTION instead.",
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
                    "UNIQUE_PROPORTION",
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
              "doc": "Standardized assertion operator\nThis field is left blank if there is no selected aggregation or metric for a particular column."
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
                  "IS_FALSE": "Value being asserted is false. Requires no parameters.",
                  "IS_TRUE": "Value being asserted is true. Requires no parameters.",
                  "LESS_THAN": "Value being asserted is less than a max value. Requires 'value' parameter.",
                  "LESS_THAN_OR_EQUAL_TO": "Value being asserted is less than or equal to some value. Requires 'value' parameter.",
                  "NOT_EQUAL_TO": "Value being asserted is not equal to value. Requires 'value' parameter.",
                  "NOT_IN": "Value being asserted is not in one of the array values. Requires 'value' parameter.",
                  "NOT_NULL": "Value being asserted is not null. Requires no parameters.",
                  "NULL": "Value being asserted is null. Requires no parameters.",
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
                  "NOT_EQUAL_TO",
                  "NULL",
                  "NOT_NULL",
                  "CONTAIN",
                  "END_WITH",
                  "START_WITH",
                  "REGEX_MATCH",
                  "IN",
                  "NOT_IN",
                  "IS_TRUE",
                  "IS_FALSE",
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
                                "symbolDocs": {
                                  "LIST": "A list of values. When used, value should be formatted as a serialized JSON array.",
                                  "NUMBER": "A numeric value",
                                  "SET": "A set of values. When used, value should be formatted as a serialized JSON array.",
                                  "STRING": "A string value",
                                  "UNKNOWN": "A value of unknown type"
                                },
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
      "doc": "A Dataset Assertion definition. This field is populated when the type is DATASET."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "FreshnessAssertionInfo",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "Searchable": {},
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "DATASET_CHANGE": "An Freshness based on Operations performed on a particular Dataset (insert, update, delete, etc) and sourced from an audit log, as\nopposed to based on the highest watermark in a timestamp column (e.g. a query). Only valid when entity is of type \"dataset\".",
                  "DATA_JOB_RUN": "An Freshness based on a successful execution of a Data Job."
                },
                "name": "FreshnessAssertionType",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "DATASET_CHANGE",
                  "DATA_JOB_RUN"
                ]
              },
              "name": "type",
              "doc": "The type of the freshness assertion being monitored."
            },
            {
              "Relationship": {
                "entityTypes": [
                  "dataset",
                  "dataJob"
                ],
                "name": "Asserts"
              },
              "Searchable": {
                "fieldType": "URN"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "entity",
              "doc": "The entity targeted by this Freshness check."
            },
            {
              "Searchable": {
                "/type": {
                  "fieldName": "scheduleType"
                }
              },
              "type": {
                "type": "record",
                "name": "FreshnessAssertionSchedule",
                "namespace": "com.linkedin.assertion",
                "fields": [
                  {
                    "type": {
                      "type": "enum",
                      "symbolDocs": {
                        "CRON": "A highly configurable recurring schedule which describes the times of events described\nby a CRON schedule, with the evaluation schedule assuming to be matching the cron schedule.\n\nIn a CRON schedule type, we compute the look-back window to be the time between the last scheduled event\nand the current event (evaluation time). This means that the evaluation schedule must match exactly\nthe schedule defined inside the cron schedule.\n\nFor example, a CRON schedule defined as \"0 8 * * *\" would represent a schedule of \"every day by 8am\". Assuming\nthat the assertion evaluation schedule is defined to match this, the freshness assertion would be evaluated in the following way:\n\n    1. Compute the \"last scheduled occurrence\" of the event using the CRON schedule. For example, yesterday at 8am.\n    2. Compute the bounds of a time window between the \"last scheduled occurrence\" (yesterday at 8am) until the \"current occurrence\" (today at 8am)\n    3. Verify that the target event has occurred within the CRON-interval window.\n    4. If the target event has occurred within the time window, then assertion passes.\n    5. If the target event has not occurred within the time window, then the assertion fails.",
                        "FIXED_INTERVAL": "A fixed interval which is used to compute a look-back window for use when evaluating the assertion relative\nto the Evaluation Time of the Assertion.\n\nTo compute the valid look-back window, we subtract the fixed interval from the evaluation time. Then, we verify\nthat the target event has occurred within that window.\n\nFor example, a fixed interval of \"24h\" would represent a schedule of \"in the last 24 hours\".\nThe 24 hour interval is relative to the evaluation time of the assertion. For example if we schedule the assertion\nto be evaluated each hour, we'd compute the result as follows:\n\n    1. Subtract the fixed interval from the current time (Evaluation time) to compute the bounds of a fixed look-back window.\n    2. Verify that the target event has occurred within the look-back window.\n    3. If the target event has occurred within the time window, then assertion passes.\n    4. If the target event has not occurred within the time window, then the assertion fails.",
                        "SINCE_THE_LAST_CHECK": "A stateful check that takes the last time this check ran to determine the look-back window.\n\nTo compute the valid look-back- window, we start at the time the monitor last evaluated this assertion,\nand we end at the point in time the check is currently running.\n\nFor example, let's say a Freshness assertion is of type SINCE_THE_LAST_CHECK, and the monitor is configured to\nrun every day at 12:00am. Let's assume this assertion was last evaluated yesterday at 12:04am. We'd compute\nthe result as follows:\n\n    1. Get the timestamp for the last run of the monitor on this assertion.\n    2. look_back_window_start_time = latest_monitor_run.timestampMillis [ie. 12:04a yesterday]\n    3. look_back_window_end_time = nowMillis [ie. 12:02a today]\n    4. If the target event has occurred within the window [ie. 12:04a yday to 12:02a today],\n       then the assertion passes.\n    5. If the target event has not occurred within the window, then the assertion fails."
                      },
                      "name": "FreshnessAssertionScheduleType",
                      "namespace": "com.linkedin.assertion",
                      "symbols": [
                        "CRON",
                        "FIXED_INTERVAL",
                        "SINCE_THE_LAST_CHECK"
                      ]
                    },
                    "name": "type",
                    "doc": "The type of a Freshness Assertion Schedule.\n\nOnce we support data-time-relative schedules (e.g. schedules relative to time partitions),\nwe will add those schedule types here."
                  },
                  {
                    "type": [
                      "null",
                      {
                        "type": "record",
                        "name": "FreshnessCronSchedule",
                        "namespace": "com.linkedin.assertion",
                        "fields": [
                          {
                            "type": "string",
                            "name": "cron",
                            "doc": "A cron-formatted execution interval, as a cron string, e.g. 1 * * * *"
                          },
                          {
                            "type": "string",
                            "name": "timezone",
                            "doc": "Timezone in which the cron interval applies, e.g. America/Los Angeles"
                          },
                          {
                            "type": [
                              "null",
                              "long"
                            ],
                            "name": "windowStartOffsetMs",
                            "default": null,
                            "doc": "An optional offset in milliseconds to SUBTRACT from the timestamp generated by the cron schedule\nto generate the lower bounds of the \"freshness window\", or the window of time in which an event must have occurred in order for the Freshness check\nto be considering passing.\n\nIf left empty, the start of the SLA window will be the _end_ of the previously evaluated Freshness window."
                          }
                        ],
                        "doc": "Attributes defining a CRON-formatted schedule used for defining a freshness assertion."
                      }
                    ],
                    "name": "cron",
                    "default": null,
                    "doc": "A cron schedule. This field is required when type is CRON."
                  },
                  {
                    "type": [
                      "null",
                      {
                        "type": "record",
                        "name": "FixedIntervalSchedule",
                        "namespace": "com.linkedin.assertion",
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
                        "doc": "Attributes defining a relative fixed interval SLA schedule."
                      }
                    ],
                    "name": "fixedInterval",
                    "default": null,
                    "doc": "A fixed interval schedule. This field is required when type is FIXED_INTERVAL."
                  }
                ],
                "doc": "Attributes defining a single Freshness schedule."
              },
              "name": "schedule",
              "doc": "Produce FAILURE Assertion Result if the asset is not updated on the cadence and within the time range described by the schedule."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "DatasetFilter",
                  "namespace": "com.linkedin.dataset",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "SQL": "The partition is represented as a an opaque, raw SQL\nclause."
                        },
                        "name": "DatasetFilterType",
                        "namespace": "com.linkedin.dataset",
                        "symbols": [
                          "SQL"
                        ]
                      },
                      "name": "type",
                      "doc": "How the partition will be represented in this model.\n\nIn the future, we'll likely add support for more structured\npredicates."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "sql",
                      "default": null,
                      "doc": "The raw where clause string which will be used for monitoring.\nRequired if the type is SQL."
                    }
                  ],
                  "doc": "A definition of filters that should be used when\nquerying an external Dataset or Table.\n\nNote that this models should NOT be used for working with\nsearch / filter on DataHub Platform itself."
                }
              ],
              "name": "filter",
              "default": null,
              "doc": "A definition of the specific filters that should be applied, when performing monitoring.\nIf not provided, there is no filter, and the full table is under consideration."
            }
          ],
          "doc": "Attributes defining a Freshness Assertion."
        }
      ],
      "name": "freshnessAssertion",
      "default": null,
      "doc": "An Freshness Assertion definition. This field is populated when the type is FRESHNESS."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "VolumeAssertionInfo",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "Searchable": {},
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "INCREMENTING_SEGMENT_ROW_COUNT_CHANGE": "A volume assertion that compares the row counts in neighboring \"segments\" or \"partitions\"\nof an incrementing column.\nThis can be used to track changes between subsequent date partition\nin a table, for example.",
                  "INCREMENTING_SEGMENT_ROW_COUNT_TOTAL": "A volume assertion that checks the latest \"segment\" in a table based on an incrementing\ncolumn to check whether it's row count falls into a particular range.\n\nThis can be used to monitor the row count of an incrementing date-partition column segment.",
                  "ROW_COUNT_CHANGE": "A volume assertion that is evaluated against an incremental row count of a dataset,\nor a row count change.",
                  "ROW_COUNT_TOTAL": "A volume assertion that is evaluated against the total row count of a dataset."
                },
                "name": "VolumeAssertionType",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "ROW_COUNT_TOTAL",
                  "ROW_COUNT_CHANGE",
                  "INCREMENTING_SEGMENT_ROW_COUNT_TOTAL",
                  "INCREMENTING_SEGMENT_ROW_COUNT_CHANGE"
                ]
              },
              "name": "type",
              "doc": "The type of the volume assertion being monitored."
            },
            {
              "Relationship": {
                "entityTypes": [
                  "dataset"
                ],
                "name": "Asserts"
              },
              "Searchable": {
                "fieldType": "URN"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "entity",
              "doc": "The entity targeted by this Volume check."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "RowCountTotal",
                  "namespace": "com.linkedin.assertion",
                  "fields": [
                    {
                      "type": "com.linkedin.assertion.AssertionStdOperator",
                      "name": "operator",
                      "doc": "The operator you'd like to apply.\n\nNote that only numeric operators are valid inputs:\nGREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,\nBETWEEN."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdParameters",
                      "name": "parameters",
                      "doc": "The parameters you'd like to provide as input to the operator.\n\nNote that only numeric parameter types are valid inputs: NUMBER."
                    }
                  ],
                  "doc": "Attributes defining a ROW_COUNT_TOTAL volume assertion."
                }
              ],
              "name": "rowCountTotal",
              "default": null,
              "doc": "Produce FAILURE Assertion Result if the row count of the asset does not meet specific requirements.\nRequired if type is 'ROW_COUNT_TOTAL'"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "RowCountChange",
                  "namespace": "com.linkedin.assertion",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "ABSOLUTE": "A change that is defined in absolute terms.",
                          "PERCENTAGE": "A change that is defined in relative terms using percentage change\nfrom the original value."
                        },
                        "name": "AssertionValueChangeType",
                        "namespace": "com.linkedin.assertion",
                        "symbols": [
                          "ABSOLUTE",
                          "PERCENTAGE"
                        ],
                        "doc": "An enum to represent a type of change in an assertion value, metric, or measurement."
                      },
                      "name": "type",
                      "doc": "The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdOperator",
                      "name": "operator",
                      "doc": "The operator you'd like to apply.\n\nNote that only numeric operators are valid inputs:\nGREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,\nBETWEEN."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdParameters",
                      "name": "parameters",
                      "doc": "The parameters you'd like to provide as input to the operator.\n\nNote that only numeric parameter types are valid inputs: NUMBER."
                    }
                  ],
                  "doc": "Attributes defining a ROW_COUNT_CHANGE volume assertion."
                }
              ],
              "name": "rowCountChange",
              "default": null,
              "doc": "Produce FAILURE Assertion Result if the delta row count of the asset does not meet specific requirements\nwithin a given period of time.\nRequired if type is 'ROW_COUNT_CHANGE'"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "IncrementingSegmentRowCountTotal",
                  "namespace": "com.linkedin.assertion",
                  "fields": [
                    {
                      "type": {
                        "type": "record",
                        "name": "IncrementingSegmentSpec",
                        "namespace": "com.linkedin.assertion",
                        "fields": [
                          {
                            "type": {
                              "type": "record",
                              "name": "SchemaFieldSpec",
                              "namespace": "com.linkedin.schema",
                              "fields": [
                                {
                                  "type": "string",
                                  "name": "path",
                                  "doc": "The field path"
                                },
                                {
                                  "type": "string",
                                  "name": "type",
                                  "doc": "The DataHub standard schema field type."
                                },
                                {
                                  "type": "string",
                                  "name": "nativeType",
                                  "doc": "The native field type"
                                }
                              ],
                              "doc": "Lightweight spec used for referencing a particular schema field."
                            },
                            "name": "field",
                            "doc": "The field to use to generate segments. It must be constantly incrementing as new rows are inserted."
                          },
                          {
                            "type": [
                              "null",
                              {
                                "type": "record",
                                "name": "IncrementingSegmentFieldTransformer",
                                "namespace": "com.linkedin.assertion",
                                "fields": [
                                  {
                                    "type": {
                                      "type": "enum",
                                      "symbolDocs": {
                                        "CEILING": "Rounds a numeric value up to the nearest integer.",
                                        "FLOOR": "Rounds a numeric value down to the nearest integer.",
                                        "NATIVE": "A backdoor to provide a native operator type specific to a given source system like\nSnowflake, Redshift, BQ, etc.",
                                        "TIMESTAMP_MS_TO_DATE": "Rounds a timestamp (in milliseconds) down to the start of the day.",
                                        "TIMESTAMP_MS_TO_HOUR": "Rounds a timestamp (in milliseconds) down to the nearest hour.",
                                        "TIMESTAMP_MS_TO_MINUTE": "Rounds a timestamp (in seconds) down to the start of the month.",
                                        "TIMESTAMP_MS_TO_MONTH": "Rounds a timestamp (in milliseconds) down to the start of the month",
                                        "TIMESTAMP_MS_TO_YEAR": "Rounds a timestamp (in milliseconds) down to the start of the year"
                                      },
                                      "name": "IncrementingSegmentFieldTransformerType",
                                      "namespace": "com.linkedin.assertion",
                                      "symbols": [
                                        "TIMESTAMP_MS_TO_MINUTE",
                                        "TIMESTAMP_MS_TO_HOUR",
                                        "TIMESTAMP_MS_TO_DATE",
                                        "TIMESTAMP_MS_TO_MONTH",
                                        "TIMESTAMP_MS_TO_YEAR",
                                        "FLOOR",
                                        "CEILING",
                                        "NATIVE"
                                      ]
                                    },
                                    "name": "type",
                                    "doc": "A 'standard' transformer type. Note that not all source systems will support all operators."
                                  },
                                  {
                                    "type": [
                                      "null",
                                      "string"
                                    ],
                                    "name": "nativeType",
                                    "default": null,
                                    "doc": "The 'native' transformer type, useful as a back door if a custom operator is required.\nThis field is required if the type is NATIVE."
                                  }
                                ],
                                "doc": "The definition of the transformer function  that should be applied to a given field / column value in a dataset\nin order to determine the segment or bucket that it belongs to, which in turn is used to evaluate\nvolume assertions."
                              }
                            ],
                            "name": "transformer",
                            "default": null,
                            "doc": "Optional transformer function to apply to the field in order to obtain the final segment or bucket identifier.\nIf not provided, then no operator will be applied to the field. (identity function)"
                          }
                        ],
                        "doc": "Core attributes required to identify an incrementing segment in a table. This type is mainly useful\nfor tables that constantly increase with new rows being added on a particular cadence (e.g. fact or event tables)\n\nAn incrementing segment represents a logical chunk of data which is INSERTED\ninto a dataset on a regular interval, along with the presence of a constantly-incrementing column\nvalue such as an event time, date partition, or last modified column.\n\nAn incrementing segment is principally identified by 2 key attributes combined:\n\n 1. A field or column that represents the incrementing value. New rows that are inserted will be identified using this column.\n    Note that the value of this column may not by itself represent the \"bucket\" or the \"segment\" in which the row falls.\n\n 2. [Optional] An transformer function that may be applied to the selected column value in order\n    to obtain the final \"segment identifier\" or \"bucket identifier\". Rows that have the same value after applying the transformation\n    will be grouped into the same segment, using which the final value (e.g. row count) will be determined."
                      },
                      "name": "segment",
                      "doc": "A specification of how the 'segment' can be derived using a column and an optional transformer function."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdOperator",
                      "name": "operator",
                      "doc": "The operator you'd like to apply.\n\nNote that only numeric operators are valid inputs:\nGREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,\nBETWEEN."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdParameters",
                      "name": "parameters",
                      "doc": "The parameters you'd like to provide as input to the operator.\n\nNote that only numeric parameter types are valid inputs: NUMBER."
                    }
                  ],
                  "doc": "Attributes defining an INCREMENTING_SEGMENT_ROW_COUNT_TOTAL volume assertion."
                }
              ],
              "name": "incrementingSegmentRowCountTotal",
              "default": null,
              "doc": "Produce FAILURE Assertion Result if the asset's latest incrementing segment row count total\ndoes not meet specific requirements. Required if type is 'INCREMENTING_SEGMENT_ROW_COUNT_TOTAL'"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "IncrementingSegmentRowCountChange",
                  "namespace": "com.linkedin.assertion",
                  "fields": [
                    {
                      "type": "com.linkedin.assertion.IncrementingSegmentSpec",
                      "name": "segment",
                      "doc": "A specification of how the 'segment' can be derived using a column and an optional transformer function."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionValueChangeType",
                      "name": "type",
                      "doc": "The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdOperator",
                      "name": "operator",
                      "doc": "The operator you'd like to apply to the row count value\n\nNote that only numeric operators are valid inputs:\nGREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,\nBETWEEN."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdParameters",
                      "name": "parameters",
                      "doc": "The parameters you'd like to provide as input to the operator.\n\nNote that only numeric parameter types are valid inputs: NUMBER."
                    }
                  ],
                  "doc": "Attributes defining an INCREMENTING_SEGMENT_ROW_COUNT_CHANGE volume assertion."
                }
              ],
              "name": "incrementingSegmentRowCountChange",
              "default": null,
              "doc": "Produce FAILURE Assertion Result if the asset's incrementing segment row count delta\ndoes not meet specific requirements. Required if type is 'INCREMENTING_SEGMENT_ROW_COUNT_CHANGE'"
            },
            {
              "type": [
                "null",
                "com.linkedin.dataset.DatasetFilter"
              ],
              "name": "filter",
              "default": null,
              "doc": "A definition of the specific filters that should be applied, when performing monitoring.\nIf not provided, there is no filter, and the full table is under consideration."
            }
          ],
          "doc": "Attributes defining a dataset Volume Assertion"
        }
      ],
      "name": "volumeAssertion",
      "default": null,
      "doc": "An Volume Assertion definition. This field is populated when the type is VOLUME."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SqlAssertionInfo",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "Searchable": {},
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "METRIC": "A SQL Metric Assertion, e.g. one based on a numeric value returned by an arbitrary SQL query.",
                  "METRIC_CHANGE": "A SQL assertion that is evaluated against the CHANGE in a metric assertion\nover time."
                },
                "name": "SqlAssertionType",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "METRIC",
                  "METRIC_CHANGE"
                ]
              },
              "name": "type",
              "doc": "The type of the SQL assertion being monitored."
            },
            {
              "Relationship": {
                "entityTypes": [
                  "dataset"
                ],
                "name": "Asserts"
              },
              "Searchable": {
                "fieldType": "URN"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "entity",
              "doc": "The entity targeted by this SQL check."
            },
            {
              "type": "string",
              "name": "statement",
              "doc": "The SQL statement to be executed when evaluating the assertion (or computing the metric).\nThis should be a valid and complete statement, executable by itself.\n\nUsually this should be a SELECT query statement."
            },
            {
              "type": [
                "null",
                "com.linkedin.assertion.AssertionValueChangeType"
              ],
              "name": "changeType",
              "default": null,
              "doc": "The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.\nThis value is required if the type is METRIC_CHANGE."
            },
            {
              "type": "com.linkedin.assertion.AssertionStdOperator",
              "name": "operator",
              "doc": "The operator you'd like to apply to the result of the SQL query.\n\nNote that at this time, only numeric operators are valid inputs:\nGREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,\nBETWEEN."
            },
            {
              "type": "com.linkedin.assertion.AssertionStdParameters",
              "name": "parameters",
              "doc": "The parameters you'd like to provide as input to the operator.\n\nNote that only numeric parameter types are valid inputs: NUMBER."
            }
          ],
          "doc": "Attributes defining a SQL Assertion"
        }
      ],
      "name": "sqlAssertion",
      "default": null,
      "doc": "A SQL Assertion definition. This field is populated when the type is SQL."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "FieldAssertionInfo",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "Searchable": {},
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FIELD_METRIC": "An assertion used to validate the value of a common field / column metric (e.g. aggregation) such as null count + percentage,\nmin, max, median, and more.",
                  "FIELD_VALUES": "An assertion used to validate the values contained with a field / column given a set of rows."
                },
                "name": "FieldAssertionType",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "FIELD_VALUES",
                  "FIELD_METRIC"
                ]
              },
              "name": "type",
              "doc": "The type of the field assertion being monitored."
            },
            {
              "Relationship": {
                "entityTypes": [
                  "dataset"
                ],
                "name": "Asserts"
              },
              "Searchable": {
                "fieldType": "URN"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "entity",
              "doc": "The entity targeted by this Field check."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "FieldValuesAssertion",
                  "namespace": "com.linkedin.assertion",
                  "fields": [
                    {
                      "Searchable": {
                        "/path": {
                          "fieldName": "fieldPath"
                        }
                      },
                      "type": "com.linkedin.schema.SchemaFieldSpec",
                      "name": "field",
                      "doc": "The field under evaluation"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FieldTransform",
                          "namespace": "com.linkedin.assertion",
                          "fields": [
                            {
                              "type": {
                                "type": "enum",
                                "symbolDocs": {
                                  "LENGTH": "Obtain the length of a string field / column (applicable to string types)"
                                },
                                "name": "FieldTransformType",
                                "namespace": "com.linkedin.assertion",
                                "symbols": [
                                  "LENGTH"
                                ]
                              },
                              "name": "type",
                              "doc": "The type of the field transform, e.g. the transformation\nfunction / operator to apply."
                            }
                          ],
                          "doc": "Definition of a transform applied to the values of a column / field.\nNote that the applicability of a field transform ultimately depends on the native type\nof the field / column.\n\nModel has single field to permit extension."
                        }
                      ],
                      "name": "transform",
                      "default": null,
                      "doc": "An optional transform to apply to field values\nbefore evaluating the operator.\n\nIf none is applied, the field value will be compared as is."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdOperator",
                      "name": "operator",
                      "doc": "The predicate to evaluate against a single value of the field.\nDepending on the operator, parameters may be required in order to successfully\nevaluate the assertion against the field value."
                    },
                    {
                      "type": [
                        "null",
                        "com.linkedin.assertion.AssertionStdParameters"
                      ],
                      "name": "parameters",
                      "default": null,
                      "doc": "Standard parameters required for the assertion. e.g. min_value, max_value, value, columns"
                    },
                    {
                      "type": {
                        "type": "record",
                        "name": "FieldValuesFailThreshold",
                        "namespace": "com.linkedin.assertion",
                        "fields": [
                          {
                            "type": {
                              "type": "enum",
                              "name": "FieldValuesFailThresholdType",
                              "namespace": "com.linkedin.assertion",
                              "symbols": [
                                "COUNT",
                                "PERCENTAGE"
                              ]
                            },
                            "name": "type",
                            "default": "COUNT",
                            "doc": "The type of failure threshold. Either based on the number\nof column values (rows) that fail the expectations, or the percentage\nof the total rows under consideration."
                          },
                          {
                            "type": "long",
                            "name": "value",
                            "default": 0,
                            "doc": "By default this is 0, meaning that ALL column values (i.e. rows) must\nmeet the defined expectations."
                          }
                        ]
                      },
                      "name": "failThreshold",
                      "doc": "Additional customization about when the assertion\nshould be officially considered failing."
                    },
                    {
                      "type": "boolean",
                      "name": "excludeNulls",
                      "default": true,
                      "doc": "Whether to ignore or allow nulls when running the values assertion. (i.e.\nconsider only non-null values) using operators OTHER than the IS_NULL operator.\n\nDefaults to true, allowing null values."
                    }
                  ],
                  "doc": "Attributes defining a field values assertion, which asserts that the values for a field / column\nof a dataset / table matches a set of expectations.\n\nIn other words, this type of assertion acts as a semantic constraint applied to fields for a specific column.\n\nTODO: We should display the \"failed row count\" to the user if the column fails the verification rules.\nTODO: Determine whether we need an \"operator\" that can be applied to the field."
                }
              ],
              "name": "fieldValuesAssertion",
              "default": null,
              "doc": "The definition of an assertion that validates individual values of a field / column for a set of rows.\nThis type of assertion verifies that each column value meets a particular requirement."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "FieldMetricAssertion",
                  "namespace": "com.linkedin.assertion",
                  "fields": [
                    {
                      "Searchable": {
                        "/path": {
                          "fieldName": "fieldPath"
                        }
                      },
                      "type": "com.linkedin.schema.SchemaFieldSpec",
                      "name": "field",
                      "doc": "The field under evaluation"
                    },
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "EMPTY_COUNT": "The number of empty string values found in the value set (applies to string columns).\nNote: This is a completely different metric different from NULL_COUNT!",
                          "EMPTY_PERCENTAGE": "The percentage of empty string values to total rows for the dataset (applies to string columns)\nNote: This is a completely different metric different from NULL_PERCENTAGE!",
                          "MAX": "The maximum value in the column set (applies to numeric columns)",
                          "MAX_LENGTH": "The maximum length found in the column set (applies to string columns)",
                          "MEAN": "The mean length found in the column set (applies to numeric columns)",
                          "MEDIAN": "The median length found in the column set (applies to numeric columns)",
                          "MIN": "The minimum value in the column set (applies to numeric columns)",
                          "MIN_LENGTH": "The minimum length found in the column set (applies to string columns)",
                          "NEGATIVE_COUNT": "The number of negative values found in the value set (applies to numeric columns)",
                          "NEGATIVE_PERCENTAGE": "The percentage of negative values to total rows for the dataset (applies to numeric columns)",
                          "NULL_COUNT": "The number of null values found in the column value set",
                          "NULL_PERCENTAGE": "The percentage of null values to total rows for the dataset",
                          "STDDEV": "The stddev length found in the column set (applies to numeric columns)",
                          "UNIQUE_COUNT": "The number of unique values found in the column value set",
                          "UNIQUE_PERCENTAGE": "The percentage of unique values to total rows for the dataset",
                          "ZERO_COUNT": "The number of zero values found in the value set (applies to numeric columns)",
                          "ZERO_PERCENTAGE": "The percentage of zero values to total rows for the dataset (applies to numeric columns)"
                        },
                        "name": "FieldMetricType",
                        "namespace": "com.linkedin.assertion",
                        "symbols": [
                          "UNIQUE_COUNT",
                          "UNIQUE_PERCENTAGE",
                          "NULL_COUNT",
                          "NULL_PERCENTAGE",
                          "MIN",
                          "MAX",
                          "MEAN",
                          "MEDIAN",
                          "STDDEV",
                          "NEGATIVE_COUNT",
                          "NEGATIVE_PERCENTAGE",
                          "ZERO_COUNT",
                          "ZERO_PERCENTAGE",
                          "MIN_LENGTH",
                          "MAX_LENGTH",
                          "EMPTY_COUNT",
                          "EMPTY_PERCENTAGE"
                        ],
                        "doc": "A standard metric that can be derived from the set of values\nfor a specific field / column of a dataset / table."
                      },
                      "name": "metric",
                      "doc": "The specific metric to assert against. This is the value that\nwill be obtained by applying a standard operation, such as an aggregation,\nto the selected field."
                    },
                    {
                      "type": "com.linkedin.assertion.AssertionStdOperator",
                      "name": "operator",
                      "doc": "The predicate to evaluate against the metric for the field / column.\nDepending on the operator, parameters may be required in order to successfully\nevaluate the assertion against the metric value."
                    },
                    {
                      "type": [
                        "null",
                        "com.linkedin.assertion.AssertionStdParameters"
                      ],
                      "name": "parameters",
                      "default": null,
                      "doc": "Standard parameters required for the assertion. e.g. min_value, max_value, value, columns"
                    }
                  ],
                  "doc": "Attributes defining a field metric assertion, which asserts an expectation against\na common metric derived from the set of field / column values, for example:\nmax, min, median, null count, null percentage, unique count, unique percentage, and more."
                }
              ],
              "name": "fieldMetricAssertion",
              "default": null,
              "doc": "The definition of an assertion that validates a common metric obtained about a field / column for a set of rows.\nThis type of assertion verifies that the value of a high-level metric obtained by aggregating over a column meets\nexpectations"
            },
            {
              "type": [
                "null",
                "com.linkedin.dataset.DatasetFilter"
              ],
              "name": "filter",
              "default": null,
              "doc": "A definition of the specific filters that should be applied, when performing monitoring.\nIf not provided, there is no filter, and the full table is under consideration.\nIf using DataHub Dataset Profiles as the assertion source type, the value of this field will be ignored."
            }
          ],
          "doc": "Attributes defining a Field Assertion."
        }
      ],
      "name": "fieldAssertion",
      "default": null,
      "doc": "A Field Assertion definition. This field is populated when the type is FIELD."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SchemaAssertionInfo",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "dataset",
                  "dataJob"
                ],
                "name": "Asserts"
              },
              "Searchable": {
                "fieldType": "URN"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "entity",
              "doc": "The entity targeted by the assertion"
            },
            {
              "type": {
                "type": "record",
                "Aspect": {
                  "name": "schemaMetadata"
                },
                "name": "SchemaMetadata",
                "namespace": "com.linkedin.schema",
                "fields": [
                  {
                    "validate": {
                      "strlen": {
                        "max": 500,
                        "min": 1
                      }
                    },
                    "type": "string",
                    "name": "schemaName",
                    "doc": "Schema name e.g. PageViewEvent, identity.Profile, ams.account_management_tracking"
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.DataPlatformUrn"
                    },
                    "type": "string",
                    "name": "platform",
                    "doc": "Standardized platform urn where schema is defined. The data platform Urn (urn:li:platform:{platform_name})"
                  },
                  {
                    "type": "long",
                    "name": "version",
                    "doc": "Every change to SchemaMetadata in the resource results in a new version. Version is server assigned. This version is differ from platform native schema version."
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
                    "java": {
                      "class": "com.linkedin.common.urn.DatasetUrn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "dataset",
                    "default": null,
                    "doc": "Dataset this schema metadata is associated with."
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "cluster",
                    "default": null,
                    "doc": "The cluster this schema metadata resides from"
                  },
                  {
                    "type": "string",
                    "name": "hash",
                    "doc": "the SHA1 hash of the schema content"
                  },
                  {
                    "type": [
                      {
                        "type": "record",
                        "name": "EspressoSchema",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "documentSchema",
                            "doc": "The native espresso document schema."
                          },
                          {
                            "type": "string",
                            "name": "tableSchema",
                            "doc": "The espresso table schema definition."
                          }
                        ],
                        "doc": "Schema text of an espresso table schema."
                      },
                      {
                        "type": "record",
                        "name": "OracleDDL",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "tableSchema",
                            "doc": "The native schema in the dataset's platform. This is a human readable (json blob) table schema."
                          }
                        ],
                        "doc": "Schema holder for oracle data definition language that describes an oracle table."
                      },
                      {
                        "type": "record",
                        "name": "MySqlDDL",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "tableSchema",
                            "doc": "The native schema in the dataset's platform. This is a human readable (json blob) table schema."
                          }
                        ],
                        "doc": "Schema holder for MySql data definition language that describes an MySql table."
                      },
                      {
                        "type": "record",
                        "name": "PrestoDDL",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "rawSchema",
                            "doc": "The raw schema in the dataset's platform. This includes the DDL and the columns extracted from DDL."
                          }
                        ],
                        "doc": "Schema holder for presto data definition language that describes a presto view."
                      },
                      {
                        "type": "record",
                        "name": "KafkaSchema",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "documentSchema",
                            "doc": "The native kafka document schema. This is a human readable avro document schema."
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "documentSchemaType",
                            "default": null,
                            "doc": "The native kafka document schema type. This can be AVRO/PROTOBUF/JSON."
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "keySchema",
                            "default": null,
                            "doc": "The native kafka key schema as retrieved from Schema Registry"
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "keySchemaType",
                            "default": null,
                            "doc": "The native kafka key schema type. This can be AVRO/PROTOBUF/JSON."
                          }
                        ],
                        "doc": "Schema holder for kafka schema."
                      },
                      {
                        "type": "record",
                        "name": "BinaryJsonSchema",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "schema",
                            "doc": "The native schema text for binary JSON file format."
                          }
                        ],
                        "doc": "Schema text of binary JSON schema."
                      },
                      {
                        "type": "record",
                        "name": "OrcSchema",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "schema",
                            "doc": "The native schema for ORC file format."
                          }
                        ],
                        "doc": "Schema text of an ORC schema."
                      },
                      {
                        "type": "record",
                        "name": "Schemaless",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "The dataset has no specific schema associated with it"
                      },
                      {
                        "type": "record",
                        "name": "KeyValueSchema",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "keySchema",
                            "doc": "The raw schema for the key in the key-value store."
                          },
                          {
                            "type": "string",
                            "name": "valueSchema",
                            "doc": "The raw schema for the value in the key-value store."
                          }
                        ],
                        "doc": "Schema text of a key-value store schema."
                      },
                      {
                        "type": "record",
                        "name": "OtherSchema",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": "string",
                            "name": "rawSchema",
                            "doc": "The native schema in the dataset's platform."
                          }
                        ],
                        "doc": "Schema holder for undefined schema types."
                      }
                    ],
                    "name": "platformSchema",
                    "doc": "The native schema in the dataset's platform."
                  },
                  {
                    "type": {
                      "type": "array",
                      "items": {
                        "type": "record",
                        "name": "SchemaField",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "Searchable": {
                              "boostScore": 1.0,
                              "fieldName": "fieldPaths",
                              "fieldType": "TEXT",
                              "queryByDefault": "true"
                            },
                            "type": "string",
                            "name": "fieldPath",
                            "doc": "Flattened name of the field. Field is computed from jsonPath field."
                          },
                          {
                            "Deprecated": true,
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "jsonPath",
                            "default": null,
                            "doc": "Flattened name of a field in JSON Path notation."
                          },
                          {
                            "type": "boolean",
                            "name": "nullable",
                            "default": false,
                            "doc": "Indicates if this field is optional or nullable"
                          },
                          {
                            "Searchable": {
                              "boostScore": 0.1,
                              "fieldName": "fieldDescriptions",
                              "fieldType": "TEXT"
                            },
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "description",
                            "default": null,
                            "doc": "Description"
                          },
                          {
                            "Deprecated": true,
                            "Searchable": {
                              "boostScore": 0.2,
                              "fieldName": "fieldLabels",
                              "fieldType": "TEXT"
                            },
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "label",
                            "default": null,
                            "doc": "Label of the field. Provides a more human-readable name for the field than field path. Some sources will\nprovide this metadata but not all sources have the concept of a label. If just one string is associated with\na field in a source, that is most likely a description.\n\nNote that this field is deprecated and is not surfaced in the UI."
                          },
                          {
                            "type": [
                              "null",
                              "com.linkedin.common.AuditStamp"
                            ],
                            "name": "created",
                            "default": null,
                            "doc": "An AuditStamp corresponding to the creation of this schema field."
                          },
                          {
                            "type": [
                              "null",
                              "com.linkedin.common.AuditStamp"
                            ],
                            "name": "lastModified",
                            "default": null,
                            "doc": "An AuditStamp corresponding to the last modification of this schema field."
                          },
                          {
                            "type": {
                              "type": "record",
                              "name": "SchemaFieldDataType",
                              "namespace": "com.linkedin.schema",
                              "fields": [
                                {
                                  "type": [
                                    {
                                      "type": "record",
                                      "name": "BooleanType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Boolean field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "FixedType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Fixed field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "StringType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "String field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "BytesType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Bytes field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "NumberType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Number data type: long, integer, short, etc.."
                                    },
                                    {
                                      "type": "record",
                                      "name": "DateType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Date field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "TimeType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Time field type. This should also be used for datetimes."
                                    },
                                    {
                                      "type": "record",
                                      "name": "EnumType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Enum field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "NullType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Null field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "MapType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [
                                        {
                                          "type": [
                                            "null",
                                            "string"
                                          ],
                                          "name": "keyType",
                                          "default": null,
                                          "doc": "Key type in a map"
                                        },
                                        {
                                          "type": [
                                            "null",
                                            "string"
                                          ],
                                          "name": "valueType",
                                          "default": null,
                                          "doc": "Type of the value in a map"
                                        }
                                      ],
                                      "doc": "Map field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "ArrayType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [
                                        {
                                          "type": [
                                            "null",
                                            {
                                              "type": "array",
                                              "items": "string"
                                            }
                                          ],
                                          "name": "nestedType",
                                          "default": null,
                                          "doc": "List of types this array holds."
                                        }
                                      ],
                                      "doc": "Array field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "UnionType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [
                                        {
                                          "type": [
                                            "null",
                                            {
                                              "type": "array",
                                              "items": "string"
                                            }
                                          ],
                                          "name": "nestedTypes",
                                          "default": null,
                                          "doc": "List of types in union type."
                                        }
                                      ],
                                      "doc": "Union field type."
                                    },
                                    {
                                      "type": "record",
                                      "name": "RecordType",
                                      "namespace": "com.linkedin.schema",
                                      "fields": [],
                                      "doc": "Record field type."
                                    }
                                  ],
                                  "name": "type",
                                  "doc": "Data platform specific types"
                                }
                              ],
                              "doc": "Schema field data types"
                            },
                            "name": "type",
                            "doc": "Platform independent field type of the field."
                          },
                          {
                            "type": "string",
                            "name": "nativeDataType",
                            "doc": "The native type of the field in the dataset's platform as declared by platform schema."
                          },
                          {
                            "type": "boolean",
                            "name": "recursive",
                            "default": false,
                            "doc": "There are use cases when a field in type B references type A. A field in A references field of type B. In such cases, we will mark the first field as recursive."
                          },
                          {
                            "Relationship": {
                              "/tags/*/tag": {
                                "entityTypes": [
                                  "tag"
                                ],
                                "name": "SchemaFieldTaggedWith"
                              }
                            },
                            "Searchable": {
                              "/tags/*/attribution/actor": {
                                "fieldName": "fieldTagAttributionActors",
                                "fieldType": "URN",
                                "queryByDefault": false
                              },
                              "/tags/*/attribution/source": {
                                "fieldName": "fieldTagAttributionSources",
                                "fieldType": "URN",
                                "queryByDefault": false
                              },
                              "/tags/*/attribution/time": {
                                "fieldName": "fieldTagAttributionDates",
                                "fieldType": "DATETIME",
                                "queryByDefault": false
                              },
                              "/tags/*/tag": {
                                "boostScore": 0.5,
                                "fieldName": "fieldTags",
                                "fieldType": "URN"
                              }
                            },
                            "type": [
                              "null",
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
                                        "filterNameOverride": "Tag",
                                        "hasValuesFieldName": "hasTags",
                                        "queryByDefault": true
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
                            ],
                            "name": "globalTags",
                            "default": null,
                            "doc": "Tags associated with the field"
                          },
                          {
                            "Relationship": {
                              "/terms/*/urn": {
                                "entityTypes": [
                                  "glossaryTerm"
                                ],
                                "name": "SchemaFieldWithGlossaryTerm"
                              }
                            },
                            "Searchable": {
                              "/terms/*/attribution/actor": {
                                "fieldName": "fieldTermAttributionActors",
                                "fieldType": "URN",
                                "queryByDefault": false
                              },
                              "/terms/*/attribution/source": {
                                "fieldName": "fieldTermAttributionSources",
                                "fieldType": "URN",
                                "queryByDefault": false
                              },
                              "/terms/*/attribution/time": {
                                "fieldName": "fieldTermAttributionDates",
                                "fieldType": "DATETIME",
                                "queryByDefault": false
                              },
                              "/terms/*/urn": {
                                "boostScore": 0.5,
                                "fieldName": "fieldGlossaryTerms",
                                "fieldType": "URN"
                              }
                            },
                            "type": [
                              "null",
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
                                              "com.linkedin.common.MetadataAttribution"
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
                                    "type": "com.linkedin.common.AuditStamp",
                                    "name": "auditStamp",
                                    "doc": "Audit stamp containing who reported the related business term"
                                  }
                                ],
                                "doc": "Related business terms information"
                              }
                            ],
                            "name": "glossaryTerms",
                            "default": null,
                            "doc": "Glossary terms associated with the field"
                          },
                          {
                            "type": "boolean",
                            "name": "isPartOfKey",
                            "default": false,
                            "doc": "For schema fields that are part of complex keys, set this field to true\nWe do this to easily distinguish between value and key fields"
                          },
                          {
                            "type": [
                              "null",
                              "boolean"
                            ],
                            "name": "isPartitioningKey",
                            "default": null,
                            "doc": "For Datasets which are partitioned, this determines the partitioning key.\nNote that multiple columns can be part of a partitioning key, but currently we do not support\nrendering the ordered partitioning key."
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "jsonProps",
                            "default": null,
                            "doc": "For schema fields that have other properties that are not modeled explicitly,\nuse this field to serialize those properties into a JSON string"
                          }
                        ],
                        "doc": "SchemaField to describe metadata related to dataset schema."
                      }
                    },
                    "name": "fields",
                    "doc": "Client provided a list of fields from document schema."
                  },
                  {
                    "type": [
                      "null",
                      {
                        "type": "array",
                        "items": "string"
                      }
                    ],
                    "name": "primaryKeys",
                    "default": null,
                    "doc": "Client provided list of fields that define primary keys to access record. Field order defines hierarchical espresso keys. Empty lists indicates absence of primary key access patter. Value is a SchemaField@fieldPath."
                  },
                  {
                    "deprecated": "Use foreignKeys instead.",
                    "type": [
                      "null",
                      {
                        "type": "map",
                        "values": {
                          "type": "record",
                          "name": "ForeignKeySpec",
                          "namespace": "com.linkedin.schema",
                          "fields": [
                            {
                              "type": [
                                {
                                  "type": "record",
                                  "name": "DatasetFieldForeignKey",
                                  "namespace": "com.linkedin.schema",
                                  "fields": [
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.DatasetUrn"
                                      },
                                      "type": "string",
                                      "name": "parentDataset",
                                      "doc": "dataset that stores the resource."
                                    },
                                    {
                                      "type": {
                                        "type": "array",
                                        "items": "string"
                                      },
                                      "name": "currentFieldPaths",
                                      "doc": "List of fields in hosting(current) SchemaMetadata that conform a foreign key. List can contain a single entry or multiple entries if several entries in hosting schema conform a foreign key in a single parent dataset."
                                    },
                                    {
                                      "type": "string",
                                      "name": "parentField",
                                      "doc": "SchemaField@fieldPath that uniquely identify field in parent dataset that this field references."
                                    }
                                  ],
                                  "doc": "For non-urn based foregin keys."
                                },
                                {
                                  "type": "record",
                                  "name": "UrnForeignKey",
                                  "namespace": "com.linkedin.schema",
                                  "fields": [
                                    {
                                      "type": "string",
                                      "name": "currentFieldPath",
                                      "doc": "Field in hosting(current) SchemaMetadata."
                                    }
                                  ],
                                  "doc": "If SchemaMetadata fields make any external references and references are of type com.linkedin.common.Urn or any children, this models can be used to mark it."
                                }
                              ],
                              "name": "foreignKey",
                              "doc": "Foreign key definition in metadata schema."
                            }
                          ],
                          "doc": "Description of a foreign key in a schema."
                        }
                      }
                    ],
                    "name": "foreignKeysSpecs",
                    "default": null,
                    "doc": "Map captures all the references schema makes to external datasets. Map key is ForeignKeySpecName typeref."
                  },
                  {
                    "type": [
                      "null",
                      {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "ForeignKeyConstraint",
                          "namespace": "com.linkedin.schema",
                          "fields": [
                            {
                              "type": "string",
                              "name": "name",
                              "doc": "Name of the constraint, likely provided from the source"
                            },
                            {
                              "Relationship": {
                                "/*": {
                                  "entityTypes": [
                                    "schemaField"
                                  ],
                                  "name": "ForeignKeyTo"
                                }
                              },
                              "type": {
                                "type": "array",
                                "items": "string"
                              },
                              "name": "foreignFields",
                              "doc": "Fields the constraint maps to on the foreign dataset"
                            },
                            {
                              "type": {
                                "type": "array",
                                "items": "string"
                              },
                              "name": "sourceFields",
                              "doc": "Fields the constraint maps to on the source dataset"
                            },
                            {
                              "Relationship": {
                                "entityTypes": [
                                  "dataset"
                                ],
                                "name": "ForeignKeyToDataset"
                              },
                              "java": {
                                "class": "com.linkedin.common.urn.Urn"
                              },
                              "type": "string",
                              "name": "foreignDataset",
                              "doc": "Reference to the foreign dataset for ease of lookup"
                            }
                          ],
                          "doc": "Description of a foreign key constraint in a schema."
                        }
                      }
                    ],
                    "name": "foreignKeys",
                    "default": null,
                    "doc": "List of foreign key constraints for the schema"
                  }
                ],
                "doc": "SchemaMetadata to describe metadata related to store schema"
              },
              "name": "schema",
              "doc": "A definition of the expected structure for the asset\n\nNote that many of the fields of this model, especially those related to metadata (tags, terms)\nwill go unused in this context."
            },
            {
              "type": [
                {
                  "type": "enum",
                  "symbolDocs": {
                    "EXACT_MATCH": "The actual schema must be exactly the same as the expected schema",
                    "SUBSET": "The actual schema must be a subset of the expected schema",
                    "SUPERSET": "The actual schema must be a superset of the expected schema"
                  },
                  "name": "SchemaAssertionCompatibility",
                  "namespace": "com.linkedin.assertion",
                  "symbols": [
                    "EXACT_MATCH",
                    "SUPERSET",
                    "SUBSET"
                  ]
                },
                "null"
              ],
              "name": "compatibility",
              "default": "EXACT_MATCH",
              "doc": "The required compatibility level for the schema assertion to pass."
            }
          ],
          "doc": "Attributes that are applicable to schema assertions"
        }
      ],
      "name": "schemaAssertion",
      "default": null,
      "doc": "An schema Assertion definition. This field is populated when the type is DATA_SCHEMA"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "CustomAssertionInfo",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "Searchable": {
                "fieldName": "customType"
              },
              "type": "string",
              "name": "type",
              "doc": "The type of custom assertion.\nThis is how your assertion will appear categorized in DataHub UI. "
            },
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
              "name": "entity",
              "doc": "The entity targeted by this assertion.\nThis can have support more entityTypes (e.g. dataJob) in future"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "schemaField"
                ],
                "name": "Asserts"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "field",
              "default": null,
              "doc": "dataset schema field targeted by this assertion.\n\nThis field is expected to be provided if the assertion is on dataset field"
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
          "doc": "Attributes that are applicable to Custom Assertions"
        }
      ],
      "name": "customAssertion",
      "default": null,
      "doc": "A Custom Assertion definition. This field is populated when type is CUSTOM."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AssertionSource",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "Searchable": {
                "fieldName": "sourceType"
              },
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "EXTERNAL": "The assertion was defined and managed externally of DataHub.",
                  "INFERRED": "The assertion was inferred, e.g. from offline AI / ML models.\nDataHub Cloud only",
                  "NATIVE": "The assertion was defined natively on DataHub by a user.\nDataHub Cloud only"
                },
                "name": "AssertionSourceType",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "NATIVE",
                  "EXTERNAL",
                  "INFERRED"
                ]
              },
              "name": "type",
              "doc": "The type of the Assertion Source"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "created",
              "default": null,
              "doc": "The time at which the assertion was initially created and the author who created it.\nThis field is only present for Native assertions created after this field was introduced."
            }
          ],
          "doc": "The source of an assertion"
        }
      ],
      "name": "source",
      "default": null,
      "doc": "The source or origin of the Assertion definition.\n\nIf the source type of the Assertion is EXTERNAL, it is expected to have a corresponding dataPlatformInstance aspect detailing\nthe platform where it was ingested from."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "lastUpdated",
      "default": null,
      "doc": "The time at which the assertion was last updated and the actor who updated it.\nThis field is only present for Native assertions updated after this field was introduced."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "An optional human-readable description of the assertion"
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

### assertionActions
The Actions about an Assertion
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "assertionActions"
  },
  "name": "AssertionActions",
  "namespace": "com.linkedin.assertion",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "AssertionAction",
          "namespace": "com.linkedin.assertion",
          "fields": [
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "RAISE_INCIDENT": "Raise an incident.",
                  "RESOLVE_INCIDENT": "Resolve open incidents related to the assertion."
                },
                "name": "AssertionActionType",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "RAISE_INCIDENT",
                  "RESOLVE_INCIDENT"
                ]
              },
              "name": "type",
              "doc": "The type of the Action"
            }
          ],
          "doc": "The Actions about an Assertion.\nIn the future, we'll likely extend this model to support additional\nparameters or options related to the assertion actions."
        }
      },
      "name": "onSuccess",
      "default": [],
      "doc": "Actions to be executed on successful assertion run."
    },
    {
      "type": {
        "type": "array",
        "items": "com.linkedin.assertion.AssertionAction"
      },
      "name": "onFailure",
      "default": [],
      "doc": "Actions to be executed on failed assertion run."
    }
  ],
  "doc": "The Actions about an Assertion"
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

### globalTags
Tag aspect used for applying tags to an entity
<details>
<summary>Schema</summary>

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
          "filterNameOverride": "Tag",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true
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
      "Searchable": {
        "fieldName": "lastCompletedTime",
        "fieldType": "DATETIME"
      },
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
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
      "name": "asserteeUrn"
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
        ],
        "doc": "The lifecycle status of an assertion run."
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
              "Searchable": {},
              "TimeseriesField": {},
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "ERROR": " The Assertion encountered an Error",
                  "FAILURE": " The Assertion Failed",
                  "INIT": " The Assertion has not yet been fully evaluated",
                  "SUCCESS": " The Assertion Succeeded"
                },
                "name": "AssertionResultType",
                "namespace": "com.linkedin.assertion",
                "symbols": [
                  "INIT",
                  "SUCCESS",
                  "FAILURE",
                  "ERROR"
                ],
                "doc": " The final result of evaluating an assertion, e.g. SUCCESS, FAILURE, or ERROR."
              },
              "name": "type",
              "doc": " The final result, e.g. either SUCCESS, FAILURE, or ERROR."
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
              "doc": "External URL where full results are available. Only present when assertion source is not native."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AssertionResultError",
                  "namespace": "com.linkedin.assertion",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "CUSTOM_SQL_ERROR": " Error while executing a custom SQL assertion",
                          "FIELD_ASSERTION_ERROR": " Error while executing a field assertion",
                          "INSUFFICIENT_DATA": " Insufficient data to evaluate the assertion",
                          "INVALID_PARAMETERS": " Invalid parameters were detected",
                          "INVALID_SOURCE_TYPE": " Event type not supported by the specified source",
                          "SOURCE_CONNECTION_ERROR": " Source is unreachable",
                          "SOURCE_QUERY_FAILED": " Source query failed to execute",
                          "UNKNOWN_ERROR": " Unknown error",
                          "UNSUPPORTED_PLATFORM": " Unsupported platform"
                        },
                        "name": "AssertionResultErrorType",
                        "namespace": "com.linkedin.assertion",
                        "symbols": [
                          "SOURCE_CONNECTION_ERROR",
                          "SOURCE_QUERY_FAILED",
                          "INSUFFICIENT_DATA",
                          "INVALID_PARAMETERS",
                          "INVALID_SOURCE_TYPE",
                          "UNSUPPORTED_PLATFORM",
                          "CUSTOM_SQL_ERROR",
                          "FIELD_ASSERTION_ERROR",
                          "UNKNOWN_ERROR"
                        ]
                      },
                      "name": "type",
                      "doc": " The type of error encountered"
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
                      "doc": " Additional metadata depending on the type of error"
                    }
                  ],
                  "doc": " An error encountered when evaluating an AssertionResult"
                }
              ],
              "name": "error",
              "default": null,
              "doc": " The error object if AssertionResultType is an Error"
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
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "assertionUrn"
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
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
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
              "doc": "Time window of the partition, if we are able to extract it from the partition key."
            },
            {
              "deprecated": true,
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
              "default": "PARTITION",
              "doc": "Unused!"
            }
          ],
          "doc": "A reference to a specific partition in a dataset."
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
   - Dataset via `assertionInfo.freshnessAssertion.entity`
   - DataJob via `assertionInfo.freshnessAssertion.entity`
   - Dataset via `assertionInfo.volumeAssertion.entity`
   - Dataset via `assertionInfo.sqlAssertion.entity`
   - Dataset via `assertionInfo.fieldAssertion.entity`
   - Dataset via `assertionInfo.schemaAssertion.entity`
   - DataJob via `assertionInfo.schemaAssertion.entity`
   - Dataset via `assertionInfo.customAssertion.entity`
   - SchemaField via `assertionInfo.customAssertion.field`
- SchemaFieldTaggedWith

   - Tag via `assertionInfo.schemaAssertion.schema.fields.globalTags`
- TaggedWith

   - Tag via `assertionInfo.schemaAssertion.schema.fields.globalTags.tags`
   - Tag via `globalTags.tags`
- SchemaFieldWithGlossaryTerm

   - GlossaryTerm via `assertionInfo.schemaAssertion.schema.fields.glossaryTerms`
- TermedWith

   - GlossaryTerm via `assertionInfo.schemaAssertion.schema.fields.glossaryTerms.terms.urn`
- ForeignKeyTo

   - SchemaField via `assertionInfo.schemaAssertion.schema.foreignKeys.foreignFields`
- ForeignKeyToDataset

   - Dataset via `assertionInfo.schemaAssertion.schema.foreignKeys.foreignDataset`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
