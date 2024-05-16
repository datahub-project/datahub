import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Assertions

## Why Would You Use Assertions APIs?

The Assertions APIs allow you to create, schedule, run, and delete Assertions with Acryl Cloud.

Supported Assertion Types include: 

- [Freshness Assertions](/docs/managed-datahub/observe/freshness-assertions)
- [Volume Assertions](/docs/managed-datahub/observe/volume-assertions)
- [Column Assertions](/docs/managed-datahub/observe/column-assertions)
- [Schema Assertions](/docs/managed-datahub/observe/schema-assertions)
- [Custom SQL Assertions](/docs/managed-datahub/observe/custom-sql-assertions)


### Goal Of This Guide

This guide will show you how to create, schedule, run and delete Assertions for a Table.

## Prerequisites

The actor making API calls must have the `Edit Assertions` and `Edit Monitors` privileges for the Tables at hand.

## Create Assertions

You can create new dataset Assertions to DataHub using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

### Freshness Assertion

To create a new freshness assertion, use the `upsertDatasetFreshnessAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetFreshnessAssertionMonitor {
  upsertDatasetFreshnessAssertionMonitor(
      input: {
        entityUrn: "<urn of entity being monitored>",
        schedule: {
          type: FIXED_INTERVAL,
          fixedInterval: { unit: HOUR, multiple: 8 }
        }
        evaluationSchedule: {
          timezone: "America/Los_Angeles",
          cron: "0 */8 * * *"
        }
        evaluationParameters: {
          sourceType: INFORMATION_SCHEMA
        }
        mode: ACTIVE
      }
  ) {
      urn
    }
}
```

For more details, see the [Freshness Assertions](/docs/managed-datahub/observe/freshness-assertions) guide.

### Volume Assertions

To create a new volume assertion, use the `upsertDatasetVolumeAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetVolumeAssertionMonitor {
  upsertDatasetVolumeAssertionMonitor(
    input: {
      entityUrn: "<urn of entity being monitored>"
      type: ROW_COUNT_TOTAL
      rowCountTotal: {
        operator: BETWEEN
        parameters: {
          minValue: {
            value: "10"
            type: NUMBER
          }
          maxValue: {
            value: "20"
            type: NUMBER
          }
        }
      }
      evaluationSchedule: {
        timezone: "America/Los_Angeles"
        cron: "0 */8 * * *"
      }
      evaluationParameters: {
        sourceType: INFORMATION_SCHEMA
      }
      mode: ACTIVE
    }
  ) {
    urn
  }
}
```

For more details, see the [Volume Assertions](/docs/managed-datahub/observe/volume-assertions) guide.

### Column Assertions

To create a new column assertion, use the `upsertDatasetVolumeAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetFieldAssertionMonitor {
  upsertDatasetFieldAssertionMonitor(
    input: {
      entityUrn: "<urn of entity being monitored>"
      type: FIELD_VALUES,
      fieldValuesAssertion: {
        field: {
          path: "<name of the column to be monitored>",
          type: "NUMBER",
          nativeType: "NUMBER(38,0)"
        },
        operator: GREATER_THAN,
        parameters: {
          value: {
            type: NUMBER,
            value: "10"
          }
        },
        failThreshold: {
          type: COUNT,
          value: 0
        },
        excludeNulls: true
      }
      evaluationSchedule: {
        timezone: "America/Los_Angeles"
        cron: "0 */8 * * *"
      }
      evaluationParameters: {
        sourceType: ALL_ROWS_QUERY
      }
      mode: ACTIVE
    }
  ){
    urn
  }
}
```

For more details, see the [Column Assertions](/docs/managed-datahub/observe/column-assertions) guide.

### Custom SQL Assertions

To create a new column assertion, use the `upsertDatasetSqlAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetSqlAssertionMonitor {
  upsertDatasetSqlAssertionMonitor(
    assertionUrn: "<urn of assertion created in earlier query>"
    input: {
      entityUrn: "<urn of entity being monitored>"
      type: METRIC,
      description: "<description of the custom assertion>",
      statement: "<SQL query to be evaluated>",
      operator: GREATER_THAN_OR_EQUAL_TO,
      parameters: {
        value: {
          value: "100",
          type: NUMBER
        }
      }
      evaluationSchedule: {
        timezone: "America/Los_Angeles"
        cron: "0 */6 * * *"
      }
      mode: ACTIVE   
    }
  ) {
    urn
  }
}
```

For more details, see the [Custom SQL Assertions](/docs/managed-datahub/observe/custom-sql-assertions) guide.

### Schema Assertions

To create a new schema assertion, use the `upsertDatasetSchemaAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetSchemaAssertionMonitor {
    upsertDatasetSchemaAssertionMonitor(
        assertionUrn: "urn:li:assertion:existing-assertion-id",
        input: {
            entityUrn: "<urn of the table to be monitored>",
            assertion: {
                compatibility: EXACT_MATCH,
                fields: [
                    {
                        path: "id",
                        type: STRING
                    },
                    {
                        path: "count",
                        type: NUMBER
                    },
                    {
                        path: "struct",
                        type: STRUCT
                    },
                    {
                        path: "struct.nestedBooleanField",
                        type: BOOLEAN
                    }
                ]
            },
            description: "<description of the schema assertion>",
            mode: ACTIVE
        }
    )
}
```

For more details, see the [Schema Assertions](/docs/managed-datahub/observe/schema-assertions) guide.


</TabItem>
</Tabs>

## Run Assertions

You can use the following APIs to trigger the assertions you've created to run on-demand. This is
particularly useful for running assertions on a custom schedule, for example from your production
data pipelines. 

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

### Run an assertion

```graphql
mutation runAssertion {
    runAssertion(urn: "urn:li:assertion:your-assertion-id", saveResult: true) {
        type 
        nativeResults {
            key
            value
        }
    }
}
```

Where **type** will contain the Result of the assertion run, either `SUCCESS`, `FAILURE`, or `ERROR`. 

The `saveResult` argument determines whether the result of the assertion will be saved to DataHub's backend,
and available to view through the DataHub UI. If this is set to false, the result will NOT be stored in DataHub's
backend. The value defaults to `true`.

If the assertion is external (not natively executed by Acryl), this API will return an error.

If running the assertion is successful, the result will be returned as follows: 

```json
{
  "data": {
    "runAssertion": {
        "type": "SUCCESS",
        "nativeResults": [
          {
            "key": "Value",
            "value": "1382"
          }
        ]
    }
  },
  "extensions": {}
}
```

### Run multiple assertions

```graphql
mutation runAssertions {
    runAssertions(urns: ["urn:li:assertion:your-assertion-id-1", "urn:li:assertion:your-assertion-id-2"], saveResults: true) {
        passingCount
        failingCount
        errorCount
        results {
            urn
            type
            nativeResults {
                key
                value
            }
        }
    }
}
```

Where **type** will contain the Result of the assertion run, either `SUCCESS`, `FAILURE`, or `ERROR`.

The `saveResults` argument determines whether the result of the assertion will be saved to DataHub's backend,
and available to view through the DataHub UI. If this is set to false, the result will NOT be stored in DataHub's
backend. The value defaults to `true`.

If any of the assertion are external (not natively executed by Acryl), they will simply be omitted from the result set. 

If running the assertions is successful, the results will be returned as follows:

```json
{
  "data": {
    "runAssertions": {
      "passingCount": 2,
      "failingCount": 0,
      "errorCount": 0,
      "results": [
        {
          "urn": "urn:li:assertion:your-assertion-id-1",
          "type": "SUCCESS",
          "nativeResults": [
            {
              "key": "Value",
              "value": "1382"
            }
          ]
        },
        {
          "urn": "urn:li:assertion:your-assertion-id-2",
          "type": "FAILURE",
          "nativeResults": [
            {
              "key": "Value",
              "value": "12323"
            }
          ]
        }
      ]
    }
  },
  "extensions": {}
}
```

Where you should see one result object for each assertion.

### Run all assertions for table

You can also run all assertions for a specific data asset using the `runAssetAssertions` mutation.

```graphql
mutation runAssertionsForAsset {
    runAssertionsForAsset(urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchase_events,PROD)", saveResults: true) {
        passingCount
        failingCount
        errorCount
        results {
            urn
            type
            nativeResults {
                key
                value
            }
        }
    }
}
```

Where `type` will contain the Result of the assertion run, either `SUCCESS`, `FAILURE`, or `ERROR`.

The `saveResults` argument determines whether the result of the assertion will be saved to DataHub's backend,
and available to view through the DataHub UI. If this is set to false, the result will NOT be stored in DataHub's
backend. The value defaults to `true`.

If any of the assertion are external (not natively executed by Acryl), they will simply be omitted from the result 
set. 

If running the assertions is successful, the results will be returned as follows:

```json
{
  "data": {
    "runAssertionsForAsset": {
      "passingCount": 2,
      "failingCount": 0,
      "errorCount": 0,
      "results": [
        {
          "urn": "urn:li:assertion:your-assertion-id-1",
          "type": "SUCCESS",
          "nativeResults": [
            {
              "key": "Value",
              "value": "1382"
            }
          ]
        },
        {
          "urn": "urn:li:assertion:your-assertion-id-2",
          "type": "FAILURE",
          "nativeResults": [
            {
              "key": "Value",
              "value": "12323"
            }
          ]
        }
      ]
    }
  },
  "extensions": {}
}
```

Where you should see one result object for each assertion.

</TabItem>


<TabItem value="python" label="Python">

### Run assertion

```python
{{ inline /metadata-ingestion/examples/library/run_assertion.py show_path_as_comment }}
```

### Run multiple assertions

```python
{{ inline /metadata-ingestion/examples/library/run_assertions.py show_path_as_comment }}
```

### Run all assertions for table

```python
{{ inline /metadata-ingestion/examples/library/run_assertions_for_asset.py show_path_as_comment }}
```

</TabItem>

</Tabs>


## Delete Assertions

You can use delete dataset operations to DataHub using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
mutation deleteAssertion {
    deleteAssertion(urn: "urn:li:assertion:test")
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "deleteAssertion": true
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/delete_assertion.py show_path_as_comment }}
```

</TabItem>
</Tabs>
