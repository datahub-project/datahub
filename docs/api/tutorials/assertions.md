import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Assertions

<FeatureAvailability saasOnly />

This guide specifically covers how to use the Assertion APIs for **DataHub Cloud** native assertions, including:

- [Freshness Assertions](/docs/managed-datahub/observe/freshness-assertions.md)
- [Volume Assertions](/docs/managed-datahub/observe/volume-assertions.md)
- [Column Assertions](/docs/managed-datahub/observe/column-assertions.md)
- [Schema Assertions](/docs/managed-datahub/observe/schema-assertions.md)
- [Custom SQL Assertions](/docs/managed-datahub/observe/custom-sql-assertions.md)

## Why Would You Use Assertions APIs?

The Assertions APIs allow you to create, schedule, run, and delete Assertions with DataHub Cloud.

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

This API will return a unique identifier (URN) for the new assertion if you were successful:

```json
{
  "data": {
    "upsertDatasetFreshnessAssertionMonitor": {
      "urn": "urn:li:assertion:your-new-assertion-id"
    }
  },
  "extensions": {}
}
```

For more details, see the [Freshness Assertions](/docs/managed-datahub/observe/freshness-assertions.md) guide.

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

This API will return a unique identifier (URN) for the new assertion if you were successful:

```json
{
  "data": {
    "upsertDatasetVolumeAssertionMonitor": {
      "urn": "urn:li:assertion:your-new-assertion-id"
    }
  },
  "extensions": {}
}
```

For more details, see the [Volume Assertions](/docs/managed-datahub/observe/volume-assertions.md) guide.

### Column Assertions

To create a new column assertion, use the `upsertDatasetFieldAssertionMonitor` GraphQL Mutation.

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

This API will return a unique identifier (URN) for the new assertion if you were successful:

```json
{
  "data": {
    "upsertDatasetFieldAssertionMonitor": {
      "urn": "urn:li:assertion:your-new-assertion-id"
    }
  },
  "extensions": {}
}
```

For more details, see the [Column Assertions](/docs/managed-datahub/observe/column-assertions.md) guide.

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

This API will return a unique identifier (URN) for the new assertion if you were successful:

```json
{
  "data": {
    "upsertDatasetSqlAssertionMonitor": {
      "urn": "urn:li:assertion:your-new-assertion-id"
    }
  },
  "extensions": {}
}
```

For more details, see the [Custom SQL Assertions](/docs/managed-datahub/observe/custom-sql-assertions.md) guide.

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

This API will return a unique identifier (URN) for the new assertion if you were successful:

```json
{
  "data": {
    "upsertDatasetSchemaAssertionMonitor": {
      "urn": "urn:li:assertion:your-new-assertion-id"
    }
  },
  "extensions": {}
}
```

For more details, see the [Schema Assertions](/docs/managed-datahub/observe/schema-assertions.md) guide.


</TabItem>
</Tabs>

## Run Assertions

You can use the following APIs to trigger the assertions you've created to run on-demand. This is
particularly useful for running assertions on a custom schedule, for example from your production
data pipelines.

> **Long-Running Assertions**: The timeout for synchronously running an assertion is currently limited to a maximum of 30 seconds. 
> Each of the following APIs support an `async` parameter, which can be set to `true` to run the assertion asynchronously.
> When set to `true`, the API will kick off the assertion run and return null immediately. To view the result of the assertion,
> simply fetching the runEvents field of the `assertion(urn: String!)` GraphQL query. 

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

### Run Assertion

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

### Run Group of Assertions

```graphql
mutation runAssertions {
    runAssertions(urns: ["urn:li:assertion:your-assertion-id-1", "urn:li:assertion:your-assertion-id-2"], saveResults: true) {
        passingCount
        failingCount
        errorCount
        results {
            urn
            result {
                type
                nativeResults {
                    key
                    value
                }
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
          "result": {
            "type": "SUCCESS",
            "nativeResults": [
              {
                "key": "Value",
                "value": "1382"
              }
            ]
          }
        },
        {
          "urn": "urn:li:assertion:your-assertion-id-2",
          "result": {
            "type": "FAILURE",
            "nativeResults": [
              {
                "key": "Value",
                "value": "12323"
              }
            ]
          }
        }
      ]
    }
  },
  "extensions": {}
}
```

Where you should see one result object for each assertion.

### Run All Assertions for Table

You can also run all assertions for a specific data asset using the `runAssertionsForAsset` mutation.

```graphql
mutation runAssertionsForAsset {
    runAssertionsForAsset(urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchase_events,PROD)", saveResults: true) {
        passingCount
        failingCount
        errorCount
        results {
            urn
            result {
                type
                nativeResults {
                    key
                    value
                }
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
          "result": {
            "type": "SUCCESS",
            "nativeResults": [
              {
                "key": "Value",
                "value": "1382"
              }
            ]
          }
        },
        {
          "urn": "urn:li:assertion:your-assertion-id-2",
          "result": {
            "type": "FAILURE",
            "nativeResults": [
              {
                "key": "Value",
                "value": "12323"
              }
            ]
          }
        }
      ]
    }
  },
  "extensions": {}
}
```

Where you should see one result object for each assertion.

### Run Group of Assertions for Table

If you don't always want to run _all_ assertions for a given table, you can also opt to run a subset of the 
table's assertions using *Assertion Tags*. First, you'll add tags to your assertions to group and categorize them,
then you'll call the `runAssertionsForAsset` mutation with the `tagUrns` argument to filter for assertions having those tags.

#### Step 1: Adding Tag to an Assertion

Currently, you can add tags to an assertion only via the DataHub GraphQL API. You can do this using the following mutation:

```graphql
mutation addTags {
    addTag(input: {
        resourceUrn: "urn:li:assertion:your-assertion",
        tagUrn: "urn:li:tag:my-important-tag",
    })
}
```

#### Step 2: Run All Assertions for a Table with Tags

Now, you can run all assertions for a table with a specific tag(s) using the `runAssertionsForAsset` mutation with the 
`tagUrns` input parameter:

```graphql
mutation runAssertionsForAsset {
    runAssertionsForAsset(urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchase_events,PROD)", tagUrns: ["urn:li:tag:my-important-tag"]) {
        passingCount
        failingCount
        errorCount
        results {
            urn
            result {
                type
                nativeResults {
                    key
                    value
                }
            }
        }
    }
}
```

**Coming Soon**: Support for adding tags to assertions through the DataHub UI.

</TabItem>

<TabItem value="python" label="Python">

### Run Assertion

```python
{{ inline /metadata-ingestion/examples/library/run_assertion.py show_path_as_comment }}
```

### Run Group of Assertions

```python
{{ inline /metadata-ingestion/examples/library/run_assertions.py show_path_as_comment }}
```

### Run All Assertions for Table

```python
{{ inline /metadata-ingestion/examples/library/run_assertions_for_asset.py show_path_as_comment }}
```

</TabItem>

</Tabs>

### Experimental: Providing Dynamic Parameters to Assertions

You can provide **dynamic parameters** to your assertions to customize their behavior. This is particularly useful for
assertions that require dynamic parameters, such as a threshold value that changes based on the time of day.

Dynamic parameters can be injected into the SQL fragment portion of any Assertion. For example, it can appear
in any part of the SQL statement in a [Custom SQL](/docs/managed-datahub/observe/custom-sql-assertions.md) Assertion, 
or it can appear in the **Advanced > Filter** section of a [Column](/docs/managed-datahub/observe/column-assertions.md),
[Volume](/docs/managed-datahub/observe/volume-assertions.md), or [Freshness](/docs/managed-datahub/observe/freshness-assertions.md) Assertion.

To do so, you'll first need to edit the SQL fragment to include the dynamic parameter. Dynamic parameters appear
as `${parameterName}` in the SQL fragment.

Next, you'll call the `runAssertion`, `runAssertions`, or `runAssertionsForAsset` mutations with the `parameters` input argument.
This argument is a list of key-value tuples, where the key is the parameter name and the value is the parameter value:

```graphql
mutation runAssertion {
    runAssertion(urn: "urn:li:assertion:your-assertion-id", parameters: [{key: "parameterName", value: "parameterValue"}]) {
        type 
        nativeResults {
            key
            value
        }
    }
}
```

At runtime, the `${parameterName}` placeholder in the SQL fragment will be replaced with the provided `parameterValue` before the query
is sent to the database for execution.

## Get Assertion Details

You can use the following APIs to 

1. Fetch existing assertion definitions + run history
2. Fetch the assertions associated with a given table + their run history. 

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

### Get Assertions for Table

To retrieve all the assertions for a table, you can use the following GraphQL Query. 

```graphql
query dataset {
    dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchases,PROD)") {
        assertions(start: 0, count: 1000) {
            start
            count
            total
            assertions {
                urn
                # Fetch the last run of each associated assertion. 
                runEvents(status: COMPLETE, limit: 1) {
                    total
                    failed
                    succeeded
                    runEvents {
                        timestampMillis
                        status
                        result {
                            type
                            nativeResults {
                                key
                                value
                            }
                        }
                    }
                }
                info {
                    type
                    description
                    lastUpdated {
                        time
                        actor
                    }
                    datasetAssertion {
                        datasetUrn
                        scope
                        aggregation
                        operator
                        parameters {
                            value {
                                value
                                type
                            }
                            minValue {
                                value
                                type
                            }
                            maxValue {
                                value
                                type
                            }
                        }
                        fields {
                            urn
                            path
                        }
                        nativeType
                        nativeParameters {
                            key
                            value
                        }
                        logic
                    }
                    freshnessAssertion {
                        type
                        entityUrn
                        schedule {
                            type
                            cron {
                                cron
                                timezone
                            }
                            fixedInterval {
                                unit
                                multiple
                            }
                        }
                        filter {
                            type
                            sql
                        }
                    }
                    sqlAssertion {
                        type
                        entityUrn
                        statement
                        changeType
                        operator
                        parameters {
                            value {
                                value
                                type
                            }
                            minValue {
                                value
                                type
                            }
                            maxValue {
                                value
                                type
                            }
                        }
                    }
                    fieldAssertion {
                        type
                        entityUrn
                        filter {
                            type
                            sql
                        }
                        fieldValuesAssertion {
                            field {
                                path
                                type
                                nativeType
                            }
                            transform {
                                type
                            }
                            operator
                            parameters {
                                value {
                                    value
                                    type
                                }
                                minValue {
                                    value
                                    type
                                }
                                maxValue {
                                    value
                                    type
                                }
                            }
                            failThreshold {
                                type
                                value
                            }
                            excludeNulls
                        }
                        fieldMetricAssertion {
                            field {
                                path
                                type
                                nativeType
                            }
                            metric
                            operator
                            parameters {
                                value {
                                    value
                                    type
                                }
                                minValue {
                                    value
                                    type
                                }
                                maxValue {
                                    value
                                    type
                                }
                            }
                        }
                    }
                    volumeAssertion {
                        type
                        entityUrn
                        filter {
                            type
                            sql
                        }
                        rowCountTotal {
                            operator
                            parameters {
                                value {
                                    value
                                    type
                                }
                                minValue {
                                    value
                                    type
                                }
                                maxValue {
                                    value
                                    type
                                }
                            }
                        }
                        rowCountChange {
                            type
                            operator
                            parameters {
                                value {
                                    value
                                    type
                                }
                                minValue {
                                    value
                                    type
                                }
                                maxValue {
                                    value
                                    type
                                }
                            }
                        }
                    }
                    schemaAssertion {
                        entityUrn
                        compatibility
                        fields {
                            path
                            type
                            nativeType
                        }
                        schema {
                            fields {
                                fieldPath
                                type
                                nativeDataType
                            }
                        }
                    }
                    source {
                        type
                        created {
                            time
                            actor
                        }
                    }
                }
            }
        }
    }
}
```

### Get Assertion Details

You can use the following GraphQL query to fetch the details for an assertion along with its evaluation history by URN.

```graphql
query getAssertion {
    assertion(urn: "urn:li:assertion:assertion-id") {
        urn
        # Fetch the last 10 runs for the assertion. 
        runEvents(status: COMPLETE, limit: 10) {
            total
            failed
            succeeded
            runEvents {
                timestampMillis
                status
                result {
                    type
                    nativeResults {
                        key
                        value
                    }
                }
            }
        }
        info {
            type
            description
            lastUpdated {
                time
                actor
            }
            datasetAssertion {
                datasetUrn
                scope
                aggregation
                operator
                parameters {
                    value {
                        value
                        type
                    }
                    minValue {
                        value
                        type
                    }
                    maxValue {
                        value
                        type
                    }
                }
                fields {
                    urn
                    path
                }
                nativeType
                nativeParameters {
                    key
                    value
                }
                logic
            }
            freshnessAssertion {
                type
                entityUrn
                schedule {
                    type
                    cron {
                        cron
                        timezone
                    }
                    fixedInterval {
                        unit
                        multiple
                    }
                }
                filter {
                    type
                    sql
                }
            }
            sqlAssertion {
                type
                entityUrn
                statement
                changeType
                operator
                parameters {
                    value {
                        value
                        type
                    }
                    minValue {
                        value
                        type
                    }
                    maxValue {
                        value
                        type
                    }
                }
            }
            fieldAssertion {
                type
                entityUrn
                filter {
                    type
                    sql
                }
                fieldValuesAssertion {
                    field {
                        path
                        type
                        nativeType
                    }
                    transform {
                        type
                    }
                    operator
                    parameters {
                        value {
                            value
                            type
                        }
                        minValue {
                            value
                            type
                        }
                        maxValue {
                            value
                            type
                        }
                    }
                    failThreshold {
                        type
                        value
                    }
                    excludeNulls
                }
                fieldMetricAssertion {
                    field {
                        path
                        type
                        nativeType
                    }
                    metric
                    operator
                    parameters {
                        value {
                            value
                            type
                        }
                        minValue {
                            value
                            type
                        }
                        maxValue {
                            value
                            type
                        }
                    }
                }
            }
            volumeAssertion {
                type
                entityUrn
                filter {
                    type
                    sql
                }
                rowCountTotal {
                    operator
                    parameters {
                        value {
                            value
                            type
                        }
                        minValue {
                            value
                            type
                        }
                        maxValue {
                            value
                            type
                        }
                    }
                }
                rowCountChange {
                    type
                    operator
                    parameters {
                        value {
                            value
                            type
                        }
                        minValue {
                            value
                            type
                        }
                        maxValue {
                            value
                            type
                        }
                    }
                }
            }
            schemaAssertion {
                entityUrn
                compatibility
                fields {
                    path
                    type
                    nativeType
                }
                schema {
                    fields {
                        fieldPath
                        type
                        nativeDataType
                    }
                }
            }
            source {
                type
                created {
                    time
                    actor
                }
            }
        }
    }
}
```

</TabItem>

<TabItem value="python" label="Python">

```python
Python support coming soon!
```

</TabItem>
</Tabs>

## Add Tag to Assertion

You can add tags to individual assertions to group and categorize them, for example by its priority or severity.
Note that the tag should already exist in DataHub, or the operation will fail. 

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
mutation addTags {
    addTag(input: {
        resourceUrn: "urn:li:assertion:your-assertion",
        tagUrn: "urn:li:tag:my-important-tag",
    })
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "addTag": true
  },
  "extensions": {}
}
```

You can create new tags using the `createTag` mutation or via the UI. 

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

## (Advanced) Create and Report Results for Custom Assertions

If you'd like to create and report results for your own custom assertions, e.g. those which are run and 
evaluated outside of Acryl, you need to generate 2 important Assertion Entity aspects, and give the assertion a unique
URN of the following format:


1. Generate a unique URN for your assertion

```plaintext
urn:li:assertion:<unique-assertion-id>
```

2. Generate the [**AssertionInfo**](/docs/generated/metamodel/entities/assertion.md#assertion-info) aspect for the assertion. You can do this using the Python SDK. Give your assertion a `type` and a `source`
with type `EXTERNAL` to mark it as an external assertion, not run by DataHub itself.

3. Generate the [**AssertionRunEvent**](/docs/generated/metamodel/entities/assertion.md#assertionrunevent-timeseries) timeseries aspect using the Python SDK. This aspect should contain the result of the assertion 
run at a given timestamp and will be shown on the results graph in DataHub's UI. 

