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

The Assertions APIs allow you to create, schedule, run, and delete Assertions with DataHub Cloud. Additionally, you can manage subscriptions to receive notifications when assertions change state or when other entity changes occur.

### Goal Of This Guide

This guide will show you how to create, schedule, run and delete Assertions for a Table.

## Prerequisites

The actor making API calls must have the `Edit Assertions` and `Edit Monitors` privileges for the Tables at hand.

## Create Assertions

You can create new dataset Assertions to DataHub using the following APIs.

### Freshness Assertion

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

To create a new freshness assertion, use the `upsertDatasetFreshnessAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetFreshnessAssertionMonitor {
  upsertDatasetFreshnessAssertionMonitor(
    input: {
      entityUrn: "<urn of entity being monitored>"
      schedule: {
        type: FIXED_INTERVAL
        fixedInterval: { unit: HOUR, multiple: 8 }
      }
      evaluationSchedule: {
        timezone: "America/Los_Angeles"
        cron: "0 */8 * * *"
      }
      evaluationParameters: { sourceType: INFORMATION_SCHEMA }
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

</TabItem>
<TabItem value="python" label="Python">

```python
from datahub.sdk import DataHubClient
from datahub.metadata.urns import DatasetUrn

# Initialize the client
client = DataHubClient(server="<your_server>", token="<your_token>")

# Create smart freshness assertion (AI-powered anomaly detection)
dataset_urn = DatasetUrn.from_string("urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)")

smart_freshness_assertion = client.assertions.sync_smart_freshness_assertion(
    dataset_urn=dataset_urn,
    display_name="Smart Freshness Anomaly Monitor",
    # Detection mechanism - information_schema is recommended
    detection_mechanism="information_schema",
    # Smart sensitivity setting
    sensitivity="medium",  # options: "low", "medium", "high"
    # Tags for grouping
    tags=["automated", "freshness", "data_quality"],
    # Enable the assertion
    enabled=True
)

print(f"Created smart freshness assertion: {smart_freshness_assertion.urn}")

# Create traditional freshness assertion (fixed interval)
freshness_assertion = client.assertions.sync_freshness_assertion(
    dataset_urn=dataset_urn,
    display_name="Fixed Interval Freshness Check",
    # Fixed interval check - table should be updated within lookback window
    freshness_schedule_check_type="fixed_interval",
    # Lookback window - table should be updated within 8 hours
    lookback_window={"unit": "HOUR", "multiple": 8},
    # Detection mechanism
    detection_mechanism="information_schema",
    # Evaluation schedule - how often to check
    schedule="0 */2 * * *",  # Check every 2 hours
    # Tags
    tags=["automated", "freshness", "fixed_interval"],
    enabled=True
)

print(f"Created freshness assertion: {freshness_assertion.urn}")

# Create since-last-check freshness assertion
since_last_check_assertion = client.assertions.sync_freshness_assertion(
    dataset_urn=dataset_urn,
    display_name="Since Last Check Freshness",
    # Since last check - table should be updated since the last evaluation
    freshness_schedule_check_type="since_the_last_check",
    # Detection mechanism with last modified column
    detection_mechanism={
        "type": "last_modified_column",
        "column_name": "updated_at",
        "additional_filter": "status = 'active'"
    },
    # Evaluation schedule - how often to check
    schedule="0 */6 * * *",  # Check every 6 hours
    # Tags
    tags=["automated", "freshness", "since_last_check"],
    enabled=True
)

print(f"Created since last check assertion: {since_last_check_assertion.urn}")

# Create freshness assertion with high watermark column
watermark_freshness_assertion = client.assertions.sync_freshness_assertion(
    dataset_urn=dataset_urn,
    display_name="High Watermark Freshness Check",
    # Fixed interval check with specific lookback window
    freshness_schedule_check_type="fixed_interval",
    # Lookback window - check for updates in the last 24 hours
    lookback_window={"unit": "DAY", "multiple": 1},
    # Detection mechanism using high watermark column (e.g., auto-incrementing ID)
    detection_mechanism={
        "type": "high_watermark_column",
        "column_name": "id",
        "additional_filter": "status != 'deleted'"
    },
    # Evaluation schedule
    schedule="0 8 * * *",  # Check daily at 8 AM
    # Tags
    tags=["automated", "freshness", "high_watermark"],
    enabled=True
)

print(f"Created watermark freshness assertion: {watermark_freshness_assertion.urn}")
```

</TabItem>
</Tabs>

For more details, see the [Freshness Assertions](/docs/managed-datahub/observe/freshness-assertions.md) guide.

### Volume Assertions

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

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
          minValue: { value: "10", type: NUMBER }
          maxValue: { value: "20", type: NUMBER }
        }
      }
      evaluationSchedule: {
        timezone: "America/Los_Angeles"
        cron: "0 */8 * * *"
      }
      evaluationParameters: { sourceType: INFORMATION_SCHEMA }
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

</TabItem>

<!-- TODO: move the python examples to metadata-ingestion/examples/library -->
<TabItem value="python" label="Python">

```python
from datahub.sdk import DataHubClient
from datahub.metadata.urns import DatasetUrn

# Initialize the client
client = DataHubClient(server="<your_server>", token="<your_token>")

# Create smart volume assertion (AI-powered anomaly detection)
dataset_urn = DatasetUrn.from_string("urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)")

smart_volume_assertion = client.assertions.sync_smart_volume_assertion(
    dataset_urn=dataset_urn,
    display_name="Smart Volume Check",
    # Detection mechanism options
    detection_mechanism="information_schema",
    # Smart sensitivity setting
    sensitivity="medium",  # options: "low", "medium", "high"
    # Tags for grouping
    tags=["automated", "volume", "data_quality"],
    # Schedule (optional - defaults to hourly)
    schedule="0 */6 * * *",  # Every 6 hours
    # Enable the assertion
    enabled=True
)

print(f"Created smart volume assertion: {smart_volume_assertion.urn}")

# Create traditional volume assertion (fixed threshold range)
volume_assertion = client.assertions.sync_volume_assertion(
    dataset_urn=dataset_urn,
    display_name="Row Count Range Check",
    criteria_condition="ROW_COUNT_IS_WITHIN_A_RANGE",
    criteria_parameters=(1000, 10000),  # Between 1000 and 10000 rows
    # Detection mechanism
    detection_mechanism="information_schema",
    # Evaluation schedule
    schedule="0 */4 * * *",  # Every 4 hours
    # Tags
    tags=["automated", "volume", "threshold_check"],
    enabled=True
)

print(f"Created volume assertion: {volume_assertion.urn}")

# Example with single threshold
min_volume_assertion = client.assertions.sync_volume_assertion(
    dataset_urn=dataset_urn,
    display_name="Minimum Row Count Check",
    criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
    criteria_parameters=500,  # At least 500 rows
    detection_mechanism="information_schema",
    schedule="0 */2 * * *",  # Every 2 hours
    tags=["automated", "volume", "minimum_check"],
    enabled=True
)

print(f"Created minimum volume assertion: {min_volume_assertion.urn}")

# Example with growth-based assertion
growth_volume_assertion = client.assertions.sync_volume_assertion(
    dataset_urn=dataset_urn,
    display_name="Daily Growth Check",
    criteria_condition="ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE",
    criteria_parameters=1000,  # Grows by at most 1000 rows between checks
    detection_mechanism="information_schema",
    schedule="0 6 * * *",  # Daily at 6 AM
    tags=["automated", "volume", "growth_check"],
    enabled=True
)

print(f"Created growth volume assertion: {growth_volume_assertion.urn}")
```

</TabItem>
</Tabs>

For more details, see the [Volume Assertions](/docs/managed-datahub/observe/volume-assertions.md) guide.

### Column Assertions

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

To create a new column assertion, use the `upsertDatasetFieldAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetFieldAssertionMonitor {
  upsertDatasetFieldAssertionMonitor(
    input: {
      entityUrn: "<urn of entity being monitored>"
      type: FIELD_VALUES
      fieldValuesAssertion: {
        field: {
          path: "<name of the column to be monitored>"
          type: "NUMBER"
          nativeType: "NUMBER(38,0)"
        }
        operator: GREATER_THAN
        parameters: { value: { type: NUMBER, value: "10" } }
        failThreshold: { type: COUNT, value: 0 }
        excludeNulls: true
      }
      evaluationSchedule: {
        timezone: "America/Los_Angeles"
        cron: "0 */8 * * *"
      }
      evaluationParameters: { sourceType: ALL_ROWS_QUERY }
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
    "upsertDatasetFieldAssertionMonitor": {
      "urn": "urn:li:assertion:your-new-assertion-id"
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from datahub.sdk import DataHubClient
from datahub.metadata.urns import DatasetUrn

# Initialize the client
client = DataHubClient(server="<your_server>", token="<your_token>")

# Create smart column metric assertion (AI-powered anomaly detection)
dataset_urn = DatasetUrn.from_string("urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)")

smart_column_assertion = client.assertions.sync_smart_column_metric_assertion(
    dataset_urn=dataset_urn,
    column_name="user_id",
    metric_type="null_count",
    display_name="Smart Null Count Check - user_id",
    # Detection mechanism for column metrics
    detection_mechanism="all_rows_query_datahub_dataset_profile",
    # Smart sensitivity setting
    sensitivity="medium",  # options: "low", "medium", "high"
    # Tags
    tags=["automated", "column_quality", "null_checks"],
    enabled=True
)

print(f"Created smart column assertion: {smart_column_assertion.urn}")

# Create regular column metric assertion (fixed threshold)
column_assertion = client.assertions.sync_column_metric_assertion(
    dataset_urn=dataset_urn,
    column_name="price",
    metric_type="min",
    operator="greater_than_or_equal_to",
    criteria_parameters=0,
    display_name="Price Minimum Check",
    # Evaluation schedule
    schedule="0 */4 * * *",  # Every 4 hours
    # Tags
    tags=["automated", "column_quality", "price_validation"],
    enabled=True
)

print(f"Created column assertion: {column_assertion.urn}")
```

</TabItem>
</Tabs>

For more details, see the [Column Assertions](/docs/managed-datahub/observe/column-assertions.md) guide.

### Custom SQL Assertions

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

To create a new custom SQL assertion, use the `upsertDatasetSqlAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetSqlAssertionMonitor {
  upsertDatasetSqlAssertionMonitor(
    assertionUrn: "<urn of assertion created in earlier query>"
    input: {
      entityUrn: "<urn of entity being monitored>"
      type: METRIC
      description: "<description of the custom assertion>"
      statement: "<SQL query to be evaluated>"
      operator: GREATER_THAN_OR_EQUAL_TO
      parameters: { value: { value: "100", type: NUMBER } }
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

</TabItem>
<TabItem value="python" label="Python">

```python
from datahub.sdk import DataHubClient
from datahub.metadata.urns import DatasetUrn

# Initialize the client
client = DataHubClient(server="<your_server>", token="<your_token>")

# Create custom SQL assertion
dataset_urn = DatasetUrn.from_string("urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)")

sql_assertion = client.assertions.sync_sql_assertion(
    dataset_urn=dataset_urn,
    display_name="Revenue Quality Check",
    statement="SELECT SUM(revenue) FROM database.schema.table WHERE date >= CURRENT_DATE - INTERVAL '1 day'",
    criteria_condition="IS_GREATER_THAN_OR_EQUAL_TO",
    criteria_parameters=1000,
    # Evaluation schedule
    schedule="0 6 * * *",  # Daily at 6 AM
    # Tags
    tags=["automated", "revenue", "data_quality"],
    enabled=True
)

print(f"Created SQL assertion: {sql_assertion.urn}")

# Example with range check
range_sql_assertion = client.assertions.sync_sql_assertion(
    dataset_urn=dataset_urn,
    display_name="Daily Order Count Range Check",
    statement="SELECT COUNT(*) FROM database.schema.orders WHERE DATE(created_at) = CURRENT_DATE",
    criteria_condition="IS_WITHIN_A_RANGE",
    criteria_parameters=(50, 500),  # Between 50 and 500 orders per day
    schedule="0 */6 * * *",  # Every 6 hours
    tags=["automated", "orders", "volume_check"],
    enabled=True
)

print(f"Created range SQL assertion: {range_sql_assertion.urn}")
```

</TabItem>
</Tabs>

For more details, see the [Custom SQL Assertions](/docs/managed-datahub/observe/custom-sql-assertions.md) guide.

### Schema Assertions

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

To create a new schema assertion, use the `upsertDatasetSchemaAssertionMonitor` GraphQL Mutation.

```graphql
mutation upsertDatasetSchemaAssertionMonitor {
  upsertDatasetSchemaAssertionMonitor(
    assertionUrn: "urn:li:assertion:existing-assertion-id"
    input: {
      entityUrn: "<urn of the table to be monitored>"
      assertion: {
        compatibility: EXACT_MATCH
        fields: [
          { path: "id", type: STRING }
          { path: "count", type: NUMBER }
          { path: "struct", type: STRUCT }
          { path: "struct.nestedBooleanField", type: BOOLEAN }
        ]
      }
      description: "<description of the schema assertion>"
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

</TabItem>
</Tabs>

For more details, see the [Schema Assertions](/docs/managed-datahub/observe/schema-assertions.md) guide.

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
backend. **Default: `true`** (results are saved when not specified).

The `async` argument controls whether the assertion runs asynchronously. When set to `true`, the API will kick off
the assertion run and return immediately. When set to `false` or omitted, the assertion runs synchronously with a
30-second timeout. **Default: `false`** (synchronous execution when not specified).

If the assertion is external (not natively executed by DataHub), this API will return an error.

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
  runAssertions(
    urns: [
      "urn:li:assertion:your-assertion-id-1"
      "urn:li:assertion:your-assertion-id-2"
    ]
    saveResults: true
  ) {
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
backend. **Default: `true`** (results are saved when not specified).

The `async` argument controls whether the assertions run asynchronously. When set to `true`, the API will kick off
the assertion runs and return immediately. When set to `false` or omitted, the assertions run synchronously with a
30-second timeout per assertion. **Default: `false`** (synchronous execution when not specified).

If any of the assertion are external (not natively executed by DataHub), they will simply be omitted from the result set.

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
  runAssertionsForAsset(
    urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchase_events,PROD)"
    saveResults: true
  ) {
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
backend. **Default: `true`** (results are saved when not specified).

The `async` argument controls whether the assertions run asynchronously. When set to `true`, the API will kick off
the assertion runs and return immediately. When set to `false` or omitted, the assertions run synchronously with a
30-second timeout per assertion. **Default: `false`** (synchronous execution when not specified).

If any of the assertion are external (not natively executed by DataHub), they will simply be omitted from the result
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
table's assertions using _Assertion Tags_. First, you'll add tags to your assertions to group and categorize them,
then you'll call the `runAssertionsForAsset` mutation with the `tagUrns` argument to filter for assertions having those tags.

#### Step 1: Adding Tag to an Assertion

Currently, you can add tags to an assertion only via the DataHub GraphQL API. You can do this using the following mutation:

```graphql
mutation addTags {
  addTag(
    input: {
      resourceUrn: "urn:li:assertion:your-assertion"
      tagUrn: "urn:li:tag:my-important-tag"
    }
  )
}
```

#### Step 2: Run All Assertions for a Table with Tags

Now, you can run all assertions for a table with a specific tag(s) using the `runAssertionsForAsset` mutation with the
`tagUrns` input parameter:

```graphql
mutation runAssertionsForAsset {
  runAssertionsForAsset(
    urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchase_events,PROD)"
    tagUrns: ["urn:li:tag:my-important-tag"]
  ) {
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

### Providing Dynamic Parameters to Assertions

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
  runAssertion(
    urn: "urn:li:assertion:your-assertion-id"
    parameters: [{ key: "parameterName", value: "parameterValue" }]
  ) {
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
  dataset(
    urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchases,PROD)"
  ) {
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
  addTag(
    input: {
      resourceUrn: "urn:li:assertion:your-assertion"
      tagUrn: "urn:li:tag:my-important-tag"
    }
  )
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
{{ inline /metadata-ingestion/examples/library/assertion_delete.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## (Advanced) Create and Report Results for Custom Assertions

If you'd like to create and report results for your own custom assertions, e.g. those which are run and
evaluated outside of DataHub Cloud, you need to generate 2 important Assertion Entity aspects, and give the assertion a unique
URN of the following format:

1. Generate a unique URN for your assertion

```plaintext
urn:li:assertion:<unique-assertion-id>
```

2. Generate the [**AssertionInfo**](/docs/generated/metamodel/entities/assertion.md#assertion-info) aspect for the assertion. You can do this using the Python SDK. Give your assertion a `type` and a `source`
   with type `EXTERNAL` to mark it as an external assertion, not run by DataHub itself.

3. Generate the [**AssertionRunEvent**](/docs/generated/metamodel/entities/assertion.md#assertionrunevent-timeseries) timeseries aspect using the Python SDK. This aspect should contain the result of the assertion
   run at a given timestamp and will be shown on the results graph in DataHub's UI.

## Create and Remove Subscriptions

Reference the [Subscriptions SDK](/docs/api/tutorials/subscriptions.md) for more information on how to create and remove subscriptions on Datasets or Assertions.
