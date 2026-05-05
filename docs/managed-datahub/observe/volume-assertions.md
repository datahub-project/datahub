---
description: This page provides an overview of working with DataHub Volume Assertions
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Volume Assertions

<FeatureAvailability saasOnly />

> The **Volume Assertions** feature is available as part of the **DataHub Cloud Observe** module of DataHub Cloud.
> If you are interested in learning more about **DataHub Cloud Observe** or trying it out, please [visit our website](https://datahub.com/products/data-observability/).

## Introduction

Can you remember a time when the meaning of Data Warehouse Table that you depended on fundamentally changed, with little or no notice?
If the answer is yes, how did you find out? We'll take a guess - someone looking at an internal reporting dashboard or worse, a user using your your product, sounded an alarm when
a number looked a bit out of the ordinary. Perhaps your table initially tracked purchases made on your company's e-commerce web store, but suddenly began to include purchases made
through your company's new mobile app.

There are many reasons why an important Table on Snowflake, Redshift, BigQuery, or Databricks may change in its meaning - application code bugs, new feature rollouts,
changes to key metric definitions, etc. Often times, these changes break important assumptions made about the data used in building key downstream data products
like reporting dashboards or data-driven product features.

What if you could reduce the time to detect these incidents, so that the people responsible for the data were made aware of data
issues _before_ anyone else? With DataHub Cloud **Volume Assertions**, you can.

DataHub Cloud allows users to define expectations about the normal volume, or size, of a particular warehouse Table,
and then monitor those expectations over time as the table grows and changes.

In this article, we'll cover the basics of monitoring Volume Assertions - what they are, how to configure them, and more - so that you and your team can
start building trust in your most important data assets.

Let's get started!

## Support

Volume Assertions are currently supported for:

1. Snowflake
2. Redshift
3. BigQuery
4. Databricks
5. DataHub Dataset Profile (collected via ingestion)

Note that an Ingestion Source _must_ be configured with the data platform of your choice in DataHub Cloud's **Ingestion**
tab.

> Note that Volume Assertions are not yet supported if you are connecting to your warehouse
> using the DataHub CLI.

## What is a Volume Assertion?

A **Volume Assertion** is a configurable Data Quality rule used to monitor a Data Warehouse Table
for unexpected or sudden changes in "volume", or row count. Volume Assertions can be particularly useful when you have frequently-changing
Tables which have a relatively stable pattern of growth or decline.

For example, imagine that we work for a company with a Snowflake Table that stores user clicks collected from our e-commerce website.
This table is updated with new data on a specific cadence: once per hour (In practice, daily or even weekly are also common).
In turn, there is a downstream Business Analytics Dashboard in Looker that shows important metrics like
the number of people clicking our "Daily Sale" banners, and this dashboard is generated from data stored in our "clicks" table.
It is important that our clicks Table is updated with the correct number of rows each hour, else it could mean
that our downstream metrics dashboard becomes incorrect. The risk of this situation is obvious: our organization
may make bad decisions based on incomplete information.

In such cases, we can use a **Volume Assertion** that checks whether the Snowflake "clicks" Table is growing in an expected
way, and that there are no sudden increases or sudden decreases in the rows being added or removed from the table.
If too many rows are added or removed within an hour, we can notify key stakeholders and begin to root cause before the problem impacts stakeholders of the data.

### Anatomy of a Volume Assertion

At the most basic level, **Volume Assertions** consist of a few important parts:

1. An **Evaluation Schedule**
2. A **Volume Condition**
3. A **Volume Source**

In this section, we'll give an overview of each.

#### 1. Evaluation Schedule

The **Evaluation Schedule**: This defines how often to check a given warehouse Table for its volume. This should usually
be configured to match the expected change frequency of the Table, although it can also be less frequently depending
on the requirements. You can also specify specific days of the week, hours in the day, or even
minutes in an hour.

#### 2. Volume Condition

The **Volume Condition**: This defines the type of condition that we'd like to monitor, or when the Assertion
should result in failure.

There are a 2 different categories of conditions: **Total** Volume and **Change** Volume.

_Total_ volume conditions are those which are defined against the point-in-time total row count for a table. They allow you to specify conditions like:

1. **Table has too many rows**: The table should always have less than 1000 rows
2. **Table has too few rows**: The table should always have more than 1000 rows
3. **Table row count is outside a range**: The table should always have between 1000 and 2000 rows.

_Change_ volume conditions are those which are defined against the growth or decline rate of a table, measured between subsequent checks
of the table volume. They allow you to specify conditions like:

1. **Table growth is too fast**: When the table volume is checked, it should have < 1000 more rows than it had during the previous check.
2. **Table growth is too slow**: When the table volume is checked, it should have > 1000 more rows than it had during the previous check.
3. **Table growth is outside a range**: When the table volume is checked, it should have between 1000 and 2000 more rows than it had during the previous check.

For change volume conditions, both _absolute_ row count deltas and relative percentage deltas are supported for identifying
table that are following an abnormal pattern of growth.

#### 3. Volume Source

The **Volume Source**: This is the mechanism that DataHub Cloud should use to determine the table volume (row count). The supported
source types vary by the platform, but generally fall into these categories:

- **Information Schema**: A system Table that is exposed by the Data Warehouse which contains live information about the Databases
  and Tables stored inside the Data Warehouse, including their row count. It is usually efficient to check, but can in some cases be slightly delayed to update
  once a change has been made to a table. This is the optimal balance between cost and accuracy for most Data Platforms.

- **Table Statistics** (Databricks Only): Uses platform-native catalog statistics to retrieve the current row count for a Table. For Databricks,
  this runs [`ANALYZE TABLE ... COMPUTE STATISTICS`](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-analyze-table.html) followed by
  `DESCRIBE TABLE EXTENDED`, which reads the cached `numRows` from the catalog rather than scanning data. On Delta tables this is a metadata-only
  operation (file-level statistics are pulled from the transaction log), making it significantly cheaper than a `COUNT(*)` query on large tables.
  This method requires `MODIFY` privilege (or ownership) on the target table so that `ANALYZE TABLE` can refresh the cached statistics,
  and is the default Volume Source for Databricks. This method is only supported for Tables, not Views.

- **Query**: A `COUNT(*)` query is used to retrieve the latest row count for a table, with optional SQL filters applied (depending on platform).
  This can be less efficient to check depending on the size of the table. This approach is more portable, as it does not involve
  system warehouse tables, it is also easily portable across Data Warehouse and Data Lake providers. This issues a query to the table, which can be more expensive than Information Schema.

- **DataHub Dataset Profile**: The DataHub Dataset Profile aspect is used to retrieve the latest row count information for a table.
  Using this option avoids contacting your data platform, and instead uses the DataHub Dataset Profile metadata to evaluate Volume Assertions.
  Note if you have not configured a managed ingestion source through DataHub, then this may be the only option available. This is the cheapest option, but requires that Dataset Profiles are reported to DataHub. By default, Ingestion will report Dataset Profiles to DataHub, which can be and infrequent. You can report Dataset Profiles via the DataHub APIs for more frequent and reliable data.

Volume Assertions also have an off switch: they can be started or stopped at any time with the click of button.

## Creating a Volume Assertion

### Prerequisites

1. **Permissions**: To create or delete Volume Assertions for a specific entity on DataHub, you'll need to be granted the
   `Edit Assertions` and `Edit Monitors` privileges for the entity. This will be granted to Entity owners as part of the `Asset Owners - Metadata Policy`
   by default.

2. (Optional) **Data Platform Connection**: In order to create a Volume Assertion that queries the source data platform directly (instead of DataHub metadata), you'll need to have an **Ingestion Source** configured to your
   Data Platform: Snowflake, BigQuery, or Redshift under the **Integrations** tab.

Once these are in place, you're ready to create your Volume Assertions!

You can also apply Smart Volume Assertions at scale using [Monitoring Rules](/docs/managed-datahub/observe/data-health-dashboard.md#monitoring-rules) on the Data Health page.

### Steps

1. Navigate to the Table that to monitor for volume
2. Click the **Quality** tab

<p align="left">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/freshness/profile-validation-tab.png"/>
</p>

3. Click **+ Create Assertion**

<p align="left">
  <img width="45%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/volume/assertion-builder-volume-choose-type.png"/>
</p>

4. Choose **Volume**

5. Configure the evaluation **schedule**. This is the frequency at which the assertion will be evaluated to produce a pass or fail result, and the times
   when the table volume will be checked.

6. Configure the evaluation **condition type**. This determines the cases in which the new assertion will fail when it is evaluated.

<p align="left">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/volume/assertion-builder-volume-condition-type.png"/>
</p>

7. (Optional) Click **Advanced** to customize the volume **source**. This is the mechanism that will be used to obtain the table
   row count metric. Each Data Platform supports different options including Information Schema, Query, and DataHub Dataset Profile.

<p align="left">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/volume/assertion-builder-volume-select-source-type.png"/>
</p>

- **Information Schema**: Check the Data Platform system metadata tables to determine the table row count.
- **Query**: Issue a `COUNT(*)` query to the table to determine the row count.
- **DataHub Dataset Profile**: Use the DataHub Dataset Profile metadata to determine the row count.

8. Configure actions that should be taken when the Volume Assertion passes or fails

<p align="left">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/assertion-builder-actions.png"/>
</p>

- **Raise incident**: Automatically raise a new DataHub `Volume` Incident for the Table whenever the Volume Assertion is failing. This
  may indicate that the Table is unfit for consumption. Configure Slack Notifications under **Settings** to be notified when
  an incident is created due to an Assertion failure.

- **Resolve incident**: Automatically resolved any incidents that were raised due to failures in this Volume Assertion. Note that
  any other incidents will not be impacted.

9. Click **Next** and provide a description.

10. Click **Save**.

And that's it! DataHub will now begin to monitor your Volume Assertion for the table.

Once your assertion has run, you will begin to see Success or Failure status for the Table

<p align="left">
  <img width="45%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/volume/profile-passing-volume-assertions-expanded.png"/>
</p>

## Anomaly Detection with Smart Assertions ⚡

As part of the **DataHub Cloud Observe** module, DataHub Cloud also provides **Smart Assertions** out of the box. These are
dynamic, AI-powered Volume Assertions that you can use to monitor the volume of important warehouse Tables, without
requiring any manual setup.

You can create smart assertions by simply selecting the `Detect with AI` option in the UI:

<p align="left">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/volume/volume-smart-assertion.png"/>
</p>

## Time-Series Bucketing

By default, volume assertions evaluate the **total row count** of a table at a point in time. With **time-series bucketing**, you can partition your data into time-based buckets (e.g., daily or weekly) and evaluate volume metrics within each bucket. This fundamentally changes what the assertion measures:

|                      | Without Bucketing                          | With Bucketing                                              |
| -------------------- | ------------------------------------------ | ----------------------------------------------------------- |
| **Total row count**  | Total table row count                      | Rows matching the timestamp-in-bucket condition             |
| Example              | "Total row count should stay above 10,000" | "Rows added per day should be above 500"                    |
| **Row count change** | Row count change since previous evaluation | Row count compared to previous bucket                       |
| Example              | "Row count should grow by at least 100"    | "Difference in rows added per week should not exceed 5,000" |

Time-series bucketing is useful when:

- Your table has a timestamp column that represents when rows were created or updated
- You want to monitor data quality at a day or week granularity rather than the whole table
- You want to detect issues like "no data arrived today" or "this week's volume is abnormally low"

### Bucketing Configuration

A time-series bucketing strategy consists of:

- **Timestamp column**: The date/time column used to partition rows into buckets (e.g., `created_at`, `event_date`).
- **Bucket interval**: The size of each time bucket. Currently supported intervals are **Daily** (1 DAY) and **Weekly** (1 WEEK).
- **Timezone**: The IANA timezone for bucket boundaries (e.g., `America/Los_Angeles`, `UTC`). This should match your timestamp column's timezone. Defaults to UTC.
- **Late arrival grace period** (optional): A buffer after the bucket end time before the bucket is considered complete. This accounts for late-arriving data. For example, a 2-day grace period on a daily bucket means the bucket for Monday won't be evaluated until Thursday at midnight (instead of Tuesday at midnight).

:::note
When time-series bucketing is enabled, the assertion's **evaluation schedule is automatically computed** based on the bucket interval and grace period. You do not need to (and cannot) manually set a cron schedule for bucketed assertions.
:::

### Limitations

- Only **Query** source type supports bucketing. Information Schema and DataHub Dataset Profile sources cannot be bucketed.
- Only single-unit bucket intervals are supported (1 DAY or 1 WEEK, not 2 DAYs).
- Bucketing configuration (timestamp column, bucket interval, timezone) **cannot be changed after creation**. The late arrival grace period can be updated.
- If a bucket is missed due to downtime, it will not be retroactively evaluated.

### Configuring Bucketing in the UI

When creating a volume assertion:

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/bucketing/volume-timeseries-bucketing.png"/>
</p>

1. In the assertion builder, expand the **Time-Series Bucketing** section.
2. Toggle bucketing **on**.
3. Select the **timestamp column** from the dropdown (filtered to date/time fields).
4. Choose the **bucket size** (Daily or Weekly).
5. Select a **timezone** that matches your timestamp column's timezone.
6. (Optional) Set a **grace period** to account for late-arriving data.

### Configuring Bucketing via the Python SDK

```python
from datahub.sdk import DataHubClient
from datahub.metadata.urns import DatasetUrn

client = DataHubClient(server="<your_server>", token="<your_token>")
dataset_urn = DatasetUrn.from_string(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)"
)

# Volume assertion with daily bucketing
volume_assertion = client.assertions.sync_volume_assertion(
    dataset_urn=dataset_urn,
    display_name="Daily Row Count Check",
    criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
    criteria_parameters=100,
    detection_mechanism="information_schema",
    time_bucketing_strategy={
        "timestamp_field_path": "created_at",
        "bucket_interval": {"unit": "DAY", "multiple": 1},
        "timezone": "America/Los_Angeles",
        "late_arrival_grace_period": {"unit": "DAY", "multiple": 2},
    },
    tags=["automated", "volume", "daily"],
    enabled=True,
)

# Smart volume assertion with weekly bucketing and backfill
smart_volume = client.assertions.sync_smart_volume_assertion(
    dataset_urn=dataset_urn,
    display_name="Weekly Volume Anomaly Monitor",
    detection_mechanism="information_schema",
    sensitivity="medium",
    time_bucketing_strategy={
        "timestamp_field_path": "event_date",
        "bucket_interval": {"unit": "WEEK", "multiple": 1},
        "timezone": "UTC",
    },
    backfill_config={"backfill_start_date_ms": 1704067200000},
    enabled=True,
)
```

See the [Assertions SDK tutorial](/docs/api/tutorials/assertions.md) for more examples.

:::info
For smart assertions with bucketing enabled, you can also configure **historical backfill** to populate the assertion's metrics history. See [Backfill Assertion History](./assertion-backfill.md) for details.
:::

## Stopping a Volume Assertion

In order to temporarily stop the evaluation of the assertion:

1. Navigate to the **Quality** tab of the Table with the assertion
2. Click **Volume** to open the Volume Assertion assertions
3. Click the "Stop" button for the assertion you wish to pause.

<p align="left">
  <img width="25%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/stop-assertion.png"/>
</p>

To resume the assertion, simply click **Start**.

<p align="left">
  <img width="25%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/start-assertion.png"/>
</p>

## Creating Volume Assertions via API

Under the hood, DataHub Cloud implements Volume Assertion Monitoring using two concepts:

- **Assertion**: The specific expectation for volume, e.g. "The table was changed int the past 7 hours"
  or "The table is changed on a schedule of every day by 8am". This is the "what".

- **Monitor**: The process responsible for evaluating the Assertion on a given evaluation schedule and using specific
  mechanisms. This is the "how".

Note that to create or delete Assertions and Monitors for a specific entity on DataHub, you'll need the
`Edit Assertions` and `Edit Monitors` privileges for it.

#### GraphQL

In order to create or update a Volume Assertion, you can use the `upsertDatasetVolumeAssertionMonitor` mutation.

##### Examples

To create a Volume Assertion Entity that verifies that the row count for a table is between 10 and 20 rows, and runs every 8 hours:

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

To create an AI Smart Freshness Assertion that runs every 8 hours:

```graphql
mutation upsertDatasetFreshnessAssertionMonitor {
  upsertDatasetFreshnessAssertionMonitor(
    input: {
      entityUrn: "<urn of entity being monitored>"
      inferWithAI: true
      type: ROW_COUNT_TOTAL
      # you can provide any value here as it will be overwritten continuously by the AI engine
      rowCountTotal: {
        operator: BETWEEN
        parameters: {
          minValue: { value: "0", type: NUMBER }
          maxValue: { value: "0", type: NUMBER }
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

The supported volume assertion types are `ROW_COUNT_TOTAL` and `ROW_COUNT_CHANGE`. Other (e.g. incrementing segment) types are not yet supported.
The supported operator types are `GREATER_THAN`, `GREATER_THAN_OR_EQUAL_TO`, `LESS_THAN`, `LESS_THAN_OR_EQUAL_TO`, and `BETWEEN` (requires minValue, maxValue).
The supported parameter types are `NUMBER`.

You can use same endpoint with assertion urn input to update an existing Volume Assertion and corresponding Monitor:

```graphql
mutation upsertDatasetVolumeAssertionMonitor {
  upsertDatasetVolumeAssertionMonitor(
    assertionUrn: "<urn of assertion created in earlier query>"
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
        cron: "0 */6 * * *"
      }
      evaluationParameters: { sourceType: INFORMATION_SCHEMA }
      mode: ACTIVE
    }
  ) {
    urn
  }
}
```

You can delete assertions along with their monitors using GraphQL mutations: `deleteAssertion` and `deleteMonitor`.

### Tips

:::info
**Authorization**

Remember to always provide a DataHub Personal Access Token when calling the GraphQL API. To do so, just add the 'Authorization' header as follows:

```
Authorization: Bearer <personal-access-token>
```

**Exploring GraphQL API**

Also, remember that you can play with an interactive version of the DataHub Cloud GraphQL API at `https://your-account-id.acryl.io/api/graphiql`
:::
