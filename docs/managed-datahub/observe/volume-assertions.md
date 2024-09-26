---
description: This page provides an overview of working with DataHub Volume Assertions
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';


# Volume Assertions

<FeatureAvailability saasOnly />

> The **Volume Assertions** feature is available as part of the **Acryl Observe** module of DataHub Cloud.
> If you are interested in learning more about **Acryl Observe** or trying it out, please [visit our website](https://www.acryldata.io/observe).

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
2. A **Volume Source**

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
  once a change has been made to a table. 

- **Query**: A `COUNT(*)` query is used to retrieve the latest row count for a table, with optional SQL filters applied (depending on platform). 
  This can be less efficient to check depending on the size of the table. This approach is more portable, as it does not involve 
  system warehouse tables, it is also easily portable across Data Warehouse and Data Lake providers.

- **DataHub Dataset Profile**: The DataHub Dataset Profile aspect is used to retrieve the latest row count information for a table.
  Using this option avoids contacting your data platform, and instead uses the DataHub Dataset Profile metadata to evaluate Volume Assertions.
  Note if you have not configured an ingestion source through DataHub, then this may be the only option available.
  
Volume Assertions also have an off switch: they can be started or stopped at any time with the click of button.


## Creating a Volume Assertion

### Prerequisites

1. **Permissions**: To create or delete Volume Assertions for a specific entity on DataHub, you'll need to be granted the
   `Edit Assertions` and `Edit Monitors` privileges for the entity. This will be granted to Entity owners as part of the `Asset Owners - Metadata Policy`
   by default.

2. (Optional) **Data Platform Connection**: In order to create a Volume Assertion that queries the source data platform directly (instead of DataHub metadata), you'll need to have an **Ingestion Source** configured to your
   Data Platform: Snowflake, BigQuery, or Redshift under the **Integrations** tab.

Once these are in place, you're ready to create your Volume Assertions!

### Steps

1. Navigate to the Table that to monitor for volume
2. Click the **Validations** tab

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


## Stopping a Volume Assertion

In order to temporarily stop the evaluation of the assertion:

1. Navigate to the **Validations** tab of the Table with the assertion
2. Click **Volume** to open the Volume Assertion assertions
3. Click the "Stop" button for the assertion you wish to pause.

<p align="left">
  <img width="25%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/stop-assertion.png"/>
</p>

To resume the assertion, simply click **Start**.

<p align="left">
  <img width="25%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/start-assertion.png"/>
</p>


## Smart Assertions ⚡

As part of the **Acryl Observe** module, DataHub Cloud also provides **Smart Assertions** out of the box. These are
dynamic, AI-powered Volume Assertions that you can use to monitor the volume of important warehouse Tables, without
requiring any manual setup.

If DataHub Cloud is able to detect a pattern in the volume of a Snowflake, Redshift, BigQuery, or Databricks Table, you'll find
a recommended Smart Assertion under the `Validations` tab on the Table profile page:

<p align="left">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/freshness/smart-assertion.png"/>
</p>

In order to enable it, simply click **Turn On**. From this point forward, the Smart Assertion will check for changes on a cadence
based on the Table history.

Don't need it anymore? Smart Assertions can just as easily be turned off by clicking the three-dot "more" button and then **Stop**.


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
        cron: "0 */6 * * *"
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

You can delete assertions along with their monitors using GraphQL mutations: `deleteAssertion` and `deleteMonitor`.

### Tips

:::info
**Authorization**

Remember to always provide a DataHub Personal Access Token when calling the GraphQL API. To do so, just add the 'Authorization' header as follows:

```
Authorization: Bearer <personal-access-token>
```

**Exploring GraphQL API**

Also, remember that you can play with an interactive version of the Acryl GraphQL API at `https://your-account-id.acryl.io/api/graphiql`
:::
