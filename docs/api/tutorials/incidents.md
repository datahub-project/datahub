---
title: Incidents API Tutorial
description: "Step-by-step tutorial for raising, resolving, and querying DataHub Incidents on datasets and dashboards via the API."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Incidents

## Why Would You Use Incidents APIs?

The Incidents APIs allow you to raise, retrieve, update and resolve data incidents via API. This is
useful for raising or resolving data incidents programmatically, for example from Airflow, Prefect, or Dagster DAGs.
Incidents are also useful for conditional Circuit Breaking in these pipelines.

### Goal Of This Guide

This guide will show you how to raise, retrieve, update and resolve data incidents via API.

## Prerequisites

The actor making API calls must have the `Edit Incidents` privileges for the Tables at hand.

## Raise Incident

You can raise a new Data Incident for an existing asset using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
mutation raiseIncident {
  raiseIncident(
    input: {
      resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,public.prod.purchases,PROD)"
      type: OPERATIONAL
      title: "Data is Delayed"
      description: "Data is delayed on May 15, 2024 because of downtime in the Spark Cluster."
      priority: HIGH
    }
  )
}
```

Where `resourceUrn` is the unique identifier for the data asset (dataset, dashboard, chart, data job, or data flow) you want to raise the incident on.

Where supported Incident Types include

- `OPERATIONAL`
- `FRESHNESS`
- `VOLUME`
- `COLUMN`
- `SQL`
- `DATA_SCHEMA`
- `CUSTOM`

When using `CUSTOM`, you must also provide `customType`. It is a free-text label naming
your incident category, and it is required: omitting it (or passing a blank string) fails with
`Failed to raise incident: customType is required when type is CUSTOM`.

```graphql
mutation raiseCustomIncident {
  raiseIncident(
    input: {
      resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,public.prod.purchases,PROD)"
      type: CUSTOM
      customType: "ML_LEAKAGE"
      title: "Feature built from post-decision data"
      description: "days_since_last_payment reads payment events recorded after loan origination."
    }
  )
}
```

### Setting a priority

`priority` is optional. Over GraphQL it accepts one of four enum names:

| `priority` | Meaning |
| ---------- | ------- |
| `CRITICAL` | P0      |
| `HIGH`     | P1      |
| `MEDIUM`   | P2      |
| `LOW`      | P3      |

Pass the enum name unquoted, as in the example above. Passing an integer fails with an
error like `Invalid input for enum 'IncidentPriority'. No value found for name '2'`.

:::note Integers vs. enum names
The stored `incidentInfo` aspect models priority as an **integer**, where a _lower_ number is
_more_ severe: `CRITICAL = 0`, `HIGH = 1`, `MEDIUM = 2`, `LOW = 3`. Those integers apply only
when you write the aspect directly through OpenAPI or RestLI. The GraphQL API in this guide
accepts and returns the enum names above and does the conversion for you — it will not accept
the integers, and the two are ordered in opposite directions, so don't mix them up.
:::

If you see the following response, a unique identifier for the new incident will be returned.

```json
{
  "data": {
    "raiseIncident": "urn:li:incident:new-incident-id"
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

```
Python SDK support coming soon!
```

</TabItem>

</Tabs>

## Get Incidents For Data Asset

You can use retrieve the incidents and their statuses for a given Data Asset using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
query getAssetIncidents {
  dataset(
    urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,public.prod.purchases,PROD)"
  ) {
    incidents(state: ACTIVE, start: 0, count: 20) {
      start
      count
      total
      incidents {
        urn
        incidentType
        title
        description
        status {
          state
          lastUpdated {
            time
            actor
          }
        }
      }
    }
  }
}
```

Where you can filter for active incidents by passing the `ACTIVE` state and resolved incidents by passing the `RESOLVED` state.
This will return all relevant incidents for the dataset.

</TabItem>

<TabItem value="python" label="Python">

```
Python SDK support coming soon!
```

</TabItem>
</Tabs>

## Resolve Incidents

You can update the status of an incident using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
mutation updateIncidentStatus {
  updateIncidentStatus(
    input: {
      state: RESOLVED
      message: "The delayed data issue was resolved at 4:55pm on May 15."
    }
  )
}
```

You can also reopen an incident by updating the state from `RESOLVED` to `ACTIVE`.

If you see the following response, the operation was successful:

```json
{
  "data": {
    "updateIncidentStatus": true
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

```
Python SDK support coming soon!
```

</TabItem>
</Tabs>
