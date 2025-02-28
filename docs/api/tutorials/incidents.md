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
          resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,public.prod.purchases,PROD)",
          type: OPERATIONAL,
          title: "Data is Delayed",
          description: "Data is delayed on May 15, 2024 because of downtime in the Spark Cluster.",
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
    dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,public.prod.purchases,PROD)") {
        incidents(
            state: ACTIVE, start: 0, count: 20
        ) {
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
            state: RESOLVED,
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