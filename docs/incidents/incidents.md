---
title: Incidents
description: This page provides an overview of working with the DataHub Incidents API.
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Incidents

<FeatureAvailability/>

## Introduction

**Incidents** are a concept used to flag particular Data Assets as being in an unhealthy state. Each incident has an independent lifecycle and details including a state (active, resolved), a title, a description, & more.

A couple scenarios in which incidents can be useful are

1. **Communicating Assets with Ongoing Issues**: You can mark a known-bad data asset as under an ongoing incident so consumers and stakeholders can be informed about the health status of a data asset via the DataHub UI. Moreover, they can follow the incident as it progresses toward resolution.
2. **Pipeline Circuit Breaking (advanced):** You can use Incidents as a basis for orchestrating and blocking data pipelines that have inputs with active issues to avoid propagating bad data downstream.

In the next section, we'll walk through how to

1. Create a new incident
2. Fetch all incidents for a data asset
3. Resolve an incident

for **Datasets**, **Dashboards**, **Charts**, **Data Pipelines** (Data Flows), and **Data Tasks** (Data Jobs) using the DataHub UI or [GraphQL API](docs/api/graphql/overview.md).

Let's get started!

## Creating an Incident

To create an incident, simply navigate to the profile page for the asset of interest, click
the 3-dot menu icon on the right side of the header, and click **Raise Incident**.

Choose an existing type, or define your own, and then author a title and description of the issue. Finally,
click `Add` to create the new issue. This will mark the asset with a health status badge indicating that it
is possibly unfit for use due to an ongoing issue.

## Resolving an Incident

To resolve an incident, simply navigate to the profile page for the asset of interest, click
the **Incidents** tab, and then click the **Resolve** button for the incident of interest.
This will resolve the incident from the list of active incidents for the asset, removing it from the
asset's health status.

## Finding Assets with Active Incidents

To view all assets with active incidents, simply apply the `Has Active Incidents` filter on the search results page of DataHub.
To view all assets first, click **Explore all** on the DataHub homepage.

## Creating an Incident via API

Oftentimes it is desirable to raise and resolve incidents for particular data assets in automated fashion using the DataHub API, e.g. as part of an
orchestration pipeline.

To create (i.e. raise) a new incident for a data asset, simply create a GraphQL request using the `raiseIncident` mutation.

```
type Mutation {
    """
    Raise a new incident for a data asset
    """
    raiseIncident(input: RaiseIncidentInput!): String! # Returns new Incident URN.
}

input RaiseIncidentInput {
  """
  The type of incident, e.g. OPERATIONAL
  """
  type: IncidentType!

  """
  A custom type of incident. Present only if type is 'CUSTOM'
  """
  customType: String

  """
  An optional title associated with the incident
  """
  title: String

  """
  An optional description associated with the incident
  """
  description: String

  """
  The resource that the incident is associated with. See the supported entity
  types below. Must be present if resourceUrns is not defined.
  """
  resourceUrn: String

  """
  The resources that the incident is associated with, for a multi-resource
  incident. Must be present and not empty if resourceUrn is not defined.
  """
  resourceUrns: [String!]

  """
  The source of the incident, i.e. how it was generated
  """
  source: IncidentSourceInput
}
```

### Supported entity types

Incident support is not a single switch. It is declared independently in several
places, and they do not all cover the same set. Four of them are backend and decide
what happens to an incident raised through the API. Every entity passed in
`resourceUrn`, and every entity in the `resourceUrns` list for a multi-resource
incident, goes through all four.

1. **Can it be raised?** The `IncidentOn` relationship in `IncidentInfo.pdl` lists
   the entity types accepted as incident resources. A type not on this list is
   rejected and nothing is written.
2. **Is it summarised?** The `incidentsSummary` aspect on the entity in
   `entity-registry.yml` is what keeps an entity's active incident count current.
   Without it, the incident exists but the entity has no rolled-up state.
3. **Is the field declared?** An `extend type` block in `incident.graphql` is what
   gives the entity an `incidents` field in the schema.
4. **Is the field wired?** The entity's GraphQL type has to appear in the
   `entitiesWithIncidents` list in `GmsGraphQLEngine.java`, which is what attaches
   the resolver behind that field.

Gates 3 and 4 are reported separately because declaring the field without wiring a
resolver behind it does not fail. The query is valid and `incidents` comes back
null, which reads as an entity with no incidents rather than as an unsupported one.

An entity can clear gate 1 and fail the rest. That is a worse outcome than a clean
rejection, because the call succeeds and returns an incident URN while the entity
it names never shows the incident.

The table below is generated at docs build time from those four files, so it cannot
drift from the code the way a hand-maintained list does.

{{ inline /docs/generated/incidents/entity-support.md.snippet }}

Any type absent from the table, including `mlModelGroup`, `mlModelDeployment` and
`dataProcessInstance`, is rejected at gate 1 and nothing is written.

A rejected destination fails the whole mutation, including any other entity sent
in the same call. The wording has changed between releases, so search for the
entity type rather than for the exact sentence. On v1.5.0.6 the rejection reads:

```
java.lang.RuntimeException: Invalid format for aspect: incident
 Cause: ERROR :: /entities/0 :: "Provided urn <urn>" is invalid:
        Entity type for urn: <urn> is not a valid destination for field path: /entities/*
```

`service` and `aiAgent` are the two types where a write can look like it worked and
then go nowhere. Both are on the `IncidentOn` list and both carry
`incidentsSummary`, so the call is accepted and returns an incident URN, but
neither has a GraphQL type to hang an `incidents` field on in the first place. The
incident itself is still readable: `Incident` implements `Entity`, so the returned
URN can be passed to the generic `entity(urn: ...)` query. What is missing is the
asset side. Nothing lists the incident from the service or agent, and nothing
renders it. Until that closes, raise the incident on a dataset the service reads or
writes and name the service in the title.

`schemaField` is a deliberate half. All four backend gates are closed, so field
level incidents can be raised, summarised and listed through the API, but there is
no Incidents tab on a field today, so the UI will not show them. Treat it as an API
feature.

Three further copies of the same list live outside the metadata model, so they are
not derived here: the `activeIncidents` badge alias and the per-type inline
fragments of `getEntityIncidents` in the frontend, and the fragments in the MCP
`list_incidents` query. Keeping the frontend two from drifting is the subject of
[#19097](https://github.com/datahub-project/datahub/pull/19097), and the wider
entity-support plan is tracked in
[#19322](https://github.com/datahub-project/datahub/issues/19322).

### Examples

First, we'll create a demo GraphQL query, then show how to represent it via CURL & Python.

Imagine we want to raise a new incident on a Dataset with URN `urn:li:dataset:(abc)` because it's failed automated quality checks. To do so, we could make the following GraphQL query:

_Request_

```
mutation raiseIncident {
  raiseIncident(input: {
    type: OPERATIONAL
    title: "Dataset Failed Quality Checks"
    description: "Dataset failed 2/6 Quality Checks for suite run id xy123mksj812pk23."
    resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"
  })
}
```

After we make this query, we will get back a unique URN for the incident.

_Response_

```
{
  "data": {
    "raiseIncident": "urn:li:incident:bfecab62-dc10-49a6-a305-78ce0cc6e5b1"
  }
}
```

Now we'll see how to issue this query using a CURL or Python.

#### CURL

To issue the above GraphQL as a CURL:

```
curl --location --request POST 'https://your-account.acryl.io/api/graphql' \
--header 'Authorization: Bearer your-access-token' \
--header 'Content-Type: application/json' \
--data-raw '{"query":"mutation raiseIncident {\n  raiseIncident(input: {\n    type: OPERATIONAL\n    title: \"Dataset Failed Quality Checks\"\n    description: \"Dataset failed 2/6 Quality Checks for suite run id xy123mksj812pk23.\"\n    resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)\"\n  })\n}","variables":{}}'
```

#### Python

To issue the above GraphQL query in Python (requests):

```
import requests

datahub_session = requests.Session()

headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer your-personal-access-token",
}

json = {
    "query": """mutation raiseIncident {\n
      raiseIncident(input: {\n
        type: OPERATIONAL\n
        resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)\"\n
      })}""",
    "variables": {},
}

response = datahub_session.post(f"https://your-account.acryl.io/api/graphql", headers=headers, json=json)
response.raise_for_status()
res_data = response.json() # Get result as JSON
```

## Retrieving Active Incidents

To fetch the the ongoing incidents for a data asset, we can use the `incidents` GraphQL field on the entity of interest.

To retrieve all incidents for a Dataset with a particular [URN](docs/what/urn.md), you can reference the 'incidents' field of the Dataset type:

```
type Dataset {
  ....
  """
  Incidents associated with the Dataset
  """
  incidents(
    """
    Optional incident state to filter by, defaults to any state.
    """
    state: IncidentState,
    """
    Optional start offset, defaults to 0.
    """
    start: Int,
    """
    Optional start offset, defaults to 20.
    """
    count: Int): EntityIncidentsResult # Returns a list of incidents.
}
```

### Examples

Now that we've raised an incident on it, imagine we want to fetch the first 10 "active" incidents for the Dataset with URN `urn:li:dataset:(abc`). To do so, we could issue the following request:

_Request_

```
query dataset {
  dataset(urn: "urn:li:dataset:(abc)") {
    incidents(state: ACTIVE, start: 0, count: 10) {
      total
      incidents {
        urn
        title
        description
        status {
          state
        }
      }
    }
  }
}
```

After we make this query, we will get back a unique URN for the incident.

_Response_

```
{
  "data": {
    "dataset": {
      "incidents": {
        "total": 1,
        "incidents": [
          {
            "urn": "urn:li:incident:bfecab62-dc10-49a6-a305-78ce0cc6e5b1",
            "title": "Dataset Failed Quality Check",
            "description": "Dataset failed 2/6 Quality Checks for suite run id xy123mksj812pk23.",
            "status": {
              "state": "ACTIVE"
            }
          }
        ]
      }
    }
  }
}
```

Now we'll see how to issue this query using a CURL or Python.

#### CURL

To issue the above GraphQL as a CURL:

```
curl --location --request POST 'https://your-account.acryl.io/api/graphql' \
--header 'Authorization: Bearer your-access-token' \
--header 'Content-Type: application/json' \
--data-raw '{"query":"query dataset {\n dataset(urn: "urn:li:dataset:(abc)") {\n incidents(state: ACTIVE, start: 0, count: 10) {\n total\n incidents {\n urn\n title\n description\n status {\n state\n }\n }\n }\n }\n}","variables":{}}'Python
```

To issue the above GraphQL query in Python (requests):

```
import requests

datahub_session = requests.Session()

headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer your-personal-access-token",
}

json = {
    "query": """query dataset {\n
                  dataset(urn: "urn:li:dataset:(abc)") {\n
                    incidents(state: ACTIVE, start: 0, count: 10) {\n
                      total\n
                      incidents {\n
                        urn\n
                        title\n
                        description\n
                        status {\n
                          state\n
                        }\n
                      }\n
                    }\n
                  }\n
                }""",
    "variables": {},
}

response = datahub_session.post(f"https://your-account.acryl.io/api/graphql", headers=headers, json=json)
response.raise_for_status()
res_data = response.json() # Get result as JSON
```

## Resolving an Incident via API

To resolve an incident for a data asset, simply create a GraphQL request using the `updateIncidentStatus` mutation. To mark an incident as resolved, simply update its state to `RESOLVED`.

```
type Mutation {
    """
    Update an existing incident for a resource (asset)
    """
    updateIncidentStatus(
      """
      The urn for an existing incident
      """
      urn: String!

      """
      Input required to update the state of an existing incident
      """
      input: UpdateIncidentStatusInput!): String
}

"""
Input required to update status of an existing incident
"""
input UpdateIncidentStatusInput {
  """
  The new state of the incident
  """
  state: IncidentState!

  """
  An optional message associated with the new state
  """
  message: String
}
```

### Examples

Imagine that we've fixed our Dataset with urn `urn:li:dataset:(abc)` so that it's passing validation. Now we want to mark the Dataset as healthy, so stakeholders and downstream consumers know it's ready to use.

To do so, we need the URN of the Incident that we raised previously.

_Request_

```
mutation updateIncidentStatus {
  updateIncidentStatus(urn: "urn:li:incident:bfecab62-dc10-49a6-a305-78ce0cc6e5b1",
  input: {
    state: RESOLVED
    message: "Dataset is now passing validations. Verified by John Joyce on Data Platform eng."
  })
}
```

_Response_

```
{
  "data": {
    "updateIncidentStatus": "true"
  }
}
```

True is returned if the incident's was successfully marked as resolved.

#### CURL

To issue the above GraphQL as a CURL:

```
curl --location --request POST 'https://your-account.acryl.io/api/graphql' \
--header 'Authorization: Bearer your-access-token' \
--header 'Content-Type: application/json' \
--data-raw '{"query":"mutation updateIncidentStatus {\n updateIncidentStatus(urn: "urn:li:incident:bfecab62-dc10-49a6-a305-78ce0cc6e5b1", \n input: {\n state: RESOLVED\n message: "Dataset is now passing validations. Verified by John Joyce on Data Platform eng."\n })\n}","variables":{}}'Python
```

To issue the above GraphQL query in Python (requests):

```
import requests

datahub_session = requests.Session()

headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer your-personal-access-token",
}

json = {
    "query": """mutation updateIncidentStatus {\n
                  updateIncidentStatus(urn: \"urn:li:incident:bfecab62-dc10-49a6-a305-78ce0cc6e5b1\",\n
                  input: {\n
                    state: RESOLVED\n
                    message: \"Dataset is now passing validations. Verified by John Joyce on Data Platform eng.\"\n
                  })\n
                }""",
    "variables": {},
}

response = datahub_session.post(f"https://your-account.acryl.io/api/graphql", headers=headers, json=json)
response.raise_for_status()
res_data = response.json() # Get result as JSON
```

## Tips

:::info
**Authorization**

Remember to always provide a DataHub Personal Access Token when calling the GraphQL API. To do so, just add the 'Authorization' header as follows:

```
Authorization: Bearer <personal-access-token>
```

**Exploring GraphQL API**

Also, remember that you can play with an interactive version of the GraphQL API at `https://your-account-id.acryl.io/api/graphiql`
:::

## Enabling Slack Notifications (DataHub Cloud Only)

In DataHub Cloud, you can configure your to send Slack notifications to a specific channel when incidents are raised or their status is changed.

These notifications are also able to tag the immediate asset's owners, along with the owners of downstream assets consuming it.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/Screen-Shot-2022-03-22-at-6.46.41-PM.png"/>
</p>

To do so, simply follow the [Slack Integration Guide](docs/managed-datahub/slack/saas-slack-setup.md) and contact your DataHub Cloud customer success team to enable the feature!
