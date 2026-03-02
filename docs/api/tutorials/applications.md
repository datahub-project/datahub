import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Applications

## Why Would You Use Applications?

Applications are groupings of assets based on a particular purpose, similar to domains and data products.
For more information on what an Application is, and how it differs from other concepts, refer to [About DataHub Applications](/docs/features/feature-guides/applications.md).

### Goal Of This Guide

This guide will show you how to

- Create an application.
- Read the application attached to a dataset.
- Add a dataset to an application
- Remove the application from a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Application

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation createApplication {
  createApplication(
    input: {
      properties: {
        name: "My New Application"
        description: "An optional description"
      }
    }
  )
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "createApplication": "<application_urn>"
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/application_create_full.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Creating Applications

You can now see the applications under `Applications` sidebar section.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/ManageApplicationsScreen.png"/>
</p>

## Read Applications

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)") {
    application {
      application {
        urn
        properties {
          name
          description
        }
      }
    }
  }
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "dataset": {
      "application": {
        "application": {
          "urn": "urn:li:application:71b3bf7b-2e3f-4686-bfe1-93172c8c4e10",
          "properties": {
            "name": "Cancellation Processing"
          }
        }
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
</Tabs>

## Add Application

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation batchSetApplication {
  batchSetApplication(
    input: {
      resourceUrns: [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,banking.public.customer,PROD)"
      ]
      applicationUrn: "urn:li:application:new-customer-signup"
    }
  )
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "batchSetApplication": true
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/application_add.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Adding Application

You can now see the application has been added to the dataset.

## Remove Applications

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation batchSetApplication {
  batchSetApplication(
    input: {
      resourceUrns: [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,banking.public.customer,PROD)"
      ],
      applicationUrn: null
    }
  )
}
```

Expected Response:

```python
{
  "data": {
    "batchSetApplication": true
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/application_remove.py show_path_as_comment }}
```

</TabItem>
</Tabs>
