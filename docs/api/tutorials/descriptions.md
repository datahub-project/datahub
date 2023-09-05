import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Description

## Why Would You Use Description on Dataset?

Adding a description and related link to a dataset can provide important information about the data, such as its source, collection methods, and potential uses. This can help others understand the context of the data and how it may be relevant to their own work or research. Including a related link can also provide access to additional resources or related datasets, further enriching the information available to users.

### Goal Of This Guide

This guide will show you how to

- Read dataset description: read a description of a dataset.
- Read column description: read a description of columns of a dataset`.
- Add dataset description: add a description and a link to dataset.
- Add column description: add a description to a column of a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before adding a description, you need to ensure the targeted dataset is already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from sample ingestion.
:::

In this example, we will add a description to `user_name `column of a dataset `fct_users_deleted`.

## Read Description on Dataset

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)") {
    properties {
      description
    }
  }
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "dataset": {
      "properties": {
        "description": "table containing all the users deleted on a single day"
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "query { dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\") { properties { description } } }", "variables":{}}'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "properties": {
        "description": "table containing all the users deleted on a single day"
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_query_description.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Read Description on Columns

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)") {
    schemaMetadata {
      fields {
        fieldPath
        description
      }
    }
  }
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "dataset": {
      "schemaMetadata": {
        "fields": [
          {
            "fieldPath": "user_name",
            "description": "Name of the user who was deleted"
          },
          ...
          {
            "fieldPath": "deletion_reason",
            "description": "Why the user chose to deactivate"
          }
        ]
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "query { dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\") { schemaMetadata { fields { fieldPath description } } } }", "variables":{}}'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "schemaMetadata": {
        "fields": [
          {
            "fieldPath": "user_name",
            "description": "Name of the user who was deleted"
          },
          {
            "fieldPath": "timestamp",
            "description": "Timestamp user was deleted at"
          },
          { "fieldPath": "user_id", "description": "Id of the user deleted" },
          {
            "fieldPath": "browser_id",
            "description": "Cookie attached to identify the browser"
          },
          {
            "fieldPath": "session_id",
            "description": "Cookie attached to identify the session"
          },
          {
            "fieldPath": "deletion_reason",
            "description": "Why the user chose to deactivate"
          }
        ]
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_query_description_on_columns.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Add Description on Dataset

<Tabs>
<TabItem value="graphQL" label="GraphQL">

```graphql
mutation updateDataset {
  updateDataset(
    urn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
    input: {
      editableProperties: {
          description: "## The Real Estate Sales Dataset\nThis is a really important Dataset that contains all the relevant information about sales that have happened organized by address.\n"
      }
      institutionalMemory: {
        elements: {
          author: "urn:li:corpuser:jdoe"
        	url: "https://wikipedia.com/real_estate"
        	description: "This is the definition of what real estate means"
        }
      }
    }
  ) {
    urn
  }
}
```

Expected Response:

```json
{
  "data": {
    "updateDataset": {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
    }
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="curl" label="Curl" default>

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{
  "query": "mutation updateDataset { updateDataset( urn:\"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\", input: { editableProperties: { description: \"## The Real Estate Sales Dataset\nThis is a really important Dataset that contains all the relevant information about sales that have happened organized by address.\n\" } institutionalMemory: { elements: { author: \"urn:li:corpuser:jdoe\", url: \"https://wikipedia.com/real_estate\", description: \"This is the definition of what real estate means\" } } } ) { urn } }",
  "variables": {}
}'
```

Expected Response:

```json
{
  "data": {
    "updateDataset": {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_documentation.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Adding Description on Dataset

You can now see the description is added to `fct_users_deleted`.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-description-added.png"/>
</p>


## Add Description on Column

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation updateDescription {
  updateDescription(
    input: {
      description: "Name of the user who was deleted. This description is updated via GrpahQL.",
      resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)",
      subResource: "user_name",
      subResourceType:DATASET_FIELD
    }
  )
}
```

Note that you can use general markdown in `description`. For example, you can do the following.

```json
mutation updateDescription {
  updateDescription(
    input: {
      description: """
      ### User Name
      The `user_name` column is a primary key column that contains the name of the user who was deleted.
      """,
      resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)",
      subResource: "user_name",
      subResourceType:DATASET_FIELD
    }
  )
}
```

`updateDescription` currently only supports Dataset Schema Fields, Containers.
For more information about the `updateDescription` mutation, please refer to [updateLineage](https://datahubproject.io/docs/graphql/mutations/#updateDescription).

If you see the following response, the operation was successful:

```json
{
  "data": {
    "updateDescription": true
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation updateDescription { updateDescription ( input: { description: \"Name of the user who was deleted. This description is updated via GrpahQL.\", resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\", subResource: \"user_name\", subResourceType:DATASET_FIELD }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "updateDescription": true }, "extensions": {} }
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_column_documentation.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Adding Description on Column

You can now see column description is added to `user_name` column of `fct_users_deleted`.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/column-description-added.png"/>
</p>

