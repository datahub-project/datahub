import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Tags

## Why Would You Use Tags on Datasets?

Tags are informal, loosely controlled labels that help in search & discovery. They can be added to datasets, dataset schemas, or containers, for an easy way to label or categorize entities – without having to associate them to a broader business glossary or vocabulary.
For more information about tags, refer to [About DataHub Tags](/docs/tags.md).

### Goal Of This Guide

This guide will show you how to

- Create: create a tag.
- Read : read tags attached to a dataset.
- Add: add a tag to a column of a dataset or a dataset itself.
- Remove: remove a tag from a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed information, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before modifying tags, you need to ensure the target dataset is already present in your DataHub instance.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from sample ingestion.
:::

For more information on how to set up for GraphQL, please refer to [How To Set Up GraphQL](/docs/api/graphql/how-to-set-up-graphql.md).

## Create Tags

The following code creates a tag `Deprecated`.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation createTag {
    createTag(input:
    {
      name: "Deprecated",
      id: "deprecated",
      description: "Having this tag means this column or table is deprecated."
    })
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "createTag": "urn:li:tag:deprecated"
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
--data-raw '{ "query": "mutation createTag { createTag(input: { name: \"Deprecated\", id: \"deprecated\",description: \"Having this tag means this column or table is deprecated.\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "createTag": "urn:li:tag:deprecated" }, "extensions": {} }
```

</TabItem>

<TabItem value="java" label="Java">

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/TagCreate.java show_path_as_comment }}
```

</TabItem>

<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/create_tag.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome of Creating Tags

You can now see the new tag `Deprecated` has been created.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/tag-created.png"/>
</p>


We can also verify this operation by programmatically searching `Deprecated` tag after running this code using the `datahub` cli.

```shell
datahub get --urn "urn:li:tag:deprecated" --aspect tagProperties

{
  "tagProperties": {
    "description": "Having this tag means this column or table is deprecated.",
    "name": "Deprecated"
  }
}
```

## Read Tags

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)") {
    tags {
      tags {
        tag {
          name
          urn
        	properties {
        	  description
        	  colorHex
        	}
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
      "tags": {
        "tags": [
          {
            "tag": {
              "name": "Legacy",
              "urn": "urn:li:tag:Legacy",
              "properties": {
                "description": "Indicates the dataset is no longer supported",
                "colorHex": null,
                "name": "Legacy"
              }
            }
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
--data-raw '{ "query": "{dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\") {tags {tags {tag {name urn properties { description colorHex } } } } } }", "variables":{}}'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "tags": {
        "tags": [
          {
            "tag": {
              "name": "Legacy",
              "urn": "urn:li:tag:Legacy",
              "properties": {
                "description": "Indicates the dataset is no longer supported",
                "colorHex": null
              }
            }
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
{{ inline /metadata-ingestion/examples/library/dataset_query_tags.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Add Tags

### Add Tags to a dataset

The following code shows you how can add tags to a dataset.
In the following code, we add a tag `Deprecated` to a dataset named `fct_users_created`.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation addTags {
    addTags(
      input: {
        tagUrns: ["urn:li:tag:deprecated"],
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
      }
    )
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "addTags": true
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
--data-raw '{ "query": "mutation addTags { addTags(input: { tagUrns: [\"urn:li:tag:deprecated\"], resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "addTags": true }, "extensions": {} }
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_tag.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Add Tags to a Column of a dataset

In the example below `subResource` is `fieldPath` in the schema.

<Tabs>
<TabItem value="graphql" label="GraphQL">

```json
mutation addTags {
    addTags(
      input: {
        tagUrns: ["urn:li:tag:deprecated"],
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        subResourceType:DATASET_FIELD,
        subResource:"user_name"})
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTags { addTags(input: { tagUrns: [\"urn:li:tag:deprecated\"], resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\", subResourceType: DATASET_FIELD, subResource: \"user_name\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "addTags": true }, "extensions": {} }
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_column_tag.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome of Adding Tags

You can now see `Deprecated` tag has been added to `user_name` column.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/tag-added.png"/>
</p>


We can also verify this operation programmatically by checking the `globalTags` aspect using the `datahub` cli.

```shell
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)" --aspect globalTags

```

## Remove Tags

The following code remove a tag from a dataset.
After running this code, `Deprecated` tag will be removed from a `user_name` column.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation removeTag {
    removeTag(
      input: {
        tagUrn: "urn:li:tag:deprecated",
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        subResourceType:DATASET_FIELD,
        subResource:"user_name"})
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation removeTag { removeTag(input: { tagUrn: \"urn:li:tag:deprecated\", resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\" }) }", "variables":{}}'
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_remove_tag_execute_graphql.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome of Removing Tags

You can now see `Deprecated` tag has been removed to `user_name` column.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/tag-removed.png"/>
</p>


We can also verify this operation programmatically by checking the `gloablTags` aspect using the `datahub` cli.

```shell
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)" --aspect globalTags

{
  "globalTags": {
    "tags": []
  }
}
```
