import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Deprecation

## Why Would You Deprecate Datasets?

The Deprecation feature on DataHub indicates the status of an entity. For datasets, keeping the deprecation status up-to-date is important to inform users and downstream systems of changes to the dataset's availability or reliability. By updating the status, you can communicate changes proactively, prevent issues and ensure users are always using highly trusted data assets.

### Goal Of This Guide

This guide will show you how to read or update deprecation status of a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before updating deprecation, you need to ensure the targeted dataset is already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from a sample ingestion.
:::

## Read Deprecation

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)") {
    deprecation {
      deprecated
      decommissionTime
    }
  }
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "dataset": {
      "deprecation": {
        "deprecated": false,
        "decommissionTime": null
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
--data-raw '{ "query": "{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\") { deprecation { deprecated decommissionTime } } }", "variables":{} }'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "deprecation": { "deprecated": false, "decommissionTime": null }
    }
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_query_deprecation.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Update Deprecation

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation updateDeprecation {
    updateDeprecation(input: { urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)", deprecated: true })
}
```

Also note that you can update deprecation status of multiple entities or subresource using `batchUpdateDeprecation`.

```json
mutation batchUpdateDeprecation {
    batchUpdateDeprecation(
      input: {
        deprecated: true,
        resources: [
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"} ,
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"} ,]
      }
    )
}

```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "updateDeprecation": true
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
--data-raw '{ "query": "mutation updateDeprecation { updateDeprecation(input: { deprecated: true, urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "removeTag": true }, "extensions": {} }
```

</TabItem>

<TabItem value="python" label="Python">

</TabItem>
</Tabs>

### Expected Outcomes of Updating Deprecation

You can now see the dataset `fct_users_created` has been marked as `Deprecated.`


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/deprecation-updated.png"/>
</p>

