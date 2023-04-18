# Reading Tags On Datasets/Columns

## Why Would You Read Tags?

Tags are informal, loosely controlled labels that help in search & discovery. They can be added to datasets, dataset schemas, or containers, for an easy way to label or categorize entities – without having to associate them to a broader business glossary or vocabulary.

For more information about tags, refer to [About DataHub Tags](/docs/tags.md).

### Goal Of This Guide

This guide will show you how to read tags attached to a dataset `SampleHiveDataset`.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before reading tags, you need to ensure the targeted dataset and the tag are already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from a sample ingestion.
If you want to know how to create tags using APIs & SDKs, please refer to [Creating Tags](/docs/api/tutorials/creating-tags.md) and [Adding Tags](/docs/api/tutorials/adding-tags.md).
:::

## Read Tags With GraphQL

:::note
Please note that there are two available endpoints (`:8000`, `:9002`) to access `graphql`.
For more information about the differences between these endpoints, please refer to [DataHub Metadata Service](../../../metadata-service/README.md#graphql-api)
:::

### GraphQL Explorer

GraphQL Explorer is the fastest way to experiment with `graphql` without any dependencies.
Navigate to GraphQL Explorer (`http://localhost:9002/api/graphiql`) and run the following query.

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

### CURL

With CURL, you need to provide tokens. To generate a token, please refer to [Access Token Management](/docs/api/graphql/token-management.md).
With `accessToken`, you can run the following command.

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

## Read Tags With Python SDK

The following code reads tags attached to a dataset `SampleHiveDataset`.

> Coming Soon!

We're using the `MetdataChangeProposalWrapper` to change entities in this example.
For more information about the `MetadataChangeProposal`, please refer to [MetadataChangeProposal & MetadataChangeLog Events](/docs/advanced/mcp-mcl.md)
