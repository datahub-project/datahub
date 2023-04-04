# Reading Domains On Datasets/Columns

## Why Would You Read Domains?

Domains are curated, top-level folders or categories where related assets can be explicitly grouped. Management of Domains can be centralized, or distributed out to Domain owners Currently, an asset can belong to only one Domain at a time.
For more information about domains, refer to [About DataHub Domains](/docs/domains.md).

### Goal Of This Guide

This guide will show you how to read domains attached to a dataset `fct_users_created`.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before reading domains, you need to ensure the targeted dataset and the domain are already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from a sample ingestion.

Specifically, we will assume that the domain `Marketing` is attached to the dataset `fct_users_created`.
To learn how to add datasets to a domain, please refer to our documentation on [Adding Domain](/docs/api/tutorials/adding-domain.md).
:::

## Read Domains With GraphQL

:::note
Please note that there are two available endpoints (`:8000`, `:9002`) to access GraphQL.
For more information about the differences between these endpoints, please refer to [DataHub Metadata Service](../../../metadata-service/README.md#graphql-api)
:::

### GraphQL Explorer

GraphQL Explorer is the fastest way to experiment with GraphQL without any dependencies.
Navigate to GraphQL Explorer (`http://localhost:9002/api/graphiql`) and run the following query.

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)") {
    domain {
      associatedUrn
      domain {
        urn
        properties {
          name
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
      "domain": {
        "associatedUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        "domain": {
          "urn": "urn:li:domain:71b3bf7b-2e3f-4686-bfe1-93172c8c4e10",
          "properties": {
            "name": "Marketing"
          }
        }
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
--data-raw '{ "query": "{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\") { domain { associatedUrn domain { urn properties { name } } } } }", "variables":{}}'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "domain": {
        "associatedUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        "domain": {
          "urn": "urn:li:domain:71b3bf7b-2e3f-4686-bfe1-93172c8c4e10",
          "properties": { "name": "Marketing" }
        }
      }
    }
  },
  "extensions": {}
}
```

## Read Domains With Python SDK

The following code reads domains attached to a dataset `fct_users_created`.

> Coming Soon!

We're using the `MetdataChangeProposalWrapper` to change entities in this example.
For more information about the `MetadataChangeProposal`, please refer to [MetadataChangeProposal & MetadataChangeLog Events](/docs/advanced/mcp-mcl.md)
