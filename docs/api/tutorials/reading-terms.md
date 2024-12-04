# Reading Terms On Datasets/Columns

## Why Would You Read Terms?

The Business Glossary(Term) feature in DataHub helps you use a shared vocabulary within the orgarnization, by providing a framework for defining a standardized set of data concepts and then associating them with the physical assets that exist within your data ecosystem.

For more information about terms, refer to [About DataHub Business Glossary](/docs/glossary/business-glossary.md).

### Goal Of This Guide

This guide will show you how to read terms attached to a dataset `SampleHiveDataset`.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before adding terms, you need to ensure the targeted dataset and the term are already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from a sample ingestion.

Specifically, we will assume that the term `CustomerAccount` is attached to a dataset `fct_users_created`.
To learn how to add terms to your own datasets, please refer to our documentation on [Adding Terms](/docs/api/tutorials/adding-terms.md).
:::

## Read Terms With GraphQL

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
    glossaryTerms {
      terms {
        term {
          urn
          glossaryTermInfo {
            name
            description
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
      "glossaryTerms": {
        "terms": [
          {
            "term": {
              "urn": "urn:li:glossaryTerm:CustomerAccount",
              "glossaryTermInfo": {
                "name": "CustomerAccount",
                "description": "account that represents an identified, named collection of balances and cumulative totals used to summarize customer transaction-related activity over a designated period of time"
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
--data-raw '{ "query": "{dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\") {glossaryTerms {terms {term {urn glossaryTermInfo { name description } } } } } }", "variables":{}}'
```

Expected Response:

````json
{"data":{"dataset":{"glossaryTerms":{"terms":[{"term":{"urn":"urn:li:glossaryTerm:CustomerAccount","glossaryTermInfo":{"name":"CustomerAccount","description":"account that represents an identified, named collection of balances and cumulative totals used to summarize customer transaction-related activity over a designated period of time"}}}]}}},"extensions":{}}```
````

## Read Terms With Python SDK

The following code reads terms attached to a dataset `fct_users_created`.

> Coming Soon!

We're using the `MetdataChangeProposalWrapper` to change entities in this example.
For more information about the `MetadataChangeProposal`, please refer to [MetadataChangeProposal & MetadataChangeLog Events](/docs/advanced/mcp-mcl.md)
