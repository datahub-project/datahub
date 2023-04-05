# Adding a Dataset to a Domain

## Why Would You Add Domains?

Domains are curated, top-level folders or categories where related assets can be explicitly grouped. Management of Domains can be centralized, or distributed out to Domain owners Currently, an asset can belong to only one Domain at a time.
For more information about domains, refer to [About DataHub Domains](/docs/domains.md).

### Goal Of This Guide

This guide will show you how to add a dataset named `fct_users_created` to a domain named `Marketing`.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before adding domains, you need to ensure the targeted dataset and the domain are already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
For more information on how to create domains, please refert to [Create Domain](/docs/api/tutorials/creating-domain.md)
:::

## Add Domains With GraphQL

:::note
Please note that there are two available endpoints (`:8000`, `:9002`) to access `graphql`.
For more information about the differences between these endpoints, please refer to [DataHub Metadata Service](../../../metadata-service/README.md#graphql-api)
:::

### GraphQL Explorer

GraphQL Explorer is the fastest way to experiment with GraphQL without any dependencies.
Navigate to GraphQL Explorer (`http://localhost:9002/api/graphiql`) and run the following query.

```json
mutation setDomain {
    setDomain(domainUrn: "urn:li:domain:marketing", entityUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)")
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "setDomain": true
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
--data-raw '{ "query": "mutation setDomain { setDomain(entityUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)", domainUrn: "urn:li:domain:marketing")) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "setDomain": true }, "extensions": {} }
```

## Add Domains With Python SDK

The following code adds a dataset `fct_users_created` to a domain named `Marketing`.

> Coming Soon!

We're using the `MetdataChangeProposalWrapper` to change entities in this example.
For more information about the `MetadataChangeProposal`, please refer to [MetadataChangeProposal & MetadataChangeLog Events](/docs/advanced/mcp-mcl.md)

## Expected Outcomes

You can now see `CustomerAccount` domain has been added to `user_name` column.

![tag-added](../../imgs/apis/tutorials/tag-added.png)
