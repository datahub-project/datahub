import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Modifying Domains On Dataset

## Why Would You Use Domains?

Domains are curated, top-level folders or categories where related assets can be explicitly grouped. Management of Domains can be centralized, or distributed out to Domain owners Currently, an asset can belong to only one Domain at a time.
For more information about domains, refer to [About DataHub Domains](/docs/domains.md).

### Goal Of This Guide

This guide will show you how to 
* create a domain named `Marketing`
* read domains attached to a dataset `fct_users_created`.
* add a dataset named `fct_users_created` to a domain named `Marketing`.
* remove the domain `Marketing` from the `fct_users_created` dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Domain 


<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation createDomain {
  createDomain(input: { name: "Marketing", description: "Entities related to the marketing department" })
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "createDomain": "<domain_urn>"
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
--data-raw '{ "query": "mutation createDomain { createDomain(input: { name: \"Marketing\", description: \"Entities related to the marketing department.\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "createDomain": "<domain_urn>" }, "extensions": {} }
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/create_domain.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Creating Domain

You can now see `Marketing` domain has been created under `Govern > Domains`.

![domain-created](../../imgs/apis/tutorials/domain-created.png)


## Read Domains

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

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

</TabItem>
<TabItem value="curl" label="Curl">

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

</TabItem>
<TabItem value="python" label="Python">

> Coming Soon!

</TabItem>
</Tabs>


## Add Domains 

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

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

</TabItem>
<TabItem value="curl" label="Curl">

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

</TabItem>
<TabItem value="python" label="Python">

> Coming Soon!

</TabItem>
</Tabs>

With GraphQL

:::note
Please note that there are two available endpoints (`:8000`, `:9002`) to access `graphql`.
For more information about the differences between these endpoints, please refer to [DataHub Metadata Service](../../../metadata-service/README.md#graphql-api)
:::

### Expected Outcomes of Adding Domain 

You can now see `CustomerAccount` domain has been added to `user_name` column.

![tag-added](../../imgs/apis/tutorials/tag-added.png)


## Remove Domains 


<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation unsetDomain {
    unsetDomain(
      entityUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
    )
}
```

Expected Response:

```python
{
  "data": {
    "removeDomain": true
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
--data-raw '{ "query": "mutation unsetDomain { unsetDomain(entityUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\") }", "variables":{}}'
```

</TabItem>
<TabItem value="python" label="Python">

> Coming Soon! 

</TabItem>
</Tabs>


### Expected Outcomes of Removing Domain

You can now see a domain `Marketing` has been removed from the `fct_users_created` dataset.

![domain-removed](../../imgs/apis/tutorials/domain-removed.png)
