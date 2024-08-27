import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Domains

## Why Would You Use Domains?

Domains are curated, top-level folders or categories where related assets can be explicitly grouped. Management of Domains can be centralized, or distributed out to Domain owners Currently, an asset can belong to only one Domain at a time.
For more information about domains, refer to [About DataHub Domains](/docs/domains.md).

### Goal Of This Guide

This guide will show you how to

- Create a domain.
- Read domains attached to a dataset.
- Add a dataset to a domain
- Remove the domain from a dataset.

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


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/domain-created.png"/>
</p>

### Creating a Nested Domain

You can also create a nested domain, or a domain within another domain.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation createDomain {
  createDomain(input: { name: "Verticals", description: "An optional description", parentDomain: "urn:li:domain:marketing" })
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation createDomain { createDomain(input: { name: \"Verticals\", description: \"Entities related to the verticals sub-domain.\", parentDomain: \"urn:li:domain:marketing\" }) }", "variables":{}}'
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/create_nested_domain.py show_path_as_comment }}
```

</TabItem>
</Tabs>

This query will create a new domain, "Verticals", under the "Marketing" domain.


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

```python
{{ inline /metadata-ingestion/examples/library/dataset_query_domain.py show_path_as_comment }}
```

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

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_domain_execute_graphql.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Adding Domain

You can now see `Marketing` domain has been added to the dataset.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/domain-added.png"/>
</p>


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

```python
{{ inline /metadata-ingestion/examples/library/dataset_remove_domain_execute_graphql.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Removing Domain

You can now see a domain `Marketing` has been removed from the `fct_users_created` dataset.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/domain-removed.png"/>
</p>

