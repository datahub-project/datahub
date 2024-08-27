import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Ownership

## Why Would You Use Users and Groups?

Users and groups are essential for managing ownership of data.
By creating or updating user accounts and assigning them to appropriate groups, administrators can ensure that the right people can access the data they need to do their jobs.
This helps to avoid confusion or conflicts over who is responsible for specific datasets and can improve the overall effectiveness.

### Goal Of This Guide

This guide will show you how to

- Create: create or update users and groups.
- Read: read owners attached to a dataset.
- Add: add user group as an owner to a dataset.
- Remove: remove the owner from a dataset.

## Pre-requisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed information, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
In this guide, ingesting sample data is optional.
:::

## Upsert Users

<Tabs>
<TabItem value="cli" label="CLI">

Save this `user.yaml` as a local file.

```yaml
- id: bar@acryl.io
  first_name: The
  last_name: Bar
  email: bar@acryl.io
  slack: "@the_bar_raiser"
  description: "I like raising the bar higher"
  groups:
    - foogroup@acryl.io
- id: datahub
  slack: "@datahubproject"
  phone: "1-800-GOT-META"
  description: "The DataHub Project"
  picture_link: "https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/datahub-logo-color-stable.svg"
```

Execute the following CLI command to ingest user data.
Since the user datahub already exists in the sample data, any updates made to the user information will overwrite the existing data.

```
datahub user upsert -f user.yaml
```

If you see the following logs, the operation was successful:

```shell
Update succeeded for urn urn:li:corpuser:bar@acryl.io.
Update succeeded for urn urn:li:corpuser:datahub.
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/upsert_user.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Upserting User

You can see the user `The bar` has been created and the user `Datahub` has been updated under `Settings > Access > Users & Groups`

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/user-upserted.png"/>
</p>


## Upsert Group

<Tabs>
<TabItem value="cli" label="CLI">

Save this `group.yaml` as a local file. Note that the group includes a list of users who are owners and members.
Within these lists, you can refer to the users by their ids or their urns, and can additionally specify their metadata inline within the group description itself. See the example below to understand how this works and feel free to make modifications to this file locally to see the effects of your changes in your local DataHub instance.

```yaml
id: foogroup@acryl.io
display_name: Foo Group
owners:
  - datahub
members:
  - bar@acryl.io # refer to a user either by id or by urn
  - id: joe@acryl.io # inline specification of user
    slack: "@joe_shmoe"
    display_name: "Joe's Hub"
```

Execute the following CLI command to ingest this group's information.

```
datahub group upsert -f group.yaml
```

If you see the following logs, the operation was successful:

```shell
Update succeeded for group urn:li:corpGroup:foogroup@acryl.io.
```

</TabItem>

<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/upsert_group.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Upserting Group

You can see the group `Foo Group` has been created under `Settings > Access > Users & Groups`

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/group-upserted.png"/>
</p>


## Read Owners

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)") {
    ownership {
      owners {
        owner {
          ... on CorpUser {
            urn
            type
          }
          ... on CorpGroup {
            urn
            type
          }
        }
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
      "ownership": {
        "owners": [
          {
            "owner": {
              "urn": "urn:li:corpuser:jdoe",
              "type": "CORP_USER"
            }
          },
          {
            "owner": {
              "urn": "urn:li:corpuser:datahub",
              "type": "CORP_USER"
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
--data-raw '{ "query": "{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\") { ownership { owners { owner { ... on CorpUser { urn type } ... on CorpGroup { urn type } } } } } }", "variables":{}}'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "ownership": {
        "owners": [
          { "owner": { "urn": "urn:li:corpuser:jdoe", "type": "CORP_USER" } },
          { "owner": { "urn": "urn:li:corpuser:datahub", "type": "CORP_USER" } }
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
{{ inline /metadata-ingestion/examples/library/dataset_query_owners.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Add Owners

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```python
mutation addOwners {
    addOwner(
      input: {
        ownerUrn: "urn:li:corpGroup:bfoo",
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        ownerEntityType: CORP_GROUP,
        type: TECHNICAL_OWNER
			}
    )
}
```

Expected Response:

```python
{
  "data": {
    "addOwner": true
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
--data-raw '{ "query": "mutation addOwners { addOwner(input: { ownerUrn: \"urn:li:corpGroup:bfoo\", resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\", ownerEntityType: CORP_GROUP, type: TECHNICAL_OWNER }) }", "variables":{}}'
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_owner.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Expected Outcomes of Adding Owner

You can now see `bfoo` has been added as an owner to the `fct_users_created` dataset.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/owner-added.png"/>
</p>


## Remove Owners

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation removeOwners {
    removeOwner(
      input: {
        ownerUrn: "urn:li:corpuser:jdoe",
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
			}
    )
}
```

Note that you can also remove owners from multiple entities or subresource using `batchRemoveOwners`.

```json
mutation batchRemoveOwners {
    batchRemoveOwners(
      input: {
        ownerUrns: ["urn:li:corpuser:jdoe"],
        resources: [
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"} ,
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"} ,]
      }
    )
}
```

Expected Response:

```python
{
  "data": {
    "removeOwner": true
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
--data-raw '{ "query": "mutation removeOwner { removeOwner(input: { ownerUrn: \"urn:li:corpuser:jdoe\", resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)\" }) }", "variables":{}}'
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_remove_owner_execute_graphql.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Removing Owners

You can now see `John Doe` has been removed as an owner from the `fct_users_created` dataset.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/owner-removed.png"/>
</p>

