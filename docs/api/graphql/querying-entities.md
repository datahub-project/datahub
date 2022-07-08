# Querying Metadata Entities 

### Learn how to fetch & update Entities in your Metadata Graph programmatically

## Queries: Reading an Entity 

DataHub provides the following GraphQL queries for retrieving entities in your Metadata Graph. 

### Getting a Metadata Entity 

To retrieve a Metadata Entity by primary key (urn), simply use the `<entityName>(urn: String!)` GraphQL Query. 

For example, to retrieve a User entity, you can issue the following GraphQL Query:

*As GraphQL*

```graphql 
{
  corpUser(urn: "urn:li:corpuser:datahub") {
    username
    urn
  }
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ corpUser(urn: \"urn:li:corpuser:datahub\") { username urn } }", "variables":{}}'
```

### Searching for a Metadata Entity 

To perform full-text search against an Entity of a particular type, use the `search(input: SearchInput!)` GraphQL Query.

As GraphQL:

```graphql 
{
  search(input: { type: DATASET, query: "my sql dataset", start: 0, count: 10 }) {
    start
    count
    total
    searchResults {
      entity {
         urn
         type
         ...on Dataset {
            name
         }
      }
    }
  }
}
```

As CURL:

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ search(input: { type: DATASET, query: \"my sql dataset\", start: 0, count: 10 }) { start count total searchResults { entity { urn type ...on Dataset { name } } } } }", "variables":{}}'
```

Note that per-field filtering criteria may additionally be provided. 

### Querying for owners of a dataset

As GraphQL:

```graphql
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)") {
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

### Querying for tags of a dataset

As GraphQL:

```graphql 
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)") {
    tags {
      tags {
        tag {
          name
        }
      }
    }
  }
}
```

### Coming soon

List Metadata Entities! listDatasets, listDashboards, listCharts, listDataFlows, listDataJobs, listTags


## Mutations: Modifying an Entity 

DataHub provides the following GraphQL mutations for updating entities in your Metadata Graph. 

### Authorization

Mutations which change Entity metadata are subject to [DataHub Access Policies](../../authorization/policies.md). This means that DataHub's server
will check whether the requesting actor is authorized to perform the action. If you're querying the GraphQL endpoint via the DataHub
Proxy Server, which is discussed more in [Getting Started](./getting-started.md), then the Session Cookie provided will carry the actor information.
If you're querying the Metadata Service API directly, then you'll have to provide this via a special `X-DataHub-Actor` HTTP header, which should
contain the URN (primary key) of the actor making the request. For example, `X-DataHub-Actor: urn:li:corpuser:datahub`. 
  
### Updating a Metadata Entity

To update an existing Metadata Entity, simply use the `update<entityName>(urn: String!, input: EntityUpdateInput!)` GraphQL Query.

For example, to update a Dashboard entity, you can issue the following GraphQL mutation:

*As GraphQL*

```graphql 
mutation updateDashboard {
    updateDashboard(
        urn: "urn:li:dashboard:(looker,baz)",
        input: {
            editableProperties: {
                description: "My new desription"
            }
        }
    ) {
        urn
    }
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation updateDashboard { updateDashboard(urn:\"urn:li:dashboard:(looker,baz)\", input: { editableProperties: { description: \"My new desription\" } } ) { urn } }", "variables":{}}'
```

**Be careful**: these APIs allow you to make significant changes to a Metadata Entity, often including
updating the entire set of Owners & Tags. 

### Adding & Removing Tags / Glossary Terms

To attach a Tag or Glossary Term to a Metadata Entity, you can use the `addTag` and `addTerm` mutations.
To remove them, you can use the `removeTag` and `removeTerm` mutations. 

For example, to add a Tag a DataFlow entity, you can issue the following GraphQL mutation: 

*As GraphQL*

```graphql 
mutation addTag {
    addTag(input: { tagUrn: "urn:li:tag:NewTag", resourceUrn: "urn:li:dataFlow:(airflow,dag_abc,PROD)" })
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTag { addTag(input: { tagUrn: \"urn:li:tag:NewTag\", resourceUrn: \"urn:li:dataFlow:(airflow,dag_abc,PROD)\" }) }", "variables":{}}'
```

### Coming soon 

**Entity Creation**: createDataset, createDashboard, createChart, etc. 
**Entity Removal**: removeDataset, removeDashboard, removeChart, etc.

## Handling Errors

In GraphQL, requests that have errors do not always result in a non-200 HTTP response body. Instead, errors will be
present in the response body inside a top-level `errors` field. 

This enables situations in which the client is able to deal gracefully will partial data returned by the application server.
To verify that no error has returned after making a GraphQL request, make sure you check *both* the `data` and `errors` fields that are returned. 

## Feedback, Feature Requests, & Support

Visit our [Slack channel](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email) to ask questions, tell us what we can do better, & make requests for what you'd like to see in the future. Or just
stop by to say 'Hi'. 
