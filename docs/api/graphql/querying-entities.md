# Working with Metadata Entities

Learn how to find, retrieve & update entities comprising your Metadata Graph programmatically.

## Reading an Entity: Queries

DataHub provides the following GraphQL queries for retrieving entities in your Metadata Graph. 

### Getting a Metadata Entity 

To retrieve a Metadata Entity by primary key (urn), simply use the `<entityName>(urn: String!)` GraphQL Query. 

For example, to retrieve a `dataset` entity, you can issue the following GraphQL Query:

*As GraphQL*

```graphql 
{
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)") {
    urn
    properties {
        name
    }
  }
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)\") { urn properties { name } } }", "variables":{}}'
```

In the following examples, we'll look at how to fetch specific types of metadata for an asset. 

#### Querying for Owners of an entity

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

#### Querying for Tags of an asset

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

#### Querying for Domain of an asset

As GraphQL:

```graphql 
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)") {
    domain {
      domain {
        urn
      }
    }
  }
}
```

#### Querying for Glossary Terms of an asset

As GraphQL:

```graphql 
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)") {
    glossaryTerms {
      terms {
        term {
          urn
        }
      }
    }
  }
}
```

#### Querying for Deprecation of an asset

As GraphQL:

```graphql 
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)") {
    deprecation {
      deprecated
      decommissionTime
    }
  }
}
```

#### Relevant Queries

- [dataset](../../../graphql/queries.md#dataset)
- [container](../../../graphql/queries.md#container)
- [dashboard](../../../graphql/queries.md#dashboard)
- [chart](../../../graphql/queries.md#chart)
- [dataFlow](../../../graphql/queries.md#dataflow)
- [dataJob](../../../graphql/queries.md#datajob)
- [domain](../../../graphql/queries.md#domain)
- [glossaryTerm](../../../graphql/queries.md#glossaryterm)
- [glossaryNode](../../../graphql/queries.md#glossarynode)
- [tag](../../../graphql/queries.md#tag)
- [notebook](../../../graphql/queries.md#notebook)
- [corpUser](../../../graphql/queries.md#corpuser)
- [corpGroup](../../../graphql/queries.md#corpgroup)


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
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ search(input: { type: DATASET, query: \"my sql dataset\", start: 0, count: 10 }) { start count total searchResults { entity { urn type ...on Dataset { name } } } } }", "variables":{}}'
```

> **Note** that by default Elasticsearch only allows pagination through 10,000 entities via the search API.
> If you need to paginate through more, you can change the default value for the `index.max_result_window` setting in Elasticsearch,
> or using the [scroll API](https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html) to read from the index directly. 

#### Relevant Queries

- [search](../../../graphql/queries.md#search)
- [searchAcrossEntities](../../../graphql/queries.md#searchacrossentities)
- [searchAcrossLineage](../../../graphql/queries.md#searchacrosslineage)
- [browse](../../../graphql/queries.md#browse)
- [browsePaths](../../../graphql/queries.md#browsepaths)


## Modifying an Entity: Mutations

### Authorization

Mutations which change Entity metadata are subject to [DataHub Access Policies](../../authorization/policies.md). This means that DataHub's server
will check whether the requesting actor is authorized to perform the action. 
  
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
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation updateDashboard { updateDashboard(urn:\"urn:li:dashboard:(looker,baz)\", input: { editableProperties: { description: \"My new desription\" } } ) { urn } }", "variables":{}}'
```

**Be careful**: these APIs allow you to make significant changes to a Metadata Entity, often including
updating the entire set of Owners & Tags. 

#### Relevant Mutations

- [updateDataset](../../../graphql/mutations.md#updatedataset)
- [updateChart](../../../graphql/mutations.md#updatechart)
- [updateDashboard](../../../graphql/mutations.md#updatedashboard)
- [updateDataFlow](../../../graphql/mutations.md#updatedataFlow)
- [updateDataJob](../../../graphql/mutations.md#updatedataJob)
- [updateNotebook](../../../graphql/mutations.md#updatenotebook)


### Adding & Removing Tags

To attach Tags to a Metadata Entity, you can use the `addTags` or `batchAddTags` mutations.
To remove them, you can use the `removeTag` or `batchRemoveTags` mutations. 

For example, to add a Tag a Pipeline entity, you can issue the following GraphQL mutation: 

*As GraphQL*

```graphql 
mutation addTags {
    addTags(input: { tagUrns: ["urn:li:tag:NewTag"], resourceUrn: "urn:li:dataFlow:(airflow,dag_abc,PROD)" })
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTags { addTags(input: { tagUrns: [\"urn:li:tag:NewTag\"], resourceUrn: \"urn:li:dataFlow:(airflow,dag_abc,PROD)\" }) }", "variables":{}}'
```

> **Pro-Tip**! You can also add or remove Tags from Dataset Schema Fields (or *Columns*) by 
> providing 2 additional fields in your Query input:
> 
> - subResourceType 
> - subResource
> 
> Where `subResourceType` is set to `DATASET_FIELD` and `subResource` is the field path of the column
> to change.

#### Relevant Mutations

- [addTags](../../../graphql/mutations.md#addtags)
- [batchAddTags](../../../graphql/mutations.md#batchaddtags)
- [removeTag](../../../graphql/mutations.md#removetag)
- [batchRemoveTags](../../../graphql/mutations.md#batchremovetags)


### Adding & Removing Glossary Terms

To attach Glossary Terms to a Metadata Entity, you can use the `addTerms` or `batchAddTerms` mutations.
To remove them, you can use the `removeTerm` or `batchRemoveTerms` mutations.

For example, to add a Glossary Term a Pipeline entity, you could issue the following GraphQL mutation:

*As GraphQL*

```graphql 
mutation addTerms {
    addTerms(input: { termUrns: ["urn:li:glossaryTerm:NewTerm"], resourceUrn: "urn:li:dataFlow:(airflow,dag_abc,PROD)" })
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTerms { addTerms(input: { termUrns: [\"urn:li:glossaryTerm:NewTerm\"], resourceUrn: \"urn:li:dataFlow:(airflow,dag_abc,PROD)\" }) }", "variables":{}}'
```

> **Pro-Tip**! You can also add or remove Glossary Terms from Dataset Schema Fields (or *Columns*) by
> providing 2 additional fields in your Query input:
>
> - subResourceType
> - subResource
>
> Where `subResourceType` is set to `DATASET_FIELD` and `subResource` is the field path of the column
> to change.

#### Relevant Mutations

- [addTerms](../../../graphql/mutations.md#addterms)
- [batchAddTerms](../../../graphql/mutations.md#batchaddterms)
- [removeTerm](../../../graphql/mutations.md#removeterm)
- [batchRemoveTerms](../../../graphql/mutations.md#batchremoveterms)


### Adding & Removing Domain

To add an entity to a Domain, you can use the `setDomain` and `batchSetDomain` mutations.
To remove entities from a Domain, you can use the `unsetDomain` mutation or the `batchSetDomain` mutation.

For example, to add a Pipeline entity to the "Marketing" Domain, you can issue the following GraphQL mutation:

*As GraphQL*

```graphql 
mutation setDomain {
    setDomain(domainUrn: "urn:li:domain:Marketing", entityUrn: "urn:li:dataFlow:(airflow,dag_abc,PROD)")
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation setDomain { setDomain(domainUrn: \"urn:li:domain:Marketing\", entityUrn: \"urn:li:dataFlow:(airflow,dag_abc,PROD)\") }", "variables":{}}'
```

#### Relevant Mutations

- [setDomain](../../../graphql/mutations.md#setdomain)
- [batchSetDomain](../../../graphql/mutations.md#batchsetdomain)
- [unsetDomain](../../../graphql/mutations.md#unsetdomain)


### Adding & Removing Owners

To attach Owners to a Metadata Entity, you can use the `addOwners` or `batchAddOwners` mutations.
To remove them, you can use the `removeOwner` or `batchRemoveOwners` mutations.

For example, to add an Owner a Pipeline entity, you can issue the following GraphQL mutation:

*As GraphQL*

```graphql 
mutation addOwners {
    addOwners(input: { owners: [ { ownerUrn: "urn:li:corpuser:datahub", ownerEntityType: CORP_USER, type: TECHNICAL_OWNER } ], resourceUrn: "urn:li:dataFlow:(airflow,dag_abc,PROD)" })
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addOwners { addOwners(input: { owners: [ { ownerUrn: \"urn:li:corpuser:datahub\", ownerEntityType: CORP_USER, type: TECHNICAL_OWNER } ], resourceUrn: \"urn:li:dataFlow:(airflow,dag_abc,PROD)\" }) }", "variables":{}}'
```

#### Relevant Mutations

- [addOwners](../../../graphql/mutations.md#addowners)
- [batchAddOwners](../../../graphql/mutations.md#batchaddowners)
- [removeOwner](../../../graphql/mutations.md#removeowner)
- [batchRemoveOwners](../../../graphql/mutations.md#batchremoveowners)


### Updating Deprecation

To update deprecation for a Metadata Entity, you can use the `updateDeprecation` or `batchUpdateDeprecation` mutations.

For example, to mark a Pipeline entity as deprecated, you can issue the following GraphQL mutation:

*As GraphQL*

```graphql 
mutation updateDeprecation {
    updateDeprecation(input: { urn: "urn:li:dataFlow:(airflow,dag_abc,PROD)", deprecated: true })
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation updateDeprecation { updateDeprecation(input: { urn: \"urn:li:dataFlow:(airflow,dag_abc,PROD)\", deprecated: true }) }", "variables":{}}'
```

> **Note** that deprecation is NOT currently supported for assets of type `container`. 

#### Relevant Mutations

- [updateDeprecation](../../../graphql/mutations.md#updatedeprecation)
- [batchUpdateDeprecation](../../../graphql/mutations.md#batchupdatedeprecation)


### Editing Description (i.e. Documentation)

> Notice that this API is currently evolving and in an experimental state. It supports the following entities today:
> - dataset
> - container
> - domain
> - glossary term
> - glossary node
> - tag
> - group 
> - notebook
> - all ML entities

To edit the documentation for an entity, you can use the `updateDescription` mutation. 

For example, to edit the documentation for a Pipeline, you can issue the following GraphQL mutation:

*As GraphQL*

```graphql 
mutation updateDescription {
    updateDescription(input: { description: "The new description!", resourceUrn: "urn:li:dataFlow:(airflow,dag_abc,PROD)" })
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation updateDescription { updateDescription(input: { description: \"The new description!\", resourceUrn: \"urn:li:dataFlow:(airflow,dag_abc,PROD)\" }) }", "variables":{}}'
```

> **Pro-Tip**! You can also edit Documentation for Dataset Schema Fields (or *Columns*) by
> providing 2 additional fields in your Query input:
>
> - subResourceType
> - subResource
>
> Where `subResourceType` is set to `DATASET_FIELD` and `subResource` is the field path of the column
> to change.
>

#### Relevant Mutations

- [updateDescription](../../../graphql/mutations.md#updatedescription)


### Soft Deleting

DataHub allows you to soft-delete entities. This will effectively hide them from the search,
browse, and lineage experiences.

To mark an entity as soft-deleted, you can use the `batchUpdateSoftDeleted` mutation.

For example, to mark a Pipeline as soft deleted, you can issue the following GraphQL mutation:

*As GraphQL*

```graphql 
mutation batchUpdateSoftDeleted {
    batchUpdateSoftDeleted(input: { : urns: ["urn:li:dataFlow:(airflow,dag_abc,PROD)"], deleted: true })
}
```

Similarly, you can "un delete" an entity by setting deleted to 'false'. 

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation batchUpdateSoftDeleted { batchUpdateSoftDeleted(input: { deleted: true, urns: [\"urn:li:dataFlow:(airflow,dag_abc,PROD)\"] }) }", "variables":{}}'
```

#### Relevant Mutations

- [batchUpdateSoftDeleted](../../../graphql/mutations.md#batchupdatesoftdeleted)


## Handling Errors

In GraphQL, requests that have errors do not always result in a non-200 HTTP response body. Instead, errors will be
present in the response body inside a top-level `errors` field. 

This enables situations in which the client is able to deal gracefully will partial data returned by the application server.
To verify that no error has returned after making a GraphQL request, make sure you check *both* the `data` and `errors` fields that are returned. 

To catch a GraphQL error, simply check the `errors` field side the GraphQL response. It will contain a message, a path, and a set of extensions
which contain a standard error code. 

```json
{
   "errors":[
      {
         "message":"Failed to change ownership for resource urn:li:dataFlow:(airflow,dag_abc,PROD). Expected a corp user urn.",
         "locations":[
            {
               "line":1,
               "column":22
            }
         ],
         "path":[
            "addOwners"
         ],
         "extensions":{
            "code":400,
            "type":"BAD_REQUEST",
            "classification":"DataFetchingException"
         }
      }
   ]
}
```

With the following error codes officially supported:

| Code | Type         | Description                                                                                    |
|------|--------------|------------------------------------------------------------------------------------------------|
| 400  | BAD_REQUEST  | The query or mutation was malformed.                                                           |
| 403  | UNAUTHORIZED | The current actor is not authorized to perform the requested action.                           |
| 404  | NOT_FOUND    | The resource is not found.                                                                     |
| 500  | SERVER_ERROR | An internal error has occurred. Check your server logs or contact your DataHub administrator.  |

## Feedback, Feature Requests, & Support

Visit our [Slack channel](https://slack.datahubproject.io) to ask questions, tell us what we can do better, & make requests for what you'd like to see in the future. Or just
stop by to say 'Hi'. 

