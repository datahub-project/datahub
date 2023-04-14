# Getting Started With GraphQL

## Reading an Entity: Queries

DataHub provides the following `graphql` queries for retrieving entities in your Metadata Graph.

### Query

The following `graphql` query retrieves the `urn` and `name` of the `properties` of a specific dataset

```json
{
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)") {
    urn
    properties {
        name
    }
  }
}
```

In addition to the URN and properties, you can also fetch other types of metadata for an asset, such as owners, tags, domains, and terms of an entity.
For more information on, please refer to the following links."

- [Querying for Owners of a Dataset](/docs/api/tutorials/reading-owners.md)
- [Querying for Tags of a Dataset](/docs/api/tutorials/reading-tags.md)
- [Querying for Domain of a Dataset](/docs/api/tutorials/reading-domains.md)
- [Querying for Glossary Terms of a Dataset](/docs/api/tutorials/reading-terms.md)
- [Querying for Deprecation of a dataset](/docs/api/tutorials/reading-deprecation.md)

### Search

To perform full-text search against an Entity of a particular type, use the search(input: `SearchInput!`) `graphql` Query.
The following `graphql` query searches for datasets that match a specific query term.

```json
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

The `search` field is used to indicate that we want to perform a search.
The `input` argument specifies the search criteria, including the type of entity being searched, the search query term, the start index of the search results, and the count of results to return.

The `query` term is used to specify the search term.
The search term can be a simple string, or it can be a more complex query using patterns.

- `*` : Search for all entities.
- `*[string]` : Search for all entities that contain aspects **starting with** the specified \[string\].
- `[string]*` : Search for all entities that contain aspects **ending with** the specified \[string\].
- `*[string]*` : Search for all entities that **match** aspects named \[string\].
- `[string]` : Search for all entities that **contain** the specified \[string\].

:::note
Note that by default Elasticsearch only allows pagination through 10,000 entities via the search API.
If you need to paginate through more, you can change the default value for the `index.max_result_window` setting in Elasticsearch, or using the scroll API to read from the index directly.
:::

## Modifying an Entity: Mutations

:::note
Mutations which change Entity metadata are subject to [DataHub Access Policies](../../authorization/policies.md).
This means that DataHub's server will check whether the requesting actor is authorized to perform the action.
:::

To update an existing Metadata Entity, simply use the `update<entityName>(urn: String!, input: EntityUpdateInput!)` GraphQL Query.
For example, to update a Dashboard entity, you can issue the following GraphQL mutation:

```json
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

For more information, please refer to following links.

- [Adding Tags](/docs/api/tutorials/adding-tags.md)
- [Adding Glossary Terms](/docs/api/tutorials/adding-terms.md)
- [Adding Domain](/docs/api/tutorials/adding-domain.md)
- [Adding Owners](/docs/api/tutorials/adding-ownerships.md)
- [Removing Tags](/docs/api/tutorials/removing-tags.md)
- [Removing Glossary Terms](/docs/api/tutorials/removing-terms.md)
- [Removing Domain](/docs/api/tutorials/removing-domains.md)
- [Removing Owners](/docs/api/tutorials/removing-owners.md)
- [Updating Deprecation](/docs/api/tutorials/update-deprecation.md)
- [Editing Description (i.e. Documentation) on Datasets](/docs/api/tutorials/adding-dataset-description.md)
- [Editing Description (i.e. Documnetation) on Columns](/docs/api/tutorials/adding-column-description.md)
- [Soft Deleting](/docs/api/tutorials/deleting-entities-by-urn.md)

Please refer to [Datahub API Comparison](/docs/api/datahub-apis.md#datahub-api-comparison) to navigate to the use-case oriented guide.

## Handling Errors

In GraphQL, requests that have errors do not always result in a non-200 HTTP response body. Instead, errors will be
present in the response body inside a top-level `errors` field.

This enables situations in which the client is able to deal gracefully will partial data returned by the application server.
To verify that no error has returned after making a GraphQL request, make sure you check _both_ the `data` and `errors` fields that are returned.

To catch a GraphQL error, simply check the `errors` field side the GraphQL response. It will contain a message, a path, and a set of extensions
which contain a standard error code.

```json
{
  "errors": [
    {
      "message": "Failed to change ownership for resource urn:li:dataFlow:(airflow,dag_abc,PROD). Expected a corp user urn.",
      "locations": [
        {
          "line": 1,
          "column": 22
        }
      ],
      "path": ["addOwners"],
      "extensions": {
        "code": 400,
        "type": "BAD_REQUEST",
        "classification": "DataFetchingException"
      }
    }
  ]
}
```

With the following error codes officially supported:

| Code | Type         | Description                                                                                   |
| ---- | ------------ | --------------------------------------------------------------------------------------------- |
| 400  | BAD_REQUEST  | The query or mutation was malformed.                                                          |
| 403  | UNAUTHORIZED | The current actor is not authorized to perform the requested action.                          |
| 404  | NOT_FOUND    | The resource is not found.                                                                    |
| 500  | SERVER_ERROR | An internal error has occurred. Check your server logs or contact your DataHub administrator. |

> Visit our [Slack channel](https://slack.datahubproject.io) to ask questions, tell us what we can do better, & make requests for what you'd like to see in the future. Or just
> stop by to say 'Hi'.
