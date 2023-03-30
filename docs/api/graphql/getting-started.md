# Getting Started

Get started using the DataHub GraphQL API.

## Setup

The first thing you'll need to use the GraphQL API is a deployed instance of DataHub with some metadata ingested. Unsure how to do that? Check out the [Deployment Quickstart](../../../docs/quickstart.md).

## Querying the GraphQL API

DataHub's GraphQL endpoint is served at the path `/api/graphql`, e.g. `https://my-company.datahub.com/api/graphql`.
There are a few options when it comes to querying the GraphQL endpoint.

For **Testing**, we recommend [Postman](https://learning.postman.com/docs/sending-requests/supported-api-frameworks/graphql/), GraphQL Explorer (described below), or CURL.
For **Production**, we recommend a GraphQL [Client SDK](https://graphql.org/code/) for the language of your choice, or a basic HTTP client.


> Notice: The DataHub GraphQL endpoint only supports POST requests at this time.

### GraphQL Explorer 

DataHub provides a browser-based GraphQL Explorer Tool ([GraphiQL](https://github.com/graphql/graphiql)) for live interaction with the GraphQL API. This tool is available at the path `/api/graphiql` (e.g. `https://my-company.datahub.com/api/graphiql`)
This interface allows you to easily craft queries and mutations against real metadata stored in your live DataHub deployment. For a detailed usage guide,
check out [How to use GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/). 

### CURL

### Postman

####  Authentication + Authorization

In general, you'll need to provide an [Access Token](../../authentication/personal-access-tokens.md) when querying the GraphQL by
providing an `Authorization` header containing a `Bearer` token. The header should take the following format:

```bash
Authorization: Bearer <access-token>
```

Authorization for actions exposed by the GraphQL endpoint will be performed based on the actor making the request.
For Personal Access Tokens, the token will carry the user's privileges. Please refer to [Access Token Management](/docs/api/graphql/token-management.md) for more information.

## Reading an Entity: Queries

DataHub provides the following GraphQL queries for retrieving entities in your Metadata Graph.

[more general explanation on queries and searches]

### Query
  * Querying for Owners of an entity
  * Querying for Tags of an asset
  * Querying for Domain of an asset
  * Querying for Glossary Terms of an asset
  * Querying for Deprecation of an asset

### Search

  * Searching for a certain dataset

## Modifying an Entity: Mutations

:::note
 Mutations which change Entity metadata are subject to [DataHub Access Policies](../../authorization/policies.md). 
This means that DataHub's server will check whether the requesting actor is authorized to perform the action. 
:::  

[more general explanation on mutation] 

Examples of mutation includes: 

* Updating a Metadata Entity
* Adding & Removing Tags
* Adding & Removing Glossary Terms
* Adding & Removing Domain
* Adding & Removing Owners
* Updating Deprecation
* Editing Description (i.e. Documentation)
* Soft Deleting

Please refer to Datahub API Comparison to navigate to the use-case oriented guide. 


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


> Visit our [Slack channel](https://slack.datahubproject.io) to ask questions, tell us what we can do better, & make requests for what you'd like to see in the future. Or just
stop by to say 'Hi'. 



> Visit our [Slack channel](https://slack.datahubproject.io) to ask questions, tell us what we can do better, & make requests for what you'd like to see in the future. Or just
stop by to say 'Hi'. 
