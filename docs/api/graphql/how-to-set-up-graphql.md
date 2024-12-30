import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# How To Set Up GraphQL

## Preparing Local Datahub Deployment

The first thing you'll need to use the GraphQL API is a deployed instance of DataHub with some metadata ingested.
For more information, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Querying the GraphQL API

DataHub's GraphQL endpoint is served at the path `/api/graphql`, e.g. `https://my-company.datahub.com/api/graphql`.
There are a few options when it comes to querying the GraphQL endpoint.

For **Testing**:

- GraphQL Explorer (GraphiQL)
- CURL
- Postman

For **Production**:

- GraphQL [Client SDK](https://graphql.org/code/) for the language of your choice
- Basic HTTP client

> Notice: The DataHub GraphQL endpoint only supports POST requests at this time.

### GraphQL Explorer (GraphiQL)

DataHub provides a browser-based GraphQL Explorer Tool ([GraphiQL](https://github.com/graphql/graphiql)) for live interaction with the GraphQL API. This tool is available at the path `/api/graphiql` (e.g. `https://my-company.datahub.com/api/graphiql`)
This interface allows you to easily craft queries and mutations against real metadata stored in your live DataHub deployment.

To experiment with GraphiQL before deploying it in your live DataHub deployment, you can access a demo site provided by DataHub at https://demo.datahubproject.io/api/graphiql.

For instance, you can create a tag by posting the following query:

```json
mutation createTag {
    createTag(input:
    {
      name: "Deprecated",
      description: "Having this tag means this column or table is deprecated."
    })
}
```

For a detailed usage guide, check out [How to use GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/).

To navigate to `GraphiQL` on the demo site or your local instance, select `GraphiQL` from the user profile drop-down menu as
shown below.

<Tab>
<TabItem value="DataHub" label="DataHub" default>
<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/api/graphql/how-to-set-up-graphql/datahub_graphiql_link.png"/>
</p>
![graphiql_link.png](../../../../../Desktop/datahub_graphiql_link.png)
</TabItem>
<TabItem value="DataHubCloud" label="DataHub Cloud">
<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/api/graphql/how-to-set-up-graphql/datahubcloud_graphiql_link.png"/>
</p>
</TabItem>
</Tab>

This link will then display the following interface for exploring GraphQL queries.

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/api/graphql/how-to-set-up-graphql/datahub_graphiql.png"/>
</p>

### CURL

CURL is a command-line tool used for transferring data using various protocols including HTTP, HTTPS, and others.
To query the DataHub GraphQL API using CURL, you can send a `POST` request to the `/api/graphql` endpoint with the GraphQL query in the request body.
Here is an example CURL command to create a tag via GraphQL API:

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation createTag { createTag(input: { name: \"Deprecated\", description: \"Having this tag means this column or table is deprecated.\" }) }", "variables":{}}'
```

### Postman

Postman is a popular API client that provides a graphical user interface for sending requests and viewing responses.
Within Postman, you can create a `POST` request and set the request URL to the `/api/graphql` endpoint.
In the request body, select the `GraphQL` option and enter your GraphQL query in the request body.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/postman-graphql.png"/>
</p>


Please refer to [Querying with GraphQL](https://learning.postman.com/docs/sending-requests/graphql/graphql-overview/) in the Postman documentation for more information.

### Authentication + Authorization

In general, you'll need to provide an [Access Token](../../authentication/personal-access-tokens.md) when querying the GraphQL by
providing an `Authorization` header containing a `Bearer` token. The header should take the following format:

```bash
Authorization: Bearer <access-token>
```

Authorization for actions exposed by the GraphQL endpoint will be performed based on the actor making the request.
For Personal Access Tokens, the token will carry the user's privileges. Please refer to [Access Token Management](/docs/api/graphql/token-management.md) for more information.

## What's Next?

Now that you are ready with GraphQL, how about browsing through some use cases?
Please refer to [Getting Started With GraphQL](/docs/api/graphql/getting-started.md) for more information.
