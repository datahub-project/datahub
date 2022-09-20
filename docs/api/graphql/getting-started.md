# Getting Started

Get started using the DataHub GraphQL API.

## Introduction to GraphQL 

The GraphQL community provides many freely available resources for learning about GraphQL. We recommend starting with [Introduction to GraphQL](https://graphql.org/learn/),
which will introduce you to key concepts like [Queries, Mutations, Variables, Schemas & more](https://graphql.org/learn/queries/). 

We'll reiterate a few important points before proceeding:

- GraphQL Operations are exposed via a single service endpoint, in the case of DataHub located at `/api/graphql`. This will be described in more detail below. 
- GraphQL supports reads using a top-level **Query** object, and writes using a top-level **Mutation** object.
- GraphQL supports [schema introspection](https://graphql.org/learn/introspection/), wherein clients can query for details about the GraphQL schema itself.

## Setup

The first thing you'll need to use the GraphQL API is a deployed instance of DataHub with some metadata ingested. Unsure how to do that? Check out the [Deployment Quickstart](../../../docs/quickstart.md).

## Querying the GraphQL API

DataHub's GraphQL endpoint is served at the path `/api/graphql`, e.g. `https://my-company.datahub.com/api/graphql`.
There are a few options when it comes to querying the GraphQL endpoint.

For **Testing**, we recommend [Postman](https://learning.postman.com/docs/sending-requests/supported-api-frameworks/graphql/), GraphQL Explorer (described below), or CURL.
For **Production**, we recommend a GraphQL [Client SDK](https://graphql.org/code/) for the language of your choice, or a basic HTTP client.

#### Authentication + Authorization

In general, you'll need to provide an [Access Token](../../authentication/personal-access-tokens.md) when querying the GraphQL by
providing an `Authorization` header containing a `Bearer` token. The header should take the following format:

```bash
Authorization: Bearer <access-token>
```

Authorization for actions exposed by the GraphQL endpoint will be performed based on the actor making the request.
For Personal Access Tokens, the token will carry the user's privileges. 

> Notice: The DataHub GraphQL endpoint only supports POST requests at this time.

### On the Horizon

- **Service Tokens**: In the near future, the DataHub team intends to introduce service users, which will provide a way to generate and use API access
tokens when querying both the Frontend Proxy Server and the Metadata Service. If you're interested in contributing, please [reach out on our Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email).
- **DataHub Client SDKs**: Libraries wrapping the DataHub GraphQL API on a per-language basis (based on community demand). 

## GraphQL Explorer 

DataHub provides a browser-based GraphQL Explorer Tool ([GraphiQL](https://github.com/graphql/graphiql)) for live interaction with the GraphQL API. This tool is available at the path `/api/graphiql` (e.g. `https://my-company.datahub.com/api/graphiql`)
This interface allows you to easily craft queries and mutations against real metadata stored in your live DataHub deployment. For a detailed usage guide,
check out [How to use GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/). 


## Where to go from here

Once you've gotten the API deployed and responding, proceed to [Working with Metadata Entities](./querying-entities.md) to learn how to read and write the Entities
on your Metadata Graph.
If you're interested in administrative actions considering have a look at [Token Management](./token-management.md) to learn how to generate, list & revoke access tokens for programmatic use in DataHub.

## Feedback, Feature Requests, & Support

Visit our [Slack channel](https://slack.datahubproject.io) to ask questions, tell us what we can do better, & make requests for what you'd like to see in the future. Or just
stop by to say 'Hi'. 
