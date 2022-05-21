# Getting Started

### Get your feet wet with the DataHub GraphQL API

## Introduction to GraphQL 

The GraphQL community provides many freely available resources for learning about GraphQL. We recommend starting with [Introduction to GraphQL](https://graphql.org/learn/),
which will introduce you to key concepts like [Queries, Mutations, Variables, Schemas & more](https://graphql.org/learn/queries/). 

We'll reiterate a few important points before proceeding:

- GraphQL Operations are exposed via a single service endpoint, in the case of DataHub located at `/api/graphql`. This will be described in more detail below. 
- GraphQL supports reads using a top-level **Query** object, and writes using a top-level **Mutation** object.
- GraphQL supports [schema introspection](https://graphql.org/learn/introspection/), wherein clients can query for details about the GraphQL schema itself.

## Setup

The first thing you'll need to use the GraphQL API is a deployed instance of DataHub with some metadata ingested. Unsure how to do that? Check out the [Deployment Quickstart](../../../docs/quickstart.md).


## The DataHub GraphQL Endpoint 

Today, DataHub's GraphQL endpoint is available for use in multiple places. The one you choose to use depends on your specific use case. 

1. **Metadata Service**: The DataHub Metadata Service (backend) is the source-of-truth for the GraphQL endpoint. The endpoint is located at `/api/graphql` path of the DNS address
where your instance of the `datahub-gms` container is deployed. For example, in local deployments it is typically located at `http://localhost:8080/api/graphql`. By default,
the Metadata Service has no explicit authentication checks. However, it does have *Authorization checks*. DataHub [Access Policies](../../../docs/policies.md) will be enforced by the GraphQL API. This means you'll need to provide an actor identity when querying the GraphQL API. 
To do so, include the `X-DataHub-Actor` header with an Authorized Corp User URN as the value in your request. Because anyone is able to set the value of this header, we recommend using this endpoint only in trusted environments, either by administrators themselves or programs that they own directly. 
   
2. **Frontend Proxy**: The DataHub Frontend Proxy Service (frontend) is a basic web server & reverse proxy to the Metadata Service. As such, the 
GraphQL endpoint is also available for query wherever the Frontend Proxy is deployed. In local deployments, this is typically `http://localhost:9002/api/v2/graphql`. By default,
the Frontend Proxy *does* have Session Cookie-based Authentication via the PLAY_SESSION cookie set at DataHub UI login time. This means
that if a request does not have a valid PLAY_SESSION cookie obtained via logging into the DataHub UI, the request will be rejected. To use this API in an untrusted environment,
you'd need to a) log into DataHub, b) extract the PLAY_SESSION cookie that is set on login, and c) provide this Cookie in your HTTP headers when
calling the endpoint.
   

### Querying the Endpoint

There are a few options when it comes to querying the GraphQL endpoint. The recommendation on which to use varies by use case.

**Testing**: [Postman](https://learning.postman.com/docs/sending-requests/supported-api-frameworks/graphql/), GraphQL Explorer (described below), CURL

**Production**: GraphQL [Client SDK](https://graphql.org/code/) for the language of your choice, or a basic HTTP client.
   
> Important: The DataHub GraphQL endpoint only supports POST requests at this time. It does not support GET requests. If this is something
> you need, let us know on Slack!

### On the Horizon

- **Service Access Tokens**: In the near future, the DataHub team intends to introduce service users, which will provide a way to generate and use API access
tokens when querying both the Frontend Proxy Server and the Metadata Service. If you're interested in contributing, please [reach out on our Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email).
- **DataHub Client SDKs**: Libraries wrapping the DataHub GraphQL API on a per-language basis (based on community demand). 

## GraphQL Explorer 

DataHub provides a browser-based GraphQL Explorer Tool ([GraphiQL](https://github.com/graphql/graphiql)) for live interaction with the GraphQL API. Today, this tool is available for use in multiple places (like the GraphQL endpoint itself):

1. **Metadata Service**: `http://<metadata-service-address>/api/graphiql`. For local deployments, `http://localhost:8080/api/graphiql`.
2. **Frontend Proxy**: `http://<frontend-service-address>/api/graphiql`. For local deployments, `http://localhost:9002/api/graphiql`.

This interface allows you to easily craft queries and mutations against real metadata stored in your live DataHub deployment. For a detailed usage guide,
check out [How to use GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/). 

The same auth restrictions described in the section above apply to these endpoints as well. 

> **Pro Tip**: We recommend you add a browser extension that will allow you to set custom HTTP headers (ie. `Cookies` or `X-DataHub-Actor`) if you plan to use GraphiQL for testing. We like [ModHeader](https://chrome.google.com/webstore/detail/modheader/idgpnmonknjnojddfkpgkljpfnnfcklj?hl=en) for Google Chrome.

## Where to go from here

Once you've gotten the API deployed and responding, proceed to [Querying Entities](./querying-entities.md) to learn how to read and write the Entities
on your Metadata Graph.
If you're interested in administrative actions considering have a look at [Token Management](./token-management.md) to learn how to generate, list & revoke access tokens for programmatic use in DataHub.

## Feedback, Feature Requests, & Support

Visit our [Slack channel](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email) to ask questions, tell us what we can do better, & make requests for what you'd like to see in the future. Or just
stop by to say 'Hi'. 