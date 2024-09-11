---
description: Getting started with the DataHub GraphQL API.
---

# Getting Started

The DataHub Cloud GraphQL API is an extension of the open source [DataHub GraphQL API.](docs/api/graphql/overview.md)

For a full reference to the Queries & Mutations available for consumption, check out [Queries](graphql/queries.md) & [Mutations](graphql/mutations.md).

### Connecting to the API


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/image-(3).png"/>
</p>


When you generate the token you will see an example of `curl` command which you can use to connect to the GraphQL API.

Note that there is a single URL mentioned there but it can be any of these

- https://`your-account`.acryl.io/api/graphql
- https://`your-account`.acryl.io/api/gms/graphql

If there is any example that requires you to connect to GMS then you can use the second URL and change the endpoints.

e.g. to get configuration of your GMS server you can use

```
curl -X GET 'https://your-account.acryl.io/api/gms/config' --header <YOUR_TOKEN>
```

e.g. to connect to ingestion endpoint for doing ingestion programmatically you can use the below URL

- https://your-account.acryl.io/api/gms/aspects?action=ingestProposal

### Exploring the API

The entire GraphQL API can be explored & [introspected](https://graphql.org/learn/introspection/) using GraphiQL, an interactive query tool which allows you to navigate the entire Acryl GraphQL schema as well as craft & issue using an intuitive UI.

[GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/) is available for each DataHub Cloud deployment, locating at `https://your-account.acryl.io/api/graphiql`.

### Querying the API

Currently, we do not offer language-specific SDKs for accessing the DataHub GraphQL API. For querying the API, you can make use of a variety of per-language client libraries. For a full list, see [GraphQL Code Libraries, Tools, & Services](https://graphql.org/code/).
