# DataHub GraphQL API

DataHub provides a rich [`graphql`](https://graphql.org/) API for programmatically interacting with the Entities & Relationships comprising your organization's Metadata Graph.

## Getting Started

To begin using the DataHub `graphql` API, please consult the [Getting Started](/docs/api/graphql/getting-started.md).

For detailed guidance on using `graphql` for specific use cases, please refer to [Datahub API Comparison](/docs/api/datahub-apis.md#datahub-api-comparison).

> **Pro Tip!** Throughout our API guides, we have examples of using GraphQL API.
> Lookout for the `| GraphQL |` tab within our tutorials.

## About GraphQL

[`graphql`](https://graphql.org/) provides a data query language and API with the following characteristics:

- A **validated specification**: The `graphql` spec verifies a _schema_ on the API server. The server in turn is responsible
  for validating incoming queries from the clients against that schema.
- **Strongly typed**: A GraphQL schema declares the universe of types and relationships composing the interface.
- **Document-oriented & hierarchical**: GraphQL makes it eay to ask for related entities using a familiar JSON document
  structure. This minimizes the number of round-trip API requests a client must make to answer a particular question.
- **Flexible & efficient**: GraphQL provides a way to ask for only the data you want, and that's it. Ignore all
  the rest. It allows you to replace multiple REST calls with one GraphQL call.
- **Large Open Source Ecosystem**: Open source GraphQL projects have been developed for [virtually every programming language](https://graphql.org/code/). With a thriving
  community, it offers a sturdy foundation to build upon.

For these reasons among others DataHub provides a GraphQL API on top of the Metadata Graph,
permitting easy exploration of the Entities & Relationships composing it.

For more information about the GraphQL specification, check out [Introduction to GraphQL](https://graphql.org/learn/).

## GraphQL Schema Reference

The Reference docs in the sidebar are generated from the DataHub GraphQL schema. Each call to the `/api/graphql` endpoint is
validated against this schema. You can use these docs to understand data that is available for retrieval and operations
that may be performed using the API.

- Available Operations: [Queries](/graphql/queries.md) (Reads) & [Mutations](/graphql/mutations.md) (Writes)
- Schema Types: [Objects](/graphql/objects.md), [Input Objects](/graphql/inputObjects.md), [Interfaces](/graphql/interfaces.md), [Unions](/graphql/unions.md), [Enums](/graphql/enums.md), [Scalars](/graphql/scalars.md)
