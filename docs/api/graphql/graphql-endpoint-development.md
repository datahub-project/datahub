# Creating a New GraphQL Endpoint in GMS

This guide will walk you through how to add a new GraphQL endpoint in GMS. 
> **listOwnershipTypes example:** The `listOwnershipTypes` endpoint will be used as an example. This endpoint was added in [this commit](https://github.com/datahub-project/datahub/commit/ea92b86e6ab4cbb18742fb8db6bc11fae8970cdb#diff-df9c96427d45d7af6d92dd6caa23a349357dbc4bdb915768ab4ce000a4286964) which can be used as reference.

## GraphQL API changes

### Adding an endpoint definition
GraphQL endpoint definitions for GMS are located in the `datahub-graphql-core/src/main/resources/` directory. New endpoints can be added to the relevant file, e.g. `entity.graphql` for entity management endpoints, `search.graphql` for search-related endpoints, etc. Or, for totally new features, new files can be added to this directory. 

> **listOwnershipTypes example:** The endpoint was added in the [`entity.graphql`](https://github.com/datahub-project/datahub/commit/ea92b86e6ab4cbb18742fb8db6bc11fae8970cdb#diff-df9c96427d45d7af6d92dd6caa23a349357dbc4bdb915768ab4ce000a4286964) file since ownership types are being added as an entity. 

#### Query or Mutation? 
Read-only functionality can go in the `Query` section, while mutations go in the `Mutation` section. The definition for new functionality can go in the appropriate section depending on the use case. 
> **listOwnershipTypes example:** The endpoint was added in the `type Query` section because it is read-only functionality. In the same commit, `createOwnershipType`, `updateOwnershipType`, and `deleteOwnershipType` were added in the `type Mutation` section as these are operations that perform writes.

#### Input and Output Types
If the new endpoint requires more than a few inputs or outputs, a struct can be created in the same file to collect these fields. 
> **listOwnershipTypes example:** Since this functionality takes and returns quite a few parameters, `input ListOwnershipTypesInput` and `type ListOwnershipTypesResult` were added to represent the input and output structs. In the same PR, no input and output structs were added for `deleteOwnershipType` since the inputs and output are primitive types.

### Building your changes
After adding the new endpoint, and new structs if necessary, building the project will generate the Java classes for the new code that can be used in making the server changes. Build the datahub project to make the new symbols available. 
> **listOwnershipTypes example:** The build step will make the new types `ListOwnershipTypesInput` and `ListOwnershipTypesResult` available in a Java IDE.

## Java Server changes
We turn now to developing the server-side functionality for the new endpoint.
### Adding a resolver
GraphQL queries are handled by `Resolver` classes located in the `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/` directory. Resolvers are classes that implement the `DataFetcher<T>` interface where `T` is `CompletableFuture<ClassForResultOfTheEndpoint>`.This interface provides a `get` method that takes in a `DataFetchingEnvironment` and returns a `CompletableFuture` of the endpoint return type. The resolver can contain any services needed to resolve the endpoint, and use them to compute the result.
> **listOwnershipTypes example:** The [`ListOwnershipTypesResolver`](https://github.com/datahub-project/datahub/commit/ea92b86e6ab4cbb18742fb8db6bc11fae8970cdb#diff-d2ad02d0ec286017d032640cfdb289fbdad554ef5f439355104766fa068513ac) class implements `DataFetcher<CompletableFuture<ListOwnershipTypesResult>>` since this is the return type of the endpoint. It contains an `EntityClient` instance variable to handle the ownership type fetching. 

Often the structure of the `Resolver` classes is to call a service to receive a response, then use a method to transform the result from the service into the GraphQL type returned.
> **listOwnershipTypes example:** The [`ListOwnershipTypesResolver`](https://github.com/datahub-project/datahub/commit/ea92b86e6ab4cbb18742fb8db6bc11fae8970cdb#diff-d2ad02d0ec286017d032640cfdb289fbdad554ef5f439355104766fa068513ac) calls the `search` method in its `EntityClient` to get the ownership types, then calls the defined `mapUnresolvedOwnershipTypes` function to transform the response into a `ListOwnershipTypesResult`.

Tip: Resolver classes can be tested with unit tests! 
> **listOwnershipTypes example:** The reference commit adds the [`ListOwnershipTypeResolverTest` class](https://github.com/datahub-project/datahub/commit/ea92b86e6ab4cbb18742fb8db6bc11fae8970cdb#diff-9443d70b221e36e9d47bfa9244673d1cd553a92ae496d03622932ad0a4832045).

### Adding the resolver to the GMS server
The main GMS server is located in [`GmsGraphQLEngine.java`](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/GmsGraphQLEngine.java). To hook up the resolver to handle the endpoint, find the relevant section based on if the new enpoint is a `Query` or a `Mutation` and add the resolver as the `dataFetcher` for the name of the endpoint. 
> **listOwnershipTypes example:** The following line of code is added in [`GmsGraphQLEngine`](https://github.com/datahub-project/datahub/commit/ea92b86e6ab4cbb18742fb8db6bc11fae8970cdb#diff-e04c9c2d80cbfd7aa7e3e0f867248464db0f6497684661132d6ead81ded21856): `.dataFetcher("listOwnershipTypes", new ListOwnershipTypesResolver(this.entityClient))`. This uses the `ListOwnershipTypes` resolver to handle queries for `listOwnershipTypes` endpoint. 

## Testing your change
In addition to unit tests for your resolver mentioned above, GraphQL functionality in datahub can be tested using the built-in [GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/) endpoint. The endpoint is located at `localhost:8080/api/graphiql` on Quickstart and at the equivalent URL for a production instance. This provides fast debug-ability for querying GraphQL. See [How to Set Up GraphQL](./how-to-set-up-graphql.md#graphql-explorer-graphiql) for more information