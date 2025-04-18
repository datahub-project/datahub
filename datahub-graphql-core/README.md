---
title: "datahub-graphql-core"
---

# DataHub GraphQL Core
DataHub GraphQL API is a shared lib module containing a GraphQL API on top of the GMS service layer. It exposes a graph-based representation
permitting reads and writes against the entities and aspects on the Metadata Graph, including Datasets, CorpUsers, & more. 

Contained within this module are 

1. **GMS Schema**: A GQL schema based on GMS models, located under [resources](https://github.com/datahub-project/datahub/tree/master/datahub-graphql-core/src/main/resources) folder.
2. **GMS Data Fetchers** (Resolvers): Components used by the GraphQL engine to resolve individual fields in the GQL schema.
3. **GMS Data Loaders**: Components used by the GraphQL engine to fetch data from downstream sources efficiently (by batching). 
4. **GraphQLEngine**: A wrapper on top of the default `GraphQL` object provided by `graphql-java`. Provides a way to configure all of the important stuff using a simple `Builder API`. 
5. **GMSGraphQLEngine**: An engine capable of resolving the GMS schema using the data fetchers + loaders mentioned above (with no additional configuration required). 

We've chosen to place these components in a library module so that GraphQL servers can be deployed in multiple "modes":

1. **Standalone**: GraphQL facade, mainly used for programmatic access to the GMS graph from a non-Java environment
2. **Embedded**: Leverageable within another Java server to surface an extended GraphQL schema. For example, we use this to extend the GMS GraphQL schema in `datahub-frontend` 


## Extending the Graph

### Adding an Entity

When adding an entity to the GMS graph, the following steps should be followed:

1. Extend [entity.graphql](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/resources/entity.graphql) schema with new `types` (Queries) or `inputs` (Mutations) required for fetching & updating your Entity.

These models should generally mirror the GMS models exactly, with notable exceptions:

- **Maps**: the GQL model must instead contain a list of { key, value } objects (e.g. Dataset.pdl 'properties' field)
- **Foreign-Keys**: Foreign-key references embedded in GMS models should be resolved if the referenced entity exists in the GQL schema,
replacing the key with the actual entity model. (Example: replacing the 'owner' urn field in 'Ownership' with an actual `CorpUser` type)

In GraphQL, the new Entity should extend the `Entity` interface. Additionally, you will need to add a new symbol to the standard 
`EntityType` enum. 

The convention we follow is to have a top-level Query for each entity that takes a single "urn" parameter. This is for primary key lookups.
See all the existing entity Query types [here](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/resources/entity.graphql#L19).

On rebuilding the module (`./gradlew datahub-graphql-core:build`) you'll find newly generated classes corresponding to 
the types you've defined inside the GraphQL schema inside the `mainGeneratedGraphQL` folder. These classes will be used in the next step.

2. Implement `EntityType` classes for any new entities 

- These 'type' classes define how to load entities from GMS, and map them to the GraphQL data model. See [DatasetType.java](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/types/dataset/DatasetType.java) as an example.

3. Implement `Mappers` to transform Pegasus model returned by GMS to an auto-generated GQL POJO. (under `/mainGeneratedGraphQL`, generated on `./gradlew datahub-graphql-core:build`) These mappers
will be used inside the type class defined in step 2. 

- If you've followed the guidance above, these mappers should be simple, mainly
providing identity mappings for fields that exist in both the GQL + Pegasus POJOs.
- In some cases, you'll need to perform small lambdas (unions, maps) to materialize the GQL object. 

4. Wire up your `EntityType` to the GraphQL schema. 

We use [GmsGraphQLEngine.java](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/GmsGraphQLEngine.java) to 
configure the wiring for the GraphQL schema. This means associating "resolvers" to specific fields present in the GraphQL schema file.

Inside of this file, you need to register your new `Type` object to be used in resolving primary-key entity queries.
To do so, simply follow the examples for other entities. 

5. Implement `EntityType` test for the new type defined in Step 2. See [ContainerTypeTest](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/test/java/com/linkedin/datahub/graphql/types/container/ContainerTypeTest.java) as an example.

6. Implement `Resolver` tests for any new `DataFetchers` that you needed to add. See [SetDomainResolverTest](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/test/java/com/linkedin/datahub/graphql/resolvers/domain/SetDomainResolverTest.java) as an example.

7. [Optional] Sometimes, your new entity will have relationships to other entities, or fields that require specific business logic
as opposed to basic mapping from the GMS model. In such cases, we tend to create an entity-specific configuration method in [GmsGraphQLEngine.java](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/GmsGraphQLEngine.java)
which allows you to wire custom resolvers (DataFetchers) to the fields in your Entity type. You also may need to do this, depending
on the complexity of the new entity. See [here](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/GmsGraphQLEngine.java#L438) for reference. 

> Note: If you want your new Entity to be "browsable" (folder navigation) via the UI, make sure you implement the `BrowsableEntityType` interface.

#### Enabling Search for a new Entity 

In order to enable searching an Entity, you'll need to modify the [SearchAcrossEntities.java](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/search/SearchAcrossEntitiesResolver.java) resolver, which enables unified search
across all DataHub entities. 

Steps: 

1. Add your new Entity type to [this list](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/search/SearchAcrossEntitiesResolver.java#L32).
2. Add a new statement to [UrnToEntityMapper.java](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/types/common/mappers/UrnToEntityMapper.java#L35). This maps
an URN to a "placeholder" GraphQL entity which is subsequently resolved by the GraphQL engine.

That should be it! 

Now, you can try to issue a search for the new entities you've ingested 
