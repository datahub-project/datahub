Notice: `datahub-graphql-core` is currently in beta, and as such is currently subject to backwards incompatible changes. 

# DataHub GraphQL Core
DataHub GraphQL API is a shared lib module containing a GraphQL API on top of the GMS service layer. It exposes a graph-based representation
permitting reads and writes against the entities and aspects on the Metadata Graph, including Datasets, CorpUsers, & more. 

Contained within this module are 

1. **GMS Schema**: A GQL schema that based on GMS models, located under `resources/gms.graphql`. 
2. **GMS Data Fetchers**: Components used by the GraphQL engine to resolve individual fields in the GQL schema.
3. **GMS Data Loaders**: Components used by the GraphQL engine to fetch data from downstream sources efficiently (by batching). 
4. **GraphQLEngine**: A wrapper on top of the default `GraphQL` object provided by `graphql-java`. Provides a way to configure all of the important stuff using a simple `Builder API`. 
5. **GMSGraphQLEngine**: An engine capable of resolving the GMS schema using the data fetchers + loaders mentioned above (with no additional configuration required). 

We've chosen to place these components in a library module so that GraphQL servers can be deployed in multiple "modes":

1. **Standalone**: GraphQL facade, mainly used for programmatic access to the GMS graph from a non-Java environment
2. **Embedded**: Leverageable within another Java server to surface an extended GraphQL schema. For example, we use this to extend the GMS GraphQL schema in `datahub-frontend` 


## Extending the Graph

### Near Term

When extending the GMS graph, the following steps should be followed:

1. Extend `gms.graphql` schema with new `types` (Queries) or `inputs` (Mutations). 
   
These should generally mirror the GMS models exactly, with notable exceptions:

- Maps: the GQL model must instead contain a list of { key, value } objects (e.g. Dataset.pdl 'properties' field)
- Foreign-Keys: Foreign-key references embedded in GMS models should be resolved if the referenced entity exists in the GQL schema,
replacing the key with the actual entity model. (Example: replacing the 'owner' urn field in 'Ownership' with an actual `CorpUser` type)

2. Implement `DataLoaders` for any `Query` data 

- DataLoaders should simply wrap GMS-provided clients to fetch data from GMS API.

3. Implement `Mappers` to transform Pegasus model returned by GMS to an auto-generated GQL POJO. (under `/mainGeneratedGraphQL`, generated on `./gradlew datahub-graphql-core:build`) 

- If you've followed the guidance above, these mappers should be simple, mainly
providing identity mappings for fields that exist in both the GQL + Pegasus POJOs.
- In some cases, you'll need to perform small lambdas (unions, maps) to materialize the GQL object. 

4. Implement `DataFetchers` for any entity-type fields 

- Each field which resolvers a full entity from a particular downstream GMS API should have it's owner resolver, 
which leverages any DataLoaders implemented in step 2 in the case of `Queries`.
- Resolvers should always return an auto-generated GQL POJO (under `/mainGeneratedGraphQL`) to minimize the risk of runtime exceptions 
  
5. Implement `DataFetcher` unit tests


### Long Term

Eventually, much of this is intended to be automatically generated from GMS models, including

- Generation of the primary entities on the GQL graph 
- Generation of Pegasus to GQL mapper logic
- Generation of DataLoaders
- Generatation of DataFetchers (Resolvers)
