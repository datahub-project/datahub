- Start Date: 12/17/2020
- RFC PR: 2042
- Implementation PR(s): 2044

# GraphQL Frontend (Part 1)

## Summary

This RFC outlines a proposal to implement a GraphQL specification in `datahub-frontend`. Ultimately, this proposal aims to model the following using GraphQL:

1. Reads against the Metadata Catalog (P1)
2. Writes against the Metadata Catalog (P2)
3. Auth, Search, & Browse against the Metadata Catalog (P2) 

We propose that this initiative take place in phases, starting with CRUD-style read support against the entities, aspects, and relationships comprising the DataHub catalog. The scope of this RFC is limited to Part 1: reading against the catalog. It will cover the topics of introducing a dedicated GraphQL endpoint into ``datahub-frontend`` and provide a recipe for onboarding GMS entities to GQL. Subsequent RFCs will address writing, searching, and browsing against the catalog. 

Along with the RFC, we've included a proof-of-concept demonstrating partial GQL read support, showcasing `Dataset` and its relationship to `CorpUser`. The following files will be useful to reference as you read along: 
- `datahub-frontend.graphql` - Where the GQL Schema is defined. 
- `datahub-frontend/app/conf/routes` - Where the frontend API routes are defined.
- `datahub-frontend/app/controllers/GraphQLController.java` - The entry point for executing GQL queries. 
- `datahub-frontend/app/graphql/resolvers` - Definition of GQL DataFetchers, discussed below. 
- `datahub-dao` - Module containing the DAOs used in fetching downstream data from GMS

It is important to note that there are some questions that would be best discussed among the community, especially those pertaining to modeling of the Metadata Catalog on the frontend. These will be covered in the **Unresolved Questions** section below.


## Motivation
Exposing a GQL API for client-side apps has numerous benefits with respect to developer productivity, maintainability, performance among other things. This RFC will not attempt to fully enumerate the benefits of GraphQL as an IDL. For a more in-depth look at these advantages, [this](https://www.apollographql.com/docs/intro/benefits/) is a good place to start.

We will provide a few reasons GraphQL is particularly suited for DataHub:
- **Reflects Reality**: The metadata managed by DataHub can naturally be represented by a graph. Providing the ability to query it as such will not only lead to a more intuitive experience for client-side developers, but also provide more numerous opportunities for code reuse within both client side apps and the frontend server. 
  
- **Minimizes Surface Area**: Different frontend use cases typically require differing quantities of information. This is shown by the numerous subresource endpoints exposed in the current api: `/datasets/urn/owners`, `/dataset/urn/institutionalmemory`, etc. Instead of requiring the creation of these endpoints individually, GraphQL allows the client to ask for exactly what it needs, in doing so reducing the number of endpoints that need to be maintained.  
  
- **Reduces API Calls**: Frontend apps are naturally oriented around pages, the data for which can typically be represented using a single document (view). DataHub is no exception. GraphQL allows frontend developers to easily materialize those views, without requiring complex frontend logic to coordinate multiple calls to the API.  

## Detailed design

This section will outline the changes required to introduce a GQL support within ``datahub-frontend``, along with a description of how we can model GMA entities in the graph.

At a high level, the following changes will be made within `datahub-frontend`: 
1. Define a GraphQL Schema spec (datahub-frontend.graphql)
   
2. Configure a `/graphql` endpoint accepting POST requests (routes) 
   
3. Introduce a dedicated `GraphQL` Play Controller
   
   a. Configure GraphQL Engine
   
   b. Parse, validate, & prepare inbound queries
   
   c. Execute Queries 
   
   d. Format & send client response
   
We will continue by taking a detailed look at each step. Throughout the design, we will reference the existing `Dataset` entity, its `Ownership` aspect, and its relationship to the `CorpUser` entity for purposes of illustration.

### Defining the GraphQL Schema 
GraphQL APIs must have a corresponding schema, representing the types & relationships present in the graph. This is usually defined centrally within a `.graphql` file. We've introduced `datahub-frontend/app/conf/datahub-frontend.graphql`
for this purpose:

*datahub-frontend.graphql*
```graphql
schema {
    query: Query
}

type Query {
    dataset(urn: String): Dataset
}

type Dataset {
   urn: String!
   ....
}

type CorpUser {
   urn: String!
   ....
}

...
```
There are two classes of object types within the GraphQL spec: **user-defined** types and **system** types. 

**User-defined** types model the entities & relationships in your domain model. In the case of DataHub: Datasets, Users, Metrics, Schemas, & more. These types can reference one another in composition, creating edges among types.

**System** types include special "root" types which provide entry points to the graph:
- `Query`: Reads against the graph (covered in RFC 2042)
- `Mutation`: Writes against the graph 
- `Subscription`: Subscribing to changes within the graph

In this design, we will focus on the `Query` type. Based on the types defined above, the following would be a valid GQL query: 

*Example Query*
```graphql
query datasets($urn: String!) { 
    dataset(urn: $urn) { 
        ownership {
           owner {
              username
           }
        }
    }
}
```

For more details about the GraphQL Schemas & Types, see [here](https://graphql.org/learn/schema/). 


### Configuring a GraphQL Endpoint
All GraphQL queries are serviced via a single endpoint. We place the new POST route in `datahub-frontend/conf/routes`:

```POST          /api/v2/graphql                                                 react.controllers.GraphQLController.execute()```

We also provide an implementation of a GraphQL Play Controller, exposing an "execute" method. The controller is responsible for 
- parsing & validating incoming queries
- delegating execution of the query
- formatting the client response

### Executing a Query

For executing queries, we will use the [graphql-java](https://www.graphql-java.com/) library. 

There are 2 components provided to the engine that enable execution:  
1. **Data Resolvers**: Resolve individual projections of a query. Defined for top-level entities and foreign key relationship fields

2. **Data Loaders**: Efficiently load data required to resolve field(s) by aggregating calls to downstream data sources

**Data Resolvers**

During read queries, the library "resolves" each field in the selection set of the query. It does so by intelligently invoking user-provided classes extending `DataFetcher`. These 'resolvers' define how to fetch a particular field in the graph. Once implemented, 'resolvers' must be registered with the query engine.

In DataHub's case, resolvers will be required for
- Entities available for query (query type fields)
- Relationships available for traversal (foreign-key fields)

*Defining resolvers* 

Below you'll find sample resolvers corresponding to
- the `dataset` query type defined above (query field resolver)
- an `owner` field within the Ownership aspect (user-defined field resolver, foreign key reference)

```java
/**
 * Resolver responsible for resolving the 'dataset' field of Query
 */
public class DatasetResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {
    @Override
    public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
        final DataLoader<String, Dataset> dataLoader = environment.getDataLoader("datasetLoader");
        return dataLoader.load(environment.getArgument("urn"))
                .thenApply(RecordTemplate::data);
    }
}

/**
 * Resolver responsible for resolving the 'owner' field of Ownership.
 */
public class OwnerResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {
    @Override
    public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
        final Map<String, Object> parent = environment.getSource();
        final DataLoader<String, CorpUser> dataLoader = environment.getDataLoader("corpUserLoader");
        return dataLoader.load((String) parent.get("owner"))
                .thenApply(RecordTemplate::data);
    }
}
```
Resolvers serve to load the correct data when requested by the GraphQL engine using the ``get`` method. Provided as input to resolvers include:
- parent field resolver result
- optional arguments 
- optional context object
- query variable map

For a more detailed look of resolvers in graphql-java, check out the [Data Fetching](https://www.graphql-java.com/documentation/v11/data-fetching/) documentation. 

*Registering resolvers*

To register resolvers, we first construct a `RuntimeWiring` object provided by graphql-java: 

```java
private static RuntimeWiring configureResolvers() {
    /*
     * Register GraphQL field Resolvers.
     */
    return newRuntimeWiring()
    /*
     * Query Resolvers
     */
    .type("Query", typeWiring -> typeWiring
        .dataFetcher("dataset", new DatasetResolver())
    )
    /*
     * Relationship Resolvers
     */
    .type("Owner", typeWiring -> typeWiring
        .dataFetcher("owner", new OwnerResolver())
    )
    .build();
}
```
This tells the engine which classes should be invoked to resolve which fields. 

The `RuntimeWiring` object is then used to create a GraphQL engine: 
```java
GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, configureResolvers());
GraphQL engine = GraphQL.newGraphQL(graphQLSchema).build();
```

You'll notice within the resolvers we use ``DataLoaders`` to materialize the desired data. We'll discuss these next. 


**Data Loaders**

DataLoaders are an abstraction provided by ``graphql-java`` to make retrieval of data from downstream sources more efficient, by batching calls for the same data types. 
DataLoaders are defined and registered with the GraphQL engine for each entity type to be loaded from a remote source. 

*Defining a DataLoader*

Below you'll find sample loaders corresponding to the ``Dataset`` and ``CorpUser`` GMA entities.

```java
// Create Dataset Loader
BatchLoader<String, com.linkedin.dataset.Dataset> datasetBatchLoader = new BatchLoader<String, com.linkedin.dataset.Dataset>() {
    @Override
    public CompletionStage<List<com.linkedin.dataset.Dataset>> load(List<String> keys) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return DaoFactory.getDatasetsDao().getDatasets(keys);
            } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
            }
        });
        }
    };
DataLoader datasetLoader = DataLoader.newDataLoader(datasetBatchLoader);

// Create CorpUser Loader
BatchLoader<String, com.linkedin.identity.CorpUser> corpUserBatchLoader = new BatchLoader<String, com.linkedin.identity.CorpUser>() {
    @Override
    public CompletionStage<List<com.linkedin.identity.CorpUser>> load(List<String> keys) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return DaoFactory.getCorpUsersDao().getCorpUsers(keys);
            } catch (Exception e) {
                throw new RuntimeException("Failed to batch load CorpUsers", e);
            }
        });
    }
};
DataLoader corpUserLoader = DataLoader.newDataLoader(corpUserBatchLoader);
```
In extending `BatchLoader`, a single batch "load" method must be provided. This API is exploited by the GraphQL engine, which aggregates calls for entities of the same type, reducing the number of downstream calls made within a single query. 

*Registering a DataLoader* 

DataLoaders are registered with the `DataLoaderRegistry` and subsequently included as input to the GraphQL engine:

```java
/*
 * Build DataLoader Registry
 */
DataLoaderRegistry registry = new DataLoaderRegistry();
registry.register("datasetLoader", datasetLoader);
registry.register("corpUserLoader", corpUserLoader);

/*
 * Construct execution input
 */
ExecutionInput executionInput = ExecutionInput.newExecutionInput()
.query(queryJson.asText())
.variables(variables)
.dataLoaderRegistry(register)
.build();

/*
 * Execute GraphQL Query
 */
ExecutionResult executionResult = _engine.execute(executionInput);
```

For more information about `DataLoaders` see the [Using DataLoader](https://www.graphql-java.com/documentation/v15/batching/) doc. 

For a full reference implementation of the query execution process, see the  ``GraphController.java`` class associated with this PR. 

### Bonus: Instrumentation
[graphql-java](https://www.graphql-java.com/) provides an [Instrumentation](https://github.com/graphql-java/graphql-java/blob/master/src/main/java/graphql/execution/instrumentation/Instrumentation.java) interface that can be implemented to record information about steps in the query execution process.

Conveniently, `graphql-java` provides a `TracingInstrumentation` implementation out of the box. This can be used to gain a deeper understanding of the performance of queries, by capturing granular (ie. field-level) tracing metrics for each query. This tracing information is included in the engine `ExecutionResult`. From there it can be sent to a remote monitoring service, logged, or simply provided as part of the GraphQL response.

For now, we will simply return the tracing results in the "extensions" portion of the GQL response, as described in [this doc](https://github.com/apollographql/apollo-tracing). In future designs, we can consider providing a more formal extension points for injecting custom remote monitoring logic. 


### Modeling Queries

The first phase of the GQL rollout will support primary-key lookups of GMA entities and projection of their associated aspects. In order to achieve this, we will
- model entities, aspects, and the relationships among them
- expose queries against top-level entities
using GQL. 

The proposed steps for onboarding a GMA entity, it's related aspects, and the relationships among them will be outlined next. 

#### 1. **Model an entity in GQL**

The entity model should include its GMA aspects, as shown below, as simple object fields. The client will need not be intimately aware of the concept of "aspects". Instead, it should be concerned with entities and their relationships.

*Modeling Dataset*
```graphql
"""
Represents the GMA Dataset Entity
"""
type Dataset {

    urn: String!

    platform: String!

    name: String!

    origin: FabricType

    description: String

    uri: String

    platformNativeType: PlatformNativeType

    tags: [String]!

    properties: [PropertyTuple]

    createdTime: Long!

    modifiedTime: Long!

    ownership: Ownership
}

"""
Represents Ownership
"""
type Ownership {

    owners: [Owner]

    lastModified: Long!
}

"""
Represents an Owner
"""
type Owner {
    """
     The fully-resolved owner
    """
    owner: CorpUser!

    """
     The type of the ownership
    """
    type: OwnershipType

    """
     Source information for the ownership
    """
    source: OwnershipSource
}
```

Notice that the Dataset's ``Ownership`` aspect includes a nested ``Owner`` field that references a ``CorpUser`` type. In the GMS model, this is represented as a foreign-key relationship (urn). In GraphQL, we include the *resolved* relationship, allowing the client to easily retrieve information about a Dataset's owners.

To support traversal of this relationship, we additionally include a ``CorpUser`` type:

```graphql
"""
Represents the CorpUser GMA Entity
"""
type CorpUser {

    urn: String!

    username: String!

    info: CorpUserInfo

    editableInfo: CorpUserEditableInfo
}
```

#### 2. **Extend the 'Query' type**: 

GraphQL defines a top-level 'Query' type that serves as the entry point for reads against the graph. This is extended to support querying the new entity type.

```graphql
type Query {
    dataset(urn: String!): Dataset # Add this! 
    datasets(urn: [String]!): [Dataset] # Or if batch support required, add this! 
}
```

#### 3. **Define & Register DataLoaders**
This is illustrated in the previous section. It involves extending `DataLoader` and registering the loader in the `DataLoaderRegistry`.

#### 4. **Define & Register DataFetcher (Data Resolver)**
This is illustrated in the previous section. It involves implementing the DataFetcher interface and attaching it to fields in the graph.

#### 5. **Query your new entity**
Deploy & start issuing queries against the graph. 

```graphql
// Input
query datasets($urn: String!) { 
    dataset(urn: $urn) { 
        urn 
        ownership {
            owners {
                owner {
                    username
                }
            }
        }
    }
}

// Output 
{
    "data": {
        "dataset": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
            "ownership": {
                "owners": [
                    {
                        "owner": {
                            "username": "james joyce",
                        }
                    }
                ]
            }
        }
    }
}
```

**Disclaimer**

It is the intention that the GraphQL type system be **auto-generated** based on the PDL models declared at the GMA layer. This means that the frontend schema should need not be maintained separately from the GMA it is derived from. 

Various changes will be required to accomplish this:

1. Introduce `readOnly` annotation into GMA PDLs used to identify which field should *not* be writable.
2. Introduce `relationship` annotation into GMA PDLs used to declare foreign-key relationships (alternate to the relationship + pairings convention that exists today)
3. Implement a GraphQL schema generator that can be configured to 
      - Load relevant entity PDLs
      - Generate `Query` types, including resolved relationships
      - Generate `Mutation` types, omitting specific fields 
      - Write generated types to GraphQL schema file 
      - Run as a build-time gradle task

We omit proposal of this portion of the design from this RFC. There will be a subsequent RFC proposing implementation of the items above.

*Auto-generated GQL resolvers generation should also be explored. This would be possible provided that standardized DAOs are available at the frontend service layer. It'd be incredible if we could
- Auto generate "client" objects from a rest spec
- Auto generate "dao" objects that use a client
- Auto generate "resolvers" that use a "dao" 

## How we teach this

We will create user guides that cover: 
- Modeling & onboarding entities to the GQL type system
- Modeling & onboarding entity aspects to the GQL type system
- Modeling & onboarding relationships among entities to the GQL type system 

## Alternatives
Keep the resource-oriented approach, with different endpoints for each entity / aspect. 

## Rollout / Adoption Strategy

1. Rollout CRUD-style reads against entities / aspects in the graph
      - Entities: Dataset, CorpUser, DataPlatform
      - Relationships: Dataset->CorpUser, Dataset->DataPlatform, [if there is demand] CorpUser -> Dataset
2. Rollout CRUD-style writes against entities / aspects in the graph
3. Rollout full text search against entities / aspects in the graph
4. Rollout browse against entities / aspects in the graph
5. Migrate client-side apps to leverage new GraphQL API 
    - Build out parallel data fecthing layer, where the existing models are populated either by the old clients or the new GQL clients. Place this behind a client-side feature flag. 
    - Allow users of DataHub to configure which API they want to run with, swap at their own pace.
    
## Unresolved questions

**How should aspects be modeled on the GQL Graph?**

Aspects should be modeled as nothing more than fields on their parent entity. The frontend clients should not require understanding of the aspect concept. Instead, fetching specific aspects should be a matter of querying the parent entities with particular projections. 

For example, retrieving the `Ownership` aspect of `Datasets` is done using the following GQL query:

```graphql
query datasets($urn: String!) { 
    dataset(urn: $urn) { 
        ownership {
            owners {
                owner {
                    username
                }
            }
        }
    }
}
```

as opposed to the following: 

```graphql
query ownership($datasetUrn: String!) { 
    ownership(datasetUrn: $datasetUrn) { 
        owners {
            owner {
                username
            }
        }
    }
}
```

Aspects should not be exposed in the top-level 'Query' model. 

**What should the GraphQL graph model contain? How should it be constructed?**

There are 2 primary options: 

1. Directly expose transposed GMS models (entities, aspects as is) via the GQL type system. One alteration would be introducing resolved relationships extending outward from the aspect objects.
- Pros: Simpler because no new POJOs need to be maintained at the `datahub-frontend` layer (can reuse the Rest.li models provided by GMS). In the longer term, GQL type system can be generated directly from GMS Pegasus models.
- Cons: Exposes frontend clients to the entire GMA graph, much of which may not be useful for presentation
2. Create a brand new presentation-layer Graph model
- Pros: Only expose what frontend clients need from the graph (simplifies topology)
- Cons: Requires that we maintain (perhaps generate) additional POJOs specific to the presentation layer (maintenance cost?)

We're interested to get community feedback on this question! 

**Should foreign-keys corresponding to outbound relationships be included in the GQL schema?**

Using the example from above, that would entail a model like: 
```graphql
"""
Represents an Owner
"""
type Owner {
    """
    Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:product:product_name
    """
    ownerUrn: String!
    
    """
    The fully resolved owner!
    """
    owner: CorpUser!

    """
     The type of the ownership
    """
    type: OwnershipType

    """
     Source information for the ownership
    """
    source: OwnershipSource
}
```
We should *not* need to include such fields. This is because we can efficiently implement the following query:

```graphql
query datasets($urn: String!) { 
    dataset(urn: $urn) { 
        urn 
        ownership {
            owners {
                owner {
                    urn
                }
            }
        }
    }
}
```

without actually calling downstream services. By cleverly implementing the "owner" field resolver, we can return quickly when the urn is the only projection:

```java
/**
* GraphQL Resolver responsible for fetching Dataset owners.
  */
  public class OwnerResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {
      @Override
      public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
          final Map<String, Object> parent = environment.getSource();
          if (environment.getSelectionSet().contains("urn") && environment.getSelectionSet().getFields().size() == 1) {
            if (parent.get("owner") != null) {
               return CompletableFuture.completedFuture(ImmutableMap.of("urn", parent.get("owner"))) 
            }
          }
          ... else load as normal 
      }
  }
}
```

**How can I play with these changes?**

1. Apply changes from this branch locally
2. Launch datahub-gms & dependencies & populate with some data as usual
3. Launch `datahub-frontend` server using ``cd datahub-frontend/run && ./run-local-frontend``.
4. Authenticate yourself at http://localhost:9001 (username: datahub) and extract the PLAY_SESSION cookie that is set in your browser.  
5. Issue a GraphQL query using CURL or a tool like Postman. For example:
```
curl --location --request POST 'http://localhost:9001/api/v2/graphql' \
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--header 'Cookie: PLAY_SESSION=<your-cookie-here>' \
--data-raw '{"query":"query datasets($urn: String!) { 
\n    dataset(urn: $urn) { 
\n        urn 
\n        ownership {
\n            owners {
\n                owner {
\n                    username
\n                    info {
\n                        manager {
\n                            username 
\n                        }
\n                    }
\n                }
\n                type
\n                source {
\n                    type
\n                    url
\n                }
\n            }\n        }
\n        platform
\n    }
\n}",
"variables":{"urn":"<your dataset urn>"}}'```


