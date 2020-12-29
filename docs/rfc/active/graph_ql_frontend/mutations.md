- Start Date: 12/28/2020
- RFC PR: ?
- Implementation PR(s): ?

# GraphQL Frontend (Part 2)

## Summary

This document comprises the second installment of a proposal to implement a GraphQL specification in `datahub-frontend`. It builds on RFC 2042, which details support for *reads* against the Metadata Catalog, in proposing a path for supporting *writes* against the catalog.

Specifically, we will begin by outlining a technical design for supporting GraphQL `Mutations`, with a focus on modeling convention, runtime execution, authorization, & error handling. We will conclude by briefly describing how we can leverage GraphQL queries and mutations to implement authentication, search, & browse functionality. 

Along with this document, we've included a proof-of-concept showcasing reference implementations of
- support for writes against the `Dataset` entity's `Ownership` aspect
- authentication, search, and browse functionality in GraphQL, which are required to achieve functional parity with existing API (applying what we've learned about `queries` & `mutations`!)

The following files will be useful to reference as you read along:
- `datahub-frontend.graphql` - Where the GQL Schema is defined.
- `datahub-frontend/app/conf/routes` - Where the frontend API routes are defined.
- `datahub-frontend/app/controllers/GraphQLController.java` - The entry point for executing GQL queries.
- `datahub-frontend/app/graphql/resolvers` - Definition of GQL DataFetchers.
- `datahub-dao` - Module containing the DAOs used for writing data to GMS


## Motivation
Please refer to the **Motivation** section of the original GQL Frontend RFC (2042). 

## Detailed design

This section will outline the changes required to introduce a GQL write support within ``datahub-frontend``, building upon the foundational building blocks introduced in RFC 2042.

At a high level, the following changes will be made within `datahub-frontend`:
1. Extend the GraphQL Schema spec to model mutations (datahub-frontend.graphql)

2. Implement resolvers (DataFetchers) for each mutation & register with GraphQL engine

We will continue by taking a detailed look at these steps. Throughout the design, we will reference the existing `Dataset` entity, its `Ownership` aspect, and its relationship to the `CorpUser` entity for purposes of illustration.

### Modeling Mutations
GraphQL APIs must have a corresponding schema, representing the types & relationships present in the graph. This is usually defined centrally within a `.graphql` schema file. We've introduced `datahub-frontend.graphql`
for this purpose.

A quick refresher: There are two categories of 'type' present in the GraphQL spec: **user-defined** types and **system** types.

**User-defined** types model the entities & relationships in your domain model. In the case of DataHub: Datasets, Users, Metrics, Schemas, & more. 

**System** types include special "root" types which provide entry points to the graph:
- `Query`: Reads against the graph (covered in RFC 2042)
- `Mutation`: Writes against the graph 
- `Subscription`: Subscribing to changes within the graph

In this design, we focus on extending our GQL schema to model `Mutations`. Let's start by looking at how we can model mutations.

**The Approach**

Continuing the theme of **entity-oriented** client side operations, we propose limiting mutation to the entity level. The avoids leaking the GMA entity-aspect distinction into the client side, simplifying the model for frontend development and minimizing the effort required to evolve GMA entities. 

We will extend our GQL schema to optionally include an 'update' mutation on a per-entity basis, which permits updating or creating aspect(s) associated with an entity 

Let's look at an example of introducing these mutation types into our GQL schema for the `Dataset` entity: 

*datahub-frontend.graphql*
```graphql
type Mutation {
    updateDataset(input: DatasetUpdateInput!): Dataset # Update then fetch a dataset
}

input DatasetUpdateInput {
   urn: String!
   ownership: OwnershipUpdate # optional
   ... other aspect update models 
}

input OwnershipUpdate {
   owners: [OwnerUpdate!]
}

input OwnerUpdate {
   # The owner URN, eg urn:li:corpuser:1
   owner: String!
   
   # The owner role type
   type: OwnershipType!
}

enum OwnershipType {
    DEVELOPER
    DATAOWNER
    DELEGATE
    PRODUCER
    CONSUMER 
    STAKEHOLDER
}
...
```
A few things to note:

- Each type provided as input to a mutation must be declared using the `input` type qualifier
- `input` types mirror the shape of `aspect` models, but omit those fields that should not be writable (ie. no "lastModified" field available to write)
- input fields aside from the primary key (urn) are *optional*, meaning partial update is permitted
- update / delete mutations are modeled at the `Dataset` level. The `DatasetUpdateInput` represents a "patch" object to be upserted into GMA.

Using this modeling approach, we can choose to include or exclude specific entity aspects that we'd like to expose for mutation. For example, if we don't want to allow changes to a `Dataset`'s schema metadata, we can omit inclusion of an `input` object representing the `Schema` aspect.

This modeling convention is

1. **Simple**: Continuing the convention introduced on the read side, we want to keep the API logically simple by orienting operations around top-level entities, as opposed to co-locating operations against entities and aspects. Keeping the model hierarchical simplifies the frontend development model. 

2. **Easily Extensible**: Adding support for writes against new aspects means adding a single new `input` type, and implementing execution logic (detailed in subsequent sections). There's no need to introduce a new mutation type.

*Example: Updating a Dataset's Owners*

Assuming the modeling above, the following query would update a Dataset's ownership details & retrieve the updated ownership information: 

```graphql
mutation updateDataset($input: DatasetUpdateInput!) { 
    updateDataset(input: $input) { 
        ownership {
           owners {
               owner {
                  urn
               }
               type
           }
        }
    }
}

variables: {
   input: {
      urn: "urn:li:dataset:123",
      ownership: {
         owners: [
            {
               owner: "urn:li:corpUser:1" # fk relationship 
               type: "DATAOWNER" 
            }, 
            {
               owner: "urn:li:corpUser:2" # fk relationship 
               type: "DELEGATE" 
            }, 
         ]
      }
   }
}
```
And return 
```graphql
 {
   ownership: {
      owners: [
         {
            owner: {
               urn: "urn:li:corpUser:1"
            }
            type: "DATAOWNER" 
         }, 
         {
            owner: {
               urn: "urn:li:corpUser:2"
            }
            type: "DELEGATE" 
         } 
      ]
   }
}
```

Alternative modeling conventions will be considered more deeply in the **Alternatives** section below. For the remainder of the design, we will assume the modeling strategy shown above.

For more details about the GraphQL Schemas & Types, see [here](https://graphql.org/learn/schema/).

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

*Auto-generated GQL resolvers generation should also be explored. This would be possible provided that standardized DAOs are available at the frontend service layer. Specifically, it'd be incredible if we could
- Auto generate "client" objects from a rest spec
- Auto generate "dao" objects that use a client
- Auto generate "resolvers" that use a "dao" 

### Executing a Mutation

For executing mutations, we will rely on the [graphql-java](https://www.graphql-java.com/) library.

As a prerequisite, we'll need to 

1. implement a GraphQL resolver 
2. register the GraphQL resolver

for each entity + operation combination. The concept of resolvers has been already introduced in RFC 2042. 

**The Approach** 

In the case of mutations, resolvers will be responsible for performing an atomic update of an entity's state. This may involve simultaneously updating different aspects for the same entity. To achieve this without the need for supporting distributed transactions, we'll leverage the "ingest" API exposed by GMA for each top-level entity. This API accepts an entity `snapshot` (eg. `DatasetSnapshot`) and updates each aspect included within. 

This approach stands in contrast to the existing recommendation around updating an entity's aspects via the GMA Rest.li API. The current recommendation involves writing to aspect-oriented Rest.li sub-resources (eg. datasets/:urn/ownership). This approach means that each request can only change one aspect. Because the proposed GQL API permits mutation of multiple entity aspects within a mutation query, adopting this approach would require implementing a transaction layer capable of preventing inconsistent states. Moreover, it would require more extensive changes across the stack to introduce new aspects to existing entities.

*Folks from DH have noted that the entity "ingest" API is intended to be private, only used by the MCE consumer logic. We propose formal support for this API (or something of equivalent function) for atomically updating multiple entity aspects at one time.

*A related point worth considering is the modeling inconsistency in APIs exposed by GMA. Today, we are able to *read* entity models like `Dataset.pdl` but are unable to *write* using this model. We should aim for clarity & consistency in the model exposed to clients of GMA by exposing `Entity` or `Snapshot` models for both read *&* write -- not a mixture of both. To work around this inconsistency, the sample code provided with this RFC includes entity-to-snapshot conversion hidden within the `Datasets` DAO, thereby allowing the API layer to concern itself only with the canonical entity model.  


We will continue the design assuming that the "ingest" API (or an endpoint of functional parity) is available for use within `datahub-frontend`. Let's now turn to an example of implementing a resolver to handle updates against the `Dataset` entity. 



#### Implementing a resolver

Let's look at an example of implementing the resolver responsible for implementing the "updateDataset" mutation introduced above.

*Example: Implementing the Dataset Update Resolver*

*UpdateDatasetResolver.java*
```java
/**
 * Resolver responsible for updating a Dataset
 */
public class UpdateDatasetResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {

   @Override
   public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
     /*
         Extract arguments
      */
      final Map<String, Object> input = environment.getArgument(INPUT_FIELD_NAME);
      final String datasetUrn = (String) input.get(URN_FIELD_NAME);

     /*
         Update Dataset using the input object. Type system assumed to be compatible with the public
         Dataset.pdl model.
      */
      try {
         Dataset dataset = new Dataset(GraphQLUtil.toDataMap(input));
         DaoFactory.getDatasetsDao().updateDataset(datasetUrn, dataset);
      } catch (Exception e) {
         throw new RuntimeException(String.format("Failed to update dataset with URN %s", datasetUrn), e);
      }

     /*
         Fetch the latest version of the dataset.
      */
      final DataLoader<String, Dataset> dataLoader = environment.getDataLoader(DATASET_LOADER_NAME);
      return dataLoader.load(datasetUrn)
              .thenApply(RecordTemplate::data);
   }
}
```

Things to note:

1. The 'updateDataset' method on the `DatasetsDao` is responsible for 
   - Transforming the partial "Dataset" RecordTemplate into a "DatasetSnapshot"
   - Calling the "ingest" API exposed by the `Datasets` GMA resource
   See `DatasetsDao.java` and `com.linkedin.dataset.client.Datasets.java` in the sample code for reference
2. We return the newly-written entity data by reusing `DataLoaders` (introduced in RFC 2042)
3. This example does not work with strongly-typed objects. In practice, we should find a way to generate type-safe objects from our GQL schema, so as to avoid working with raw maps in resolvers.

##### Authorizating a mutation 
By default, a `QueryContext` object will be accessible within all resolvers using the "environment" object provided by graphql-java. 

The context object will contain 

- The authenticated session cookie (PLAY_SESSION)
- The authenticated user details (eg user name)

Which may be leveraged to authorize write operations inside a resolver implementation. 


*Example: Authorizing a mutation*

Imagine we want to prevent a user from updating someone else's information. Within a resolver, we'd compare the authenticated user to the user to be updated, and deny the request if they are different 

```java
/**
 * Resolver responsible for updating a CorpUser
 */
public class UpdateCorpUserResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {

    @Override
    public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {

       final String userUrn = (String) input.get("urn");
       final QueryContext context = environment.getContext();
       final String currentUserUrn = createCorpUserUrn(context.getUser());

       if (userUrn != currentUserUrn) {
          throw new AuthorizationError("Unauthorized to update CorpUser with urn " + userUrn);
       }
    }
        ....
          
```
#### Registering a resolver

To register the new resolver, simply modify the "configureResolvers" method within `GraphQLController.java`

```java
private static RuntimeWiring configureResolvers() {
   /*
   * Register GraphQL Resolvers.
   */
   return newRuntimeWiring()
      /*
      * Mutation Resolvers
      */
      .type("Mutation", typeWiring -> typeWiring 
           .dataFetcher("updateDataset", new UpdateDatasetResolver()) // Add this!
      )
      ....
      .build();
}
```

#### Error Handling 

If your mutation fails, the reason will appear in the "errors" portion of the standard GraphQL response. Specifically we'll include a message + path associated with the error. Additionally we'll define 2 custom extensions to `GraphQLError`:

- **Authorization error**: Unable to apply mutation to entity
- **Value Validation error**: Failed to validate mutation input data (value constraints, not shape) 

For usage within your resolvers. For more details about error response in GraphQL, see [Errors](https://spec.graphql.org/June2018/#sec-Errors) in the GQL spec.

### Bonus: Supporting authentication, search, & browse

This section will briefly outline our approach to mapping existing frontend functionality to the GraphQL world. 

1. **Authentication** - Logging a user in & authenticating their identity in subsequent requests 
2. **Search** - Full text search & autocomplete against the catalog 
3. **Browse** - Browse + filter lists of catalog entities

#### Authentication

There are 2 pieces to authentication:

1. Logging a user in 
2. Authenticating requests from a logged-in user

**Logging In**

To sign a user in, we introduce a `logIn` mutation type to our GQL schema. 

```graphql
type Mutation {
    ...
    logIn(username: String!, password: String!): CorpUser
}
```
which will accept 'username' and 'password' arguments & return the corresponding `CorpUser` object.

Under the hood, the resolver responsible for implementing this mutation will invoke the `AuthenticationManager` to authenticate based on the provided credentials & initialize a Play session object for the authenticated user. This will result in the Play server setting a PLAY_SESSION cookie in the response, which is used to authenticate future requests from the client. This mirrors the login in the existing "authenticate" route within the `Application` controller. 

For reference, check out `LogInResolver.java` included in the PoC PR. 

**Authenticating Requests**

To authenticate requests from a signed-in user, we introduce an abstract base GraphQL resolver which is responsible for validating the current client's access token, in turn depending on the `QueryContext` object which internally stores the Play-provided "session" object. Any GraphQL resolver that needs to be behind an authentication wall should extend this base class, overriding the `authenticatedGet` method. 

For reference, check out `AuthenticatedResolver.java` included in the PoC PR.

#### Search 

There are 2 pieces of functionality comprising search:

1. Full-text search against DH entities 
2. AutoComplete of partial search text

**Full-text Search**

Full text search allows a user to query for entities of a particular type by a search term. To implement full-text search, we introduce the `search` query 

```graphql
type Query {
    ...
    search(input: SearchInput!): SearchResults
}
```

which accepts a`SearchInput` 

```graphql
input SearchInput {
	type: EntityType!
	query: String!
	start: Int
	count: Int
	filters: [FacetFilterInput]
}
```

and returns `SearchResults` 

```graphql
type SearchResults {
    start: Int!
    count: Int!
    total: Int!
    elements: [SearchResult]!
    facets: [FacetMetadata]
}

union SearchResult = Dataset | CorpUser
```

Behind the scenes, we implement a resolver which executes the downstream search against GMA and fits the returned data to the GQL schema.

For reference, check out `SearchResolver.java` included in the PoC PR.

**Auto Complete**

Auto complete involves providing suggestions as a user enters types in the search box. To implement auto-complete, we introduce an `autoComplete` query

```graphql
type Query {
    ....
    autoComplete(input: AutoCompleteInput!): AutoCompleteResults
}
```
which accepts an `AutoCompleteInput`

```graphql
input AutoCompleteInput {
	type: EntityType!
	query: String!
	field: String! # Field name
	limit: Int
	filters: [FacetFilterInput]
}
```

and returns `AutoCompelteResults`

```graphql
type AutoCompleteResults {toCom
	query: String!
	suggestions: [String]!
}
```

We also introduce a resolver for autocomplete. For reference, check out `AutoCompleteResolver.java` included in the PoC PR.

#### Browse

There are 2 pieces of functionality comprising the browse experience:

1. Browsing lists of entities by navigating through a tree of paths
2. Retrieving the list of full paths corresponding to a particular entity

**Browsing Entities**

Browsing entities requires the ability to retrieve all groups or entities under a particular path. To achieve, this we introduce a `browse` query 

```graphql
type Query {
    ...
    browse(input: BrowseInput!): BrowseResults
}
```

which accepts a`BrowseInput`

```graphql
input BrowseInput {
	type: EntityType!
	path: String
	start: Int
	count: Int
	filters: [FacetFilterInput]
}
```

and returns `BrowseResults`

```graphql
type BrowseResults {
	entities: [BrowseResultEntity]!
	start: Int!
	count: Int!
	total: Int!
	metadata: BrowseResultMetadata!
}

type BrowseResultEntity {
	name: String!
	urn: String!
}
```

Check out `AutoCompleteResolver.java` included in the PoC PR for reference. 

**Retrieve Browse Paths**

Retrieving the paths corresponding to an entity instance can be achieved via a `browsePaths` query

```graphql
type Query {
    ...
    browsePaths(input: BrowsePathsInput!): [String]
}
```

which accepts a`BrowsePathsInput`

```graphql
input BrowsePathsInput {
	type: EntityType!
	urn: String!
}
```

and returns a list of strings representing the paths associated the entity. 

Check out `BrowsePathsResolver.java` included in the PoC PR for reference.


## How we teach this

Once we have agreement on API-layer modeling, we will begin implementation of a PDL -> GQL conversion layer to ensure that the minimum number of different models need to be maintained. 

We will create user guides that cover:
- Modeling & evolving entities + their aspects 
- Generating GQL schema from PDLs models 


## Alternatives
### Alternatives to GQL

Keep the resource-oriented approach, with different endpoints for each aspect needing updated. 

### Alternate GQL Modeling Convention 

*Mutation-per-aspect*

An alternative to modeling mutations is to model top-level mutations on a per-aspect basis. 

Using the `Dataset`-`Ownership` example above, we'd model as follows: 

```graphql
type Mutation {
    updateDatasetOwnership(input: DatasetOwnershipUpdateInput!): Ownership
}

type DatasetOwnershipUpdateInput {
   urn: String!
   ownership: OwnershipInput!
}

type OwnershipInput {
   owners: [OwnerInput]
}

.... everything else remains the same
...
```

This means that mutations against entity aspects would each be issued independently. 

The pros of this convention:
- **Separation of Concerns**: Reduced likelihood that introducing a new aspect will impact existing behavior. Each aspect can have clearly segmented authorization at the top level. 
- **API-Layer Validation**: The API layer validates that mutations are valid without requiring runtime code checks

The cons of this convention: 
- **Mixing of Entities + Aspects**: Semantically it is confusing to model aspects and entities next to each other in this way. The `Query` side adopts an approach where clients need not understand the GMA distinction among entities & their aspects. Additionally, this will result in an explosion of the number of mutations at the top-level API layer. In my opinion, it is cleaner to orient operations hierarchically around fewer primary entities. 
- **More difficult to extend**: Introducing a new mutation will require adding a new top-level mutation type, in addition to the changes required by the proposed modeling approach.  

Feel free to let me know if we've missed other viable alternatives! This is a great topic for community discussion & debate. 

## Rollout / Adoption Strategy

Copied from RFC 2042: 

1. Rollout CRUD-style reads against entities / aspects in the graph
    - Entities: Dataset, CorpUser, DataPlatform
    - Relationships: Dataset->CorpUser, Dataset->DataPlatform, [if there is demand] CorpUser -> Dataset
2. Rollout CRUD-style writes against entities / aspects in the graph
3. Rollout login, full text search, browse against entities / aspects in the graph
5. Migrate client-side apps to leverage new GraphQL API
    - Build out parallel data fecthing layer, where the existing models are populated either by the old clients or the new GQL clients. Place this behind a client-side feature flag.
    - Allow users of DataHub to configure which API they want to run with, swap at their own pace.

## Questions

**How should aspects be modeled in mutations?**

As noted above, we are proposing a model where *entities* are the exclusive, top-level unit that can be updated. Aspects are modeled as nested fields within entities that must be updated independently (atomic units). 

We are *very* welcoming of alternate proposals on this topic, but feel that the proposed layout provides the best balance of readability, extensibility, and correctness-guarantees.

**Should "creates" against entities be supported?**

Don't see a super clear use case with the existing entities. However, in the future it may make sense to allow the creation of top-level entities from the UI (for example, business term set).

In such scenarios, we can additionally add "createX" mutation types modeled using the same general conventions. 

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
--header 'Cookie: PLAY_SESSION=<your-play-session>' \
--data-raw '{
   "query":
      "mutation updateDataset($input: DatasetUpdateInput!) { 
      \n    updateDataset(input: $input) { 
      \n        urn 
      \n        ownership {
      \n            owners {
      \n                owner {
      \n                    username
      \n                }
      \n            }
      \n        }
      \n    }
      \n}",
      "variables":{
         "input":{
            "urn": <your-dataset-urn>,
            "ownership":{
               "owners":
                  [
                     {"owner":"urn:li:corpuser:test","type":"DATAOWNER"}
                  ]
               }
            }
         }
      }'```


