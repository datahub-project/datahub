# Coupling Surface — Upgrade Checklist

Read this file on every DataHub version bump. For each row, verify the listed signature
still exists in the new version; if not, update the corresponding wrapper.

## Framework types (third-party — very stable)

| Type | Used in | Notes |
|---|---|---|
| `graphql.execution.instrumentation.Instrumentation` | `OwnershipInstrumentation` | graphql-java framework hook — stable since 14.x |
| `graphql.execution.instrumentation.SimplePerformantInstrumentation` | `OwnershipInstrumentation` | base class |
| `graphql.GraphQL.transform(Consumer<Builder>)` | `OwnershipFilterConfiguration.BeanPostProcessor` | stable public API |
| `graphql.GraphQL.getInstrumentation()` | same | stable public API |
| `graphql.schema.DataFetcher` | wrapping fetchers in instrumentation | core interface |

## DataHub types (project-internal)

| Type | Used in | Coupling kind |
|---|---|---|
| `com.linkedin.datahub.graphql.GraphQLEngine` (Spring bean `graphQLEngine`) | `BeanPostProcessor` target | Bean-name + reflection field name `_graphQL` |
| `com.linkedin.datahub.graphql.QueryContext` | accessed via `DataFetchingEnvironment.getGraphQlContext().get(QueryContext.class)` | API path |
| `com.datahub.authentication.group.GroupService.getGroupsForUser(opCtx, userUrn)` | `CachedGroupResolver` | method signature |
| `com.datahub.plugins.auth.authorization.Authorizer` interface | `OwnershipAuthorizer` | full interface contract |
| `com.linkedin.entity.client.SystemEntityClient.getV2(...)` | `OwnershipAuthorizer.init` | method signature |
| `com.linkedin.common.Ownership` PDL aspect | `OwnershipAuthorizer` | PDL schema |
| `com.linkedin.identity.GroupMembership` PDL aspect | `OwnershipAuthorizer` | PDL schema |
| `com.datahub.plugins.auth.authentication.Authenticator` interface | `KeycloakJwtAuthenticator` | full interface contract |
| `com.datahub.authentication.{Authentication,Actor,ActorType,AuthenticationRequest}` | `KeycloakJwtAuthenticator` | constructors / accessors |
| `com.linkedin.gms.factory.config.ConfigurationProvider` (Spring bean `configurationProvider`) | `keycloakAuthenticatorRegistrar` BeanPostProcessor | Bean-name + `getAuthentication()` |
| `com.datahub.authentication.{AuthenticationConfiguration,AuthenticatorConfiguration}` | registrar BeanPostProcessor | `getAuthenticators()/setAuthenticators()` + `setType/setConfigs` |
| `io.jsonwebtoken:jjwt 0.11.2` (`Jwts.parserBuilder`, `SigningKeyResolver`) | `KeycloakJwtAuthenticator`, `JwksSigningKeyResolver` | library API (0.12.x removed these — pin/verify on upgrade) |

## GraphQL field names (DataHub schema — stable contract)

Allowlist of GraphQL Query field names whose `DataFetcher` we wrap:

| Field name | Input type | Filter slot |
|---|---|---|
| `searchAcrossEntities` | `SearchAcrossEntitiesInput` | `orFilters: [AndFilterInput!]` |
| `scrollAcrossEntities` | `ScrollAcrossEntitiesInput` | `orFilters: [AndFilterInput!]` |
| `searchAcrossLineage` | `SearchAcrossLineageInput` | `orFilters: [AndFilterInput!]` |
| `scrollAcrossLineage` | `ScrollAcrossLineageInput` | `orFilters: [AndFilterInput!]` |
| `autoComplete` | `AutoCompleteInput` | `filters: [FacetFilterInput!]` (verify field name in v1.4) |
| `autoCompleteForMultiple` | `AutoCompleteMultipleInput` | same |
| `browse` | `BrowseInput` | `filters: [FacetFilterInput!]` (verify) |
| `browseV2` | `BrowseV2Input` | `orFilters: [AndFilterInput!]` (verify) |
| `aggregateAcrossEntities` | `AggregateAcrossEntitiesInput` | `orFilters: [AndFilterInput!]` |
| `search` (legacy) | `SearchInput` | `filters: [FacetFilterInput!]` |

## GraphQL input shapes (DataHub schema)

| Type | Fields we use |
|---|---|
| `FacetFilterInput` | `field: String!, values: [String!], condition: FilterOperator, negated: Boolean` |
| `AndFilterInput` | `and: [FacetFilterInput!]` |
| `FilterOperator.EQUAL` | enum value |

## Verification command

```bash
# On every upgrade, run this and compare to the row above:
grep -h "input AutoCompleteInput\|input BrowseInput\|input BrowseV2Input" \
     datahub-graphql-core/src/main/resources/*.graphql -A 10
```
