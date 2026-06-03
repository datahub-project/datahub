# datahub-ownership-filter

Restricts DataHub OSS GraphQL search/browse/lineage/autocomplete paths so each actor sees only
assets they own (directly or via group membership). Admins bypass the filter. The module is a
standalone Gradle subproject that installs itself into DataHub's `datahub-gms` Spring context at
boot time via auto-configuration — no upstream Java changes are needed.

**Target version:** DataHub OSS **v1.3.0.1** (branch `feature/graphql-security-v1301`). Verified
against this release: graphql-java 22.3, Spring Boot 3.4.5, `GraphQLEngine._graphQL` + `getGraphQL()`,
and all 10 gated GraphQL Query fields present. The module also builds unchanged on later releases
(it was originally developed post-v1.4.0).

## Visibility rules

**Assets** (datasets, dashboards, charts, jobs, ML entities, containers, …) — an actor may see an
asset when **any** of these hold:

1. **They own it** — `owners` contains the actor's URN or one of their group URNs.
2. **Nobody owns it** — the `owners` field is absent or empty. Unowned assets are visible to **all**
   users. Implemented as a second disjunct in the ownership predicate using
   `condition: EXISTS, negated: true`.

**Domains & Data Platforms** are not gated by their own (non-existent) ownership; they are
**access-scoped**: an actor sees a domain/platform only if it contains at least one asset the actor
can see (per the asset rules above). This is computed per actor by aggregating the `domains` /
`platform` facets over the actor's visible assets (`DomainPlatformAccessResolver`, cached ~60s) and
enforced everywhere they appear:

- **Domain/platform search** (`searchAcrossEntities(types:[DOMAIN])`, the Govern → Domains page,
  domain/platform pickers): the query is restricted to `urn IN <accessible set>`.
- **Home-page cards** (`listRecommendations`): the Domains/Platforms module content is post-filtered
  to drop entries outside the accessible set.

**Glossary Terms/Nodes and Tags** are navigational and remain visible to all users.

**Admins** (configurable user/group URN allowlists) bypass all of the above.

> Known residual: a global/all-entity search (no entity-type scope) can still surface a domain or
> platform *entity* via the unowned branch, since that path uses the asset filter. The dedicated
> domain/platform surfaces above are fully scoped.

## How it works

### Search / lineage / autocomplete / browse (active)

`OwnershipInstrumentation` is a graphql-java `Instrumentation` that wraps the `DataFetcher` for
each of the 10 registered GraphQL Query fields. Before delegating to the original fetcher it:

1. Resolves the acting user's URN from `QueryContext` (extracted from `DataFetchingEnvironment`).
2. Checks `AdminBypass` — admins pass through unmodified.
3. Calls `CachedGroupResolver` to expand the user's group memberships (5-minute Caffeine cache).
4. Calls `OwnershipFilterBuilder` to build an ownership clause in the format expected by each
   field's input type (`orFilters: [AndFilterInput!]` or `filters: [FacetFilterInput!]`).
5. Mutates the input argument map (via `FieldArgumentMutators`) to inject the clause, then
   delegates to the original fetcher with the modified environment.

The instrumentation is injected into the `graphQLEngine` Spring bean by a `BeanPostProcessor`
defined in `OwnershipFilterConfiguration`. The BeanPostProcessor reads the inner `graphql.GraphQL`
via the public `getGraphQL()` getter, wraps the existing `Instrumentation` in a
`ChainedInstrumentation`, and writes the replacement back via reflection on the `_graphQL` field.
`StartupValidator` then asserts at startup that the wrap succeeded; GMS refuses to boot if it did
not.

### Authentication — plain JWT / Keycloak (optional)

`KeycloakJwtAuthenticator` lets DataHub accept a plain RS256 JWT minted by an external IdP
(Keycloak by default) **in addition to** DataHub's own access tokens. It validates the token's
signature against the IdP's JWKS endpoint (matching the token `kid`, with key caching), checks
issuer/audience/expiry, and maps a configurable claim (default `email`) to a DataHub corpuser
(`urn:li:corpuser:<email>`). On any token it can't positively validate it returns `null`, so the
authenticator chain falls through and DataHub's own `DataHubTokenAuthenticator` continues to handle
DataHub tokens unchanged.

It is registered into DataHub's authenticator chain (right after `DataHubTokenAuthenticator`) by a
`BeanPostProcessor` in `OwnershipFilterConfiguration` that appends an entry to
`ConfigurationProvider.getAuthentication().getAuthenticators()` before the auth filter's
`@PostConstruct` builds the chain — so no `application.yaml` edit is needed. It is **inert unless
`KEYCLOAK_JWKS_URI` is set**.

Because the ownership filter reads the actor from `QueryContext` (populated by whichever
authenticator succeeded), filtering applies automatically to JWT-authenticated actors with no extra
wiring. jjwt 0.11.2 (on the GMS classpath) does the JWT parsing/verification.

### Entity-page direct access (currently disabled)

`OwnershipAuthorizer` is a DataHub Authorizer plugin skeleton that was intended to enforce
`VIEW_ENTITY_PAGE` / `GET_ENTITY` checks. It is currently **a no-op in production**: DataHub's
upstream `AuthorizerChainFactory` does not populate `EntityClient` or `OperationContext` into
`AuthorizerContext.data()`, so the authorizer cannot look up ownership data at runtime. In
disabled mode it returns ALLOW for all requests and logs a single WARN at initialization.

Practical impact: users who already know a URN can still fetch that entity directly via GraphQL
(e.g. `dataset(urn:...)`) or REST. Search, browse, lineage, and autocomplete — the dominant
discovery paths — are enforced. The gap is documented as future work pending a 2-line patch to
`AuthorizerChainFactory.java` upstream.

## Coupling surface

Full upgrade checklist: [`INTERFACES.md`](./INTERFACES.md)

Summary of the five categories:

| Category | Count | Stability |
|---|---|---|
| graphql-java framework types | 5 | Very stable — unchanged since graphql-java 14.x |
| DataHub bean name | 1 (`graphQLEngine`) | Stable; verified at boot |
| Reflection field on `GraphQLEngine` | 1 (`_graphQL` — located by type scan, not name) | Fragile; boot fails loud if gone |
| GraphQL field names (asserted by `SchemaContractTest`) | 10 | Checked at build time |
| GraphQL input types | 4 (`FacetFilterInput`, `AndFilterInput`, `FilterOperator`, `SearchAcrossEntitiesInput` family) | Checked at build time |

On every DataHub version bump: run `SchemaContractTest` and review INTERFACES.md row by row.

## Configuration

### Spring properties (GMS `application.yml`)

| Property | Default | Description |
|---|---|---|
| `ownership.filter.adminUserUrns` | `urn:li:corpuser:datahub` | Comma-separated user URNs that bypass the filter |
| `ownership.filter.adminGroupUrns` | `urn:li:corpGroup:admins` | Comma-separated group URNs whose members bypass the filter |

Invalid URNs in these properties cause a `RuntimeException` at startup.

### Keycloak / plain-JWT authentication (env vars)

The `KeycloakJwtAuthenticator` is registered only when `KEYCLOAK_JWKS_URI` is set; otherwise the
plugin logs a notice and leaves the authenticator chain unchanged.

| Env var | Default | Description |
|---|---|---|
| `KEYCLOAK_JWKS_URI` | _(unset → disabled)_ | IdP JWKS endpoint, e.g. `https://<kc>/realms/<realm>/protocol/openid-connect/certs` |
| `KEYCLOAK_TRUSTED_ISSUERS` | _(none)_ | Comma-separated allowed `iss` values; if set, the token issuer must match |
| `KEYCLOAK_ALLOWED_AUDIENCES` | _(none)_ | Comma-separated allowed `aud` values; if set, the token audience must intersect |
| `KEYCLOAK_USER_CLAIM` | `email` | JWT claim whose value becomes the corpuser id (`urn:li:corpuser:<value>`) |

### Authorizer plugin config (`src/main/resources/config.yml`)

Deployed to the GMS plugin directory. Currently has no runtime effect (see "Entity-page direct
access" above). Properties mirror the Spring properties:

```yaml
plugins:
  - name: "ownership-authorizer"
    type: "authorizer"
    enabled: "true"
    params:
      className: "io.datahub.ownership.auth.OwnershipAuthorizer"
      configs:
        adminUserUrns: "urn:li:corpuser:datahub"
        adminGroupUrns: "urn:li:corpGroup:admins"
        gatedPrivileges: "VIEW_ENTITY_PAGE,GET_ENTITY,VIEW_DATASET_USAGE,VIEW_DATASET_PROFILE"
```

## Failure modes (all loud)

- **`OwnershipInstrumentation` failed to install**: The `BeanPostProcessor` throws
  `IllegalStateException` during context refresh, AND `StartupValidator` double-checks
  `WrapStatus` after context is up. Either path prevents GMS from serving traffic.
- **Schema field name renamed upstream**: `SchemaContractTest` walks the `.graphql` files in
  `datahub-graphql-core` and asserts every registered field name exists. Build fails before
  artifact is produced.
- **`graphql.GraphQL` field renamed or removed upstream**: `findGraphQLField()` throws
  `NoSuchFieldException` wrapped in `IllegalStateException` — boot fails. Note: the field is
  located by type (`graphql.GraphQL`) not by name, so a rename alone is tolerated; removal is not.

## Known limits

- **Only `Ownership` aspect is checked.** Tag- and domain-based ownership signals are not
  considered.
- **Group membership cache TTL is 5 minutes.** Adding a user to a group takes effect within that
  window.
- **Legacy flat-filter fields** (`autoComplete`, `autoCompleteForMultiple`, `browse`, `search`)
  use `filters: [FacetFilterInput!]` instead of `orFilters`. The ownership clause is AND'd into
  any pre-existing filters.
- **REST/OpenAPI direct entity reads are NOT covered.** `OwnershipInstrumentation` sits entirely
  in the GraphQL execution layer.
- **Single-entity GraphQL fetchers are NOT covered** (e.g. `dataset(urn:...)`). The instrumented
  field allowlist does not include single-entity resolvers, and their input types do not accept a
  filter argument.
- **`OwnershipAuthorizer` is currently a no-op in production.** Consequence of the above two
  points: a user who knows a URN can retrieve that entity directly even if they would not see it
  in search results. This is a defense-in-depth gap. Mitigation: URNs are not guessable from
  outside the system without first going through the filtered discovery paths.
- **graphql-java 22.3 is assumed.** The `SimplePerformantInstrumentation` base class and the
  `GraphQL.transform()` API have been stable since 14.x but are third-party surface.

## Running the smoke test

```bash
# Start a local DataHub instance (see AGENTS.md)
scripts/dev/datahub-dev.sh start

# Run the ownership-filter smoke test
scripts/dev/datahub-dev.sh test smoke-test/tests/policies/test_ownership_filter.py
```

The smoke test creates a test dataset, assigns ownership, then asserts that a non-owner actor
gets an empty search result while the owner sees the asset. It requires a live GMS instance with
the module on the classpath.

## Module structure

```
datahub-ownership-filter/
├── INTERFACES.md                          # Upgrade checklist for every coupling point
├── NOTES-reflection-target.md             # Notes on the reflection approach for _graphQL
├── build.gradle
├── src/
│   ├── main/
│   │   ├── java/io/datahub/ownership/
│   │   │   ├── admin/
│   │   │   │   └── AdminBypass.java           # URN allowlist bypass check
│   │   │   ├── auth/
│   │   │   │   └── OwnershipAuthorizer.java   # Authorizer plugin (currently disabled)
│   │   │   ├── config/
│   │   │   │   ├── OwnershipFilterConfiguration.java  # Spring @Configuration + BeanPostProcessor
│   │   │   │   └── StartupValidator.java              # Fail-loud boot check
│   │   │   ├── filter/
│   │   │   │   └── OwnershipFilterBuilder.java        # Builds [AndFilterInput!] ownership clauses
│   │   │   ├── group/
│   │   │   │   └── CachedGroupResolver.java           # Caffeine-backed group membership lookup
│   │   │   └── instrumentation/
│   │   │       ├── FieldArgumentMutators.java         # Per-field input mutation registry
│   │   │       └── OwnershipInstrumentation.java      # graphql-java Instrumentation
│   │   └── resources/
│   │       ├── META-INF/spring/                       # Spring Boot auto-configuration entries
│   │       └── config.yml                             # Authorizer plugin descriptor
│   └── test/
│       └── java/io/datahub/ownership/
│           ├── admin/AdminBypassTest.java
│           ├── auth/OwnershipAuthorizerTest.java
│           ├── config/SchemaContractTest.java         # Build-time schema field name assertions
│           ├── filter/OwnershipFilterBuilderTest.java
│           ├── group/CachedGroupResolverTest.java
│           └── instrumentation/
│               ├── FieldArgumentMutatorsTest.java
│               └── OwnershipInstrumentationTest.java
└── smoke-test/tests/policies/test_ownership_filter.py  # Integration test (lives in root smoke-test/)
```

23 unit tests across 7 test classes. `SchemaContractTest` is the only one with external file
dependencies (the `.graphql` files in `datahub-graphql-core`).
