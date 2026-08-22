# Reflection Target Notes: GraphQLEngine

Verified against commit `647398f588` (DataHub OSS, targeting v1.4.0).
Source file: `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/GraphQLEngine.java`

---

## Step 1: GraphQLEngine field inspection

### 1. Field name holding the `graphql.GraphQL` instance

```
private final GraphQL _graphQL;
```

The field is named **`_graphQL`** (line 49).

### 2. Declared type

`graphql.GraphQL` — imported directly as `graphql.GraphQL` (line 12). No wrapper or subclass.

### 3. Public getter — IMPORTANT for Task 6/7

**There IS a public getter:**

```java
public GraphQL getGraphQL() {
    return _graphQL;
}
```

(lines 147–149)

**Consequence for later tasks:** Reflection on `_graphQL` is unnecessary. Use `engine.getGraphQL()`
instead. Task 6 (and any task that planned to use reflection to read the field) should call the
public getter directly. Reflection is still needed only if we need to *replace* the `GraphQL`
instance held by the engine (i.e., write back a transformed instance) — `GraphQLEngine` provides
no setter for `_graphQL`, so field-mutation via reflection would still be required for an
inject-and-replace approach.

### 4. Constructor argument names

The private constructor signature (lines 54–59):

```java
private GraphQLEngine(
    @Nonnull final List<String> schemas,
    @Nonnull final RuntimeWiring runtimeWiring,
    @Nonnull final Map<String, Function<QueryContext, DataLoader<?, ?>>> dataLoaderSuppliers,
    @Nonnull GraphQLConfiguration graphQLConfiguration,
    MetricUtils metricUtils)
```

Parameters in order:
1. `schemas` — `List<String>` of SDL schema strings
2. `runtimeWiring` — `graphql.schema.idl.RuntimeWiring`
3. `dataLoaderSuppliers` — `Map<String, Function<QueryContext, DataLoader<?, ?>>>`
4. `graphQLConfiguration` — `com.linkedin.metadata.config.GraphQLConfiguration`
5. `metricUtils` — `com.linkedin.metadata.utils.metrics.MetricUtils` (nullable — constructor is
   called with `null` in tests)

The constructor is `private`; construction goes through `GraphQLEngine.Builder`. The builder
exposes: `addSchema()`, `addDataLoader()`, `addDataLoaders()`, `configureRuntimeWiring()`,
`setGraphQLConfiguration()`, `setMetricUtils()`, then `build()`.

---

## Step 2: graphql-java version and API surface

### Pinned version

```
com.graphql-java:graphql-java:22.3
```

Declared in `build.gradle` under `externalDependency.graphqlJava`.

### `GraphQL.transform()` and `GraphQL.getInstrumentation()`

Both methods have been stable public API since graphql-java **14.x** (circa 2019).

- **`GraphQL.transform(Consumer<Builder> builderConsumer)`** — copies the current `GraphQL`
  instance into a new `Builder`, applies the consumer, and returns a new `GraphQL`. Available in
  22.x; the standard way to swap/augment instrumentation without reconstructing from scratch.
- **`GraphQL.getInstrumentation()`** — returns the `Instrumentation` registered on the instance.
  In 22.x this is the `ChainedInstrumentation` wrapping all instrumentations. Available and public
  in 22.x.

At version 22.3 neither method is deprecated. Both are confirmed present in the graphql-java 22.x
changelog and source (https://github.com/graphql-java/graphql-java).

---

## Summary for ownership-filter architecture

| Question | Answer |
|---|---|
| Field name | `_graphQL` |
| Field type | `graphql.GraphQL` |
| Public getter? | **Yes** — `getGraphQL()` (no reflection needed to *read*) |
| Setter? | No — reflection required to *replace* `_graphQL` with a transformed instance |
| graphql-java version | `22.3` |
| `transform()` available? | Yes (stable since 14.x, present in 22.3) |
| `getInstrumentation()` available? | Yes (stable since 14.x, present in 22.3) |

### Recommended inject strategy (for Task 6)

1. Read the existing `GraphQL` via `engine.getGraphQL()` (no reflection needed).
2. Call `existingGraphQL.transform(b -> b.instrumentation(newChainedInstrumentation))` to produce
   a new `GraphQL` with the ownership filter instrumentation prepended to the chain.
3. Write the new `GraphQL` back into `engine._graphQL` via reflection (field is `private final`,
   so `setAccessible(true)` is required — or use a `VarHandle` on Java 9+).

Alternatively, wrap `GraphQLEngine.execute()` at the call site (e.g., in the servlet layer) to
inject the instrumentation per-request via `ExecutionInput`, which avoids field mutation entirely.
