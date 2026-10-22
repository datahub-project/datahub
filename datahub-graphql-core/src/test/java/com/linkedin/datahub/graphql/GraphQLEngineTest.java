package com.linkedin.datahub.graphql;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.instrumentation.RootFieldNameInstrumentation;
import com.linkedin.metadata.config.GraphQLConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLConcurrencyConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLDocumentCacheConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLMetricsConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.ExecutionResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.*;
import java.util.function.Function;
import org.dataloader.DataLoader;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphQLEngineTest {

  @Mock private MetricUtils mockMetricUtils;

  @Mock private QueryContext mockQueryContext;

  private GraphQLConfiguration graphQLConfiguration;
  private GraphQLEngine graphQLEngine;

  private static final String TEST_SCHEMA =
      """
        type Query {
            hello: String
            user(id: ID!): User
            users: [User]
            node: Node
            searchAcrossEntities(input: SearchInput): SearchResults
        }

        type User {
            id: ID!
            name: String
            email: String
        }

        type Node {
            name: String
            child: Node
        }

        input SearchInput {
            query: String
            count: Int
        }

        type SearchResults {
            searchResults: SearchResultsWrapper
        }

        type SearchResultsWrapper {
            entity: Entity
        }

        type Entity {
            urn: String
        }

        type Mutation {
            createUser(name: String!, email: String!): User
        }
        """;

  private static final String LINEAGE_FRAGMENT = "fragment searchResultFields on Entity { urn }";

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Setup real GraphQL configuration
    graphQLConfiguration = createDefaultConfiguration();
  }

  private GraphQLConfiguration createDefaultConfiguration() {
    GraphQLConfiguration config = new GraphQLConfiguration();

    // Query configuration
    GraphQLQueryConfiguration queryConfig = new GraphQLQueryConfiguration();
    queryConfig.setComplexityLimit(1000);
    queryConfig.setDepthLimit(10);
    queryConfig.setIntrospectionEnabled(true);
    config.setQuery(queryConfig);

    // Metrics configuration
    GraphQLMetricsConfiguration metricsConfig = new GraphQLMetricsConfiguration();
    metricsConfig.setEnabled(false);
    metricsConfig.setFieldLevelEnabled(false);
    metricsConfig.setFieldLevelOperations("Query.search");
    metricsConfig.setFieldLevelPathEnabled(false);
    metricsConfig.setFieldLevelPaths("/search");
    metricsConfig.setTrivialDataFetchersEnabled(false);
    config.setMetrics(metricsConfig);

    // Concurrency configuration (if needed)
    GraphQLConcurrencyConfiguration concurrencyConfig = new GraphQLConcurrencyConfiguration();
    config.setConcurrency(concurrencyConfig);

    return config;
  }

  @Test
  public void testBuilderWithBasicSchema() {
    // Build GraphQL engine with basic configuration
    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertNotNull(graphQLEngine);
    assertNotNull(graphQLEngine.getGraphQL());
  }

  @Test
  public void testExecuteSimpleQuery() {
    // Setup
    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    // Execute query
    String query = "{ hello }";
    ExecutionResult result = graphQLEngine.execute(query, null, Map.of(), mockQueryContext);

    // Verify
    assertNotNull(result);
    assertEquals(result.getExtensions().get(RootFieldNameInstrumentation.EXTENSION_KEY), "hello");
    assertFalse(result.getErrors().isEmpty() == false && result.getErrors().size() > 0);
    Map<String, Object> data = result.getData();
    assertNotNull(data);
    assertEquals(data.get("hello"), "World");
  }

  @Test
  public void testRepeatedQueryIsServedFromDocumentCache() {
    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    String query = "query cachedHello { hello }";
    graphQLEngine.execute(query, null, Map.of(), mockQueryContext);
    graphQLEngine.execute(query, null, Map.of(), mockQueryContext);

    assertEquals(graphQLEngine.getDocumentCache().stats().hitCount(), 1);
  }

  @Test
  public void testQueryCostIsComputedOncePerBooleanCombination() {
    graphQLEngine = buildEngineWithLimits(10, 1000);
    String query =
        "query q($deep: Boolean = false) { node { child @include(if: $deep) { name } } }";

    graphQLEngine.execute(query, null, Map.of(), mockQueryContext);
    graphQLEngine.execute(query, null, Map.of(), mockQueryContext);
    graphQLEngine.execute(query, null, Map.of("deep", true), mockQueryContext);

    GraphqlDocumentCache.Entry entry =
        graphQLEngine
            .getDocumentCache()
            .getOrCompute(
                query,
                () -> {
                  throw new AssertionError("document should be cached");
                });
    assertEquals(entry.costs().size(), 2);
  }

  @Test
  public void testDocumentCacheCanBeDisabledViaConfiguration() {
    GraphQLDocumentCacheConfiguration documentCacheConfig = new GraphQLDocumentCacheConfiguration();
    documentCacheConfig.setEnabled(false);
    graphQLConfiguration.setDocumentCache(documentCacheConfig);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    String query = "query disabledCacheHello { hello }";
    graphQLEngine.execute(query, null, Map.of(), mockQueryContext);
    graphQLEngine.execute(query, null, Map.of(), mockQueryContext);

    assertEquals(graphQLEngine.getDocumentCache().stats().hitCount(), 0);
  }

  @Test
  public void testDocumentCacheMaximumWeightIsConfigurable() {
    GraphQLDocumentCacheConfiguration documentCacheConfig = new GraphQLDocumentCacheConfiguration();
    documentCacheConfig.setEnabled(true);
    documentCacheConfig.setMaximumWeightBytes(4096);
    graphQLConfiguration.setDocumentCache(documentCacheConfig);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertEquals(graphQLEngine.getDocumentCache().getMaximumWeightBytes(), 4096);
  }

  @Test
  public void testDocumentCacheDefaultMaximumWeightAppliesWhenUnconfigured() {
    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertEquals(
        graphQLEngine.getDocumentCache().getMaximumWeightBytes(),
        GraphqlDocumentCache.DEFAULT_MAXIMUM_WEIGHT_BYTES);
  }

  @Test
  public void testDocumentCacheEnabledByDefaultWhenConfigPresentButUnset() {
    // A present-but-unset config block (e.g. hand-built, not YAML-bound) must still default to
    // enabled -- only an explicit `enabled: false` should turn caching off.
    graphQLConfiguration.setDocumentCache(new GraphQLDocumentCacheConfiguration());

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    String query = "query defaultEnabledHello { hello }";
    graphQLEngine.execute(query, null, Map.of(), mockQueryContext);
    graphQLEngine.execute(query, null, Map.of(), mockQueryContext);

    assertEquals(graphQLEngine.getDocumentCache().stats().hitCount(), 1);
  }

  @Test
  public void testCachedDocumentIsReadOnlyBeforeAnyExecution() {
    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertNull(graphQLEngine.getCachedDocument("{ hello }"));

    graphQLEngine.execute("{ hello }", null, Map.of(), mockQueryContext);

    assertNotNull(graphQLEngine.getCachedDocument("{ hello }"));
  }

  @Test
  public void testExecuteQueryWithVariables() {
    // Setup
    Map<String, User> userDatabase = new HashMap<>();
    userDatabase.put("1", new User("1", "John Doe", "john@example.com"));
    userDatabase.put("2", new User("2", "Jane Smith", "jane@example.com"));

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query",
                        typeWiring ->
                            typeWiring.dataFetcher(
                                "user",
                                env -> {
                                  String id = env.getArgument("id");
                                  return userDatabase.get(id);
                                })))
            .build();

    // Execute query with variables
    String query = "query getUser($userId: ID!) { user(id: $userId) { id name email } }";
    Map<String, Object> variables = new HashMap<>();
    variables.put("userId", "1");

    ExecutionResult result = graphQLEngine.execute(query, "getUser", variables, mockQueryContext);

    // Verify
    assertNotNull(result);
    assertTrue(result.getErrors().isEmpty());
    Map<String, Object> data = result.getData();
    assertNotNull(data);
    Map<String, Object> user = (Map<String, Object>) data.get("user");
    assertEquals(user.get("id"), "1");
    assertEquals(user.get("name"), "John Doe");
    assertEquals(user.get("email"), "john@example.com");
  }

  @Test
  public void testBuilderWithMultipleSchemas() {
    String schema1 = "type Query { field1: String }";
    String schema2 = "extend type Query { field2: Int }";

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(schema1)
            .addSchema(schema2)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query",
                        typeWiring ->
                            typeWiring
                                .dataFetcher("field1", env -> "value1")
                                .dataFetcher("field2", env -> 42)))
            .build();

    assertNotNull(graphQLEngine);

    // Test both fields
    ExecutionResult result =
        graphQLEngine.execute("{ field1 field2 }", null, Map.of(), mockQueryContext);
    assertNotNull(result);
    Map<String, Object> data = result.getData();
    assertEquals(data.get("field1"), "value1");
    assertEquals(data.get("field2"), 42);
  }

  @Test
  public void testDataLoaderIntegration() {
    // Setup data loader
    Function<QueryContext, DataLoader<?, ?>> userLoaderSupplier =
        context ->
            DataLoader.newDataLoader(
                keys -> {
                  List<User> users = new ArrayList<>();
                  for (Object key : keys) {
                    users.add(new User((String) key, "User " + key, "user" + key + "@example.com"));
                  }
                  return java.util.concurrent.CompletableFuture.completedFuture(users);
                });

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .addDataLoader("userLoader", userLoaderSupplier)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query",
                        typeWiring ->
                            typeWiring.dataFetcher(
                                "user",
                                env -> {
                                  String id = env.getArgument("id");
                                  DataLoader<String, User> loader = env.getDataLoader("userLoader");
                                  return loader.load(id);
                                })))
            .build();

    // Execute query
    String query = "{ user(id: \"123\") { id name } }";
    ExecutionResult result = graphQLEngine.execute(query, null, Map.of(), mockQueryContext);

    // Verify
    assertNotNull(result);
    assertTrue(result.getErrors().isEmpty());
    Map<String, Object> data = result.getData();
    Map<String, Object> user = (Map<String, Object>) data.get("user");
    assertEquals(user.get("id"), "123");
    assertEquals(user.get("name"), "User 123");
  }

  @Test
  public void testBuilderWithMultipleDataLoaders() {
    Map<String, Function<QueryContext, DataLoader<?, ?>>> loaders = new HashMap<>();
    loaders.put(
        "loader1",
        ctx ->
            DataLoader.newDataLoader(
                keys ->
                    java.util.concurrent.CompletableFuture.completedFuture(new ArrayList<>(keys))));
    loaders.put(
        "loader2",
        ctx ->
            DataLoader.newDataLoader(
                keys ->
                    java.util.concurrent.CompletableFuture.completedFuture(new ArrayList<>(keys))));

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .addDataLoaders(loaders)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertNotNull(graphQLEngine);
  }

  @Test
  public void testMetricsEnabled() {
    // Setup metrics
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    when(mockMetricUtils.getRegistry()).thenReturn(meterRegistry);

    // Enable metrics in configuration
    GraphQLConfiguration metricsEnabledConfig = createDefaultConfiguration();
    metricsEnabledConfig.getMetrics().setEnabled(true);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(metricsEnabledConfig)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    // Execute query
    ExecutionResult result = graphQLEngine.execute("{ hello }", null, Map.of(), mockQueryContext);

    // Verify
    assertNotNull(result);
    verify(mockMetricUtils, times(1)).getRegistry();
  }

  @Test
  public void testIntrospectionDisabled() {
    // Setup with introspection disabled
    GraphQLConfiguration introspectionDisabledConfig = createDefaultConfiguration();
    introspectionDisabledConfig.getQuery().setIntrospectionEnabled(false);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(introspectionDisabledConfig)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertNotNull(graphQLEngine);
  }

  @Test
  public void testComplexityAndDepthLimits() {
    // Setup with custom limits
    GraphQLConfiguration customLimitsConfig = createDefaultConfiguration();
    customLimitsConfig.getQuery().setComplexityLimit(500);
    customLimitsConfig.getQuery().setDepthLimit(5);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(customLimitsConfig)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertNotNull(graphQLEngine);

    // A query well within both limits executes normally.
    ExecutionResult result = graphQLEngine.execute("{ hello }", null, Map.of(), mockQueryContext);
    assertNotNull(result);
    assertTrue(result.getErrors().isEmpty());
  }

  private GraphQLEngine buildEngineWithLimits(int depthLimit, int complexityLimit) {
    GraphQLConfiguration config = createDefaultConfiguration();
    config.getQuery().setDepthLimit(depthLimit);
    config.getQuery().setComplexityLimit(complexityLimit);
    return GraphQLEngine.builder()
        .addSchema(TEST_SCHEMA)
        .setGraphQLConfiguration(config)
        .setMetricUtils(mockMetricUtils)
        .configureRuntimeWiring(
            wiring ->
                wiring.type(
                    "Query",
                    typeWiring ->
                        typeWiring
                            .dataFetcher("hello", env -> "World")
                            .dataFetcher("node", env -> new NodeData())
                            .dataFetcher("searchAcrossEntities", env -> new SearchResultsData())))
        .build();
  }

  @Test
  public void testDepthWithinLimitSucceeds() {
    graphQLEngine = buildEngineWithLimits(3, 1000);

    ExecutionResult result =
        graphQLEngine.execute("{ node { child { name } } }", null, Map.of(), mockQueryContext);

    assertTrue(result.getErrors().isEmpty());
  }

  @Test
  public void testDepthExceedingLimitIsRejectedBeforeExecution() {
    graphQLEngine = buildEngineWithLimits(2, 1000);

    ExecutionResult result =
        graphQLEngine.execute("{ node { child { name } } }", null, Map.of(), mockQueryContext);

    assertNull(result.getData());
    assertEquals(result.getErrors().size(), 1);
    assertTrue(result.getErrors().get(0).getMessage().contains("maximum query depth exceeded"));
    assertTrue(result.getErrors().get(0).getMessage().contains("3 > 2"));
  }

  @Test
  public void testEmptyOperationNameStillChecksTheOperationThatRuns() {
    graphQLEngine = buildEngineWithLimits(2, 1000);

    // graphql-java runs the first operation for an empty name, so that's the one to check.
    ExecutionResult result =
        graphQLEngine.execute(
            "query Heavy { node { child { name } } } query Light { hello }",
            "",
            Map.of(),
            mockQueryContext);

    assertNull(result.getData());
    assertTrue(result.getErrors().get(0).getMessage().contains("maximum query depth exceeded"));
  }

  @Test
  public void testVariableConditionsAreEvaluatedPerRequest() {
    graphQLEngine = buildEngineWithLimits(2, 1000);
    String query =
        "query q($deep: Boolean = false) { node { child @include(if: $deep) { name } } }";

    // Each Boolean combination is computed and cached separately, so alternating values must
    // never serve one request the other's result.
    for (int i = 0; i < 2; i++) {
      assertTrue(
          graphQLEngine.execute(query, null, Map.of(), mockQueryContext).getErrors().isEmpty());
      ExecutionResult deep =
          graphQLEngine.execute(query, null, Map.of("deep", true), mockQueryContext);
      assertTrue(
          deep.getErrors().get(0).getMessage().contains("maximum query depth exceeded 3 > 2"));
    }
  }

  @Test
  public void testComplexityExceedingLimitWithLiteralCountIsRejected() {
    graphQLEngine = buildEngineWithLimits(10, 5);

    String query =
        "{ searchAcrossEntities(input: {count: 10}) { searchResults { entity { ...searchResultFields } } } } "
            + LINEAGE_FRAGMENT;
    ExecutionResult result = graphQLEngine.execute(query, null, Map.of(), mockQueryContext);

    assertNull(result.getData());
    assertEquals(result.getErrors().size(), 1);
    assertTrue(
        result.getErrors().get(0).getMessage().contains("maximum query complexity exceeded"));
    // searchAcrossEntities + searchResults + entity + urn (4) + 2*10 = 24
    assertTrue(result.getErrors().get(0).getMessage().contains("24 > 5"));
  }

  @Test
  public void testComplexityWithVariableBoundCountDependsOnRequestNotCachedVerdict() {
    // Same cached query text (and hence the same precomputed structural analysis) executed twice
    // with different `count` values must be evaluated independently each time -- the whole point
    // of only caching the query-text-invariant part of the analysis.
    graphQLEngine = buildEngineWithLimits(10, 5);
    String query =
        "query Q($input: SearchInput) { searchAcrossEntities(input: $input) { searchResults { entity { ...searchResultFields } } } } "
            + LINEAGE_FRAGMENT;

    ExecutionResult withinLimit =
        graphQLEngine.execute(query, "Q", Map.of("input", Map.of("count", 1)), mockQueryContext);
    assertTrue(withinLimit.getErrors().isEmpty());

    ExecutionResult overLimit =
        graphQLEngine.execute(query, "Q", Map.of("input", Map.of("count", 10)), mockQueryContext);
    assertEquals(overLimit.getErrors().size(), 1);
    assertTrue(
        overLimit.getErrors().get(0).getMessage().contains("maximum query complexity exceeded"));

    // Confirms both executions hit the same cached document (and so the same cached structural
    // analysis) -- the differing verdicts above are solely from re-evaluating live variables.
    assertEquals(graphQLEngine.getDocumentCache().stats().hitCount(), 1);
  }

  public static class NodeData {
    public String getName() {
      return "node";
    }

    public NodeData getChild() {
      return new NodeData();
    }
  }

  public static class SearchResultsData {
    public SearchResultsWrapperData getSearchResults() {
      return new SearchResultsWrapperData();
    }
  }

  public static class SearchResultsWrapperData {
    public EntityData getEntity() {
      return new EntityData();
    }
  }

  public static class EntityData {
    public String getUrn() {
      return "urn:li:dataset:(urn:li:dataPlatform:test,example,PROD)";
    }
  }

  @Test
  public void testExecuteWithNullOperationName() {
    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    ExecutionResult result = graphQLEngine.execute("{ hello }", null, Map.of(), mockQueryContext);
    assertNotNull(result);
    assertTrue(result.getErrors().isEmpty());
  }

  @Test
  public void testExecuteWithEmptyVariables() {
    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    ExecutionResult result =
        graphQLEngine.execute("{ hello }", null, new HashMap<>(), mockQueryContext);
    assertNotNull(result);
    assertTrue(result.getErrors().isEmpty());
  }

  @Test
  public void testFieldLevelMetricsConfiguration() {
    // Setup with field level metrics enabled
    GraphQLConfiguration fieldMetricsConfig = createDefaultConfiguration();
    GraphQLMetricsConfiguration metrics = fieldMetricsConfig.getMetrics();
    metrics.setEnabled(true);
    metrics.setFieldLevelEnabled(true);
    metrics.setFieldLevelOperations("Query.search,Query.user");
    metrics.setFieldLevelPathEnabled(true);
    metrics.setFieldLevelPaths("/search,/user");
    metrics.setTrivialDataFetchersEnabled(true);

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    when(mockMetricUtils.getRegistry()).thenReturn(meterRegistry);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(fieldMetricsConfig)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query",
                        typeWiring ->
                            typeWiring
                                .dataFetcher("hello", env -> "World")
                                .dataFetcher(
                                    "user", env -> new User("1", "Test", "test@example.com"))))
            .build();

    // Execute query
    ExecutionResult result =
        graphQLEngine.execute("{ user(id: \"1\") { name } }", null, Map.of(), mockQueryContext);

    // Verify
    assertNotNull(result);
    assertTrue(result.getErrors().isEmpty());
  }

  @Test
  public void testNullMetricUtils() {
    // Test with null MetricUtils
    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(graphQLConfiguration)
            .setMetricUtils(null)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertNotNull(graphQLEngine);

    // Should work fine without metrics
    ExecutionResult result = graphQLEngine.execute("{ hello }", null, Map.of(), mockQueryContext);
    assertNotNull(result);
    assertTrue(result.getErrors().isEmpty());
  }

  @Test
  public void testMetricsWithEmptyRegistry() {
    // Setup metrics with empty registry
    when(mockMetricUtils.getRegistry()).thenReturn(new SimpleMeterRegistry());

    GraphQLConfiguration metricsEnabledConfig = createDefaultConfiguration();
    metricsEnabledConfig.getMetrics().setEnabled(true);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(metricsEnabledConfig)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "World")))
            .build();

    assertNotNull(graphQLEngine);
  }

  // Helper class for testing
  public static class User {
    private final String id;
    private final String name;
    private final String email;

    public User(String id, String name, String email) {
      this.id = id;
      this.name = name;
      this.email = email;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public String getEmail() {
      return email;
    }
  }
}
