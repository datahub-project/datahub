package com.linkedin.datahub.graphql;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.GraphQLConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLConcurrencyConfiguration;
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
        }

        type User {
            id: ID!
            name: String
            email: String
        }

        type Mutation {
            createUser(name: String!, email: String!): User
        }
        """;

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
    assertFalse(result.getErrors().isEmpty() == false && result.getErrors().size() > 0);
    Map<String, Object> data = result.getData();
    assertNotNull(data);
    assertEquals(data.get("hello"), "World");
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
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

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

    // A deeply nested query would be rejected based on depth limit
    // This is a simple test to ensure the engine is created with limits
    ExecutionResult result = graphQLEngine.execute("{ hello }", null, Map.of(), mockQueryContext);
    assertNotNull(result);
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
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

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
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.empty());

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
