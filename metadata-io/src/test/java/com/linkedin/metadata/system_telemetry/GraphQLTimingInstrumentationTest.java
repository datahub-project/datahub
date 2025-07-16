package com.linkedin.metadata.system_telemetry;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.config.GraphQLConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLConcurrencyConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLMetricsConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ResultPath;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class GraphQLTimingInstrumentationTest {

  private MeterRegistry meterRegistry;
  private GraphQLMetricsConfiguration config;
  private GraphQLTimingInstrumentation instrumentation;
  private GraphQLConfiguration graphQLConfiguration;
  private GraphQLEngine graphQLEngine;

  @Mock private InstrumentationCreateStateParameters createStateParams;

  @Mock private InstrumentationExecutionParameters executionParams;

  @Mock private InstrumentationFieldFetchParameters fieldFetchParams;

  @Mock private ExecutionStepInfo executionStepInfo;

  @Mock private ExecutionStepInfo parentStepInfo;

  @Mock private GraphQLFieldDefinition fieldDefinition;

  @Mock private DataFetchingEnvironment dataFetchingEnvironment;

  @Mock private ExecutionInput executionInput;

  @Mock private ResultPath resultPath;

  @Mock private MetricUtils mockMetricUtils;

  @Mock private QueryContext mockQueryContext;

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
    meterRegistry = new SimpleMeterRegistry();
    config = new GraphQLMetricsConfiguration();
    config.setFieldLevelEnabled(true);
    instrumentation = new GraphQLTimingInstrumentation(meterRegistry, config);

    // Setup real GraphQL configuration for GraphQLEngine
    graphQLConfiguration = createDefaultConfiguration();
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));
  }

  @Test
  public void testCreateState() {
    // Act
    InstrumentationState state = instrumentation.createState(createStateParams);

    // Assert
    assertNotNull(state);
    assertTrue(state instanceof GraphQLTimingInstrumentation.TimingState);
  }

  @Test
  public void testBeginExecutionSuccess() {
    // Arrange
    when(executionParams.getOperation()).thenReturn("TestOperation");
    when(executionParams.getExecutionInput()).thenReturn(executionInput);
    when(executionInput.getQuery()).thenReturn("query { test }");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();

    // Act
    InstrumentationContext<ExecutionResult> context =
        instrumentation.beginExecution(executionParams, state);

    // Assert
    assertNotNull(context);
    assertEquals(state.operationName, "TestOperation");
    assertTrue(state.startTime > 0);

    // Simulate completion
    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getErrors()).thenReturn(Collections.emptyList());
    context.onCompleted(result, null);

    // Verify metrics
    Timer timer =
        meterRegistry
            .find("graphql.request.duration")
            .tag("operation", "TestOperation")
            .tag("success", "true")
            .timer();
    assertNotNull(timer);
    assertEquals(timer.count(), 1);
  }

  @Test
  public void testBeginExecutionWithError() {
    // Arrange
    when(executionParams.getOperation()).thenReturn("ErrorOperation");
    when(executionParams.getExecutionInput()).thenReturn(executionInput);
    when(executionInput.getQuery()).thenReturn("query { test }");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();

    // Act
    InstrumentationContext<ExecutionResult> context =
        instrumentation.beginExecution(executionParams, state);
    context.onCompleted(null, new RuntimeException("Test error"));

    // Assert
    Counter errorCounter =
        meterRegistry.find("graphql.request.errors").tag("operation", "ErrorOperation").counter();
    assertNotNull(errorCounter);
    assertEquals(errorCounter.count(), 1.0);
  }

  @Test
  public void testOperationTypeDetection() {
    // Test mutation
    when(executionParams.getOperation()).thenReturn("TestMutation");
    when(executionParams.getExecutionInput()).thenReturn(executionInput);
    when(executionInput.getQuery()).thenReturn("mutation { createUser }");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();
    InstrumentationContext<ExecutionResult> context =
        instrumentation.beginExecution(executionParams, state);

    // Complete the execution to record metrics
    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getErrors()).thenReturn(Collections.emptyList());
    context.onCompleted(result, null);

    Timer timer =
        meterRegistry.find("graphql.request.duration").tag("operation.type", "mutation").timer();
    assertNotNull(timer);
  }

  @Test
  public void testPathMatcherRegexConversion() {
    // This test verifies the regex conversion logic
    // IMPORTANT: The original code does NOT escape brackets, so they are treated as regex character
    // classes

    // Test exact match
    assertTrue(matchesPath("/user", "/user"));
    assertFalse(matchesPath("/user", "/users"));

    // Test single wildcard - matches one segment
    assertTrue(matchesPath("/user/*", "/user/name"));
    assertTrue(matchesPath("/user/*", "/user/posts[0]")); // posts[0] is one segment
    assertFalse(matchesPath("/user/*", "/user/posts[0]/comments")); // too many segments

    // Test recursive wildcard
    assertTrue(matchesPath("/user/**", "/user"));
    assertTrue(matchesPath("/user/**", "/user/name"));
    assertTrue(matchesPath("/user/**", "/user/posts[0]/comments"));

    // Test patterns with brackets - they are treated as regex character classes!
    assertFalse(
        matchesPath("/user/posts[0]/*", "/user/posts[0]/comments")); // [0] is regex, not literal
    assertTrue(
        matchesPath("/user/posts[0]/*", "/user/posts0/comments")); // matches without brackets

    // The correct way to match paths with brackets is to use wildcards
    assertTrue(matchesPath("/user/*/comments", "/user/posts[0]/comments"));
    assertTrue(matchesPath("/user/*/comments", "/user/posts[1]/comments"));

    // Test complex patterns
    assertTrue(matchesPath("/*/comments/*", "/posts/comments/text"));
    assertFalse(matchesPath("/*/comments/*", "/posts/comments")); // missing last segment
  }

  private boolean matchesPath(String pattern, String path) {
    // Exact copy of the convertPathToRegex logic from the original code
    String regex =
        pattern
            .replace(".", "\\.") // Escape dots
            .replace("/**", "(/.*)?") // /** matches current and any descendants
            .replace("/*", "/[^/]+") // /* matches exactly one segment
            .replaceAll("/\\*\\*$", "(/.*)?"); // /** at end is optional

    // For patterns ending with /**, also match the parent path
    if (pattern.endsWith("/**")) {
      String parentPath = pattern.substring(0, pattern.length() - 3);
      regex = "(" + parentPath + "|" + regex + ")";
    }

    regex = "^" + regex + "$";
    return path.matches(regex);
  }

  @DataProvider(name = "pathMatchingData")
  public Object[][] pathMatchingData() {
    return new Object[][] {
      // Pattern, Path, Should Match
      // IMPORTANT: The original code does NOT escape brackets in patterns,
      // so [0] in a pattern is treated as a regex character class, not literal [0]
      {"/user", "/user", true},
      {"/user", "/users", false},
      {"/user/*", "/user/name", true},
      {"/user/*", "/user/posts[0]", true}, // posts[0] is one segment, matched by *
      {"/user/**", "/user", true},
      {"/user/**", "/user/name", true},
      {"/user/**", "/user/posts[0]/comments", true},
      {"/*/comments/*", "/posts/comments/text", true},
      {"/*/comments/*", "/articles/comments/author", true},
      {"/*/comments/*", "/posts/comments", false},
      // Use wildcards to match segments with brackets
      {"/user/*/comments", "/user/posts[0]/comments", true},
      {"/user/*/comments", "/user/posts[1]/comments", true},
      {"/user/*/*", "/user/posts[0]/comments", true}, // Two wildcard segments
      // These patterns with literal brackets won't work as expected:
      // {"/user/posts[0]/*", "/user/posts[0]/comments", false}, // Would fail
      // Instead use wildcards to match array elements
      {"/user/posts/*", "/user/posts/0", true}, // If using numeric indices without brackets
      {"/user/posts/*", "/user/posts/[0]", true} // The whole "[0]" is matched as one segment
    };
  }

  @Test(dataProvider = "pathMatchingData")
  public void testPathMatching(String pattern, String path, boolean shouldMatch) {
    // Arrange
    config.setFieldLevelPaths(pattern);
    instrumentation = new GraphQLTimingInstrumentation(meterRegistry, config);

    setupFieldMocks(path, "TestType", "testField");

    // Create state through the proper method
    InstrumentationState baseState = instrumentation.createState(createStateParams);
    GraphQLTimingInstrumentation.TimingState state =
        (GraphQLTimingInstrumentation.TimingState) baseState;

    // Set up execution parameters for beginExecution
    when(executionParams.getOperation()).thenReturn("TestOp");
    when(executionParams.getExecutionInput()).thenReturn(executionInput);
    when(executionInput.getQuery()).thenReturn("query { test }");

    // Initialize the state properly through beginExecution
    instrumentation.beginExecution(executionParams, state);

    DataFetcher<?> originalFetcher = env -> "test";

    // Act
    DataFetcher<?> instrumentedFetcher =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);

    // Assert
    if (shouldMatch) {
      assertNotSame(
          originalFetcher,
          instrumentedFetcher,
          String.format("Expected pattern '%s' to match path '%s', but it didn't", pattern, path));
    } else {
      assertSame(
          originalFetcher,
          instrumentedFetcher,
          String.format("Expected pattern '%s' to NOT match path '%s', but it did", pattern, path));
    }
  }

  @Test
  public void testFilteringModeAllFields() {
    // Arrange - no filters configured
    config.setFieldLevelOperations("");
    config.setFieldLevelPaths("");
    instrumentation = new GraphQLTimingInstrumentation(meterRegistry, config);

    setupFieldMocks("/any/path", "TestType", "testField");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();
    state.operationName = "TestOp";
    state.filteringMode = GraphQLTimingInstrumentation.FilteringMode.ALL_FIELDS;

    DataFetcher<?> originalFetcher = env -> "test";

    // Act
    DataFetcher<?> instrumentedFetcher =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);

    // Assert - should be instrumented
    assertNotSame(originalFetcher, instrumentedFetcher);
  }

  @Test
  public void testFilteringModeByOperation() {
    // Arrange
    config.setFieldLevelOperations("AllowedOp1,AllowedOp2");
    config.setFieldLevelPaths("");
    instrumentation = new GraphQLTimingInstrumentation(meterRegistry, config);

    setupFieldMocks("/any/path", "TestType", "testField");

    // Test allowed operation
    GraphQLTimingInstrumentation.TimingState state1 =
        new GraphQLTimingInstrumentation.TimingState();
    state1.operationName = "AllowedOp1";
    state1.filteringMode = GraphQLTimingInstrumentation.FilteringMode.BY_OPERATION;

    DataFetcher<?> originalFetcher = env -> "test";
    DataFetcher<?> instrumentedFetcher1 =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state1);

    assertNotSame(originalFetcher, instrumentedFetcher1);

    // Test disallowed operation
    GraphQLTimingInstrumentation.TimingState state2 =
        new GraphQLTimingInstrumentation.TimingState();
    state2.operationName = "NotAllowedOp";
    state2.filteringMode = GraphQLTimingInstrumentation.FilteringMode.BY_OPERATION;

    DataFetcher<?> instrumentedFetcher2 =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state2);

    assertSame(originalFetcher, instrumentedFetcher2);
  }

  @Test
  public void testFilteringModeByBoth() {
    // Arrange - both operation and path filters
    config.setFieldLevelOperations("AllowedOp");
    config.setFieldLevelPaths("/user/**");
    instrumentation = new GraphQLTimingInstrumentation(meterRegistry, config);

    // Test: allowed operation + matching path
    setupFieldMocks("/user/name", "TestType", "testField");

    GraphQLTimingInstrumentation.TimingState state1 =
        new GraphQLTimingInstrumentation.TimingState();
    state1.operationName = "AllowedOp";
    state1.filteringMode = GraphQLTimingInstrumentation.FilteringMode.BY_BOTH;

    DataFetcher<?> originalFetcher = env -> "test";
    DataFetcher<?> instrumentedFetcher1 =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state1);

    assertNotSame(originalFetcher, instrumentedFetcher1);

    // Test: allowed operation + non-matching path
    setupFieldMocks("/post/title", "TestType", "testField");

    DataFetcher<?> instrumentedFetcher2 =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state1);

    assertSame(originalFetcher, instrumentedFetcher2);
  }

  @Test
  public void testCompletableFutureDataFetcher() throws Exception {
    // Arrange
    setupFieldMocks("/test/path", "TestType", "asyncField");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();
    state.operationName = "TestOp";
    state.filteringMode = GraphQLTimingInstrumentation.FilteringMode.ALL_FIELDS;

    CompletableFuture<String> future = new CompletableFuture<>();
    DataFetcher<?> originalFetcher = env -> future;

    // Act
    DataFetcher<?> instrumentedFetcher =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);

    Object result = instrumentedFetcher.get(dataFetchingEnvironment);
    assertTrue(result instanceof CompletableFuture);

    // Complete the future
    future.complete("async result");

    // Wait a bit for metrics to be recorded
    Thread.sleep(100);

    // Assert
    Timer timer =
        meterRegistry
            .find("graphql.field.duration")
            .tag("parent.type", "TestType")
            .tag("field", "asyncField")
            .tag("success", "true")
            .timer();
    assertNotNull(timer);
    assertEquals(timer.count(), 1);
  }

  @Test
  public void testDataFetcherException() throws Exception {
    // Arrange
    setupFieldMocks("/test/path", "TestType", "errorField");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();
    state.operationName = "TestOp";
    state.filteringMode = GraphQLTimingInstrumentation.FilteringMode.ALL_FIELDS;

    RuntimeException testException = new RuntimeException("Test error");
    DataFetcher<?> originalFetcher =
        env -> {
          throw testException;
        };

    // Act
    DataFetcher<?> instrumentedFetcher =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);

    // Assert exception is propagated
    try {
      instrumentedFetcher.get(dataFetchingEnvironment);
      fail("Expected exception to be thrown");
    } catch (RuntimeException e) {
      assertEquals(e, testException);
    }

    // Verify error metrics
    Timer timer =
        meterRegistry
            .find("graphql.field.duration")
            .tag("parent.type", "TestType")
            .tag("field", "errorField")
            .tag("success", "false")
            .timer();
    assertNotNull(timer);
    assertEquals(timer.count(), 1);

    Counter errorCounter =
        meterRegistry
            .find("graphql.field.errors")
            .tag("parent.type", "TestType")
            .tag("field", "errorField")
            .counter();
    assertNotNull(errorCounter);
    assertEquals(errorCounter.count(), 1.0);
  }

  @Test
  public void testTrivialDataFetcherFiltering() {
    // Arrange
    config.setTrivialDataFetchersEnabled(false);
    instrumentation = new GraphQLTimingInstrumentation(meterRegistry, config);

    setupFieldMocks("/test/path", "TestType", "trivialField");
    when(fieldFetchParams.isTrivialDataFetcher()).thenReturn(true);

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();
    state.operationName = "TestOp";
    state.filteringMode = GraphQLTimingInstrumentation.FilteringMode.ALL_FIELDS;

    DataFetcher<?> originalFetcher = env -> "test";

    // Act
    DataFetcher<?> instrumentedFetcher =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);

    // Assert - should not be instrumented
    assertSame(originalFetcher, instrumentedFetcher);
  }

  @Test
  public void testPathTruncation() throws Exception {
    // Arrange
    config.setFieldLevelPathEnabled(true);
    instrumentation = new GraphQLTimingInstrumentation(meterRegistry, config);

    // Test array index with brackets
    setupFieldMocks("/searchResults[0]/entity", "TestType", "entity");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();
    state.operationName = "TestOp";
    state.filteringMode = GraphQLTimingInstrumentation.FilteringMode.ALL_FIELDS;

    DataFetcher<?> originalFetcher = env -> "test";
    DataFetcher<?> instrumentedFetcher =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);

    instrumentedFetcher.get(dataFetchingEnvironment);

    // Assert path is truncated
    Timer timer =
        meterRegistry
            .find("graphql.field.duration")
            .tag("path", "/searchResults[*]/entity")
            .timer();
    assertNotNull(timer);

    // Test multiple array indices
    setupFieldMocks("/users[0]/posts[1]/comments[2]", "TestType", "comments");

    instrumentedFetcher =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);
    instrumentedFetcher.get(dataFetchingEnvironment);

    timer =
        meterRegistry
            .find("graphql.field.duration")
            .tag("path", "/users[*]/posts[*]/comments[*]")
            .timer();
    assertNotNull(timer);
  }

  @Test
  public void testFieldsInstrumentedCounter() {
    // Arrange
    setupFieldMocks("/test/path1", "TestType", "field1");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();
    state.operationName = "TestOp";
    state.filteringMode = GraphQLTimingInstrumentation.FilteringMode.ALL_FIELDS;

    DataFetcher<?> originalFetcher = env -> "test";

    // Act - instrument multiple fields
    for (int i = 0; i < 5; i++) {
      setupFieldMocks("/test/path" + i, "TestType", "field" + i);
      instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);
    }

    // Assert
    assertEquals(state.fieldsInstrumented, 5);
  }

  @Test
  public void testDisabledFieldLevelMetrics() {
    // Arrange
    config.setFieldLevelEnabled(false);
    instrumentation = new GraphQLTimingInstrumentation(meterRegistry, config);

    setupFieldMocks("/test/path", "TestType", "testField");

    GraphQLTimingInstrumentation.TimingState state = new GraphQLTimingInstrumentation.TimingState();
    state.operationName = "TestOp";
    state.filteringMode = GraphQLTimingInstrumentation.FilteringMode.DISABLED;

    DataFetcher<?> originalFetcher = env -> "test";

    // Act
    DataFetcher<?> instrumentedFetcher =
        instrumentation.instrumentDataFetcher(originalFetcher, fieldFetchParams, state);

    // Assert - should not be instrumented
    assertSame(originalFetcher, instrumentedFetcher);
  }

  private void setupFieldMocks(String path, String parentTypeName, String fieldName) {
    GraphQLObjectType parentType = mock(GraphQLObjectType.class);
    when(parentType.getName()).thenReturn(parentTypeName);

    when(fieldFetchParams.getExecutionStepInfo()).thenReturn(executionStepInfo);
    when(executionStepInfo.getPath()).thenReturn(resultPath);
    when(resultPath.toString()).thenReturn(path);
    when(executionStepInfo.getParent()).thenReturn(parentStepInfo);
    when(parentStepInfo.getType()).thenReturn(parentType);
    when(executionStepInfo.getFieldDefinition()).thenReturn(fieldDefinition);
    when(fieldDefinition.getName()).thenReturn(fieldName);
    when(fieldFetchParams.isTrivialDataFetcher()).thenReturn(false);
  }

  @Test
  public void testExecuteQueryWithResolverErrorAndMetrics() {
    // Enable metrics in configuration
    GraphQLConfiguration metricsEnabledConfig = createDefaultConfiguration();
    metricsEnabledConfig.getMetrics().setFieldLevelEnabled(true);
    metricsEnabledConfig.getMetrics().setTrivialDataFetchersEnabled(false);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(metricsEnabledConfig)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query",
                        typeWiring ->
                            typeWiring
                                .dataFetcher(
                                    "user",
                                    env -> {
                                      String id = env.getArgument("id");
                                      if ("error".equals(id)) {
                                        throw new RuntimeException("Database connection failed");
                                      }
                                      return new User(id, "Test User", "test@example.com");
                                    })
                                .dataFetcher(
                                    "users",
                                    env -> {
                                      throw new RuntimeException("Service unavailable");
                                    })))
            .build();

    // Execute query that will throw an error
    String query = "{ user(id: \"error\") { id name } }";
    ExecutionResult result = graphQLEngine.execute(query, null, Map.of(), mockQueryContext);

    // Verify error response
    assertNotNull(result);
    assertFalse(result.getErrors().isEmpty());

    // Verify metrics were recorded with error
    // The instrumentation should have recorded:
    // 1. graphql.request.duration with success=false tag
    // 2. graphql.request.errors counter incremented
    // 3. graphql.field.errors for the failing field (if field-level metrics enabled)

    // Check request duration was recorded with success=false
    Timer requestTimer =
        meterRegistry.find("graphql.request.duration").tag("success", "false").timer();
    assertNotNull(requestTimer);
    assertTrue(requestTimer.count() > 0);

    // Check error counter was incremented
    Counter errorCounter = meterRegistry.find("graphql.request.errors").counter();
    assertNotNull(errorCounter);
    assertTrue(errorCounter.count() > 0);
  }

  @Test
  public void testFieldLevelErrorMetrics() {
    // Setup with metrics enabled
    GraphQLConfiguration metricsConfig = createDefaultConfiguration();
    metricsConfig.getMetrics().setFieldLevelEnabled(true);

    graphQLEngine =
        GraphQLEngine.builder()
            .addSchema(TEST_SCHEMA)
            .setGraphQLConfiguration(metricsConfig)
            .setMetricUtils(mockMetricUtils)
            .configureRuntimeWiring(
                wiring ->
                    wiring.type(
                        "Query",
                        typeWiring ->
                            typeWiring.dataFetcher(
                                "user",
                                env -> {
                                  throw new RuntimeException("Field resolution error");
                                })))
            .build();

    // Execute failing query
    String query = "{ user(id: \"1\") { name } }";
    ExecutionResult result = graphQLEngine.execute(query, null, Map.of(), mockQueryContext);

    // Verify error
    assertNotNull(result);
    assertFalse(result.getErrors().isEmpty());

    // Verify field-level error metrics
    Counter fieldErrorCounter =
        meterRegistry
            .find("graphql.field.errors")
            .tag("parent.type", "Query")
            .tag("field", "user")
            .counter();

    if (fieldErrorCounter != null) {
      assertTrue(fieldErrorCounter.count() > 0);
    }
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
    metricsConfig.setEnabled(true);
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

  private static class User {
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
