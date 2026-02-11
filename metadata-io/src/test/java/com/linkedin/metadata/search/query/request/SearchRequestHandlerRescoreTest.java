package com.linkedin.metadata.search.query.request;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.RescoreConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestOptions;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@Import(SearchCommonTestConfiguration.class)
public class SearchRequestHandlerRescoreTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Qualifier("queryOperationContext")
  private OperationContext operationContext;

  @Test
  public void testRescoreDisabledByDefault() {
    // Create SearchConfiguration with rescore disabled
    SearchConfiguration searchConfig = new SearchConfiguration();
    RescoreConfiguration rescoreConfig = RescoreConfiguration.builder().enabled(false).build();
    searchConfig.setRescore(rescoreConfig);

    // Update operation context with search configuration
    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            testOpContext, SearchRequestOptions.builder().input("test query").size(10).build());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify no rescore is applied
    assertTrue(
        sourceBuilder.rescores() == null || sourceBuilder.rescores().isEmpty(),
        "Expected no rescore when rescore is disabled");
  }

  @Test
  public void testRescoreEnabledWithConfiguration() {
    // Create SearchConfiguration with rescore enabled
    SearchConfiguration searchConfig = new SearchConfiguration();

    // Create function score config
    Map<String, Object> functionScore = new HashMap<>();
    List<Map<String, Object>> functions =
        List.of(
            Map.of(
                "filter",
                Map.of("term", Map.of("hasDescription", Map.of("value", true))),
                "weight",
                1.5));
    functionScore.put("functions", functions);
    functionScore.put("score_mode", "multiply");
    functionScore.put("boost_mode", "multiply");

    RescoreConfiguration rescoreConfig =
        RescoreConfiguration.builder()
            .enabled(true)
            .windowSize(500)
            .queryWeight(0.7f)
            .rescoreQueryWeight(0.3f)
            .functionScore(functionScore)
            .build();
    searchConfig.setRescore(rescoreConfig);

    // Update operation context with search configuration
    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            testOpContext, SearchRequestOptions.builder().input("test query").size(10).build());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify rescore is applied
    assertNotNull(sourceBuilder.rescores(), "Expected rescore to be present");
    assertFalse(sourceBuilder.rescores().isEmpty(), "Expected rescore list to not be empty");
    assertEquals(sourceBuilder.rescores().size(), 1, "Expected exactly one rescore");
  }

  @Test
  public void testRescoreDisabledViaSearchFlags() {
    // Create SearchConfiguration with rescore enabled globally
    SearchConfiguration searchConfig = new SearchConfiguration();
    Map<String, Object> functionScore = new HashMap<>();
    List<Map<String, Object>> functions =
        List.of(
            Map.of(
                "filter",
                Map.of("term", Map.of("hasDescription", Map.of("value", true))),
                "weight",
                1.5));
    functionScore.put("functions", functions);
    functionScore.put("score_mode", "multiply");
    functionScore.put("boost_mode", "multiply");

    RescoreConfiguration rescoreConfig =
        RescoreConfiguration.builder()
            .enabled(true)
            .windowSize(500)
            .queryWeight(0.7f)
            .rescoreQueryWeight(0.3f)
            .functionScore(functionScore)
            .build();
    searchConfig.setRescore(rescoreConfig);

    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Disable rescore via SearchFlags
    OperationContext opContextWithFlags =
        testOpContext.withSearchFlags(flags -> flags.setRescoreEnabled(false));

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            opContextWithFlags, "test query", null, null, null, null, null, 10, List.of(), null);

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify rescore is NOT applied due to SearchFlags override
    assertTrue(
        sourceBuilder.rescores() == null || sourceBuilder.rescores().isEmpty(),
        "Expected no rescore when rescoreEnabled=false in SearchFlags");
  }

  @Test
  public void testRescoreOverrideViaSearchFlags() throws Exception {
    // Create SearchConfiguration with rescore enabled
    SearchConfiguration searchConfig = new SearchConfiguration();
    Map<String, Object> defaultFunctionScore = new HashMap<>();
    List<Map<String, Object>> defaultFunctions =
        List.of(
            Map.of(
                "filter",
                Map.of("term", Map.of("hasDescription", Map.of("value", true))),
                "weight",
                1.5));
    defaultFunctionScore.put("functions", defaultFunctions);
    defaultFunctionScore.put("score_mode", "multiply");
    defaultFunctionScore.put("boost_mode", "multiply");

    RescoreConfiguration rescoreConfig =
        RescoreConfiguration.builder()
            .enabled(true)
            .windowSize(500)
            .queryWeight(0.7f)
            .rescoreQueryWeight(0.3f)
            .functionScore(defaultFunctionScore)
            .build();
    searchConfig.setRescore(rescoreConfig);

    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Create rescore override JSON
    Map<String, Object> overrideFunctionScore = new HashMap<>();
    List<Map<String, Object>> overrideFunctions =
        List.of(
            Map.of(
                "filter",
                Map.of("term", Map.of("hasOwners", Map.of("value", true))),
                "weight",
                2.0));
    overrideFunctionScore.put("functions", overrideFunctions);
    overrideFunctionScore.put("score_mode", "sum");
    overrideFunctionScore.put("boost_mode", "sum");

    Map<String, Object> rescoreOverride = new HashMap<>();
    rescoreOverride.put("window_size", 200);
    rescoreOverride.put("query_weight", 0.5f);
    rescoreOverride.put("rescore_query_weight", 0.5f);
    rescoreOverride.put("function_score", overrideFunctionScore);

    String rescoreOverrideJson = new ObjectMapper().writeValueAsString(rescoreOverride);

    // Apply rescore override via SearchFlags
    OperationContext opContextWithOverride =
        testOpContext.withSearchFlags(flags -> flags.setRescoreOverride(rescoreOverrideJson));

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            opContextWithOverride, "test query", null, null, null, null, null, 10, List.of(), null);

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify rescore is applied (we can't easily verify the override content without reflection)
    assertNotNull(sourceBuilder.rescores(), "Expected rescore to be present");
    assertFalse(sourceBuilder.rescores().isEmpty(), "Expected rescore list to not be empty");
  }

  @Test
  public void testRescoreWithNullConfiguration() {
    // Create SearchConfiguration with null rescore
    SearchConfiguration searchConfig = new SearchConfiguration();
    searchConfig.setRescore(null);

    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            testOpContext, SearchRequestOptions.builder().input("test query").size(10).build());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify no rescore is applied when configuration is null
    assertTrue(
        sourceBuilder.rescores() == null || sourceBuilder.rescores().isEmpty(),
        "Expected no rescore when rescore configuration is null");
  }

  @Test
  public void testRescoreEnabledTrueViaSearchFlags() {
    // Create SearchConfiguration with rescore disabled globally
    SearchConfiguration searchConfig = new SearchConfiguration();
    Map<String, Object> functionScore = new HashMap<>();
    List<Map<String, Object>> functions =
        List.of(
            Map.of(
                "filter",
                Map.of("term", Map.of("hasDescription", Map.of("value", true))),
                "weight",
                1.5));
    functionScore.put("functions", functions);
    functionScore.put("score_mode", "multiply");
    functionScore.put("boost_mode", "multiply");

    RescoreConfiguration rescoreConfig =
        RescoreConfiguration.builder()
            .enabled(false) // Disabled globally
            .windowSize(500)
            .queryWeight(0.7f)
            .rescoreQueryWeight(0.3f)
            .functionScore(functionScore)
            .build();
    searchConfig.setRescore(rescoreConfig);

    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Enable rescore via SearchFlags (should override global config)
    OperationContext opContextWithFlags =
        testOpContext.withSearchFlags(flags -> flags.setRescoreEnabled(true));

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            opContextWithFlags, "test query", null, null, null, null, null, 10, List.of(), null);

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify rescore IS applied due to SearchFlags override
    assertNotNull(
        sourceBuilder.rescores(),
        "Expected rescore to be present when rescoreEnabled=true in SearchFlags");
    assertFalse(sourceBuilder.rescores().isEmpty(), "Expected rescore list to not be empty");
  }

  @Test
  public void testRescoreOverrideActuallyReplacesWindowSize() throws Exception {
    // Create SearchConfiguration with default rescore
    SearchConfiguration searchConfig = new SearchConfiguration();
    Map<String, Object> defaultFunctionScore = new HashMap<>();
    defaultFunctionScore.put("functions", List.of());
    defaultFunctionScore.put("score_mode", "multiply");

    RescoreConfiguration rescoreConfig =
        RescoreConfiguration.builder()
            .enabled(true)
            .windowSize(500) // Default window size
            .queryWeight(0.7f)
            .rescoreQueryWeight(0.3f)
            .functionScore(defaultFunctionScore)
            .build();
    searchConfig.setRescore(rescoreConfig);

    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Create rescore override with different window size
    Map<String, Object> rescoreOverride = new HashMap<>();
    rescoreOverride.put("window_size", 200); // Override to 200 (was 500)
    rescoreOverride.put("query_weight", 0.5f);
    rescoreOverride.put("rescore_query_weight", 0.5f);
    rescoreOverride.put("function_score", defaultFunctionScore);

    String rescoreOverrideJson = new ObjectMapper().writeValueAsString(rescoreOverride);

    OperationContext opContextWithOverride =
        testOpContext.withSearchFlags(flags -> flags.setRescoreOverride(rescoreOverrideJson));

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            opContextWithOverride, "test query", null, null, null, null, null, 10, List.of(), null);

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify rescore is applied with overridden window size
    assertNotNull(sourceBuilder.rescores(), "Expected rescore to be present");
    assertFalse(sourceBuilder.rescores().isEmpty(), "Expected rescore list to not be empty");

    // Extract the rescore builder and verify window size
    org.opensearch.search.rescore.RescorerBuilder<?> rescorerBuilder =
        sourceBuilder.rescores().get(0);
    assertTrue(
        rescorerBuilder instanceof org.opensearch.search.rescore.QueryRescorerBuilder,
        "Expected QueryRescorerBuilder");

    org.opensearch.search.rescore.QueryRescorerBuilder queryRescorer =
        (org.opensearch.search.rescore.QueryRescorerBuilder) rescorerBuilder;

    // Verify window size was overridden (200, not default 500)
    assertEquals(
        queryRescorer.windowSize().intValue(), 200, "Expected window size to be overridden to 200");
  }

  @Test
  public void testRescoreOverrideActuallyReplacesWeights() throws Exception {
    // Create SearchConfiguration with default rescore
    SearchConfiguration searchConfig = new SearchConfiguration();
    Map<String, Object> defaultFunctionScore = new HashMap<>();
    defaultFunctionScore.put("functions", List.of());
    defaultFunctionScore.put("score_mode", "multiply");

    RescoreConfiguration rescoreConfig =
        RescoreConfiguration.builder()
            .enabled(true)
            .windowSize(500)
            .queryWeight(0.7f) // Default weights
            .rescoreQueryWeight(0.3f)
            .functionScore(defaultFunctionScore)
            .build();
    searchConfig.setRescore(rescoreConfig);

    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Create rescore override with different weights
    Map<String, Object> rescoreOverride = new HashMap<>();
    rescoreOverride.put("window_size", 500);
    rescoreOverride.put("query_weight", 0.5f); // Override to 0.5 (was 0.7)
    rescoreOverride.put("rescore_query_weight", 0.5f); // Override to 0.5 (was 0.3)
    rescoreOverride.put("function_score", defaultFunctionScore);

    String rescoreOverrideJson = new ObjectMapper().writeValueAsString(rescoreOverride);

    OperationContext opContextWithOverride =
        testOpContext.withSearchFlags(flags -> flags.setRescoreOverride(rescoreOverrideJson));

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            opContextWithOverride, "test query", null, null, null, null, null, 10, List.of(), null);

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify rescore is applied
    assertNotNull(sourceBuilder.rescores(), "Expected rescore to be present");
    assertFalse(sourceBuilder.rescores().isEmpty(), "Expected rescore list to not be empty");

    // Extract and verify weights
    org.opensearch.search.rescore.QueryRescorerBuilder queryRescorer =
        (org.opensearch.search.rescore.QueryRescorerBuilder) sourceBuilder.rescores().get(0);

    // Verify weights were overridden
    assertEquals(
        queryRescorer.getQueryWeight(),
        0.5f,
        0.001f,
        "Expected query weight to be overridden to 0.5");
    assertEquals(
        queryRescorer.getRescoreQueryWeight(),
        0.5f,
        0.001f,
        "Expected rescore query weight to be overridden to 0.5");
  }

  @Test
  public void testRescoreUsesMultiplicativeScoreMode() {
    // Create SearchConfiguration with rescore enabled
    SearchConfiguration searchConfig = new SearchConfiguration();
    Map<String, Object> functionScore = new HashMap<>();
    List<Map<String, Object>> functions =
        List.of(
            Map.of(
                "filter",
                Map.of("term", Map.of("hasDescription", Map.of("value", true))),
                "weight",
                1.5));
    functionScore.put("functions", functions);
    functionScore.put("score_mode", "multiply");
    functionScore.put("boost_mode", "multiply");

    RescoreConfiguration rescoreConfig =
        RescoreConfiguration.builder()
            .enabled(true)
            .windowSize(500)
            .queryWeight(1.0f)
            .rescoreQueryWeight(1.0f)
            .scoreMode("multiply")
            .functionScore(functionScore)
            .build();
    searchConfig.setRescore(rescoreConfig);

    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            testOpContext, SearchRequestOptions.builder().input("test query").size(10).build());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify rescore is applied
    assertNotNull(sourceBuilder.rescores(), "Expected rescore to be present");
    assertFalse(sourceBuilder.rescores().isEmpty(), "Expected rescore list to not be empty");

    // Extract and verify score mode is Multiply
    org.opensearch.search.rescore.QueryRescorerBuilder queryRescorer =
        (org.opensearch.search.rescore.QueryRescorerBuilder) sourceBuilder.rescores().get(0);

    // Verify score mode is set to Multiply (the key change in the ranking improvement)
    assertEquals(
        queryRescorer.getScoreMode(),
        org.opensearch.search.rescore.QueryRescoreMode.Multiply,
        "Expected score mode to be Multiply for multiplicative rescoring");

    // Verify weights are 1.0 for pure multiplication
    assertEquals(
        queryRescorer.getQueryWeight(),
        1.0f,
        0.001f,
        "Expected query weight to be 1.0 for pure multiplication");
    assertEquals(
        queryRescorer.getRescoreQueryWeight(),
        1.0f,
        0.001f,
        "Expected rescore query weight to be 1.0 for pure multiplication");
  }

  @Test
  public void testRescoreInvalidOverrideFallsBackToDefault() throws Exception {
    // Create SearchConfiguration with valid rescore
    SearchConfiguration searchConfig = new SearchConfiguration();
    Map<String, Object> defaultFunctionScore = new HashMap<>();
    defaultFunctionScore.put("functions", List.of());
    defaultFunctionScore.put("score_mode", "multiply");

    RescoreConfiguration rescoreConfig =
        RescoreConfiguration.builder()
            .enabled(true)
            .windowSize(500)
            .queryWeight(0.7f)
            .rescoreQueryWeight(0.3f)
            .functionScore(defaultFunctionScore)
            .build();
    searchConfig.setRescore(rescoreConfig);

    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchConfiguration(searchConfig).build();
    OperationContext testOpContext =
        operationContext.toBuilder()
            .searchContext(searchContext)
            .build(operationContext.getSessionAuthentication(), false);

    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            testOpContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Create invalid rescore override (malformed JSON)
    String invalidJson = "{invalid json}";

    OperationContext opContextWithInvalidOverride =
        testOpContext.withSearchFlags(flags -> flags.setRescoreOverride(invalidJson));

    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            opContextWithInvalidOverride,
            "test query",
            null,
            null,
            null,
            null,
            null,
            10,
            List.of(),
            null);

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify rescore still applied with default config (graceful fallback)
    assertNotNull(
        sourceBuilder.rescores(), "Expected rescore with default config after invalid override");
    assertFalse(sourceBuilder.rescores().isEmpty(), "Expected rescore list to not be empty");

    // Verify it's using default window size (500, not from invalid override)
    org.opensearch.search.rescore.QueryRescorerBuilder queryRescorer =
        (org.opensearch.search.rescore.QueryRescorerBuilder) sourceBuilder.rescores().get(0);
    assertEquals(
        queryRescorer.windowSize().intValue(),
        500,
        "Expected default window size after invalid override");
  }
}
