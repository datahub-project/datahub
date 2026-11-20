package com.linkedin.metadata.search.query.request;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.*;

import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.util.List;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@Import(SearchCommonTestConfiguration.class)
public class SearchRequestHandlerExplainTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Qualifier("queryOperationContext")
  private OperationContext operationContext;

  @Test
  public void testIncludeExplainEnabled() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Create search request with includeExplain = true
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            operationContext.withSearchFlags(
                flags -> flags.setFulltext(false).setIncludeExplain(true)),
            "testQuery",
            null,
            null,
            0,
            10,
            List.of());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify that explain is enabled
    assertTrue(
        sourceBuilder.explain(), "Expected explain to be enabled when includeExplain flag is true");
  }

  @Test
  public void testIncludeExplainDisabled() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Create search request with includeExplain = false (or not set)
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            operationContext.withSearchFlags(
                flags -> flags.setFulltext(false).setIncludeExplain(false)),
            "testQuery",
            null,
            null,
            0,
            10,
            List.of());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify that explain is NOT enabled (null or false)
    Boolean explainValue = sourceBuilder.explain();
    assertTrue(
        explainValue == null || !explainValue,
        "Expected explain to be disabled when includeExplain flag is false");
  }

  @Test
  public void testIncludeExplainDefaultValue() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Create search request without setting includeExplain (should default to false)
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
            "testQuery",
            null,
            null,
            0,
            10,
            List.of());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify that explain is NOT enabled by default (null or false)
    Boolean explainValue = sourceBuilder.explain();
    assertTrue(
        explainValue == null || !explainValue,
        "Expected explain to be disabled by default when includeExplain is not set");
  }
}
