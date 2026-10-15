package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.Test;

public class ESSearchDAOAutocompleteTest {

  @Test
  public void testV3KeywordReadsReturnNoAutocomplete() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    OperationContext opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoValidate(), client);
    ESSearchDAO dao = dao(v3KeywordReads());

    AutoCompleteResult result = dao.autoComplete(opContext, "dataset", "orders", null, null, 10);

    assertEquals(result.getQuery(), "orders");
    assertTrue(result.getSuggestions().isEmpty());
    assertTrue(result.getEntities().isEmpty());
    verify(client, never()).search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testV2AutocompleteBuildsAQuery() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    OperationContext opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoValidate(), client);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0f));
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(response);
    ESSearchDAO dao = dao(TEST_OS_SEARCH_CONFIG);

    AutoCompleteResult result = dao.autoComplete(opContext, "dataset", "orders", null, null, 10);

    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client).search(eq(opContext), request.capture(), eq(RequestOptions.DEFAULT));
    assertTrue(request.getValue().source().query() instanceof FunctionScoreQueryBuilder);
    assertTrue(request.getValue().source().query().toString().contains("orders"));
    assertEquals(result.getQuery(), "orders");
  }

  private static ESSearchDAO dao(ElasticSearchConfiguration config) {
    return new ESSearchDAO(
        false, config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG);
  }

  private static ElasticSearchConfiguration v3KeywordReads() {
    return TEST_OS_SEARCH_CONFIG.toBuilder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
                .v3(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(true)
                        .keywordReadEnabled(true)
                        .build())
                .build())
        .build();
  }
}
