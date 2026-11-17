package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.Test;

public class ESSearchDAOAutocompleteTest {

  @Test
  public void testV3AutocompleteRendersAShortPrefix() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    OperationContext opContext = operationContext(client);
    SearchResponse response = emptyResponse();
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(response);
    ESSearchDAO dao = dao(v3KeywordReads());
    Filter filter = platformFilter();

    dao.autoComplete(opContext, "dataset", "t", null, filter, 10);
    dao.autoComplete(opContext, "dataset", "te", null, filter, 10);

    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client, org.mockito.Mockito.times(2))
        .search(eq(opContext), request.capture(), eq(RequestOptions.DEFAULT));
    for (SearchRequest captured : request.getAllValues()) {
      String body = captured.source().toString().replace(" ", "");
      assertTrue(body.contains("\"match_phrase_prefix\""), body);
      assertTrue(body.contains("\"name.text\""), body);
      assertTrue(body.contains("\"analyzer\":\"partial\""), body);
      assertTrue(body.contains("\"max_expansions\":50"), body);
      // -1 is the client's disabled total-hit sentinel. The ES 8 shim sends that as enabled: false.
      assertEquals(captured.source().trackTotalHitsUpTo().intValue(), -1);
      assertTrue(body.contains("urn:li:dataPlatform:hive"), body);
      assertNull(captured.source().aggregations());
    }
    assertTrue(request.getAllValues().get(0).source().query().toString().contains("\"t\""));
    assertTrue(request.getAllValues().get(1).source().query().toString().contains("te"));
  }

  @Test
  public void testV3AutocompleteUsesTheCallerField() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    OperationContext opContext = operationContext(client);
    SearchResponse response = emptyResponse();
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(response);
    ESSearchDAO dao = dao(v3KeywordReads());

    dao.autoComplete(opContext, "dataset", "te", "qualifiedName", null, 10);

    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client).search(eq(opContext), request.capture(), eq(RequestOptions.DEFAULT));
    String body = request.getValue().source().toString().replace(" ", "");
    assertTrue(body.contains("\"qualifiedName.text\""), body);
    assertFalse(body.contains("\"name.text\""), body);
  }

  @Test
  public void testBlankV3QueryDoesNotSearch() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    OperationContext opContext = operationContext(client);
    ESSearchDAO dao = dao(v3KeywordReads());

    AutoCompleteResult result = dao.autoComplete(opContext, "dataset", "  ", null, null, 10);

    assertEquals(result.getQuery(), "  ");
    assertTrue(result.getSuggestions().isEmpty());
    assertTrue(result.getEntities().isEmpty());
    verify(client, never()).search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testV3HitMapsStoredNameWithoutAHighlight() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    OperationContext opContext = operationContext(client);
    SearchHit named = new SearchHit(1);
    named.sourceRef(
        new BytesArray(
            "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)\",\"name\":\"orders\"}"));
    SearchHit unnamed = new SearchHit(2);
    unnamed.sourceRef(new BytesArray("{\"urn\":\"urn:li:corpuser:ada\"}"));
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {named, unnamed},
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                0f));
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(response);
    ESSearchDAO dao = dao(v3KeywordReads());

    AutoCompleteResult result = dao.autoComplete(opContext, "dataset", "or", null, null, 10);

    assertEquals(result.getSuggestions().size(), 2);
    assertEquals(result.getEntities().size(), 2);
    assertEquals(result.getSuggestions().get(0), "orders");
    assertEquals(
        result.getEntities().get(0).getUrn().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)");
    assertEquals(result.getSuggestions().get(1), "ada");
    assertEquals(result.getEntities().get(1).getUrn().toString(), "urn:li:corpuser:ada");
  }

  @Test
  public void testSearchPageDoesNotDisableTotalHits() {
    OperationContext opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoValidate(), mock(SearchClientShim.class));
    ESSearchDAO dao = dao(v3KeywordReads());

    SearchRequest page = dao.buildFilterRequest(opContext, "dataset", null, null, 0, 10);

    assertFalse(
        page.source().toString().replace(" ", "").contains("\"track_total_hits\":false"),
        page.source().toString());
  }

  @Test
  public void testV2AutocompleteBuildsAQuery() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    OperationContext opContext = operationContext(client);
    SearchResponse response = emptyResponse();
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(response);
    ESSearchDAO dao = dao(TEST_OS_SEARCH_CONFIG);

    AutoCompleteResult result = dao.autoComplete(opContext, "dataset", "orders", null, null, 10);

    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client).search(eq(opContext), request.capture(), eq(RequestOptions.DEFAULT));
    String query = request.getValue().source().query().toString();
    assertTrue(request.getValue().source().query() instanceof FunctionScoreQueryBuilder);
    assertTrue(query.contains("orders"), query);
    assertTrue(query.contains("ngram"), query);
    assertEquals(result.getQuery(), "orders");
  }

  private static OperationContext operationContext(SearchClientShim<?> client) {
    return TestOperationContexts.withFixedSearchClient(
        TestOperationContexts.systemContextNoValidate(), client);
  }

  private static SearchResponse emptyResponse() {
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0f));
    return response;
  }

  private static Filter platformFilter() {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            buildCriterion(
                                "platform", Condition.EQUAL, "urn:li:dataPlatform:hive")))));
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
