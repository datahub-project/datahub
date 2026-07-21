package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.ParsedTopHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSearchDAOSearchLatestPerGroupTest {

  private static final String SOURCE_A = "urn:li:dataHubIngestionSource:source-a";
  private static final String SOURCE_B = "urn:li:dataHubIngestionSource:source-b";
  private static final String SOURCE_MISSING = "urn:li:dataHubIngestionSource:missing";
  private static final Urn EXEC_A = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:exec-a");
  private static final Urn EXEC_B = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:exec-b");

  private ESSearchDAO esSearchDAO;
  private OperationContext opContext;
  private SearchClientShim<?> mockClient;

  @BeforeMethod
  public void setUp() {
    mockClient = mock(SearchClientShim.class);
    opContext = TestOperationContexts.systemContextNoValidate();
    esSearchDAO =
        new ESSearchDAO(
            mockClient,
            false,
            TEST_OS_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
  }

  @Test
  public void testEmptyGroupValuesDoesNotCallClient() throws Exception {
    Map<String, SearchResult> results =
        esSearchDAO.searchLatestPerGroup(
            opContext,
            Constants.EXECUTION_REQUEST_ENTITY_NAME,
            "ingestionSource",
            Collections.emptyList(),
            List.of(new SortCriterion().setField("requestTimeMs").setOrder(SortOrder.DESCENDING)));

    assertTrue(results.isEmpty());
    verify(mockClient, never()).search(any(), any(), any());
  }

  @Test
  public void testMapsLatestUrnTotalsAndEmptyGroups() throws Exception {
    SearchHit hitA = mockHit(EXEC_A.toString());
    SearchHit hitB = mockHit(EXEC_B.toString());

    Terms.Bucket bucketA = mockBucket(SOURCE_A, 5, hitA);
    Terms.Bucket bucketB = mockBucket(SOURCE_B, 2, hitB);

    ParsedTerms parsedTerms = mock(ParsedTerms.class);
    doReturn(List.of(bucketA, bucketB)).when(parsedTerms).getBuckets();

    Aggregations topLevelAggs = mock(Aggregations.class);
    when(topLevelAggs.get(ESSearchDAO.GROUP_BUCKETS_AGG)).thenReturn(parsedTerms);

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getAggregations()).thenReturn(topLevelAggs);
    when(mockClient.search(
            any(OperationContext.class), any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    Map<String, SearchResult> results =
        esSearchDAO.searchLatestPerGroup(
            opContext,
            Constants.EXECUTION_REQUEST_ENTITY_NAME,
            "ingestionSource",
            List.of(SOURCE_A, SOURCE_B, SOURCE_MISSING),
            List.of(new SortCriterion().setField("requestTimeMs").setOrder(SortOrder.DESCENDING)));

    // Every input key is present.
    assertEquals(results.size(), 3);

    SearchResult resultA = results.get(SOURCE_A);
    assertEquals(resultA.getFrom(), 0);
    assertEquals(resultA.getPageSize(), 1);
    assertEquals(resultA.getNumEntities(), 5);
    assertEquals(resultA.getEntities().size(), 1);
    assertEquals(resultA.getEntities().get(0).getEntity(), EXEC_A);

    SearchResult resultB = results.get(SOURCE_B);
    assertEquals(resultB.getNumEntities(), 2);
    assertEquals(resultB.getEntities().get(0).getEntity(), EXEC_B);

    SearchResult missing = results.get(SOURCE_MISSING);
    assertEquals(missing.getNumEntities(), 0);
    assertTrue(missing.getEntities().isEmpty());

    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient)
        .search(any(OperationContext.class), requestCaptor.capture(), eq(RequestOptions.DEFAULT));
    SearchRequest request = requestCaptor.getValue();
    assertEquals(request.source().size(), 0);
    assertTrue(
        request.source().aggregations().getAggregatorFactories().stream()
            .anyMatch(factory -> ESSearchDAO.GROUP_BUCKETS_AGG.equals(factory.getName())));
  }

  @Test
  public void testAggregationFailureFailsLoud() throws Exception {
    doThrow(new IOException("boom"))
        .when(mockClient)
        .search(any(OperationContext.class), any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    assertThrows(
        ESQueryException.class,
        () ->
            esSearchDAO.searchLatestPerGroup(
                opContext,
                Constants.EXECUTION_REQUEST_ENTITY_NAME,
                "ingestionSource",
                List.of(SOURCE_A, SOURCE_B),
                List.of(
                    new SortCriterion().setField("requestTimeMs").setOrder(SortOrder.DESCENDING))));
  }

  private static SearchHit mockHit(String urn) {
    SearchHit hit = mock(SearchHit.class);
    Map<String, Object> source = new HashMap<>();
    source.put("urn", urn);
    when(hit.getSourceAsMap()).thenReturn(source);
    return hit;
  }

  private static Terms.Bucket mockBucket(String key, long docCount, SearchHit hit) {
    SearchHits topHitsResult = mock(SearchHits.class);
    when(topHitsResult.getHits()).thenReturn(new SearchHit[] {hit});

    ParsedTopHits parsedTopHits = mock(ParsedTopHits.class);
    when(parsedTopHits.getHits()).thenReturn(topHitsResult);

    Aggregations bucketAggs = mock(Aggregations.class);
    when(bucketAggs.get(ESSearchDAO.LATEST_TOP_HITS_AGG)).thenReturn(parsedTopHits);

    Terms.Bucket bucket = mock(Terms.Bucket.class);
    when(bucket.getKeyAsString()).thenReturn(key);
    when(bucket.getDocCount()).thenReturn(docCount);
    when(bucket.getAggregations()).thenReturn(bucketAggs);
    return bucket;
  }
}
