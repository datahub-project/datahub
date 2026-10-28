package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.LatestIngestionRuns;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSearchDAOLatestIngestionRunsTest {

  private static final Urn SOURCE_A = UrnUtils.getUrn("urn:li:dataHubIngestionSource:a");
  private static final Urn SOURCE_B = UrnUtils.getUrn("urn:li:dataHubIngestionSource:b");

  private SearchClientShim<?> mockClient;
  private ESSearchDAO esSearchDAO;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    mockClient = Mockito.mock(SearchClientShim.class);
    opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoSearchAuthorization(), mockClient);
    esSearchDAO =
        new ESSearchDAO(
            false,
            TEST_OS_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
  }

  @Test
  public void testRequestGroupsBySourceWithLatestRunPerStatus() {
    String source =
        esSearchDAO
            .buildLatestIngestionRunsRequest(opContext, Set.of(SOURCE_A, SOURCE_B))
            .source()
            .toString();

    assertTrue(source.contains("\"field\":\"ingestionSource.keyword\""), source);
    assertTrue(source.contains("\"top_hits\""), source);
    assertTrue(source.contains("requestTimeMs"), source);
    assertTrue(
        source.contains("{\"executionResultStatus.keyword\":{\"value\":\"SUCCESS\""), source);
    assertTrue(
        source.contains("{\"executionResultStatus.keyword\":{\"value\":\"FAILURE\""), source);
  }

  @Test
  public void testParsesLatestRunsAndSkipsSourcesWithoutRuns() throws Exception {
    SearchResponse response = Mockito.mock(SearchResponse.class);
    Aggregations aggs = Mockito.mock(Aggregations.class);
    Terms bySource = Mockito.mock(Terms.class);
    Mockito.when(response.getAggregations()).thenReturn(aggs);
    Mockito.when(aggs.<Terms>get("byIngestionSource")).thenReturn(bySource);
    Mockito.doReturn(
            List.of(
                bucket(SOURCE_A, "urn:li:dataHubExecutionRequest:a3", "a1", "a3"),
                bucket(SOURCE_B, null, null, null)))
        .when(bySource)
        .getBuckets();
    Mockito.when(
            mockClient.search(
                Mockito.any(),
                Mockito.any(SearchRequest.class),
                Mockito.eq(RequestOptions.DEFAULT)))
        .thenReturn(response);

    Map<Urn, LatestIngestionRuns> runs =
        esSearchDAO.getLatestIngestionRuns(opContext, Set.of(SOURCE_A, SOURCE_B));

    assertEquals(runs.keySet(), Set.of(SOURCE_A));
    LatestIngestionRuns a = runs.get(SOURCE_A);
    assertEquals(a.getLatestRun(), UrnUtils.getUrn("urn:li:dataHubExecutionRequest:a3"));
    assertEquals(a.getLatestSuccessfulRun(), UrnUtils.getUrn("urn:li:dataHubExecutionRequest:a1"));
    assertEquals(a.getLatestFailedRun(), UrnUtils.getUrn("urn:li:dataHubExecutionRequest:a3"));
  }

  @Test
  public void testPartitionsLargeSourceSets() throws Exception {
    int batchSize = ESSearchDAO.INGESTION_RUN_STATS_URN_BATCH_SIZE;
    Set<Urn> sources = new LinkedHashSet<>();
    for (int i = 0; i <= batchSize; i++) {
      sources.add(UrnUtils.getUrn("urn:li:dataHubIngestionSource:s" + i));
    }
    SearchResponse empty = Mockito.mock(SearchResponse.class);
    Mockito.when(
            mockClient.search(
                Mockito.any(),
                Mockito.any(SearchRequest.class),
                Mockito.eq(RequestOptions.DEFAULT)))
        .thenReturn(empty);

    assertTrue(esSearchDAO.getLatestIngestionRuns(opContext, sources).isEmpty());

    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    Mockito.verify(mockClient, Mockito.times(2))
        .search(Mockito.any(), captor.capture(), Mockito.eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testEmptyInputSkipsSearch() {
    assertTrue(esSearchDAO.getLatestIngestionRuns(opContext, Set.of()).isEmpty());
    Mockito.verifyNoInteractions(mockClient);
  }

  private static Terms.Bucket bucket(
      Urn source, @Nullable String latest, @Nullable String successful, @Nullable String failed) {
    Terms.Bucket bucket = Mockito.mock(Terms.Bucket.class);
    Mockito.when(bucket.getKeyAsString()).thenReturn(source.toString());
    Aggregations sub = Mockito.mock(Aggregations.class);
    Mockito.when(bucket.getAggregations()).thenReturn(sub);
    TopHits latestHits = topHits(latest);
    Mockito.when(sub.<TopHits>get("latestRun")).thenReturn(latestHits);
    Filter successfulRuns = filtered(successful);
    Filter failedRuns = filtered(failed);
    Mockito.when(sub.<Filter>get("successfulRuns")).thenReturn(successfulRuns);
    Mockito.when(sub.<Filter>get("failedRuns")).thenReturn(failedRuns);
    return bucket;
  }

  private static Filter filtered(@Nullable String runId) {
    Filter filter = Mockito.mock(Filter.class);
    Aggregations sub = Mockito.mock(Aggregations.class);
    Mockito.when(filter.getAggregations()).thenReturn(sub);
    TopHits hits = topHits(runId == null ? null : "urn:li:dataHubExecutionRequest:" + runId);
    Mockito.when(sub.<TopHits>get("latestRun")).thenReturn(hits);
    return filter;
  }

  private static TopHits topHits(@Nullable String urn) {
    TopHits topHits = Mockito.mock(TopHits.class);
    SearchHits hits = Mockito.mock(SearchHits.class);
    Mockito.when(topHits.getHits()).thenReturn(hits);
    if (urn == null) {
      Mockito.when(hits.getHits()).thenReturn(new SearchHit[0]);
    } else {
      SearchHit hit = Mockito.mock(SearchHit.class);
      Mockito.when(hit.getSourceAsMap()).thenReturn(Map.<String, Object>of("urn", urn));
      Mockito.when(hits.getHits()).thenReturn(new SearchHit[] {hit});
    }
    return topHits;
  }
}
