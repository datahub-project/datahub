package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.IncidentStats;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
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
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSearchDAOIncidentStatsTest {

  private SearchClientShim<?> mockClient;
  private ESSearchDAO esSearchDAO;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    mockClient = Mockito.mock(SearchClientShim.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
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
  public void testBuildActiveIncidentStatsRequestShape() throws Exception {
    final Set<Urn> urns =
        Set.of(
            Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:x,a,PROD)"),
            Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:x,b,PROD)"));

    final SearchRequest request = esSearchDAO.buildActiveIncidentStatsRequest(opContext, urns);
    final String source = request.source().toString();

    assertTrue(source.contains("\"terms\""), "expected terms aggregation");
    assertTrue(source.contains("entities.keyword"), "expected group-by on entities.keyword");
    assertTrue(source.contains("\"top_hits\""), "expected top_hits sub-agg for latest incident");
    assertTrue(source.contains("lastUpdated"), "expected sort by lastUpdated");
    assertTrue(source.contains("ACTIVE"), "expected active-state filter");
  }

  @Test
  public void testGetActiveIncidentStatsPartitionsLargeUrnSet() throws Exception {
    final int batchSize = ESSearchDAO.INCIDENT_STATS_URN_BATCH_SIZE;
    // One more than two full batches forces exactly three partitioned requests.
    final int urnCount = 2 * batchSize + 1;
    final Set<Urn> urns = new LinkedHashSet<>();
    for (int i = 0; i < urnCount; i++) {
      urns.add(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:x,tbl" + i + ",PROD)"));
    }

    // Aggregation-less response -> extractIncidentStats returns empty; we assert on request shape.
    final SearchResponse emptyResponse = Mockito.mock(SearchResponse.class);
    Mockito.when(emptyResponse.getAggregations()).thenReturn(null);
    Mockito.when(
            mockClient.search(
                Mockito.any(),
                Mockito.any(SearchRequest.class),
                Mockito.eq(RequestOptions.DEFAULT)))
        .thenReturn(emptyResponse);

    esSearchDAO.getActiveIncidentStats(opContext, urns);

    final ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    Mockito.verify(mockClient, Mockito.times(3))
        .search(Mockito.any(), captor.capture(), Mockito.eq(RequestOptions.DEFAULT));

    int total = 0;
    for (SearchRequest request : captor.getAllValues()) {
      // Each URN appears twice per request (terms filter + include array), so occurrences / 2 is
      // the URN count carried by that request.
      final int urnsInRequest = countOccurrences(request.source().toString(), ",tbl") / 2;
      assertTrue(
          urnsInRequest > 0 && urnsInRequest <= batchSize,
          "each partitioned request must carry between 1 and "
              + batchSize
              + " URNs, got "
              + urnsInRequest);
      total += urnsInRequest;
    }
    assertTrue(total == urnCount, "partitions must cover every URN exactly once");
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    for (int idx = haystack.indexOf(needle); idx >= 0; idx = haystack.indexOf(needle, idx + 1)) {
      count++;
    }
    return count;
  }

  @Test
  public void testGetActiveIncidentStatsParsesAggregation() throws Exception {
    final Urn dsA = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:x,a,PROD)");
    final Urn dsB = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:x,b,PROD)");
    final String latestA = "urn:li:incident:a-latest";

    final SearchResponse response = Mockito.mock(SearchResponse.class);
    final Aggregations aggs = Mockito.mock(Aggregations.class);
    final Terms byEntity = Mockito.mock(Terms.class);
    Mockito.when(response.getAggregations()).thenReturn(aggs);
    Mockito.when(aggs.<Terms>get("byEntity")).thenReturn(byEntity);
    // dsA: 3 active incidents with a resolved latest-incident urn; dsB: 1 active, no top hit.
    Mockito.doReturn(List.of(bucket(dsA.toString(), 3, latestA), bucket(dsB.toString(), 1, null)))
        .when(byEntity)
        .getBuckets();
    Mockito.when(
            mockClient.search(
                Mockito.any(),
                Mockito.any(SearchRequest.class),
                Mockito.eq(RequestOptions.DEFAULT)))
        .thenReturn(response);

    final Map<Urn, IncidentStats> stats =
        esSearchDAO.getActiveIncidentStats(opContext, Set.of(dsA, dsB));

    assertEquals(stats.get(dsA).getActiveCount(), 3);
    assertEquals(stats.get(dsA).getLatestIncidentUrn(), UrnUtils.getUrn(latestA));
    assertEquals(stats.get(dsB).getActiveCount(), 1);
    assertNull(stats.get(dsB).getLatestIncidentUrn(), "no top hit -> no latest incident urn");
  }

  @Test
  public void testGetActiveIncidentStatsEmptyInputSkipsSearch() throws Exception {
    assertTrue(esSearchDAO.getActiveIncidentStats(opContext, Set.of()).isEmpty());
    // Empty input must short-circuit before issuing any ES request.
    Mockito.verifyNoInteractions(mockClient);
  }

  @Test
  public void testGetActiveIncidentStatsWrapsSearchFailure() throws Exception {
    Mockito.when(
            mockClient.search(
                Mockito.any(),
                Mockito.any(SearchRequest.class),
                Mockito.eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("es down"));
    final Urn ds = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:x,a,PROD)");
    assertThrows(
        ESQueryException.class, () -> esSearchDAO.getActiveIncidentStats(opContext, Set.of(ds)));
  }

  private static Terms.Bucket bucket(String key, long docCount, @Nullable String latestUrn) {
    final Terms.Bucket b = Mockito.mock(Terms.Bucket.class);
    Mockito.when(b.getKeyAsString()).thenReturn(key);
    Mockito.when(b.getDocCount()).thenReturn(docCount);
    final Aggregations sub = Mockito.mock(Aggregations.class);
    Mockito.when(b.getAggregations()).thenReturn(sub);
    final TopHits topHits = Mockito.mock(TopHits.class);
    Mockito.when(sub.<TopHits>get("latestIncident")).thenReturn(topHits);
    final SearchHits hits = Mockito.mock(SearchHits.class);
    Mockito.when(topHits.getHits()).thenReturn(hits);
    if (latestUrn == null) {
      Mockito.when(hits.getHits()).thenReturn(new SearchHit[0]);
    } else {
      final SearchHit hit = Mockito.mock(SearchHit.class);
      Mockito.when(hit.getSourceAsMap()).thenReturn(Map.<String, Object>of("urn", latestUrn));
      Mockito.when(hits.getHits()).thenReturn(new SearchHit[] {hit});
    }
    return b;
  }
}
