package com.linkedin.metadata.graph.elastic;

import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.Test;

/**
 * A hop waits for its batches until the hop deadline, not for a fresh budget once the batches have
 * been submitted. The lineage pool has no queue, so a batch submitted while every thread is busy
 * runs on the submitting thread and uses part of the budget before the wait begins.
 */
public class GraphQueryBaseDAOBatchWaitTest {

  private static final String DATASET = "dataset";
  private static final long HOP_BUDGET_MILLIS = 1_000;
  private static final long INLINE_BATCH_MILLIS = 600;
  private static final long POOLED_BATCH_MILLIS = 2_500;
  private static final long OVERSHOOT_ALLOWANCE_MILLIS = 300;

  @Test(timeOut = 30_000)
  public void testHopWaitStopsAtTheHopDeadlineWhenABatchRunsInline() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // One pool thread and one URN per batch: the first batch takes the pool thread and the second
    // runs inline. The pooled batch stands in for a batch stuck past the deadline.
    when(client.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenAnswer(
            invocation -> {
              boolean pooled = Thread.currentThread().getName().startsWith("lineage-worker-");
              Thread.sleep(pooled ? POOLED_BATCH_MILLIS : INLINE_BATCH_MILLIS);
              return emptySearchResponse();
            });

    GraphQueryConfiguration graphConfig =
        GraphQueryConfiguration.builder()
            .timeoutSeconds(1)
            .batchSize(1)
            .enableMultiPathSearch(false)
            .maxThreads(1)
            .sliceFutureDrainTimeoutSeconds(2)
            .build();
    ElasticSearchConfiguration config =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(TEST_OS_SEARCH_CONFIG.getSearch().toBuilder().graph(graphConfig).build())
            .build();
    GraphQueryPITDAO dao = new GraphQueryPITDAO(client, TEST_GRAPH_SERVICE_CONFIG, config, null);
    try {
      OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
      long start = System.nanoTime();
      dao.getLineageRelationshipsInBatches(
          opContext,
          List.of(urn(0), urn(1)),
          LineageGraphFilters.forEntityType(
              opContext.getLineageRegistry(), DATASET, LineageDirection.DOWNSTREAM),
          ConcurrentHashMap.newKeySet(),
          ConcurrentHashMap.newKeySet(),
          1,
          1,
          HOP_BUDGET_MILLIS,
          new ThreadSafePathStore(),
          false);
      long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

      assertTrue(
          elapsedMillis < HOP_BUDGET_MILLIS + OVERSHOOT_ALLOWANCE_MILLIS,
          "the hop must return at its deadline, but took " + elapsedMillis + "ms");
    } finally {
      dao.shutdown();
    }
  }

  private static Urn urn(int index) {
    return UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,batch" + index + ",PROD)");
  }

  private static SearchResponse emptySearchResponse() {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits searchHits = mock(SearchHits.class);
    when(searchHits.getHits()).thenReturn(new SearchHit[0]);
    when(searchHits.getTotalHits()).thenReturn(new TotalHits(0, TotalHits.Relation.EQUAL_TO));
    when(response.getHits()).thenReturn(searchHits);
    return response;
  }
}
