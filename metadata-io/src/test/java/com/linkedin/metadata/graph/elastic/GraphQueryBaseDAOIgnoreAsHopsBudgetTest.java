package com.linkedin.metadata.graph.elastic;

import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.Test;

/**
 * Entities followed as ignored hops are explored within the same hop, so the whole chain has to fit
 * in the hop's time budget rather than getting a fresh budget per level.
 */
public class GraphQueryBaseDAOIgnoreAsHopsBudgetTest {

  private static final String DATASET = "dataset";
  private static final int CHAIN_LENGTH = 6;
  private static final long SEARCH_MILLIS = 600;

  @Test(timeOut = 30_000)
  public void testIgnoredHopChainSharesTheHopBudget() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Each search takes 600ms and returns one new dataset downstream of the previous one, up to
    // CHAIN_LENGTH levels. Every dataset is ignored as a hop, so the walk keeps following the chain
    // inside hop 1 for as long as it is allowed to.
    AtomicInteger searches = new AtomicInteger();
    when(client.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenAnswer(
            invocation -> {
              Thread.sleep(SEARCH_MILLIS);
              int level = searches.incrementAndGet();
              if (level > CHAIN_LENGTH) {
                return searchResponse();
              }
              return searchResponse(edge(level - 1, level));
            });

    GraphQueryConfiguration graphConfig =
        GraphQueryConfiguration.builder()
            .timeoutSeconds(1)
            .batchSize(25)
            .enableMultiPathSearch(false)
            .maxThreads(1)
            .sliceFutureDrainTimeoutSeconds(2)
            .build();
    ElasticSearchConfiguration config =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(TEST_OS_SEARCH_CONFIG.getSearch().toBuilder().graph(graphConfig).build())
            .build();
    ESGraphQueryDAO dao = new ESGraphQueryDAO(client, TEST_GRAPH_SERVICE_CONFIG, config, null);
    try {
      OperationContext opContext =
          TestOperationContexts.systemContextNoSearchAuthorization()
              .withLineageFlags(f -> new LineageFlags().setIgnoreAsHops(ignoreDatasets()));
      LineageResponse response =
          dao.getLineage(
              opContext,
              urn(0),
              LineageGraphFilters.forEntityType(
                  opContext.getLineageRegistry(), DATASET, LineageDirection.DOWNSTREAM),
              0,
              100,
              3);

      assertTrue(response.isPartial(), "the chain outran the hop budget");
      assertTrue(
          searches.get() < CHAIN_LENGTH,
          "ignored hops must stop when the hop budget is spent, but ran " + searches.get());
    } finally {
      dao.destroy();
    }
  }

  private static UrnArrayMap ignoreDatasets() {
    UrnArrayMap ignoreAsHops = new UrnArrayMap();
    ignoreAsHops.put(DATASET, new UrnArray());
    return ignoreAsHops;
  }

  private static Urn urn(int level) {
    return UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,level" + level + ",PROD)");
  }

  private static SearchHit edge(int from, int to) {
    Map<String, Object> source = new HashMap<>();
    source.put("urn", urn(to).toString());
    source.put("entityType", DATASET);
    Map<String, Object> destination = new HashMap<>();
    destination.put("urn", urn(from).toString());
    destination.put("entityType", DATASET);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("source", source);
    sourceMap.put("destination", destination);
    sourceMap.put("relationshipType", "DownstreamOf");
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);
    return hit;
  }

  private static SearchResponse searchResponse(SearchHit... hits) {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits searchHits = mock(SearchHits.class);
    when(searchHits.getHits()).thenReturn(hits);
    when(searchHits.getTotalHits())
        .thenReturn(new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO));
    when(response.getHits()).thenReturn(searchHits);
    return response;
  }
}
