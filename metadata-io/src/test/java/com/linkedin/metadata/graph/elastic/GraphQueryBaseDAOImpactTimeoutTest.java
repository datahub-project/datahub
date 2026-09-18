package com.linkedin.metadata.graph.elastic;

import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageTimeoutException;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.mockito.ArgumentCaptor;
import org.opensearch.index.query.QueryBuilder;
import org.testng.annotations.Test;

/**
 * Drives getImpactLineage past its wall-clock budget BETWEEN hops, i.e. the BFS-level timeout site
 * (remainingTime < 0 at the top of the loop). The slice-level sites are covered by the PIT
 * DAO tests; nothing else in the suite reaches this branch.
 */
public class GraphQueryBaseDAOImpactTimeoutTest {

  private static final String DATASET = "dataset";
  private static final String ERRORS_METRIC = "datahub.lineage.graph_walk.errors";

  /**
   * Every hop sleeps past the budget and returns one edge to a fresh URN so the BFS has a next
   * level.
   */
  private static final class SlowHopHarness extends GraphQueryBaseDAO {
    private final long hopSleepMs;
    private final AtomicInteger hop = new AtomicInteger();

    SlowHopHarness(ElasticSearchConfiguration config, MetricUtils metricUtils, long hopSleepMs) {
      super(TEST_GRAPH_SERVICE_CONFIG, config, metricUtils);
      this.hopSleepMs = hopSleepMs;
    }

    @Override
    protected SearchClientShim<?> getClient() {
      return mock(SearchClientShim.class);
    }

    @Override
    protected LineageSliceFetchResult searchWithSlices(
        @Nonnull OperationContext opContext,
        @Nonnull QueryBuilder query,
        LineageGraphFilters lineageGraphFilters,
        Set<Urn> visitedEntities,
        Set<Urn> viaEntities,
        int numHops,
        int remainingHops,
        ThreadSafePathStore existingPaths,
        int maxRelations,
        int defaultPageSize,
        int slices,
        long remainingTime,
        Set<Urn> entityUrns,
        boolean allowPartialResults) {
      try {
        Thread.sleep(hopSleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      LineageRelationship rel = new LineageRelationship();
      try {
        rel.setEntity(
            Urn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:test,hop" + hop.incrementAndGet() + ",PROD)"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      rel.setType("DownstreamOf");
      rel.setDegree(numHops);
      return new LineageSliceFetchResult(List.of(rel), false);
    }
  }

  private static ElasticSearchConfiguration configWithBudget(
      int timeoutSeconds, boolean partialResults) {
    return TEST_OS_SEARCH_CONFIG.toBuilder()
        .search(
            TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                .graph(
                    TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                        .timeoutSeconds(timeoutSeconds)
                        .impact(
                            TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                .maxRelations(-1)
                                .partialResults(partialResults)
                                .build())
                        .build())
                .build())
        .build();
  }

  private static LineageGraphFilters downstreamOfDataset(OperationContext opContext) {
    return LineageGraphFilters.forEntityType(
        opContext.getLineageRegistry(), DATASET, LineageDirection.DOWNSTREAM);
  }

  @Test(timeOut = 15_000)
  public void testStrictMode_budgetExhaustedBetweenHops_throwsLineageTimeoutAndRecordsError()
      throws Exception {
    MetricUtils metricUtils = mock(MetricUtils.class);
    // 1s budget, strict: hop 0 overruns (1.3s), so the top-of-loop check before hop 1 must throw.
    SlowHopHarness dao = new SlowHopHarness(configWithBudget(1, false), metricUtils, 1_300);
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Urn source = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,source,PROD)");

    LineageTimeoutException thrown =
        expectThrows(
            LineageTimeoutException.class,
            () -> dao.getImpactLineage(opContext, source, downstreamOfDataset(opContext), 3));

    assertTrue(
        thrown.getMessage().contains("Lineage operation timed out after 1 seconds"),
        thrown.getMessage());
    assertTimeoutRecorded(metricUtils);
  }

  @Test(timeOut = 15_000)
  public void testPartialMode_budgetExhaustedBetweenHops_returnsPartialAndRecordsError()
      throws Exception {
    MetricUtils metricUtils = mock(MetricUtils.class);
    // 1s budget minus the 20% reservation = 800ms; hop 0 overruns, hop 1 must not start.
    SlowHopHarness dao = new SlowHopHarness(configWithBudget(1, true), metricUtils, 1_300);
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Urn source = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,source,PROD)");

    LineageResponse response =
        dao.getImpactLineage(opContext, source, downstreamOfDataset(opContext), 3);

    assertTrue(response.isPartial(), "budget exhausted between hops must be reported partial");
    assertEquals(response.getTotal(), 1, "only the hop that overran contributed");
    assertTimeoutRecorded(metricUtils);
  }

  /**
   * The cascade emits .errors{error_type=timeout} once on close; nothing else in the walk records
   * an error.
   */
  private static void assertTimeoutRecorded(MetricUtils metricUtils) {
    ArgumentCaptor<String[]> tags = ArgumentCaptor.forClass(String[].class);
    verify(metricUtils).incrementMicrometer(eq(ERRORS_METRIC), eq(1.0), tags.capture());
    List<String> tagList = Arrays.asList(tags.getValue());
    assertTrue(tagList.containsAll(List.of("error_type", "timeout")), "tags: " + tagList);
  }
}
