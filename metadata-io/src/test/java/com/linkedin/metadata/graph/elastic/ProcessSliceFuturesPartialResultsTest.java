package com.linkedin.metadata.graph.elastic;

import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.opensearch.index.query.QueryBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Direct coverage of {@link GraphQueryBaseDAO#processSliceFutures}: per-slice {@link
 * java.util.concurrent.TimeoutException} and {@link java.util.concurrent.ExecutionException} with
 * {@code allowPartialResults} true vs false (single-future cancel vs fail-fast mass cancel).
 */
public class ProcessSliceFuturesPartialResultsTest {

  private static LineageRelationship rel(String urn) throws Exception {
    LineageRelationship r = new LineageRelationship();
    r.setEntity(Urn.createFromString(urn));
    r.setType("DownstreamOf");
    r.setDegree(1);
    return r;
  }

  private static final class Harness extends GraphQueryBaseDAO {
    Harness(ElasticSearchConfiguration config) {
      super(TEST_GRAPH_SERVICE_CONFIG, config, null);
    }

    LineageSliceFetchResult invokeProcessSliceFutures(
        List<CompletableFuture<List<LineageRelationship>>> sliceFutures,
        long remainingTimeMs,
        boolean allowPartialResults) {
      return processSliceFutures(sliceFutures, remainingTimeMs, allowPartialResults);
    }

    @Override
    protected SearchClientShim<?> getClient() {
      throw new UnsupportedOperationException();
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
      throw new UnsupportedOperationException();
    }
  }

  @Test(timeOut = 15_000)
  public void testAllowPartial_secondSliceTimesOut_mergesFirstSliceAndSetsPartial()
      throws Exception {
    Harness harness = new Harness(TEST_OS_SEARCH_CONFIG);
    List<CompletableFuture<List<LineageRelationship>>> futures = new ArrayList<>();
    LineageRelationship a = rel("urn:li:dataset:(urn:li:dataPlatform:test,a,PROD)");
    futures.add(CompletableFuture.completedFuture(List.of(a)));
    futures.add(new CompletableFuture<>());

    LineageSliceFetchResult result = harness.invokeProcessSliceFutures(futures, 500, true);

    assertTrue(result.isPartial(), "timeout on slice 1 should set partial");
    assertEquals(result.getLineageRelationships().size(), 1);
    assertEquals(result.getLineageRelationships().get(0).getEntity(), a.getEntity());
    assertTrue(futures.get(0).isDone() && !futures.get(0).isCompletedExceptionally());
    assertTrue(
        futures.get(1).isCancelled(),
        "incomplete slice 1 must be cancelled after TimeoutException");
  }

  @Test
  public void testAllowPartial_secondSliceExecutionException_mergesFirstSliceAndSetsPartial()
      throws Exception {
    Harness harness = new Harness(TEST_OS_SEARCH_CONFIG);
    List<CompletableFuture<List<LineageRelationship>>> futures = new ArrayList<>();
    LineageRelationship b = rel("urn:li:dataset:(urn:li:dataPlatform:test,b,PROD)");
    futures.add(CompletableFuture.completedFuture(List.of(b)));
    futures.add(new CompletableFuture<>());
    futures.get(1).completeExceptionally(new RuntimeException("slice 1 search failed"));

    LineageSliceFetchResult result = harness.invokeProcessSliceFutures(futures, 60_000, true);

    assertTrue(result.isPartial());
    assertEquals(result.getLineageRelationships().size(), 1);
    assertEquals(result.getLineageRelationships().get(0).getEntity(), b.getEntity());
    assertTrue(futures.get(1).isCompletedExceptionally());
  }

  @Test(timeOut = 15_000)
  public void testDisallowPartial_secondSliceTimesOut_throwsAndCancelsAllSlices() {
    Harness harness = new Harness(TEST_OS_SEARCH_CONFIG);
    List<CompletableFuture<List<LineageRelationship>>> futures = new ArrayList<>();
    futures.add(CompletableFuture.completedFuture(List.of()));
    futures.add(new CompletableFuture<>());

    RuntimeException thrown = null;
    try {
      harness.invokeProcessSliceFutures(futures, 500, false);
      Assert.fail("expected RuntimeException when partialResults=false and slice times out");
    } catch (RuntimeException e) {
      thrown = e;
    }
    assertNotNull(thrown);
    assertNotNull(thrown.getCause());
    assertTrue(thrown.getCause() instanceof TimeoutException);
    assertTrue(
        futures.get(1).isCancelled(),
        "fail-fast cancels siblings; incomplete slice 1 must be cancelled");
  }

  @Test
  public void testDisallowPartial_secondSliceExecutionException_throwsAndCancelsAllSlices() {
    Harness harness = new Harness(TEST_OS_SEARCH_CONFIG);
    List<CompletableFuture<List<LineageRelationship>>> futures = new ArrayList<>();
    futures.add(CompletableFuture.completedFuture(List.of()));
    futures.add(new CompletableFuture<>());
    futures.get(1).completeExceptionally(new IllegalStateException("boom"));

    RuntimeException thrown = null;
    try {
      harness.invokeProcessSliceFutures(futures, 60_000, false);
      Assert.fail("expected RuntimeException when partialResults=false and slice fails");
    } catch (RuntimeException e) {
      thrown = e;
    }
    assertNotNull(thrown);
    assertTrue(
        thrown instanceof IllegalStateException
            || thrown.getCause() instanceof IllegalStateException);
    assertTrue(
        futures.get(1).isCompletedExceptionally(),
        "slice 1 failed before fail-fast cancel-all ran");
  }
}
