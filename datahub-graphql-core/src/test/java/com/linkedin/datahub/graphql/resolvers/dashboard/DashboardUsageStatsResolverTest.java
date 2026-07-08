package com.linkedin.datahub.graphql.resolvers.dashboard;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.dashboard.DashboardUsageStatistics;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregation;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregationMetrics;
import com.linkedin.datahub.graphql.generated.DashboardUsageQueryResult;
import com.linkedin.datahub.graphql.resolvers.load.DashboardUsageBucketsBatchLoader;
import com.linkedin.datahub.graphql.resolvers.load.TimeseriesAspectBatchLoader;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.timeseries.GenericTable;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DashboardUsageStatsResolverTest {

  private static final String DASHBOARD_URN = "urn:li:dashboard:(looker,test-dashboard)";

  @Test
  public void testBatchEnabledRoutesMetricsThroughLoader() throws Exception {
    // Flag ON with the timeseries loader registered: the absolute-usage-metrics doc fetch must go
    // through the DataLoader, not the resolver's direct per-URN getAspectValues call.
    TimeseriesAspectService tsService = Mockito.mock(TimeseriesAspectService.class);
    mockEmptyAggregations(tsService);

    QueryContext context = getMockAllowContext();
    Harness h = harnessWithTimeseriesLoader(context, Collections.singletonList(tsDoc(42)));

    DashboardUsageQueryResult result =
        drive(new DashboardUsageStatsResolver(tsService, true, false), h);

    assertNotNull(result);
    assertEquals(result.getMetrics().size(), 1);
    assertEquals((int) result.getMetrics().get(0).getViewsCount(), 42);
    // Doc came from the loader, not the direct per-URN getAspectValues call.
    Mockito.verify(tsService, Mockito.never())
        .getAspectValues(any(), any(), any(), any(), any(), any(), any(), any());
    // The non-batchable bucket/aggregation path still queries getAggregatedStats.
    Mockito.verify(tsService, Mockito.atLeastOnce())
        .getAggregatedStats(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testBatchDisabledUsesPerUrnPath() throws Exception {
    // Flag OFF: metrics must be fetched via the direct per-URN getAspectValues call.
    TimeseriesAspectService tsService = Mockito.mock(TimeseriesAspectService.class);
    Mockito.when(tsService.getAspectValues(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Collections.singletonList(tsDoc(13)));
    mockEmptyAggregations(tsService);

    QueryContext context = getMockAllowContext();
    Harness h = harnessWithTimeseriesLoader(context, Collections.singletonList(tsDoc(999)));

    DashboardUsageQueryResult result =
        drive(new DashboardUsageStatsResolver(tsService, false, false), h);

    assertNotNull(result);
    assertEquals((int) result.getMetrics().get(0).getViewsCount(), 13);
    // Per-URN path: getAspectValues used directly, loader ignored.
    Mockito.verify(tsService, Mockito.atLeastOnce())
        .getAspectValues(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testBatchEnabledButLoaderMissingFallsBackToPerUrn() throws Exception {
    // Flag ON but no TimeseriesAspect loader registered: maybeLoadUsageMetrics returns null and the
    // resolver falls back to the direct per-URN path.
    TimeseriesAspectService tsService = Mockito.mock(TimeseriesAspectService.class);
    Mockito.when(tsService.getAspectValues(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Collections.singletonList(tsDoc(7)));
    mockEmptyAggregations(tsService);

    QueryContext context = getMockAllowContext();
    Harness h = harnessWithoutLoaders(context);

    DashboardUsageQueryResult result =
        drive(new DashboardUsageStatsResolver(tsService, true, false), h);

    assertNotNull(result);
    assertEquals((int) result.getMetrics().get(0).getViewsCount(), 7);
    Mockito.verify(tsService, Mockito.atLeastOnce())
        .getAspectValues(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testAggBatchEnabledRoutesBucketsThroughLoader() throws Exception {
    // Agg-batch flag ON with the buckets loader registered: the daily-usage buckets must come from
    // the DataLoader, not the per-URN getAggregatedStats path. The per-user aggregation is not
    // batched, so getAggregatedStats still fires exactly once (for it) rather than twice.
    TimeseriesAspectService tsService = Mockito.mock(TimeseriesAspectService.class);
    mockEmptyAggregations(tsService);

    QueryContext context = getMockAllowContext();
    Harness h = harnessWithBucketsLoader(context, Collections.singletonList(usageBucket(0L, 55)));

    // batchLoadEnabled=false (metrics per-URN), aggBatchLoadEnabled=true (buckets batched).
    DashboardUsageQueryResult result =
        drive(new DashboardUsageStatsResolver(tsService, false, true), h);

    assertNotNull(result);
    assertEquals(result.getBuckets().size(), 1);
    assertEquals((int) result.getBuckets().get(0).getMetrics().getViewsCount(), 55);
    // Buckets came from the loader → getAggregatedStats fired only for the per-user aggregation.
    Mockito.verify(tsService, Mockito.times(1))
        .getAggregatedStats(any(), any(), any(), any(), any(), any());
  }

  // ---- helpers ----

  private static final class Harness {
    final DataFetchingEnvironment env;
    final DataLoaderRegistry registry;

    Harness(DataFetchingEnvironment env, DataLoaderRegistry registry) {
      this.env = env;
      this.registry = registry;
    }
  }

  private static Harness harnessWithTimeseriesLoader(
      QueryContext context, List<EnvelopedAspect> docs) {
    // Stub timeseries loader returning the supplied docs per key, proving the resolver routes the
    // doc fetch through the loader rather than the direct per-URN service call.
    DataLoader<TimeseriesAspectBatchLoader.Key, List<EnvelopedAspect>> tsLoader =
        DataLoader.newDataLoader(
            keys -> CompletableFuture.completedFuture(Collections.nCopies(keys.size(), docs)));

    DataLoaderRegistry registry = new DataLoaderRegistry();
    registry.register(TimeseriesAspectBatchLoader.LOADER_NAME, tsLoader);
    return new Harness(mockEnv(context, registry), registry);
  }

  private static Harness harnessWithBucketsLoader(
      QueryContext context, List<DashboardUsageAggregation> buckets) {
    // Stub buckets loader returning the supplied aggregations per key, proving the resolver sources
    // the daily buckets from the loader rather than the per-URN getAggregatedStats call.
    DataLoader<DashboardUsageBucketsBatchLoader.Key, List<DashboardUsageAggregation>>
        bucketsLoader =
            DataLoader.newDataLoader(
                keys ->
                    CompletableFuture.completedFuture(Collections.nCopies(keys.size(), buckets)));

    DataLoaderRegistry registry = new DataLoaderRegistry();
    registry.register(DashboardUsageBucketsBatchLoader.LOADER_NAME, bucketsLoader);
    return new Harness(mockEnv(context, registry), registry);
  }

  private static DashboardUsageAggregation usageBucket(long bucket, int viewsCount) {
    DashboardUsageAggregationMetrics metrics = new DashboardUsageAggregationMetrics();
    metrics.setViewsCount(viewsCount);
    DashboardUsageAggregation aggregation = new DashboardUsageAggregation();
    aggregation.setBucket(bucket);
    aggregation.setMetrics(metrics);
    return aggregation;
  }

  private static Harness harnessWithoutLoaders(QueryContext context) {
    DataLoaderRegistry registry = new DataLoaderRegistry();
    return new Harness(mockEnv(context, registry), registry);
  }

  private static DataFetchingEnvironment mockEnv(
      QueryContext context, DataLoaderRegistry registry) {
    Dashboard source = new Dashboard();
    source.setUrn(DASHBOARD_URN);

    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getSource()).thenReturn(source);
    Mockito.when(env.getContext()).thenReturn(context);
    Mockito.when(env.getDataLoaderRegistry()).thenReturn(registry);
    return env;
  }

  private static void mockEmptyAggregations(TimeseriesAspectService tsService) {
    // getBuckets and the per-user aggregation both call getAggregatedStats; empty rows are enough.
    Mockito.when(tsService.getAggregatedStats(any(), any(), any(), any(), any(), any()))
        .thenReturn(new GenericTable().setRows(new StringArrayArray()));
  }

  private static EnvelopedAspect tsDoc(int viewsCount) {
    DashboardUsageStatistics stats =
        new DashboardUsageStatistics().setTimestampMillis(0L).setViewsCount(viewsCount);
    return new EnvelopedAspect().setAspect(GenericRecordUtils.serializeAspect(stats));
  }

  /**
   * Drives the resolver and resolves its future. The timeseries loader key is enqueued eagerly in
   * {@code get()}, so dispatching after invocation fires it; we loop until the combined future
   * (bucket stats run concurrently on the executor) completes.
   */
  private static DashboardUsageQueryResult drive(DashboardUsageStatsResolver resolver, Harness h)
      throws Exception {
    CompletableFuture<DashboardUsageQueryResult> future = resolver.get(h.env);
    final long deadline = System.currentTimeMillis() + 5000;
    while (!future.isDone() && System.currentTimeMillis() < deadline) {
      h.registry.dispatchAll();
      try {
        return future.get(50, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ignored) {
        // Bucket-stats future may still be running on the executor; loop and dispatch again.
      }
    }
    return future.get(5, TimeUnit.SECONDS);
  }
}
