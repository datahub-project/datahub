package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregation;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.GenericTable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DashboardUsageBucketsBatchLoaderTest {

  private static final Urn URN_1 = UrnUtils.getUrn("urn:li:dashboard:(looker,d1)");
  private static final Urn URN_2 = UrnUtils.getUrn("urn:li:dashboard:(looker,d2)");
  private static final Urn URN_3 = UrnUtils.getUrn("urn:li:dashboard:(looker,d3)");
  private static final Urn URN_4 = UrnUtils.getUrn("urn:li:dashboard:(looker,d4)");

  private static DashboardUsageBucketsBatchLoader.Key key(Urn urn) {
    return new DashboardUsageBucketsBatchLoader.Key(urn.toString(), 0L, 100L);
  }

  private static DashboardUsageBucketsBatchLoader.Key key(Urn urn, long start, long end) {
    return new DashboardUsageBucketsBatchLoader.Key(urn.toString(), start, end);
  }

  private static GenericTable emptyTable() {
    return new GenericTable()
        .setRows(new StringArrayArray())
        .setColumnNames(new StringArray())
        .setColumnTypes(new StringArray());
  }

  @Test
  public void testSingleUrnUsesPerUrnPathNotBatch() {
    // One URN → no fan-out to collapse. The guard must route to the single-URN getAggregatedStats
    // path and skip batchGetAggregatedStats entirely (avoids the terms(batch_urn_outer) overhead).
    TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);
    Mockito.when(ts.getAggregatedStats(any(), any(), any(), any(), any(), any()))
        .thenReturn(emptyTable());

    QueryContext context = getMockAllowContext();
    DashboardUsageBucketsBatchLoader loader = new DashboardUsageBucketsBatchLoader(ts);

    List<List<DashboardUsageAggregation>> out =
        loader.batchLoad(Collections.singletonList(key(URN_1)), context);

    Assert.assertEquals(out.size(), 1);
    Mockito.verify(ts, Mockito.times(1))
        .getAggregatedStats(any(), any(), any(), any(), any(), any());
    Mockito.verify(ts, Mockito.never())
        .batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testMultipleUrnsSameWindowUseBatch() {
    // Two URNs sharing a time window → one batchGetAggregatedStats, no per-URN getAggregatedStats.
    TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);
    Map<Urn, GenericTable> batchResult = new HashMap<>();
    batchResult.put(URN_1, emptyTable());
    batchResult.put(URN_2, emptyTable());
    Mockito.when(ts.batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(batchResult);

    QueryContext context = getMockAllowContext();
    DashboardUsageBucketsBatchLoader loader = new DashboardUsageBucketsBatchLoader(ts);

    List<List<DashboardUsageAggregation>> out =
        loader.batchLoad(List.of(key(URN_1), key(URN_2)), context);

    Assert.assertEquals(out.size(), 2);
    Mockito.verify(ts, Mockito.times(1))
        .batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any());
    Mockito.verify(ts, Mockito.never())
        .getAggregatedStats(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testDistinctWindowsProduceSeparateBatchCalls() {
    // Dashboards queried with different time windows must land in separate batch groups: one
    // batchGetAggregatedStats per window over exactly that window's URNs (the loader documents
    // this invariant because batchGetAggregatedStats applies one shared filter per call).
    TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);
    Map<Urn, GenericTable> batchResult = new HashMap<>();
    batchResult.put(URN_1, emptyTable());
    batchResult.put(URN_2, emptyTable());
    batchResult.put(URN_3, emptyTable());
    batchResult.put(URN_4, emptyTable());
    Mockito.when(ts.batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(batchResult);

    QueryContext context = getMockAllowContext();
    DashboardUsageBucketsBatchLoader loader = new DashboardUsageBucketsBatchLoader(ts);

    // URN_1/URN_2 share window [0,100); URN_3/URN_4 share window [200,300).
    List<List<DashboardUsageAggregation>> out =
        loader.batchLoad(
            List.of(
                key(URN_1, 0L, 100L),
                key(URN_2, 0L, 100L),
                key(URN_3, 200L, 300L),
                key(URN_4, 200L, 300L)),
            context);

    Assert.assertEquals(out.size(), 4);
    // Two distinct windows → exactly two batch calls; never the per-URN fallback.
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Urn>> urnsCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(ts, Mockito.times(2))
        .batchGetAggregatedStats(any(), any(), any(), any(), urnsCaptor.capture(), any(), any());
    Mockito.verify(ts, Mockito.never())
        .getAggregatedStats(any(), any(), any(), any(), any(), any());

    // Each window's call covered exactly that window's URNs (windows iterate in first-seen order).
    List<List<Urn>> calls = urnsCaptor.getAllValues();
    Assert.assertEquals(calls.size(), 2);
    Assert.assertEquals(calls.get(0).size(), 2);
    Assert.assertTrue(calls.get(0).containsAll(List.of(URN_1, URN_2)));
    Assert.assertEquals(calls.get(1).size(), 2);
    Assert.assertTrue(calls.get(1).containsAll(List.of(URN_3, URN_4)));
  }
}
