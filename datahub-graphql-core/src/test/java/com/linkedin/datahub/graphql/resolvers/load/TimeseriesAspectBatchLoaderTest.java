package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link TimeseriesAspectBatchLoader#batchLoad(List, QueryContext)}.
 *
 * <p>Verifies the direct-path / batch-path routing decision, per-group splitting, positional
 * contract, and exception propagation.
 */
public class TimeseriesAspectBatchLoaderTest {

  private static final Urn URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.t1,PROD)");
  private static final Urn URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.t2,PROD)");
  private static final Urn URN_3 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.t3,PROD)");

  // ---------- Helpers ----------

  private static QueryContext mockQueryContext() {
    final QueryContext ctx = Mockito.mock(QueryContext.class);
    Mockito.when(ctx.getOperationContext()).thenReturn(Mockito.mock(OperationContext.class));
    return ctx;
  }

  /** Constructs a Key with the given URN and limit; all other params are null. */
  private static TimeseriesAspectBatchLoader.Key key(final Urn urn, final Integer limit) {
    return new TimeseriesAspectBatchLoader.Key(
        urn.toString(), "dataset", "datasetProfile", null, null, limit, null, null);
  }

  /** Returns a minimal non-empty list of {@link EnvelopedAspect} stubs for the given URN. */
  private static List<EnvelopedAspect> aspects(final Urn urn) {
    // A real EnvelopedAspect carries no mandatory constructor args; create a bare mock so we can
    // verify which per-URN list ended up in which result slot.
    final EnvelopedAspect aspect = Mockito.mock(EnvelopedAspect.class);
    Mockito.doReturn(urn.toString()).when(aspect).toString();
    return ImmutableList.of(aspect);
  }

  // ---------- Tests ----------

  /**
   * A single-key batch must use the direct {@code getAspectValues} path, regardless of the limit
   * value, and must never call {@code batchGetAspectValues}.
   */
  @Test
  public void testSingleUrnCallsDirectPath() {
    final TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    final QueryContext ctx = mockQueryContext();
    final List<EnvelopedAspect> expected = aspects(URN_1);

    Mockito.when(
            svc.getAspectValues(
                nullable(OperationContext.class),
                eq(URN_1),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(1),
                eq(null),
                eq(null)))
        .thenReturn(expected);

    final TimeseriesAspectBatchLoader loader = new TimeseriesAspectBatchLoader(svc);
    final List<List<EnvelopedAspect>> result =
        loader.batchLoad(ImmutableList.of(key(URN_1, 1)), ctx);

    assertEquals(result.size(), 1);
    assertEquals(result.get(0), expected);
    Mockito.verify(svc, Mockito.times(1))
        .getAspectValues(any(), eq(URN_1), any(), any(), any(), any(), any(), any(), any());
    Mockito.verify(svc, Mockito.never())
        .batchGetAspectValues(
            any(), any(), any(), any(), any(), any(), any(int.class), any(), any());
  }

  /**
   * When three keys share the same aspect parameters but have a null limit, each must be served by
   * an individual {@code getAspectValues} call — null limit cannot use the top_hits aggregation
   * path.
   */
  @Test
  public void testNullLimitCallsDirectPath() {
    final TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    final QueryContext ctx = mockQueryContext();

    // Pre-compute before any when() chain to avoid corrupting Mockito's state machine.
    final List<EnvelopedAspect> a1 = aspects(URN_1);
    final List<EnvelopedAspect> a2 = aspects(URN_2);
    final List<EnvelopedAspect> a3 = aspects(URN_3);

    // Null limit → direct path for every key in the group, even when there are multiple URNs.
    Mockito.when(
            svc.getAspectValues(
                nullable(OperationContext.class),
                eq(URN_1),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(null),
                eq(null),
                eq(null)))
        .thenReturn(a1);
    Mockito.when(
            svc.getAspectValues(
                nullable(OperationContext.class),
                eq(URN_2),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(null),
                eq(null),
                eq(null)))
        .thenReturn(a2);
    Mockito.when(
            svc.getAspectValues(
                nullable(OperationContext.class),
                eq(URN_3),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(null),
                eq(null),
                eq(null)))
        .thenReturn(a3);

    final TimeseriesAspectBatchLoader loader = new TimeseriesAspectBatchLoader(svc);
    final List<List<EnvelopedAspect>> result =
        loader.batchLoad(
            ImmutableList.of(key(URN_1, null), key(URN_2, null), key(URN_3, null)), ctx);

    assertEquals(result.size(), 3);
    Mockito.verify(svc, Mockito.times(3))
        .getAspectValues(any(), any(), any(), any(), any(), any(), any(), any(), any());
    Mockito.verify(svc, Mockito.never())
        .batchGetAspectValues(
            any(), any(), any(), any(), any(), any(), any(int.class), any(), any());
  }

  /**
   * Three keys with identical non-null parameters must be merged into a single {@code
   * batchGetAspectValues} call. Results must be mapped back to the correct input positions.
   */
  @Test
  public void testMultiUrnCallsBatchPath() {
    final TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    final QueryContext ctx = mockQueryContext();

    // Capture the list instances here so the equality assertions below compare the same objects
    // that the loader received from the service, not freshly-created mocks.
    final List<EnvelopedAspect> urn1Aspects = aspects(URN_1);
    final List<EnvelopedAspect> urn2Aspects = aspects(URN_2);
    final List<EnvelopedAspect> urn3Aspects = aspects(URN_3);
    final Map<Urn, List<EnvelopedAspect>> batchResult =
        ImmutableMap.of(URN_1, urn1Aspects, URN_2, urn2Aspects, URN_3, urn3Aspects);

    Mockito.when(
            svc.batchGetAspectValues(
                nullable(OperationContext.class),
                Mockito.anySet(),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(1),
                eq(null),
                eq(null)))
        .thenReturn(batchResult);

    final TimeseriesAspectBatchLoader loader = new TimeseriesAspectBatchLoader(svc);
    final List<List<EnvelopedAspect>> result =
        loader.batchLoad(ImmutableList.of(key(URN_1, 1), key(URN_2, 1), key(URN_3, 1)), ctx);

    assertEquals(result.size(), 3);
    assertEquals(result.get(0), urn1Aspects);
    assertEquals(result.get(1), urn2Aspects);
    assertEquals(result.get(2), urn3Aspects);
    Mockito.verify(svc, Mockito.times(1))
        .batchGetAspectValues(
            any(), Mockito.anySet(), any(), any(), any(), any(), any(int.class), any(), any());
    Mockito.verify(svc, Mockito.never())
        .getAspectValues(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  /**
   * Two keys with limit=1 and two keys with limit=5 must produce two separate {@code
   * batchGetAspectValues} calls — one per distinct group.
   */
  @Test
  public void testDifferentParamsCreateSeparateGroups() {
    final TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    final QueryContext ctx = mockQueryContext();

    final List<EnvelopedAspect> a1 = aspects(URN_1);
    final List<EnvelopedAspect> a2 = aspects(URN_2);
    final List<EnvelopedAspect> a3 = aspects(URN_3);

    Mockito.when(
            svc.batchGetAspectValues(
                nullable(OperationContext.class),
                Mockito.anySet(),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(1),
                eq(null),
                eq(null)))
        .thenReturn(ImmutableMap.of(URN_1, a1, URN_2, a2));

    Mockito.when(
            svc.batchGetAspectValues(
                nullable(OperationContext.class),
                Mockito.anySet(),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(5),
                eq(null),
                eq(null)))
        .thenReturn(ImmutableMap.of(URN_1, a1, URN_3, a3));

    // Use a fresh Key builder for URN_1 with limit=5 to create a second distinct key type.
    final TimeseriesAspectBatchLoader.Key urn1Limit5 =
        new TimeseriesAspectBatchLoader.Key(
            URN_1.toString(), "dataset", "datasetProfile", null, null, 5, null, null);
    final TimeseriesAspectBatchLoader.Key urn3Limit5 =
        new TimeseriesAspectBatchLoader.Key(
            URN_3.toString(), "dataset", "datasetProfile", null, null, 5, null, null);

    final TimeseriesAspectBatchLoader loader = new TimeseriesAspectBatchLoader(svc);
    final List<List<EnvelopedAspect>> result =
        loader.batchLoad(
            ImmutableList.of(key(URN_1, 1), key(URN_2, 1), urn1Limit5, urn3Limit5), ctx);

    assertEquals(result.size(), 4);
    Mockito.verify(svc, Mockito.times(2))
        .batchGetAspectValues(
            any(), Mockito.anySet(), any(), any(), any(), any(), any(int.class), any(), any());
  }

  /**
   * A {@link RuntimeException} thrown by {@code batchGetAspectValues} must propagate out of {@code
   * batchLoad} without being swallowed.
   */
  @Test
  public void testExceptionPropagatesFromBatchPath() {
    final TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    final QueryContext ctx = mockQueryContext();

    Mockito.when(
            svc.batchGetAspectValues(
                nullable(OperationContext.class),
                Mockito.anySet(),
                any(),
                any(),
                any(),
                any(),
                any(int.class),
                any(),
                any()))
        .thenThrow(new RuntimeException("batch exploded"));

    final TimeseriesAspectBatchLoader loader = new TimeseriesAspectBatchLoader(svc);
    assertThrows(
        RuntimeException.class,
        () -> loader.batchLoad(ImmutableList.of(key(URN_1, 1), key(URN_2, 1)), ctx));
  }

  /**
   * A {@link RuntimeException} thrown by {@code getAspectValues} must propagate out of {@code
   * batchLoad} without being swallowed.
   */
  @Test
  public void testExceptionPropagatesFromDirectPath() {
    final TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    final QueryContext ctx = mockQueryContext();

    Mockito.when(
            svc.getAspectValues(
                nullable(OperationContext.class),
                any(),
                any(),
                any(),
                any(),
                any(),
                any(),
                any(),
                any()))
        .thenThrow(new RuntimeException("direct exploded"));

    final TimeseriesAspectBatchLoader loader = new TimeseriesAspectBatchLoader(svc);
    // Single key → direct path.
    assertThrows(
        RuntimeException.class, () -> loader.batchLoad(ImmutableList.of(key(URN_1, 1)), ctx));
  }

  /**
   * When {@code batchGetAspectValues} returns a map that is missing one of the requested URNs, the
   * corresponding result slot must be an empty list (not null).
   */
  @Test
  public void testMissingUrnGetsFallbackEmptyList() {
    final TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    final QueryContext ctx = mockQueryContext();

    // Capture list instances so equality assertions compare the same objects returned by the mock.
    final List<EnvelopedAspect> urn1Aspects = aspects(URN_1);
    final List<EnvelopedAspect> urn2Aspects = aspects(URN_2);

    // Batch response covers only URN_1 and URN_2; URN_3 is absent.
    Mockito.when(
            svc.batchGetAspectValues(
                nullable(OperationContext.class),
                Mockito.anySet(),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(1),
                eq(null),
                eq(null)))
        .thenReturn(ImmutableMap.of(URN_1, urn1Aspects, URN_2, urn2Aspects));

    final TimeseriesAspectBatchLoader loader = new TimeseriesAspectBatchLoader(svc);
    final List<List<EnvelopedAspect>> result =
        loader.batchLoad(ImmutableList.of(key(URN_1, 1), key(URN_2, 1), key(URN_3, 1)), ctx);

    assertEquals(result.size(), 3);
    assertEquals(result.get(0), urn1Aspects);
    assertEquals(result.get(1), urn2Aspects);
    assertEquals(result.get(2), Collections.emptyList());
  }

  /**
   * The output list must map input positions to results regardless of the order in which the
   * returned map iterates over its entries.
   */
  @Test
  public void testPositionalContractPreserved() {
    final TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    final QueryContext ctx = mockQueryContext();

    // Pre-compute to avoid Mockito state corruption when used inside ImmutableMap.of().
    final List<EnvelopedAspect> a1 = aspects(URN_1);
    final List<EnvelopedAspect> a2 = aspects(URN_2);
    final List<EnvelopedAspect> a3 = aspects(URN_3);

    // Input order: URN_1, URN_2, URN_3. The returned map uses ImmutableMap (insertion order), but
    // the implementation must derive positions from the input key list, not from map iteration.
    final Map<Urn, List<EnvelopedAspect>> batchResult =
        ImmutableMap.of(URN_3, a3, URN_1, a1, URN_2, a2);

    Mockito.when(
            svc.batchGetAspectValues(
                nullable(OperationContext.class),
                Mockito.anySet(),
                eq("dataset"),
                eq("datasetProfile"),
                eq(null),
                eq(null),
                eq(1),
                eq(null),
                eq(null)))
        .thenReturn(batchResult);

    final TimeseriesAspectBatchLoader loader = new TimeseriesAspectBatchLoader(svc);
    // Input order: URN_1 at index 0, URN_2 at index 1, URN_3 at index 2.
    final List<List<EnvelopedAspect>> result =
        loader.batchLoad(ImmutableList.of(key(URN_1, 1), key(URN_2, 1), key(URN_3, 1)), ctx);

    assertEquals(result.size(), 3);

    // Verify each slot holds the aspects list that belongs to the corresponding input URN.
    // aspects(urnX) returns a single-element list; the element's toString() contains the URN
    // string, which we use as a proxy identity check.
    assertTrue(
        result.get(0).get(0).toString().contains(URN_1.toString()),
        "slot 0 should hold URN_1 aspects");
    assertTrue(
        result.get(1).get(0).toString().contains(URN_2.toString()),
        "slot 1 should hold URN_2 aspects");
    assertTrue(
        result.get(2).get(0).toString().contains(URN_3.toString()),
        "slot 2 should hold URN_3 aspects");
  }
}
