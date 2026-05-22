package com.linkedin.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.WeaklyTypedAspectsResolver.AspectsKey;
import com.linkedin.datahub.graphql.generated.AspectParams;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RawAspect;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link WeaklyTypedAspectsResolver#batchLoad(java.util.List, QueryContext,
 * EntityClient, EntityRegistry)}, the DataLoader batch function. Verifies grouping by {@code
 * (entityType, aspectNames, autoRenderOnly)}, positional contract, key normalization, and error
 * semantics.
 */
public class WeaklyTypedAspectsResolverBatchTest {

  private static final String DATASET_TYPE = "dataset";
  private static final String CHART_TYPE = "chart";

  private static final String ASPECT_A = "aspectA";
  private static final String ASPECT_B = "aspectB";
  private static final String ASPECT_C = "aspectC";

  private static final Urn DATASET_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.t1,PROD)");
  private static final Urn DATASET_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.t2,PROD)");
  private static final Urn DATASET_URN_3 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.t3,PROD)");
  private static final Urn CHART_URN_1 = UrnUtils.getUrn("urn:li:chart:(looker,chart1)");

  // ---------- Helpers ----------

  // Use doReturn(...).when(...) instead of when(...).thenReturn(...) so these helpers can be
  // safely called inline as arguments to other .thenReturn(...) calls without tripping Mockito's
  // "Unfinished stubbing detected" state machine.
  private static AspectSpec mockAspectSpec(
      final String name, final boolean autoRender, final DataMap renderSpec) {
    final AspectSpec spec = Mockito.mock(AspectSpec.class);
    Mockito.doReturn(name).when(spec).getName();
    Mockito.doReturn(autoRender).when(spec).isAutoRender();
    Mockito.doReturn(renderSpec).when(spec).getRenderSpec();
    return spec;
  }

  private static EntitySpec mockEntitySpec(final List<AspectSpec> aspectSpecs) {
    final EntitySpec entitySpec = Mockito.mock(EntitySpec.class);
    Mockito.doReturn(aspectSpecs).when(entitySpec).getAspectSpecs();
    return entitySpec;
  }

  private static void stubEntitySpec(
      final EntityRegistry registry, final String entityType, final List<AspectSpec> aspectSpecs) {
    final EntitySpec entitySpec = mockEntitySpec(aspectSpecs);
    Mockito.doReturn(entitySpec).when(registry).getEntitySpec(entityType);
  }

  private static EntityResponse entityResponseWithAspects(
      final Urn urn, final String... aspectNames) {
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    for (String name : aspectNames) {
      // Stamp the URN into each aspect payload so tests can verify which response landed
      // in which result slot (the positional DataLoader contract).
      final DataMap payload = new DataMap();
      payload.put("_testUrn", urn.toString());
      aspectMap.put(name, new EnvelopedAspect().setValue(new Aspect(payload)));
    }
    return new EntityResponse().setUrn(urn).setAspects(aspectMap);
  }

  private static QueryContext mockQueryContext() {
    final QueryContext ctx = Mockito.mock(QueryContext.class);
    Mockito.when(ctx.getOperationContext()).thenReturn(Mockito.mock(OperationContext.class));
    return ctx;
  }

  private static AspectsKey datasetKey(final Urn urn, final Set<String> aspectNames) {
    final AspectParams params = new AspectParams();
    params.setAspectNames(ImmutableList.copyOf(aspectNames));
    return AspectsKey.from(urn.toString(), DATASET_TYPE, params);
  }

  // ---------- Tests ----------

  @Test
  public void testSameGroupSingleBatchGetCall() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(ImmutableSet.of(DATASET_URN_1, DATASET_URN_2, DATASET_URN_3)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A),
                DATASET_URN_2, entityResponseWithAspects(DATASET_URN_2, ASPECT_A),
                DATASET_URN_3, entityResponseWithAspects(DATASET_URN_3, ASPECT_A)));

    final List<AspectsKey> keys =
        ImmutableList.of(
            datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)),
            datasetKey(DATASET_URN_2, ImmutableSet.of(ASPECT_A)),
            datasetKey(DATASET_URN_3, ImmutableSet.of(ASPECT_A)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.size(), 3);
    for (List<RawAspect> per : result) {
      assertEquals(per.size(), 1);
      assertEquals(per.get(0).getAspectName(), ASPECT_A);
    }
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            nullable(OperationContext.class), eq(DATASET_TYPE), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testMixedEntityTypesOneCallPerType() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec datasetSpecA = mockAspectSpec(ASPECT_A, false, null);
    final AspectSpec chartSpecA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(datasetSpecA));
    stubEntitySpec(registry, CHART_TYPE, ImmutableList.of(chartSpecA));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(ImmutableSet.of(DATASET_URN_1, DATASET_URN_2)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A),
                DATASET_URN_2, entityResponseWithAspects(DATASET_URN_2, ASPECT_A)));
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(CHART_TYPE),
                eq(ImmutableSet.of(CHART_URN_1)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(ImmutableMap.of(CHART_URN_1, entityResponseWithAspects(CHART_URN_1, ASPECT_A)));

    final AspectParams paramsA = new AspectParams();
    paramsA.setAspectNames(ImmutableList.of(ASPECT_A));

    final List<AspectsKey> keys =
        ImmutableList.of(
            datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)),
            AspectsKey.from(CHART_URN_1.toString(), CHART_TYPE, paramsA),
            datasetKey(DATASET_URN_2, ImmutableSet.of(ASPECT_A)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.size(), 3);
    // Input order interleaves entity types ([dataset, chart, dataset]); grouping by type
    // could easily re-order the output if the positional contract is broken.
    assertEquals(result.get(0).get(0).getAspectName(), ASPECT_A);
    assertTrue(result.get(0).get(0).getPayload().contains(DATASET_URN_1.toString()));
    assertEquals(result.get(1).get(0).getAspectName(), ASPECT_A);
    assertTrue(result.get(1).get(0).getPayload().contains(CHART_URN_1.toString()));
    assertEquals(result.get(2).get(0).getAspectName(), ASPECT_A);
    assertTrue(result.get(2).get(0).getPayload().contains(DATASET_URN_2.toString()));
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            nullable(OperationContext.class), eq(DATASET_TYPE), Mockito.anySet(), Mockito.anySet());
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            nullable(OperationContext.class), eq(CHART_TYPE), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testDifferentAspectNamesSetsSeparateCalls() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    final AspectSpec specB = mockAspectSpec(ASPECT_B, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA, specB));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_2)),
                eq(Collections.singleton(ASPECT_B))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_2, entityResponseWithAspects(DATASET_URN_2, ASPECT_B)));

    final List<AspectsKey> keys =
        ImmutableList.of(
            datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)),
            datasetKey(DATASET_URN_2, ImmutableSet.of(ASPECT_B)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.get(0).get(0).getAspectName(), ASPECT_A);
    assertEquals(result.get(1).get(0).getAspectName(), ASPECT_B);
    Mockito.verify(client, Mockito.times(2))
        .batchGetV2(
            nullable(OperationContext.class), eq(DATASET_TYPE), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testAspectNamesOrderIndependentKeyEquality() {
    final AspectParams pAB = new AspectParams();
    pAB.setAspectNames(Arrays.asList(ASPECT_A, ASPECT_B));
    final AspectParams pBA = new AspectParams();
    pBA.setAspectNames(Arrays.asList(ASPECT_B, ASPECT_A));

    final AspectsKey k1 = AspectsKey.from(DATASET_URN_1.toString(), DATASET_TYPE, pAB);
    final AspectsKey k2 = AspectsKey.from(DATASET_URN_1.toString(), DATASET_TYPE, pBA);

    assertEquals(k1, k2);
    assertEquals(k1.hashCode(), k2.hashCode());
  }

  @Test
  public void testAspectsKeyNormalizesNulls() {
    final AspectsKey nullParams = AspectsKey.from(DATASET_URN_1.toString(), DATASET_TYPE, null);
    final AspectParams empty = new AspectParams(); // null aspectNames, null autoRenderOnly
    final AspectsKey emptyParams = AspectsKey.from(DATASET_URN_1.toString(), DATASET_TYPE, empty);

    assertEquals(nullParams, emptyParams);
    assertEquals(nullParams.getAspectNames(), Collections.emptySet());
    assertEquals(nullParams.isAutoRenderOnly(), false);
  }

  @Test
  public void testAutoRenderOnlyNarrowsFetchedAspects() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, true, null); // auto-render
    final AspectSpec specB = mockAspectSpec(ASPECT_B, false, null);
    final AspectSpec specC = mockAspectSpec(ASPECT_C, true, null); // auto-render
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA, specB, specC));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(ImmutableSet.of(ASPECT_A, ASPECT_C))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A, ASPECT_C)));

    final AspectParams autoOnly = new AspectParams();
    autoOnly.setAutoRenderOnly(true);

    final List<AspectsKey> keys =
        ImmutableList.of(AspectsKey.from(DATASET_URN_1.toString(), DATASET_TYPE, autoOnly));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.size(), 1);
    final List<RawAspect> raws = result.get(0);
    assertEquals(raws.size(), 2);
    final Set<String> names = new HashSet<>();
    for (RawAspect r : raws) {
      names.add(r.getAspectName());
    }
    assertEquals(names, ImmutableSet.of(ASPECT_A, ASPECT_C));
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            nullable(OperationContext.class), eq(DATASET_TYPE), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testEmptyResolvedAspectSetSkipsClientCall() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    // Registry has aspectA only, but caller asks for aspectB (no match).
    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA));

    final List<AspectsKey> keys =
        ImmutableList.of(datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_B)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).isEmpty());
    Mockito.verify(client, Mockito.never())
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testMissingUrnInResponseYieldsEmptyList() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA));

    // Response only has URN_1; URN_2 is missing (entity not found).
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(ImmutableSet.of(DATASET_URN_1, DATASET_URN_2)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));

    final List<AspectsKey> keys =
        ImmutableList.of(
            datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)),
            datasetKey(DATASET_URN_2, ImmutableSet.of(ASPECT_A)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.get(0).size(), 1);
    assertEquals(result.get(0).get(0).getAspectName(), ASPECT_A);
    assertTrue(result.get(1).isEmpty());
  }

  @Test
  public void testPositionalContractPreservesInputOrder() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                Mockito.anySet(),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A),
                DATASET_URN_2, entityResponseWithAspects(DATASET_URN_2, ASPECT_A),
                DATASET_URN_3, entityResponseWithAspects(DATASET_URN_3, ASPECT_A)));

    // Input order: 3, 1, 2 — result slots must follow the same order. Each URN's mocked
    // response stamps its URN into the aspect payload so we can detect any permutation.
    final List<Urn> expectedOrder = ImmutableList.of(DATASET_URN_3, DATASET_URN_1, DATASET_URN_2);
    final List<AspectsKey> keys =
        ImmutableList.of(
            datasetKey(DATASET_URN_3, ImmutableSet.of(ASPECT_A)),
            datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)),
            datasetKey(DATASET_URN_2, ImmutableSet.of(ASPECT_A)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      assertEquals(result.get(i).size(), 1, "index " + i);
      assertEquals(result.get(i).get(0).getAspectName(), ASPECT_A);
      assertTrue(
          result.get(i).get(0).getPayload().contains(expectedOrder.get(i).toString()),
          "slot " + i + " should hold response for " + expectedOrder.get(i));
    }
  }

  /**
   * Documents the actual current behavior: a {@code batchGetV2} failure inside any group bubbles
   * out of {@code batchLoad} as a {@code RuntimeException}, failing all keys in the batch (not just
   * the failing group). Recorded as an open question against the plan — current implementation does
   * not isolate per-group failures.
   */
  @Test
  public void testBatchGetV2ExceptionPropagatesAsRuntimeException() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenThrow(new RemoteInvocationException("boom"));

    final List<AspectsKey> keys =
        ImmutableList.of(datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)));

    assertThrows(
        RuntimeException.class,
        () -> WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry));
  }

  @Test
  public void testEmptyInputKeyListReturnsEmptyResult() {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(Collections.emptyList(), ctx, client, registry);

    assertTrue(result.isEmpty());
    Mockito.verifyNoInteractions(client);
    Mockito.verifyNoInteractions(registry);
  }

  /**
   * When the same effective key is passed twice (same urn, entityType, aspectNames, autoRenderOnly)
   * the batchLoad still produces the right number of slots in input order. DataLoader normally
   * de-dupes before invoking batchLoad, but the batch function itself must not assume uniqueness.
   */
  @Test
  public void testDuplicateKeysFillCorrespondingSlots() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));

    final AspectsKey k = datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A));
    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(ImmutableList.of(k, k), ctx, client, registry);

    assertEquals(result.size(), 2);
    assertEquals(result.get(0).get(0).getAspectName(), ASPECT_A);
    assertEquals(result.get(1).get(0).getAspectName(), ASPECT_A);
    // Single batchGetV2 call — the same URN is collapsed in the request set.
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            nullable(OperationContext.class), eq(DATASET_TYPE), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testRawAspectRenderSpecPopulated() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final DataMap renderSpec = new DataMap();
    renderSpec.put("displayType", "table");
    renderSpec.put("displayName", "Aspect A");
    renderSpec.put("key", "key-a");
    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, renderSpec);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));

    final List<AspectsKey> keys =
        ImmutableList.of(datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    final RawAspect raw = result.get(0).get(0);
    assertEquals(raw.getRenderSpec().getDisplayType(), "table");
    assertEquals(raw.getRenderSpec().getDisplayName(), "Aspect A");
    assertEquals(raw.getRenderSpec().getKey(), "key-a");
  }

  @Test
  public void testAutoRenderOnlyFalseIncludesAllMatchingNames() throws Exception {
    // Sanity check that autoRenderOnly=false (default) does not filter on auto-render flag.
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    final AspectSpec specB = mockAspectSpec(ASPECT_B, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA, specB));

    final Set<String> bothAspects = ImmutableSet.of(ASPECT_A, ASPECT_B);
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(bothAspects)))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A, ASPECT_B)));

    final List<AspectsKey> keys = ImmutableList.of(datasetKey(DATASET_URN_1, bothAspects));
    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.get(0).size(), 2);
    final Set<String> got = new HashSet<>();
    for (RawAspect r : result.get(0)) {
      got.add(r.getAspectName());
    }
    assertEquals(got, bothAspects);
  }

  /** Captures the empty-aspectNames-fetch-everything semantic (no name filter, no auto filter). */
  @Test
  public void testEmptyAspectNamesFetchesAllAspects() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    final AspectSpec specB = mockAspectSpec(ASPECT_B, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA, specB));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(ImmutableSet.of(ASPECT_A, ASPECT_B))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A, ASPECT_B)));

    // Empty aspectNames → no filter → all aspects fetched.
    final List<AspectsKey> keys =
        ImmutableList.of(
            AspectsKey.from(DATASET_URN_1.toString(), DATASET_TYPE, new AspectParams()));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.get(0).size(), 2);
  }

  /**
   * Constructed groups for the same entity type but different {@code autoRenderOnly} must not be
   * merged.
   */
  @Test
  public void testAutoRenderOnlySplitsGroups() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, true, null);
    final AspectSpec specB = mockAspectSpec(ASPECT_B, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA, specB));

    // Group 1: autoRenderOnly=true → fetch {ASPECT_A}
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));
    // Group 2: autoRenderOnly=false, empty names → fetch {ASPECT_A, ASPECT_B}
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_2)),
                eq(ImmutableSet.of(ASPECT_A, ASPECT_B))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN_2, entityResponseWithAspects(DATASET_URN_2, ASPECT_A, ASPECT_B)));

    final AspectParams autoOnly = new AspectParams();
    autoOnly.setAutoRenderOnly(true);
    final AspectParams allParams = new AspectParams();

    final List<AspectsKey> keys =
        ImmutableList.of(
            AspectsKey.from(DATASET_URN_1.toString(), DATASET_TYPE, autoOnly),
            AspectsKey.from(DATASET_URN_2.toString(), DATASET_TYPE, allParams));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.get(0).size(), 1);
    assertEquals(result.get(0).get(0).getAspectName(), ASPECT_A);
    assertEquals(result.get(1).size(), 2);
    Mockito.verify(client, Mockito.times(2))
        .batchGetV2(
            nullable(OperationContext.class), eq(DATASET_TYPE), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testGroupKeyForSameUrnDifferentAspectNamesNotMerged() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    final AspectSpec specB = mockAspectSpec(ASPECT_B, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA, specB));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(Collections.singleton(ASPECT_B))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_B)));

    final List<AspectsKey> keys =
        ImmutableList.of(
            datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)),
            datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_B)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.get(0).get(0).getAspectName(), ASPECT_A);
    assertEquals(result.get(1).get(0).getAspectName(), ASPECT_B);
    Mockito.verify(client, Mockito.times(2))
        .batchGetV2(
            nullable(OperationContext.class), eq(DATASET_TYPE), Mockito.anySet(), Mockito.anySet());
  }

  /**
   * Mixed entity-type keys where one group resolves to an empty aspect set must not affect the
   * other group's results.
   */
  @Test
  public void testEmptyGroupCoexistsWithNonEmptyGroup() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    // dataset: aspectA present
    final AspectSpec datasetSpecA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(datasetSpecA));
    // chart: only aspectC, caller asks for aspectA — no match -> empty
    final AspectSpec chartSpecC = mockAspectSpec(ASPECT_C, false, null);
    stubEntitySpec(registry, CHART_TYPE, ImmutableList.of(chartSpecC));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));

    final AspectParams params = new AspectParams();
    params.setAspectNames(ImmutableList.of(ASPECT_A));

    final List<AspectsKey> keys =
        ImmutableList.of(
            AspectsKey.from(DATASET_URN_1.toString(), DATASET_TYPE, params),
            AspectsKey.from(CHART_URN_1.toString(), CHART_TYPE, params));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.get(0).size(), 1);
    assertTrue(result.get(1).isEmpty());
    // Only the dataset group should have issued a batchGetV2 call.
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            nullable(OperationContext.class), eq(DATASET_TYPE), Mockito.anySet(), Mockito.anySet());
    Mockito.verify(client, Mockito.never())
        .batchGetV2(
            nullable(OperationContext.class), eq(CHART_TYPE), Mockito.anySet(), Mockito.anySet());
  }

  /**
   * Verifies the resolver respects the aspectSpec iteration order in {@code buildRawAspectList}.
   */
  @Test
  public void testRawAspectListOrderMatchesEntitySpecOrder() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    // EntitySpec exposes A then B then C; caller asks for {B, A, C}.
    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    final AspectSpec specB = mockAspectSpec(ASPECT_B, false, null);
    final AspectSpec specC = mockAspectSpec(ASPECT_C, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA, specB, specC));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(ImmutableSet.of(ASPECT_A, ASPECT_B, ASPECT_C))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN_1,
                entityResponseWithAspects(DATASET_URN_1, ASPECT_A, ASPECT_B, ASPECT_C)));

    final List<AspectsKey> keys =
        ImmutableList.of(datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_B, ASPECT_A, ASPECT_C)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    final List<RawAspect> raws = result.get(0);
    assertEquals(raws.size(), 3);
    assertEquals(raws.get(0).getAspectName(), ASPECT_A);
    assertEquals(raws.get(1).getAspectName(), ASPECT_B);
    assertEquals(raws.get(2).getAspectName(), ASPECT_C);
  }

  @Test
  public void testRawAspectListSkipsAspectsMissingFromResponse() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    // Caller asks for {A, B}; backend returns only A.
    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    final AspectSpec specB = mockAspectSpec(ASPECT_B, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA, specB));

    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(ImmutableSet.of(ASPECT_A, ASPECT_B))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));

    final List<AspectsKey> keys =
        ImmutableList.of(datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A, ASPECT_B)));

    final List<List<RawAspect>> result =
        WeaklyTypedAspectsResolver.batchLoad(keys, ctx, client, registry);

    assertEquals(result.get(0).size(), 1);
    assertEquals(result.get(0).get(0).getAspectName(), ASPECT_A);
  }

  /**
   * Covers {@link WeaklyTypedAspectsResolver#get(DataFetchingEnvironment)}: the resolver pulls the
   * named loader from the registry and dispatches an {@link AspectsKey} built from the source URN,
   * the source entity type, and the {@code input} argument.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testGetDispatchesToDataLoader() {
    final DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    final Entity source = Mockito.mock(Entity.class);
    Mockito.when(source.getUrn()).thenReturn(DATASET_URN_1.toString());
    Mockito.when(source.getType()).thenReturn(EntityType.DATASET);
    Mockito.when(env.getSource()).thenReturn(source);
    Mockito.when(env.getArgument("input")).thenReturn(null);

    final DataLoader<AspectsKey, List<RawAspect>> loader =
        (DataLoader<AspectsKey, List<RawAspect>>) Mockito.mock(DataLoader.class);
    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    Mockito.doReturn(loader).when(registry).getDataLoader(WeaklyTypedAspectsResolver.LOADER_NAME);
    Mockito.when(env.getDataLoaderRegistry()).thenReturn(registry);

    final CompletableFuture<List<RawAspect>> expected =
        CompletableFuture.completedFuture(Collections.emptyList());
    Mockito.when(loader.load(any(AspectsKey.class))).thenReturn(expected);

    final CompletableFuture<List<RawAspect>> result = new WeaklyTypedAspectsResolver().get(env);

    assertEquals(result, expected);
    final ArgumentCaptor<AspectsKey> captor = ArgumentCaptor.forClass(AspectsKey.class);
    Mockito.verify(loader).load(captor.capture());
    final AspectsKey actual = captor.getValue();
    assertEquals(actual.getUrn(), DATASET_URN_1.toString());
    assertEquals(actual.getEntityType(), DATASET_TYPE);
    assertTrue(actual.getAspectNames().isEmpty());
    assertEquals(actual.isAutoRenderOnly(), false);
  }

  /**
   * Covers {@link WeaklyTypedAspectsResolver#createDataLoader(EntityClient, EntityRegistry,
   * QueryContext)}: the returned loader, when dispatched, routes the registered key through {@code
   * batchLoad} and emits a populated {@link RawAspect}.
   */
  @Test
  public void testCreateDataLoaderRoutesThroughBatchLoad() throws Exception {
    final EntityClient client = Mockito.mock(EntityClient.class);
    final EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    final QueryContext ctx = mockQueryContext();

    final AspectSpec specA = mockAspectSpec(ASPECT_A, false, null);
    stubEntitySpec(registry, DATASET_TYPE, ImmutableList.of(specA));
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                eq(DATASET_TYPE),
                eq(Collections.singleton(DATASET_URN_1)),
                eq(Collections.singleton(ASPECT_A))))
        .thenReturn(
            ImmutableMap.of(DATASET_URN_1, entityResponseWithAspects(DATASET_URN_1, ASPECT_A)));

    final DataLoader<AspectsKey, List<RawAspect>> loader =
        WeaklyTypedAspectsResolver.createDataLoader(client, registry, ctx);
    final CompletableFuture<List<RawAspect>> future =
        loader.load(datasetKey(DATASET_URN_1, ImmutableSet.of(ASPECT_A)));
    loader.dispatch();

    final List<RawAspect> result = future.get();
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getAspectName(), ASPECT_A);
    assertTrue(result.get(0).getPayload().contains(DATASET_URN_1.toString()));
  }
}
