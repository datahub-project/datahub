package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;
import org.dataloader.Try;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParentContainersBatchLoaderTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:container:root");
  private static final Urn MID = UrnUtils.getUrn("urn:li:container:mid");
  private static final Urn LEAF_A = UrnUtils.getUrn("urn:li:container:leafA");
  private static final Urn LEAF_B = UrnUtils.getUrn("urn:li:container:leafB");

  private EntityClient _entityClient;
  private QueryContext _context;

  @BeforeMethod
  public void setup() {
    _entityClient = Mockito.mock(EntityClient.class);
    _context = getMockAllowContext();
  }

  private static EntityResponse containerResponse(Urn urn) {
    final RecordTemplate props = new ContainerProperties().setName(urn.getId());
    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.CONTAINER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(Constants.CONTAINER_ENTITY_NAME)
        .setAspects(new EnvelopedAspectMap(aspects));
  }

  @SuppressWarnings("unchecked")
  private void stubBatchGet(Set<Urn> known) throws Exception {
    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenAnswer(
            inv -> {
              // Honour the entity type: a urn asked for under the wrong type resolves to nothing,
              // the same as the real client.
              final String entityName = inv.getArgument(1);
              final Set<Urn> requested = inv.getArgument(2);
              final Map<Urn, EntityResponse> out = new HashMap<>();
              for (Urn u : requested) {
                if (known.contains(u) && u.getEntityType().equals(entityName)) {
                  out.put(u, containerResponse(u));
                }
              }
              return out;
            });
  }

  @SuppressWarnings("unchecked")
  private ArgumentCaptor<Set<Urn>> captureBatchGet(int times) throws Exception {
    final ArgumentCaptor<Set<Urn>> captor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(_entityClient, Mockito.times(times))
        .batchGetV2(any(), any(String.class), captor.capture(), nullable(Set.class));
    return captor;
  }

  /**
   * The point of the change: ancestors of every key are fetched together, and an ancestor shared by
   * two keys is fetched once.
   */
  @Test
  public void testSharedAncestorsFetchedOnce() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains =
        Map.of(LEAF_A, List.of(MID, ROOT), LEAF_B, List.of(MID, ROOT));

    final List<Try<ParentContainersResult>> out =
        assemble1(List.of(LEAF_A, LEAF_B), _context, _entityClient, chains);

    assertEquals(out.size(), 2);
    assertEquals(out.get(0).get().getCount(), 2);
    assertEquals(out.get(1).get().getCount(), 2);
    // One call for both keys, carrying the union of ancestors with no duplicates.
    assertEquals(captureBatchGet(1).getValue(), Set.of(MID, ROOT));
  }

  /** Hierarchy order must survive the shared, unordered response map. */
  @Test
  public void testHierarchyOrderPreserved() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains = Map.of(LEAF_A, List.of(MID, ROOT));

    final ParentContainersResult result =
        assemble1(List.of(LEAF_A), _context, _entityClient, chains).get(0).get();

    assertEquals(result.getContainers().get(0).getUrn(), MID.toString());
    assertEquals(result.getContainers().get(1).getUrn(), ROOT.toString());
  }

  /** A top-level entity has no ancestors, so it must not trigger a fetch at all. */
  @Test
  public void testNoAncestorsIssuesNoFetch() throws Exception {
    stubBatchGet(Set.of());
    final var out = assemble1(List.of(ROOT), _context, _entityClient, Map.of(ROOT, List.of()));

    assertEquals(out.get(0).get().getCount(), 0);
    Mockito.verify(_entityClient, Mockito.never())
        .batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class));
  }

  /** An ancestor the caller cannot see is skipped, not rendered as an empty container. */
  @Test
  public void testUnauthorizedAncestorSkipped() throws Exception {
    stubBatchGet(Set.of(ROOT)); // MID withheld
    final var out =
        assemble1(List.of(LEAF_A), _context, _entityClient, Map.of(LEAF_A, List.of(MID, ROOT)));

    assertEquals(out.get(0).get().getCount(), 1);
    assertEquals(out.get(0).get().getContainers().get(0).getUrn(), ROOT.toString());
  }

  /**
   * End-to-end through the real batchLoad, including the concurrent ancestor walk. Two keys share a
   * hierarchy, so the whole batch must cost one hydration call.
   */
  @Test
  public void testRealBatchLoadWalksConcurrentlyAndFetchesOnce() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final QueryContext ctx =
        contextWithAncestors(Map.of(LEAF_A, List.of(MID, ROOT), LEAF_B, List.of(MID, ROOT)));

    final List<Try<ParentContainersResult>> out =
        ParentContainersBatchLoader.batchLoad(List.of(LEAF_A, LEAF_B), ctx, _entityClient).join();

    assertEquals(out.size(), 2);
    assertEquals(out.get(0).get().getCount(), 2);
    assertEquals(out.get(0).get().getContainers().get(0).getUrn(), MID.toString());
    assertEquals(out.get(1).get().getCount(), 2);
    assertEquals(captureBatchGet(1).getValue(), Set.of(MID, ROOT));
  }

  /**
   * The reported defect: a throwing hierarchy-cache read used to propagate out of the concurrent
   * walk and fail every key. Goes through the real batchLoad, not the test seam.
   */
  @Test
  public void testThrowingWalkFailsOnlyItsOwnKeyThroughRealBatchLoad() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final QueryContext ctx =
        contextWithAncestors(Map.of(LEAF_B, List.of(MID, ROOT)), LEAF_A, "hazelcast read failed");

    final List<Try<ParentContainersResult>> out =
        ParentContainersBatchLoader.batchLoad(List.of(LEAF_A, LEAF_B), ctx, _entityClient).join();

    assertEquals(out.size(), 2);
    assertTrue(out.get(0).isFailure(), "the throwing walk should fail only its own key");
    assertTrue(out.get(1).isSuccess(), "the other key must still resolve");
    assertEquals(out.get(1).get().getCount(), 2);
  }

  /** An entity with no ancestors never used the shared fetch, so its failure must not touch it. */
  @Test
  public void testEmptyChainSurvivesFetchFailure() throws Exception {
    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenThrow(new RuntimeException("entity client unavailable"));

    final List<Try<ParentContainersResult>> out =
        assemble1(
            List.of(LEAF_A, ROOT),
            _context,
            _entityClient,
            Map.of(LEAF_A, List.of(MID), ROOT, List.of()));

    assertTrue(out.get(0).isFailure(), "the key that needed the fetch should fail");
    assertTrue(out.get(1).isSuccess(), "a top-level container must not be affected");
    assertEquals(out.get(1).get().getCount(), 0);
  }

  /** The wired DataLoader must dispatch and surface per-key failures through the future. */
  @Test
  public void testCreatedLoaderDispatchesAndIsolatesFailures() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final QueryContext ctx =
        contextWithAncestors(Map.of(LEAF_B, List.of(MID, ROOT)), LEAF_A, "hazelcast read failed");

    final DataLoader<Urn, ParentContainersResult> loader =
        ParentContainersBatchLoader.create(_entityClient, ctx);
    final CompletableFuture<ParentContainersResult> bad = loader.load(LEAF_A);
    final CompletableFuture<ParentContainersResult> good = loader.load(LEAF_B);
    loader.dispatch().get(30, TimeUnit.SECONDS);

    assertTrue(
        bad.isCompletedExceptionally(), "failing key's future should complete exceptionally");
    assertEquals(good.get().getCount(), 2);
  }

  /** A QueryContext whose hierarchy cache answers the walk for each supplied seed. */
  private static QueryContext contextWithAncestors(Map<Urn, List<Urn>> chains) {
    return contextWithAncestors(chains, null, null);
  }

  private static QueryContext contextWithAncestors(
      Map<Urn, List<Urn>> chains, Urn throwingSeed, String throwMessage) {
    final EntityGraphCache cache = Mockito.mock(EntityGraphCache.class);
    final EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("container").source(GraphSnapshotSource.GRAPH).build();
    Mockito.when(cache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
        .thenReturn(Optional.of(binding));
    chains.forEach(
        (seed, ancestors) ->
            Mockito.when(
                    cache.walkOrderedForwardAncestors(
                        eq("container"),
                        eq(GraphSnapshotSource.GRAPH),
                        eq(seed.toString()),
                        eq(50),
                        eq(ReadMode.CACHED)))
                .thenReturn(
                    AncestorWalkResult.fromAncestors(
                        ancestors.stream().map(Urn::toString).collect(Collectors.toList()))));

    if (throwingSeed != null) {
      Mockito.when(
              cache.walkOrderedForwardAncestors(
                  eq("container"),
                  eq(GraphSnapshotSource.GRAPH),
                  eq(throwingSeed.toString()),
                  eq(50),
                  eq(ReadMode.CACHED)))
          .thenThrow(new RuntimeException(throwMessage));
    }

    final OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    final OperationContext opContext =
        base.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(Mockito.mock(AspectRetriever.class))
                    .entityGraphCache(cache)
                    .build())
            .build(base.getSessionAuthentication(), false);

    final QueryContext ctx = Mockito.mock(QueryContext.class);
    Mockito.when(ctx.getOperationContext()).thenReturn(opContext);
    Mockito.when(ctx.getMaxParentDepth()).thenReturn(50);
    return ctx;
  }

  /** A walk that failed must fail only its own key, not the batch. */
  @Test
  public void testFailedWalkFailsOnlyItsOwnKey() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, Try<List<Urn>>> chains = new java.util.LinkedHashMap<>();
    chains.put(LEAF_A, Try.failed(new RuntimeException("hierarchy cache read failed")));
    chains.put(LEAF_B, Try.succeeded(List.of(MID, ROOT)));

    final List<Try<ParentContainersResult>> out =
        assemble2(List.of(LEAF_A, LEAF_B), _context, _entityClient, chains);

    assertTrue(out.get(0).isFailure(), "the failed walk should fail its own key");
    assertTrue(out.get(1).isSuccess(), "the other key must still resolve");
    assertEquals(out.get(1).get().getCount(), 2);
  }

  /** A hydration failure is shared, so every key in the batch sees it. */
  @Test
  public void testFetchFailurePropagatesToAllKeys() throws Exception {
    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenThrow(new RuntimeException("entity client unavailable"));

    final List<Try<ParentContainersResult>> out =
        assemble1(
            List.of(LEAF_A, LEAF_B),
            _context,
            _entityClient,
            Map.of(LEAF_A, List.of(ROOT), LEAF_B, List.of(ROOT)));

    assertTrue(out.get(0).isFailure());
    assertTrue(out.get(1).isFailure());
  }

  /** DataLoader's contract is positional, including when a key repeats in one batch. */
  @Test
  public void testDuplicateKeysMapBackPositionally() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains = Map.of(LEAF_A, List.of(MID, ROOT), LEAF_B, List.of(ROOT));

    final var out = assemble1(List.of(LEAF_A, LEAF_B, LEAF_A), _context, _entityClient, chains);

    assertEquals(out.size(), 3);
    assertEquals(out.get(0).get().getCount(), 2);
    assertEquals(out.get(1).get().getCount(), 1);
    assertEquals(out.get(2).get().getCount(), 2);
  }

  /** Local seam: wrap plain chains as successful Trys and assemble. */
  private static List<Try<ParentContainersResult>> assemble1(
      List<Urn> urns, QueryContext ctx, EntityClient client, Map<Urn, List<Urn>> chains) {
    Map<Urn, Try<List<Urn>>> asTry = new java.util.LinkedHashMap<>();
    chains.forEach((urn, chain) -> asTry.put(urn, Try.succeeded(chain)));
    return assemble2(urns, ctx, client, asTry);
  }

  private static List<Try<ParentContainersResult>> assemble2(
      List<Urn> urns, QueryContext ctx, EntityClient client, Map<Urn, Try<List<Urn>>> chains) {
    return ParentContainersBatchLoader.assemble(
        urns,
        urns.stream().distinct().collect(java.util.stream.Collectors.toList()),
        ctx,
        client,
        chains);
  }
}
