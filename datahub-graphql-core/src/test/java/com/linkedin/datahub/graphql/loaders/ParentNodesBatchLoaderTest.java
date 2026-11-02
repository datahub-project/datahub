package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.ParentNodesResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParentNodesBatchLoaderTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:glossaryNode:root");
  private static final Urn MID = UrnUtils.getUrn("urn:li:glossaryNode:mid");
  private static final Urn TERM_A = UrnUtils.getUrn("urn:li:glossaryTerm:a");
  private static final Urn TERM_B = UrnUtils.getUrn("urn:li:glossaryTerm:b");

  private EntityClient _entityClient;
  private QueryContext _context;

  @BeforeMethod
  public void setup() {
    _entityClient = Mockito.mock(EntityClient.class);
    _context = getMockAllowContext();
  }

  private static EntityResponse nodeResponse(Urn urn) {
    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new Aspect(new GlossaryNodeInfo().setName(urn.getId()).setDefinition("d").data())));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(Constants.GLOSSARY_NODE_ENTITY_NAME)
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
                  out.put(u, nodeResponse(u));
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

  /** Two terms under the same node must cost one hydration call, not two. */
  @Test
  public void testSharedAncestorsFetchedOnce() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains =
        Map.of(TERM_A, List.of(MID, ROOT), TERM_B, List.of(MID, ROOT));

    final List<Try<ParentNodesResult>> out =
        assemble1(List.of(TERM_A, TERM_B), _context, _entityClient, chains);

    assertEquals(out.get(0).get().getCount(), 2);
    assertEquals(out.get(1).get().getCount(), 2);
    assertEquals(captureBatchGet(1).getValue(), Set.of(MID, ROOT));
  }

  /** Hierarchy order must survive the shared, unordered response map. */
  @Test
  public void testHierarchyOrderPreserved() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));

    final ParentNodesResult result =
        assemble1(List.of(TERM_A), _context, _entityClient, Map.of(TERM_A, List.of(MID, ROOT)))
            .get(0)
            .get();

    assertEquals(result.getNodes().get(0).getUrn(), MID.toString());
    assertEquals(result.getNodes().get(1).getUrn(), ROOT.toString());
  }

  @Test
  public void testNoAncestorsIssuesNoFetch() throws Exception {
    stubBatchGet(Set.of());

    final List<Try<ParentNodesResult>> out =
        assemble1(List.of(TERM_A), _context, _entityClient, Map.of(TERM_A, List.of()));

    assertEquals(out.get(0).get().getCount(), 0);
    Mockito.verify(_entityClient, Mockito.never())
        .batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class));
  }

  /**
   * An unresolvable ancestor fails only the entity that referenced it. The unbatched resolver threw
   * for that one field, so sharing a batch must not spread the failure.
   */
  @Test
  public void testMissingAncestorFailsOnlyItsOwnKey() throws Exception {
    stubBatchGet(Set.of(ROOT)); // MID unresolvable
    final Map<Urn, List<Urn>> chains = Map.of(TERM_A, List.of(MID, ROOT), TERM_B, List.of(ROOT));

    final List<Try<ParentNodesResult>> out =
        assemble1(List.of(TERM_A, TERM_B), _context, _entityClient, chains);

    assertTrue(out.get(0).isFailure(), "term with the missing ancestor should fail");
    assertTrue(out.get(1).isSuccess(), "the other term must still resolve");
    assertEquals(out.get(1).get().getCount(), 1);
  }

  /** A failed hydration is shared by the whole batch, so every key sees it. */
  @Test
  public void testFetchFailurePropagatesToAllKeys() throws Exception {
    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenThrow(new RuntimeException("entity client unavailable"));

    final List<Try<ParentNodesResult>> out =
        assemble1(
            List.of(TERM_A, TERM_B),
            _context,
            _entityClient,
            Map.of(TERM_A, List.of(ROOT), TERM_B, List.of(ROOT)));

    assertTrue(out.get(0).isFailure());
    assertTrue(out.get(1).isFailure());
  }

  /** A walk that failed must fail only its own key, not the batch. */
  @Test
  public void testFailedWalkFailsOnlyItsOwnKey() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, Try<List<Urn>>> chains = new java.util.LinkedHashMap<>();
    chains.put(TERM_A, Try.failed(new RuntimeException("hierarchy cache read failed")));
    chains.put(TERM_B, Try.succeeded(List.of(MID, ROOT)));

    final List<Try<ParentNodesResult>> out =
        assemble2(List.of(TERM_A, TERM_B), _context, _entityClient, chains);

    assertTrue(out.get(0).isFailure(), "the failed walk should fail its own key");
    assertTrue(out.get(1).isSuccess(), "the other term must still resolve");
    assertEquals(out.get(1).get().getCount(), 2);
  }

  /** An entity with no ancestors never used the shared fetch, so its failure must not touch it. */
  @Test
  public void testEmptyChainSurvivesFetchFailure() throws Exception {
    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenThrow(new RuntimeException("entity client unavailable"));

    final List<Try<ParentNodesResult>> out =
        assemble1(
            List.of(TERM_A, TERM_B),
            _context,
            _entityClient,
            Map.of(TERM_A, List.of(ROOT), TERM_B, List.of()));

    assertTrue(out.get(0).isFailure(), "the term that needed the fetch should fail");
    assertTrue(out.get(1).isSuccess(), "a term with no ancestors must not be affected");
    assertEquals(out.get(1).get().getCount(), 0);
  }

  /** Exercises the real batchLoad, including the concurrent walk against the hierarchy cache. */
  @Test
  public void testRealBatchLoadResolvesThroughTheWalk() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final QueryContext ctx = contextWithAncestors(Map.of(TERM_A, List.of(MID, ROOT)), null, null);

    final List<Try<ParentNodesResult>> out =
        ParentNodesBatchLoader.batchLoad(List.of(TERM_A), ctx, _entityClient).join();

    assertEquals(out.get(0).get().getCount(), 2);
    assertEquals(out.get(0).get().getNodes().get(0).getUrn(), MID.toString());
  }

  /** A throwing hierarchy-cache read must fail only its own key, through the real batchLoad. */
  @Test
  public void testThrowingWalkFailsOnlyItsOwnKeyThroughRealBatchLoad() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final QueryContext ctx =
        contextWithAncestors(Map.of(TERM_B, List.of(MID, ROOT)), TERM_A, "hazelcast read failed");

    final List<Try<ParentNodesResult>> out =
        ParentNodesBatchLoader.batchLoad(List.of(TERM_A, TERM_B), ctx, _entityClient).join();

    assertTrue(out.get(0).isFailure(), "the throwing walk should fail only its own key");
    assertTrue(out.get(1).isSuccess(), "the other term must still resolve");
    assertEquals(out.get(1).get().getCount(), 2);
  }

  /** The wired DataLoader must dispatch and surface per-key failures through the future. */
  @Test
  public void testCreatedLoaderDispatchesAndIsolatesFailures() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final QueryContext ctx =
        contextWithAncestors(Map.of(TERM_B, List.of(MID, ROOT)), TERM_A, "hazelcast read failed");

    final DataLoader<Urn, ParentNodesResult> loader =
        ParentNodesBatchLoader.create(_entityClient, ctx);
    final CompletableFuture<ParentNodesResult> bad = loader.load(TERM_A);
    final CompletableFuture<ParentNodesResult> good = loader.load(TERM_B);
    loader.dispatch().get(30, TimeUnit.SECONDS);

    assertTrue(
        bad.isCompletedExceptionally(), "failing key's future should complete exceptionally");
    assertEquals(good.get().getCount(), 2);
  }

  /** A QueryContext whose glossary hierarchy cache answers the walk for each supplied seed. */
  private static QueryContext contextWithAncestors(
      Map<Urn, List<Urn>> chains, Urn throwingSeed, String throwMessage) {
    final EntityGraphCache cache = Mockito.mock(EntityGraphCache.class);
    final EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    Mockito.when(cache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    chains.forEach(
        (seed, ancestors) ->
            Mockito.when(
                    cache.walkOrderedForwardAncestors(
                        eq("glossary"),
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
                  eq("glossary"),
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

  /** An ancestor the viewer cannot see is filtered out of that entity's chain. */
  @Test
  public void testAncestorHiddenByRelationshipVisibility() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    // CALLS_REAL_METHODS: the mapper also consults AuthorizationUtils, so only the relationship
    // check may be overridden.
    try (MockedStatic<AuthorizationUtils> auth =
        mockStatic(AuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      auth.when(() -> AuthorizationUtils.canViewRelationship(any(), eq(MID), eq(TERM_A)))
          .thenReturn(false);
      auth.when(() -> AuthorizationUtils.canViewRelationship(any(), eq(ROOT), eq(TERM_A)))
          .thenReturn(true);

      final ParentNodesResult result =
          assemble1(List.of(TERM_A), _context, _entityClient, Map.of(TERM_A, List.of(MID, ROOT)))
              .get(0)
              .get();

      assertEquals(result.getCount(), 1);
      assertEquals(result.getNodes().get(0).getUrn(), ROOT.toString());
    }
  }

  /** A mapping failure for one entity must not escape into the other keys. */
  @Test
  public void testMapperFailureIsScopedToItsKey() throws Exception {
    // A glossaryNodeInfo aspect missing its required fields makes the mapper throw.
    final Map<String, EnvelopedAspect> broken = new HashMap<>();
    broken.put(
        Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(new com.linkedin.data.DataMap())));
    final EntityResponse brokenResponse =
        new EntityResponse()
            .setUrn(MID)
            .setEntityName(Constants.GLOSSARY_NODE_ENTITY_NAME)
            .setAspects(new EnvelopedAspectMap(broken));

    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenAnswer(
            inv -> {
              final Map<Urn, EntityResponse> out = new HashMap<>();
              for (Urn u : (Set<Urn>) inv.getArgument(2)) {
                out.put(u, u.equals(MID) ? brokenResponse : nodeResponse(u));
              }
              return out;
            });

    final List<Try<ParentNodesResult>> out =
        assemble1(
            List.of(TERM_A, TERM_B),
            _context,
            _entityClient,
            Map.of(TERM_A, List.of(MID), TERM_B, List.of(ROOT)));

    assertTrue(out.get(0).isFailure(), "the entity whose ancestor failed to map should fail");
    assertTrue(out.get(1).isSuccess(), "the other entity must still resolve");
    assertEquals(out.get(1).get().getCount(), 1);
  }

  /** Ancestors are grouped by entity type, so a mixed chain costs one call per type. */
  @Test
  public void testAncestorsGroupedByEntityType() throws Exception {
    final Urn container = UrnUtils.getUrn("urn:li:container:mixed");
    stubBatchGet(Set.of(ROOT, container));

    assemble1(List.of(TERM_A), _context, _entityClient, Map.of(TERM_A, List.of(ROOT, container)));

    // Each type must be fetched under its own entity name, carrying only its own urns — a count
    // alone would still pass if the two groups were swapped.
    final ArgumentCaptor<String> types = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Set<Urn>> urns = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(_entityClient, Mockito.times(2))
        .batchGetV2(any(), types.capture(), urns.capture(), nullable(Set.class));

    final Map<String, Set<Urn>> grouped = new HashMap<>();
    for (int i = 0; i < types.getAllValues().size(); i++) {
      grouped.put(types.getAllValues().get(i), urns.getAllValues().get(i));
    }
    assertEquals(grouped, Map.of("glossaryNode", Set.of(ROOT), "container", Set.of(container)));
  }

  /** DataLoader's contract is positional, including when a key repeats in one batch. */
  @Test
  public void testDuplicateKeysMapBackPositionally() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains = Map.of(TERM_A, List.of(MID, ROOT), TERM_B, List.of(ROOT));

    final List<Try<ParentNodesResult>> out =
        assemble1(List.of(TERM_A, TERM_B, TERM_A), _context, _entityClient, chains);

    assertEquals(out.size(), 3);
    assertEquals(out.get(0).get().getCount(), 2);
    assertEquals(out.get(1).get().getCount(), 1);
    assertEquals(out.get(2).get().getCount(), 2);
  }

  /** Local seam: wrap plain chains as successful Trys and assemble. */
  private static List<Try<ParentNodesResult>> assemble1(
      List<Urn> urns, QueryContext ctx, EntityClient client, Map<Urn, List<Urn>> chains) {
    Map<Urn, Try<List<Urn>>> asTry = new java.util.LinkedHashMap<>();
    chains.forEach((urn, chain) -> asTry.put(urn, Try.succeeded(chain)));
    return assemble2(urns, ctx, client, asTry);
  }

  private static List<Try<ParentNodesResult>> assemble2(
      List<Urn> urns, QueryContext ctx, EntityClient client, Map<Urn, Try<List<Urn>>> chains) {
    return ParentNodesBatchLoader.assemble(
        urns,
        urns.stream().distinct().collect(java.util.stream.Collectors.toList()),
        ctx,
        client,
        chains);
  }
}
