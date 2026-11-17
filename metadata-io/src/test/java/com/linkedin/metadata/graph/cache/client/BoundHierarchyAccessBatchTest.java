package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Batched ancestor walks: bounded DB reads, cache only for graph vertices, no silent truncation.
 */
public class BoundHierarchyAccessBatchTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:container:root");
  private static final Urn MID = UrnUtils.getUrn("urn:li:container:mid");
  private static final Urn LEAF = UrnUtils.getUrn("urn:li:container:leaf");
  private static final Urn DATASET_A =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,a,PROD)");
  private static final Urn DATASET_B =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,b,PROD)");
  private static final Urn CHART = UrnUtils.getUrn("urn:li:chart:(looker,c)");
  private static final Urn ORPHAN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,orphan,PROD)");
  private static final Urn SHALLOW = UrnUtils.getUrn("urn:li:container:shallow");
  private static final Urn DATASET_C =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,c,PROD)");
  private static final RuntimeException BOOM =
      new RuntimeException("Unsuccessfully waited for a connection");

  private AspectRetriever aspectRetriever;
  private EntityGraphCache cache;
  private OperationContext opContext;
  private HierarchyReadSpec spec;
  private final Map<Urn, Urn> parentByUrn = new HashMap<>();
  // A read whose urn set contains any of these fails, as one DB read group would.
  private final Set<Urn> failOn = new java.util.HashSet<>();

  @BeforeMethod
  public void setUp() {
    parentByUrn.clear();
    failOn.clear();
    aspectRetriever = mock(AspectRetriever.class);
    cache = mock(EntityGraphCache.class);
    when(cache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
        .thenReturn(
            Optional.of(
                EntityGraphBinding.builder()
                    .graphId("container")
                    .source(GraphSnapshotSource.GRAPH)
                    .build()));
    // Default: every cache walk misses.
    when(cache.walkOrderedForwardAncestors(anyString(), any(), anyString(), anyInt(), any()))
        .thenReturn(AncestorWalkResult.miss(ReadMissReason.ABSENT));

    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    opContext =
        base.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(aspectRetriever)
                    .entityGraphCache(cache)
                    .build())
            .build(base.getSessionAuthentication(), false);
    spec = HierarchyBindings.containerSpec(opContext);

    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              if (urns.stream().anyMatch(failOn::contains)) {
                throw BOOM;
              }
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              for (Urn urn : urns) {
                Urn parent = parentByUrn.get(urn);
                if (parent != null) {
                  result.put(
                      urn,
                      Map.of(
                          CONTAINER_ASPECT_NAME,
                          new Aspect(new Container().setContainer(parent).data())));
                }
              }
              return result;
            });
  }

  private void hierarchy() {
    parentByUrn.put(DATASET_A, LEAF);
    parentByUrn.put(DATASET_B, LEAF);
    parentByUrn.put(CHART, MID);
    parentByUrn.put(LEAF, MID);
    parentByUrn.put(MID, ROOT);
  }

  @SuppressWarnings("unchecked")
  private List<Set<Urn>> aspectReads(int expected) {
    ArgumentCaptor<Set<Urn>> captor = ArgumentCaptor.forClass(Set.class);
    verify(aspectRetriever, times(expected)).getLatestAspectObjects(any(), captor.capture(), any());
    return captor.getAllValues();
  }

  @Test
  public void aspectBatchReadsEachLevelOnceForAllSeeds() {
    hierarchy();

    Map<Urn, List<Urn>> chains =
        AspectParentWalker.orderedParentsBatch(opContext, spec, List.of(LEAF, MID), 10).getChains();

    assertEquals(chains.get(LEAF), List.of(MID, ROOT));
    assertEquals(chains.get(MID), List.of(ROOT));
    // Level 1 reads {LEAF, MID}; level 2 reads only ROOT (MID's parent is already known).
    List<Set<Urn>> reads = aspectReads(2);
    assertEquals(reads.get(0), Set.of(LEAF, MID));
    assertEquals(reads.get(1), Set.of(ROOT));
  }

  @Test
  public void aspectBatchHonoursMaxDepthAndCycles() {
    parentByUrn.put(LEAF, MID);
    parentByUrn.put(MID, LEAF); // cycle

    Map<Urn, List<Urn>> chains =
        AspectParentWalker.orderedParentsBatch(opContext, spec, List.of(LEAF), 10).getChains();
    assertEquals(chains.get(LEAF), List.of(MID));

    hierarchy();
    chains = AspectParentWalker.orderedParentsBatch(opContext, spec, List.of(LEAF), 1).getChains();
    assertEquals(chains.get(LEAF), List.of(MID));
  }

  @Test
  public void aspectBatchFailsTheChainInsteadOfTruncating() {
    hierarchy();
    failOn.add(MID);

    AspectParentWalker.ChainLookup lookup =
        AspectParentWalker.orderedParentsBatch(opContext, spec, List.of(LEAF), 10);

    assertTrue(lookup.getChains().isEmpty(), "no shortened [MID] chain is returned");
    assertSame(lookup.getFailures().get(LEAF), BOOM);
  }

  @Test
  public void aspectBatchKeepsChainsThatFinishedBeforeAFailure() {
    hierarchy(); // LEAF -> MID -> ROOT; SHALLOW has no parent.
    failOn.add(MID); // the second level read fails

    AspectParentWalker.ChainLookup lookup =
        AspectParentWalker.orderedParentsBatch(opContext, spec, List.of(LEAF, SHALLOW), 10);

    assertEquals(lookup.getChains().get(SHALLOW), List.of());
    assertSame(lookup.getFailures().get(LEAF), BOOM);
    assertTrue(!lookup.getFailures().containsKey(SHALLOW));
  }

  @Test
  public void singleChainWalkStillSwallowsReadErrors() {
    doThrow(new RuntimeException("db down"))
        .when(aspectRetriever)
        .getLatestAspectObjects(any(), any(), any());
    // Long-standing behaviour for the non-batch callers is unchanged.
    assertEquals(AspectParentWalker.orderedParents(opContext, spec, LEAF, 10), List.of());
  }

  @Test
  public void nonVertexSeedsNeverUseTheCacheAndShareParentLookups() {
    hierarchy();
    // Even though the cache could serve the container chains above LEAF and MID, parents of seeds
    // the cache cannot hold are read live, as a single-seed walk would after a miss.
    when(cache.walkOrderedForwardAncestors(
            eq("container"), eq(GraphSnapshotSource.GRAPH), eq(LEAF.toString()), anyInt(), any()))
        .thenReturn(AncestorWalkResult.fromAncestors(List.of(MID.toString(), ROOT.toString())));
    when(cache.walkOrderedForwardAncestors(
            eq("container"), eq(GraphSnapshotSource.GRAPH), eq(MID.toString()), anyInt(), any()))
        .thenReturn(AncestorWalkResult.fromAncestors(List.of(ROOT.toString())));

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(
            opContext, spec, List.of(DATASET_A, DATASET_B, CHART, ORPHAN), 50);

    assertTrue(batch.getFailures().isEmpty());
    assertEquals(batch.getParents().get(DATASET_A), List.of(LEAF, MID, ROOT));
    assertEquals(batch.getParents().get(DATASET_B), List.of(LEAF, MID, ROOT));
    assertEquals(batch.getParents().get(CHART), List.of(MID, ROOT));
    assertEquals(batch.getParents().get(ORPHAN), List.of());
    assertEquals(
        new ArrayList<>(batch.getParents().keySet()),
        List.of(DATASET_A, DATASET_B, CHART, ORPHAN),
        "results keep input order");

    verify(cache, never())
        .walkOrderedForwardAncestors(anyString(), any(), anyString(), anyInt(), any());

    // One direct-parent read per entity type, then one read per level for the distinct parents.
    List<Set<Urn>> reads = aspectReads(4);
    assertEquals(
        reads.subList(0, 2).stream()
            .map(set -> set.stream().map(Urn::getEntityType).collect(Collectors.toSet()))
            .collect(Collectors.toSet()),
        Set.of(Set.of("dataset"), Set.of("chart")),
        "each read carries one entity type");
    assertEquals(reads.get(2), Set.of(LEAF, MID));
    assertEquals(reads.get(3), Set.of(ROOT));
  }

  @Test
  public void cacheMissFallsBackToBatchedAspectWalk() {
    hierarchy();

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(
            opContext, spec, List.of(DATASET_A, DATASET_B), 50);

    assertEquals(batch.getParents().get(DATASET_A), List.of(LEAF, MID, ROOT));
    assertEquals(batch.getParents().get(DATASET_B), List.of(LEAF, MID, ROOT));
    // seeds -> LEAF, then LEAF -> MID, then MID -> ROOT, then ROOT -> none.
    List<Set<Urn>> reads = aspectReads(4);
    assertEquals(reads.get(0), Set.of(DATASET_A, DATASET_B));
    assertEquals(reads.get(1), Set.of(LEAF));
  }

  @Test
  public void vertexSeedsStillUseTheCachePerSeed() {
    hierarchy();
    when(cache.walkOrderedForwardAncestors(
            eq("container"), eq(GraphSnapshotSource.GRAPH), eq(LEAF.toString()), eq(50), any()))
        .thenReturn(AncestorWalkResult.fromAncestors(List.of(MID.toString(), ROOT.toString())));
    when(cache.walkOrderedForwardAncestors(
            eq("container"), eq(GraphSnapshotSource.GRAPH), eq(MID.toString()), eq(50), any()))
        .thenThrow(new RuntimeException("hazelcast read failed"));

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(LEAF, MID), 50);

    assertEquals(batch.getParents().get(LEAF), List.of(MID, ROOT));
    assertTrue(batch.getFailures().containsKey(MID), "a seed's cache error fails only that seed");
    aspectReads(0);
  }

  @Test
  public void readFailureFailsDependentSeedsNotTheWholeBatch() {
    hierarchy();
    when(cache.walkOrderedForwardAncestors(
            eq("container"), eq(GraphSnapshotSource.GRAPH), eq(LEAF.toString()), eq(50), any()))
        .thenReturn(AncestorWalkResult.fromAncestors(List.of(MID.toString(), ROOT.toString())));
    RuntimeException boom = new RuntimeException("pool exhausted");
    doThrow(boom).when(aspectRetriever).getLatestAspectObjects(any(), any(), any());

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(LEAF, DATASET_A), 50);

    assertEquals(batch.getParents().get(LEAF), List.of(MID, ROOT));
    assertSame(batch.getFailures().get(DATASET_A), boom);
  }

  @Test
  public void fallbackWalkFailureKeepsShorterFinishedChain() {
    hierarchy();
    parentByUrn.put(DATASET_C, SHALLOW); // SHALLOW is top-level
    failOn.add(MID); // LEAF's chain needs MID's read; SHALLOW's chain finished before it

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(
            opContext, spec, List.of(DATASET_A, DATASET_C), 50);

    assertEquals(batch.getParents().get(DATASET_C), List.of(SHALLOW));
    assertSame(batch.getFailures().get(DATASET_A), BOOM);
    assertTrue(!batch.getFailures().containsKey(DATASET_C));
  }

  /**
   * Dataset A -> Container A (root); Dataset B -> Container B -> Container C (root). Both cache
   * miss. The direct-parent read and the {A, B} container read succeed; Container C's read fails.
   * Dataset A's chain was already complete, so it must succeed; Dataset B must fail, not come back
   * as the partial [B, C].
   */
  @Test
  public void laterReadFailureKeepsChainCompletedInSameWalk() {
    final Urn containerA = UrnUtils.getUrn("urn:li:container:a");
    final Urn containerB = UrnUtils.getUrn("urn:li:container:b");
    final Urn containerC = UrnUtils.getUrn("urn:li:container:c");
    parentByUrn.put(DATASET_A, containerA);
    parentByUrn.put(DATASET_B, containerB);
    parentByUrn.put(containerB, containerC);
    failOn.add(containerC);

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(
            opContext, spec, List.of(DATASET_A, DATASET_B), 50);

    assertEquals(batch.getParents().get(DATASET_A), List.of(containerA));
    assertSame(batch.getFailures().get(DATASET_B), BOOM);
    assertTrue(!batch.getParents().containsKey(DATASET_B), "no partial [B, C] chain");
    // datasets -> {A, B} -> {C}: the failure is on the third read, after A's chain completed.
    List<Set<Urn>> reads = aspectReads(3);
    assertEquals(reads.get(1), Set.of(containerA, containerB));
    assertEquals(reads.get(2), Set.of(containerC));
  }

  @Test
  public void directReadFailureFailsOnlyItsEntityType() {
    hierarchy();
    failOn.add(CHART); // the chart read group fails; the dataset group does not

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(DATASET_A, CHART), 50);

    assertSame(batch.getFailures().get(CHART), BOOM);
    assertEquals(batch.getParents().get(DATASET_A), List.of(LEAF, MID, ROOT));
  }

  @Test
  public void zeroDepthReturnsEmptyChains() {
    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(DATASET_A), 0);
    assertEquals(batch.getParents().get(DATASET_A), List.of());
    assertTrue(batch.getFailures().isEmpty());
    aspectReads(0);
  }

  @Test
  public void unboundCacheIsNeverConsulted() {
    hierarchy();
    when(cache.bindingForKnownGraph(KnownEntityGraph.CONTAINER)).thenReturn(Optional.empty());

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(LEAF, DATASET_A), 50);

    assertEquals(batch.getParents().get(LEAF), List.of(MID, ROOT));
    assertEquals(batch.getParents().get(DATASET_A), List.of(LEAF, MID, ROOT));
    verify(cache, never())
        .walkOrderedForwardAncestors(anyString(), any(), anyString(), anyInt(), any());
  }

  @Test
  public void vertexSeedCacheMissFallsBackToAspects() {
    hierarchy();

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(LEAF), 50);

    assertEquals(batch.getParents().get(LEAF), List.of(MID, ROOT));
    // The seed is tried in the cache once (a miss); its ancestors are then read live.
    verify(cache, times(1))
        .walkOrderedForwardAncestors(anyString(), any(), eq(LEAF.toString()), anyInt(), any());
    verify(cache, never())
        .walkOrderedForwardAncestors(anyString(), any(), eq(MID.toString()), anyInt(), any());
  }

  @Test
  public void absentMissKeepsTryingTheCacheForOtherSeeds() {
    hierarchy();

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(LEAF, MID), 50);

    assertEquals(batch.getParents().get(LEAF), List.of(MID, ROOT));
    assertEquals(batch.getParents().get(MID), List.of(ROOT));
    // ABSENT only means "not in the graph"; the next seed may still hit.
    verify(cache, times(1))
        .walkOrderedForwardAncestors(anyString(), any(), eq(LEAF.toString()), anyInt(), any());
    verify(cache, times(1))
        .walkOrderedForwardAncestors(anyString(), any(), eq(MID.toString()), anyInt(), any());
  }

  @Test
  public void nonAbsentMissStopsUsingTheCacheForTheRestOfTheBatch() {
    hierarchy();
    when(cache.walkOrderedForwardAncestors(
            eq("container"), eq(GraphSnapshotSource.GRAPH), eq(LEAF.toString()), anyInt(), any()))
        .thenReturn(AncestorWalkResult.miss(ReadMissReason.STALE_BLOCKED));

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(LEAF, MID), 50);

    assertTrue(batch.getFailures().isEmpty());
    assertEquals(batch.getParents().get(LEAF), List.of(MID, ROOT));
    assertEquals(batch.getParents().get(MID), List.of(ROOT));
    verify(cache, never())
        .walkOrderedForwardAncestors(anyString(), any(), eq(MID.toString()), anyInt(), any());
  }

  @Test
  public void maxDepthOneReturnsOnlyTheDirectParent() {
    hierarchy();

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(DATASET_A), 1);

    assertEquals(batch.getParents().get(DATASET_A), List.of(LEAF));
    aspectReads(1);
    verify(cache, never())
        .walkOrderedForwardAncestors(anyString(), any(), anyString(), anyInt(), any());
  }

  @Test
  public void chainStopsWhereItCyclesBackToTheSeed() {
    parentByUrn.put(LEAF, MID);
    parentByUrn.put(MID, LEAF);

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(LEAF), 50);

    assertEquals(batch.getParents().get(LEAF), List.of(MID));
  }

  @Test
  public void seedTypeWithoutAParentAspectHasNoParents() {
    final Urn user = UrnUtils.getUrn("urn:li:corpuser:someone");

    BoundHierarchyAccess.BatchParents batch =
        BoundHierarchyAccess.orderedParentsBatch(opContext, spec, List.of(user), 50);

    assertEquals(batch.getParents().get(user), List.of());
    aspectReads(0);
  }

  @Test
  public void aspectBatchDeduplicatesSeedsAndHandlesEmptyReads() {
    hierarchy();

    AspectParentWalker.ChainLookup lookup =
        AspectParentWalker.orderedParentsBatch(opContext, spec, List.of(LEAF, LEAF), 10);
    assertEquals(lookup.getChains().size(), 1);
    assertEquals(lookup.getChains().get(LEAF), List.of(MID, ROOT));

    AspectParentWalker.ParentLookup empty =
        AspectParentWalker.batchParents(opContext, spec, Set.of());
    assertTrue(empty.getParents().isEmpty());
    assertTrue(empty.getFailures().isEmpty());
  }

  @Test
  public void aspectBatchDoesNotRetryAReadThatAlreadyFailed() {
    // MID is a seed whose read fails (container group); DATASET_C reaches MID one level later.
    parentByUrn.put(DATASET_C, MID);
    failOn.add(MID);

    AspectParentWalker.ChainLookup lookup =
        AspectParentWalker.orderedParentsBatch(opContext, spec, List.of(MID, DATASET_C), 10);

    assertSame(lookup.getFailures().get(MID), BOOM);
    assertSame(lookup.getFailures().get(DATASET_C), BOOM);
    assertTrue(lookup.getChains().isEmpty());
    // One read per group at the first level; MID's failed read is not repeated for DATASET_C.
    assertEquals(new java.util.HashSet<>(aspectReads(2)), Set.of(Set.of(MID), Set.of(DATASET_C)));
  }

  /**
   * Randomized parity guard: on random parent forests (with cycles, self-loops, mixed entity types
   * and every depth), the batch walk returns exactly what the single-seed {@code orderedParents}
   * returns for each seed, with the cache both unbound and always missing.
   */
  @Test
  public void batchMatchesSingleSeedWalkOnRandomForests() {
    final java.util.Random random = new java.util.Random(20261112L);
    final List<Urn> containers = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      containers.add(UrnUtils.getUrn("urn:li:container:c" + i));
    }
    final List<Urn> assets = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      assets.add(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,d" + i + ",PROD)"));
      assets.add(UrnUtils.getUrn("urn:li:chart:(looker,ch" + i + ")"));
    }
    final List<Urn> all = new ArrayList<>(containers);
    all.addAll(assets);

    for (int iteration = 0; iteration < 300; iteration++) {
      parentByUrn.clear();
      for (Urn urn : all) {
        // Parents are always containers; any container may point back up the chain or at itself.
        if (random.nextInt(10) < 8) {
          parentByUrn.put(urn, containers.get(random.nextInt(containers.size())));
        }
      }
      final List<Urn> seeds = new ArrayList<>();
      final int seedCount = 1 + random.nextInt(20);
      for (int i = 0; i < seedCount; i++) {
        seeds.add(all.get(random.nextInt(all.size())));
      }
      final int maxDepth = 1 + random.nextInt(12);
      final boolean cacheBound = random.nextBoolean();
      when(cache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
          .thenReturn(
              cacheBound
                  ? Optional.of(
                      EntityGraphBinding.builder()
                          .graphId("container")
                          .source(GraphSnapshotSource.GRAPH)
                          .build())
                  : Optional.empty());

      final BoundHierarchyAccess.BatchParents batch =
          BoundHierarchyAccess.orderedParentsBatch(opContext, spec, seeds, maxDepth);

      assertTrue(batch.getFailures().isEmpty(), "iteration " + iteration);
      for (Urn seed : seeds) {
        assertEquals(
            batch.getParents().get(seed),
            AspectParentWalker.orderedParents(opContext, spec, seed, maxDepth),
            "iteration " + iteration + " seed " + seed + " maxDepth " + maxDepth);
      }
    }
  }
}
