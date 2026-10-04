package com.linkedin.metadata.graph.cache.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.FullWalkEdge;
import com.linkedin.metadata.graph.cache.FullWalkPublishResult;
import com.linkedin.metadata.graph.cache.FullWalkWriteBack;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBindings;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBounds;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.LocalEvictionLimits;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotSerializer;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import com.linkedin.metadata.graph.cache.store.EntityGraphOperationalStatus;
import com.linkedin.metadata.graph.cache.store.EntityGraphOperationalStatusSerializer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityGraphFullWalkWriteBackTest {

  private static final String GLOSSARY = "glossary";
  private static final String TIGHT = "glossary-tight";
  private static final String FULL = "domain";
  private static final GraphSnapshotSource SOURCE = GraphSnapshotSource.GRAPH;
  private static final String PARENT = "urn:li:glossaryNode:parent";
  private static final String SEED = "urn:li:glossaryNode:seed";
  private static final String CHILD = "urn:li:glossaryTerm:child";
  private static final String GRANDCHILD = "urn:li:glossaryTerm:grandchild";
  private static final String BRANCH = "urn:li:glossaryNode:branch";

  private HazelcastInstance hazelcast;
  private EntityGraphRegistry registry;
  private EntityGraphDistributedStore store;
  private EntityGraphLocalViewCache localViews;
  private EntityGraphSnapshotBuilder snapshotBuilder;
  private EntityGraphCacheService service;
  private SimpleMeterRegistry meterRegistry;
  private ExecutorService rebuildExecutor;
  private EntityGraphDefinition glossary;

  @BeforeMethod
  public void setUp() {
    Config config = new Config();
    config.setInstanceName("entity-graph-full-walk-" + java.util.UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config
        .getSerializationConfig()
        .addSerializerConfig(
            new SerializerConfig()
                .setTypeClass(EntityGraphSnapshot.class)
                .setImplementation(new EntityGraphSnapshotSerializer()))
        .addSerializerConfig(
            new SerializerConfig()
                .setTypeClass(EntityGraphOperationalStatus.class)
                .setImplementation(new EntityGraphOperationalStatusSerializer()));
    hazelcast = Hazelcast.newHazelcastInstance(config);

    glossary = partialDefinition(GLOSSARY, 1, 100, 100);
    EntityGraphDefinition tight = partialDefinition(TIGHT, 5, 2, 1);
    EntityGraphDefinition full =
        EntityGraphDefinition.builder()
            .graphId(FULL)
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(100)).build())
            .bindings(GraphBindings.builder().build())
            .populationIntervalSeconds(3600)
            .buildSource(GraphSnapshotSource.SEARCH)
            .localEviction(localEviction())
            .enabled(true)
            .build();

    registry = mock(EntityGraphRegistry.class);
    when(registry.hasFullScopeGraphs()).thenReturn(true);
    when(registry.getGraphsById()).thenReturn(Map.of(GLOSSARY, glossary, TIGHT, tight, FULL, full));
    when(registry.getDefinition(GLOSSARY)).thenReturn(glossary);
    when(registry.getDefinition(TIGHT)).thenReturn(tight);
    when(registry.getDefinition(FULL)).thenReturn(full);

    localViews = new EntityGraphLocalViewCache();
    store =
        new EntityGraphDistributedStore(
            hazelcast, registry, cacheKey -> localViews.evict(cacheKey));
    snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();
    OperationContext operationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () ->
                    SystemTelemetryContext.builder()
                        .metricUtils(metricUtils)
                        .tracer(OpenTelemetry.noop().getTracer("test"))
                        .build())
            .buildSystemContext();
    rebuildExecutor = Executors.newSingleThreadExecutor();
    service =
        new EntityGraphCacheService(
            EntityGraphCacheProperties.builder().enabled(true).build(),
            registry,
            store,
            localViews,
            snapshotBuilder,
            operationContext,
            rebuildExecutor);
  }

  @AfterMethod
  public void tearDown() {
    if (rebuildExecutor != null) {
      rebuildExecutor.shutdownNow();
    }
    if (hazelcast != null) {
      hazelcast.shutdown();
    }
  }

  @Test
  public void fullPathExpandHitsAfterPublishOnThisPodAndAnother() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "fp-initial");
    long builtAt = System.currentTimeMillis();
    store.publish(initialSnapshot(cacheKey, builtAt), CacheStatus.ACTIVE);

    FullWalkPublishResult published =
        service.publishFullWalk(
            writeBack(
                GLOSSARY,
                Set.of(SEED),
                List.of(walkEdge(CHILD, SEED), walkEdge(GRANDCHILD, CHILD)),
                false,
                0L));

    assertEquals(published, FullWalkPublishResult.PUBLISHED);
    EntityGraphSnapshot updated = store.getSnapshot(cacheKey);
    assertEquals(updated.getCacheKey(), cacheKey);
    assertTrue(updated.getBuiltAtMillis() >= builtAt);
    assertEquals(updated.getGeneration(), 2L);
    assertEquals(store.getInvalidationGeneration(GLOSSARY), 0L);
    assertEquals(store.getStatus(cacheKey), CacheStatus.ACTIVE);
    assertTrue(hasEdge(updated, SEED, PARENT));
    assertTrue(hasEdge(updated, BRANCH, PARENT));
    assertTrue(hasEdge(updated, CHILD, SEED));
    assertTrue(hasEdge(updated, GRANDCHILD, CHILD));
    assertFalse(hasEdge(updated, GRANDCHILD, SEED));
    DirectionCoverage reverse =
        updated.getTraversalCoverage().getDirection(TraversalDirection.REVERSE);
    assertTrue(reverse.isTrustedFullWalk());
    assertEquals(reverse.getTrustedSeeds(), List.of(SEED));
    assertEquals(reverse.getExploredDepth(), 1);
    assertEquals(reverse.getConfiguredMaxDepth(), 1);
    assertFalse(updated.getTraversalCoverage().isTrustedFullWalk(TraversalDirection.FORWARD));
    assertEquals(
        updated.getTraversalCoverage().getDirection(TraversalDirection.FORWARD).getExploredDepth(),
        1);

    Set<String> fullPath =
        vertices(expand(service, true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH));
    assertTrue(fullPath.contains(SEED));
    assertTrue(fullPath.contains(CHILD));
    assertTrue(fullPath.contains(GRANDCHILD));
    assertFalse(fullPath.contains(PARENT));
    Set<String> ordinary =
        vertices(expand(service, false, EntityGraphCache.USE_DEFINITION_MAX_DEPTH));
    assertTrue(ordinary.contains(CHILD));
    assertFalse(ordinary.contains(GRANDCHILD));
    assertMiss(
        expandRoots(Set.of(PARENT), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
        ReadMissReason.INSUFFICIENT_COVERAGE);
    assertTrue(
        vertices(expandRoots(Set.of(CHILD), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH))
            .contains(GRANDCHILD));

    Set<String> oneLevel = vertices(expand(service, true, 1));
    assertTrue(oneLevel.contains(CHILD));
    assertFalse(oneLevel.contains(GRANDCHILD));

    EntityGraphCacheService otherPod = serviceOnFreshStore();
    Set<String> otherHit =
        vertices(expand(otherPod, true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH));
    assertTrue(otherHit.contains(GRANDCHILD));
    verify(snapshotBuilder, never())
        .buildPartial(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void reparentWalkDoesNotTrustLeftoverDescendants() {
    String oldParent = "urn:li:glossaryNode:oldParent";
    String newParent = "urn:li:glossaryNode:newParent";
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "reparent");
    store.publish(
        snapshot(
            cacheKey,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(CHILD, oldParent), directed(GRANDCHILD, CHILD)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false)),
        CacheStatus.ACTIVE);
    assertEquals(
        service.publishFullWalk(
            writeBack(GLOSSARY, Set.of(newParent), List.of(walkEdge(CHILD, newParent)), false, 0L)),
        FullWalkPublishResult.PUBLISHED);

    Set<String> fullPath =
        vertices(expandRoots(Set.of(newParent), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH));
    assertTrue(fullPath.contains(CHILD));
    assertFalse(fullPath.contains(GRANDCHILD));
    Set<String> ordinaryChild =
        vertices(expandRoots(Set.of(CHILD), false, EntityGraphCache.USE_DEFINITION_MAX_DEPTH));
    assertTrue(ordinaryChild.contains(GRANDCHILD));
  }

  @Test
  public void builderCompleteSnapshotMissesFullPathAndStaysClamped() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "fp-builder");
    store.publish(
        snapshot(
            cacheKey,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(CHILD, SEED), directed(GRANDCHILD, CHILD)),
            directionCoverage(TraversalDirection.REVERSE, 2, 1, true, false),
            directionCoverage(TraversalDirection.FORWARD, 1, 1, true, false)),
        CacheStatus.ACTIVE);

    GraphReadResult fullPath = expand(service, true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH);
    assertTrue(fullPath.isMiss());
    assertEquals(((GraphReadResult.Miss) fullPath).reason(), ReadMissReason.INSUFFICIENT_COVERAGE);
    Set<String> bounded = vertices(expand(service, true, 1));
    assertTrue(bounded.contains(CHILD));
    assertFalse(bounded.contains(GRANDCHILD));
    assertMiss(expand(service, true, 3), ReadMissReason.INSUFFICIENT_COVERAGE);

    Set<String> clamped =
        vertices(expand(service, false, EntityGraphCache.USE_DEFINITION_MAX_DEPTH));
    assertTrue(clamped.contains(CHILD));
    assertFalse(clamped.contains(GRANDCHILD));
    verify(snapshotBuilder, never())
        .buildPartial(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void boundsAtCapPublishAndOneOverLeavesTheSnapshot() {
    FullWalkPublishResult atCap =
        service.publishFullWalk(
            writeBack(TIGHT, Set.of(SEED), List.of(walkEdge(CHILD, SEED)), false, 0L));
    assertEquals(atCap, FullWalkPublishResult.PUBLISHED);
    String cacheKey = store.findCacheKeyForSeeds(TIGHT, SOURCE, Set.of(SEED)).orElseThrow();
    EntityGraphSnapshot published = store.getSnapshot(cacheKey);
    assertEquals(published.getVertexCount(), 2);
    assertEquals(published.getEdgeCount(), 1);
    assertEquals(published.getGeneration(), 1L);
    assertNotEquals(store.getStatus(cacheKey), CacheStatus.OVER_LIMIT);

    FullWalkPublishResult over =
        service.publishFullWalk(
            writeBack(
                TIGHT,
                Set.of(SEED),
                List.of(walkEdge(CHILD, SEED), walkEdge(GRANDCHILD, CHILD)),
                false,
                0L));
    assertEquals(over, FullWalkPublishResult.REJECTED_BOUNDS);
    EntityGraphSnapshot unchanged = store.getSnapshot(cacheKey);
    assertEquals(unchanged.getGeneration(), 1L);
    assertEquals(unchanged.getEdgeCount(), 1);
    assertEquals(store.getStatus(cacheKey), CacheStatus.ACTIVE);
    assertEquals(suppressed("rejected_bounds"), 1.0);
  }

  @Test
  public void refusesSignalTruncationFullScopeSplitGenerationAndEmptyCreate() {
    assertEquals(
        service.publishFullWalk(
            writeBack(GLOSSARY, Set.of(), List.of(walkEdge(CHILD, SEED)), false, 0L)),
        FullWalkPublishResult.REJECTED_SIGNAL);
    assertEquals(
        service.publishFullWalk(
            writeBack(GLOSSARY, Set.of(SEED), List.of(walkEdge(CHILD, SEED)), true, 0L)),
        FullWalkPublishResult.REJECTED_TRUNCATED);
    assertEquals(
        service.publishFullWalk(
            writeBack(FULL, Set.of(SEED), List.of(walkEdge(CHILD, SEED)), false, 0L)),
        FullWalkPublishResult.REJECTED_NOT_PARTIAL);
    assertEquals(
        service.publishFullWalk(writeBack(GLOSSARY, Set.of(SEED), List.of(), false, 0L)),
        FullWalkPublishResult.REJECTED_EMPTY_CREATE);

    String left = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "left");
    String right = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "right");
    store.publish(
        snapshot(
            left,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(CHILD, SEED)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false)),
        CacheStatus.ACTIVE);
    store.publish(
        snapshot(
            right,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(GRANDCHILD, PARENT)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false)),
        CacheStatus.ACTIVE);
    assertEquals(
        service.publishFullWalk(
            writeBack(GLOSSARY, Set.of(SEED, PARENT), List.of(walkEdge(CHILD, SEED)), false, 0L)),
        FullWalkPublishResult.REJECTED_SPLIT_COMPONENTS);
    assertEquals(store.getSnapshot(left).getGeneration(), 1L);

    assertEquals(
        service.publishFullWalk(
            writeBack(GLOSSARY, Set.of(SEED), List.of(walkEdge(CHILD, SEED)), false, 9L)),
        FullWalkPublishResult.REJECTED_GENERATION);
    assertEquals(store.getSnapshot(left).getGeneration(), 1L);

    long generation = store.getInvalidationGeneration(GLOSSARY);
    store.dropPartialGraph(GLOSSARY);
    assertEquals(
        service.publishFullWalk(
            writeBack(GLOSSARY, Set.of(SEED), List.of(walkEdge(CHILD, SEED)), false, generation)),
        FullWalkPublishResult.REJECTED_GENERATION);
    assertEquals(store.findCacheKeyForSeeds(GLOSSARY, SOURCE, Set.of(SEED)).isPresent(), false);
  }

  @Test
  public void zeroEdgeEditKeepsAnExistingSeed() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "up-only");
    store.publish(
        snapshot(
            cacheKey,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(SEED, PARENT)),
            directionCoverage(TraversalDirection.REVERSE, 0, 1, true, false)),
        CacheStatus.ACTIVE);

    assertEquals(
        service.publishFullWalk(writeBack(GLOSSARY, Set.of(SEED), List.of(), false, 0L)),
        FullWalkPublishResult.PUBLISHED);
    assertTrue(hasEdge(store.getSnapshot(cacheKey), SEED, PARENT));
    GraphReadResult expanded = expand(service, true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH);
    assertTrue(expanded instanceof GraphReadResult.EmptyHit);
  }

  @Test
  public void freshTrustedSnapshotSkipsSameFingerprintAndStaleOnePublishes() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "trusted");
    store.publish(initialSnapshot(cacheKey, System.currentTimeMillis()), CacheStatus.ACTIVE);
    service.publishFullWalk(
        writeBack(
            GLOSSARY,
            Set.of(SEED),
            List.of(walkEdge(CHILD, SEED), walkEdge(GRANDCHILD, CHILD)),
            false,
            0L));
    EntityGraphSnapshot trusted = store.getSnapshot(cacheKey);
    assertTrue(
        store.shouldSkipPublish(cacheKey, candidate(trusted, trusted.getBuiltAtMillis(), false)));
    assertTrue(
        store.shouldSkipPublish(cacheKey, candidate(trusted, trusted.getBuiltAtMillis(), true)));
    assertFalse(
        store.shouldSkipPublish(
            cacheKey, candidate(trusted, trusted.getBuiltAtMillis(), true, true)));

    String staleSeed = "urn:li:glossaryNode:staleSeed";
    String staleChild = "urn:li:glossaryTerm:staleChild";
    String staleGrandchild = "urn:li:glossaryTerm:staleGrandchild";
    String staleKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "stale");
    store.publish(
        snapshot(
            staleKey,
            GLOSSARY,
            0L,
            List.of(directed(staleChild, staleSeed), directed(staleGrandchild, staleSeed)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false),
            directionCoverage(TraversalDirection.FORWARD, 1, 1, true, false)),
        CacheStatus.ACTIVE);
    assertEquals(
        service.publishFullWalk(
            writeBack(
                GLOSSARY,
                Set.of(staleSeed),
                List.of(walkEdge(staleChild, staleSeed), walkEdge(staleGrandchild, staleChild)),
                false,
                0L)),
        FullWalkPublishResult.PUBLISHED);
    EntityGraphSnapshot refreshed = store.getSnapshot(staleKey);
    assertTrue(refreshed.getBuiltAtMillis() > 0L);
    assertTrue(
        vertices(expandRoots(Set.of(staleSeed), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH))
            .contains(staleGrandchild));

    String stillStaleKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "still-stale");
    store.publish(
        snapshot(
            stillStaleKey,
            GLOSSARY,
            0L,
            List.of(directed(staleChild, staleSeed), directed(staleGrandchild, staleChild)),
            directionCoverage(TraversalDirection.REVERSE, 2, 1, true, true),
            directionCoverage(TraversalDirection.FORWARD, 1, 1, true, false)),
        CacheStatus.ACTIVE);
    EntityGraphSnapshot stillStale = store.getSnapshot(stillStaleKey);
    assertFalse(store.shouldSkipPublish(stillStaleKey, candidate(stillStale, 0L, false)));
    assertTrue(store.tryClaimRebuild(stillStaleKey, 60_000L));
    assertFalse(store.shouldSkipPublish(stillStaleKey, candidate(stillStale, 0L, true)));
  }

  @Test
  public void fullPathMissesWhenTheSeedIsAbsentOrTheComponentIsStale() {
    assertMiss(
        expandRoots(
            Set.of("urn:li:glossaryNode:missing"), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
        ReadMissReason.ABSENT);

    String unreadSeed = "urn:li:glossaryNode:staleUnread";
    store.publish(
        snapshot(
            EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "stale-unread"),
            GLOSSARY,
            0L,
            List.of(directed("urn:li:glossaryTerm:staleUnreadChild", unreadSeed)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false)),
        CacheStatus.ACTIVE);
    assertMiss(
        expandRoots(Set.of(unreadSeed), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
        ReadMissReason.STALE_BLOCKED);

    String trustedSeed = "urn:li:glossaryNode:staleTrusted";
    store.publish(
        snapshot(
            EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "stale-trusted"),
            GLOSSARY,
            0L,
            List.of(directed("urn:li:glossaryTerm:staleTrustedChild", trustedSeed)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, true)),
        CacheStatus.ACTIVE);
    assertMiss(
        expandRoots(Set.of(trustedSeed), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
        ReadMissReason.STALE_BLOCKED);
    verifyNoInteractions(snapshotBuilder);
  }

  @Test
  public void fullPathMissesWhenSeedsResolveToDifferentComponents() {
    String left = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "read-left");
    String right = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "read-right");
    store.publish(
        snapshot(
            left,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(CHILD, SEED)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, true)),
        CacheStatus.ACTIVE);
    store.publish(
        snapshot(
            right,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(GRANDCHILD, PARENT)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, true)),
        CacheStatus.ACTIVE);

    assertMiss(
        expandRoots(Set.of(SEED, PARENT), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
        ReadMissReason.INSUFFICIENT_COVERAGE);
    assertEquals(store.getSnapshot(left).getGeneration(), 1L);
    assertEquals(store.getSnapshot(right).getGeneration(), 1L);
    verifyNoInteractions(snapshotBuilder);
  }

  @Test
  public void tombstonedComponentIsAbsentOnFullPath() {
    for (CacheStatus failure :
        List.of(CacheStatus.OVER_LIMIT, CacheStatus.COOLDOWN, CacheStatus.INVALID)) {
      String seed = "urn:li:glossaryNode:" + failure.name();
      String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, failure.name());
      store.publish(
          snapshot(
              cacheKey,
              GLOSSARY,
              System.currentTimeMillis(),
              List.of(directed("urn:li:glossaryTerm:child-" + failure.name(), seed)),
              directionCoverage(TraversalDirection.REVERSE, 1, 1, true, true)),
          CacheStatus.ACTIVE);
      switch (failure) {
        case OVER_LIMIT -> store.markOverLimit(cacheKey);
        case COOLDOWN -> store.markCooldown(cacheKey);
        case INVALID -> store.markInvalid(cacheKey);
        default -> throw new IllegalStateException(failure.name());
      }
      assertMiss(
          expandRoots(Set.of(seed), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
          ReadMissReason.ABSENT);
      assertEquals(store.getStatus(cacheKey), failure);
    }
    verifyNoInteractions(snapshotBuilder);
  }

  @Test
  public void buildingLeaseDoesNotHideAFreshTrustedWalk() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "building");
    store.publish(initialSnapshot(cacheKey, System.currentTimeMillis()), CacheStatus.ACTIVE);
    assertEquals(
        service.publishFullWalk(
            writeBack(
                GLOSSARY,
                Set.of(SEED),
                List.of(walkEdge(CHILD, SEED), walkEdge(GRANDCHILD, CHILD)),
                false,
                0L)),
        FullWalkPublishResult.PUBLISHED);

    assertTrue(store.tryClaimRebuild(cacheKey, 60_000L));
    assertEquals(store.getStatus(cacheKey), CacheStatus.BUILDING);
    assertTrue(
        vertices(expand(service, true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH))
            .contains(GRANDCHILD));
    EntityGraphSnapshot trusted = store.getSnapshot(cacheKey);
    assertTrue(
        store.shouldSkipPublish(cacheKey, candidate(trusted, trusted.getBuiltAtMillis(), false)));
    assertTrue(
        store.shouldSkipPublish(cacheKey, candidate(trusted, trusted.getBuiltAtMillis(), true)));
  }

  @Test
  public void nestedSeedsKeepTheAncestorWalkOnAFullPathExpand() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "nested-seeds");
    store.publish(initialSnapshot(cacheKey, System.currentTimeMillis()), CacheStatus.ACTIVE);
    assertEquals(
        service.publishFullWalk(
            writeBack(
                GLOSSARY,
                Set.of(SEED, CHILD),
                List.of(walkEdge(CHILD, SEED), walkEdge(GRANDCHILD, CHILD)),
                false,
                0L)),
        FullWalkPublishResult.PUBLISHED);

    DirectionCoverage reverse =
        store.getSnapshot(cacheKey).getTraversalCoverage().getDirection(TraversalDirection.REVERSE);
    assertEquals(reverse.getExploredDepth(), 1);
    assertTrue(
        vertices(expandRoots(Set.of(SEED), true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH))
            .contains(GRANDCHILD));
  }

  @Test
  public void shorterWalkDoesNotTruncateExpandOfOtherRoots() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "short-walk");
    store.publish(initialSnapshot(cacheKey, System.currentTimeMillis()), CacheStatus.ACTIVE);
    assertEquals(
        service.publishFullWalk(writeBack(GLOSSARY, Set.of(GRANDCHILD), List.of(), false, 0L)),
        FullWalkPublishResult.PUBLISHED);

    assertEquals(
        store
            .getSnapshot(cacheKey)
            .getTraversalCoverage()
            .getDirection(TraversalDirection.REVERSE)
            .getExploredDepth(),
        1);
    assertTrue(
        vertices(expandRoots(Set.of(SEED), false, EntityGraphCache.USE_DEFINITION_MAX_DEPTH))
            .contains(CHILD));
  }

  @Test
  public void trustedExpandUsesCallerDepthUpToTheWalkedDepth() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "depth");
    store.publish(initialSnapshot(cacheKey, System.currentTimeMillis()), CacheStatus.ACTIVE);
    service.publishFullWalk(
        writeBack(
            GLOSSARY,
            Set.of(SEED),
            List.of(walkEdge(CHILD, SEED), walkEdge(GRANDCHILD, CHILD)),
            false,
            0L));

    assertTrue(vertices(expand(service, true, 5)).contains(GRANDCHILD));
    assertFalse(vertices(expand(service, true, 1)).contains(GRANDCHILD));
    assertTrue(vertices(expand(service, true, 0)).contains(GRANDCHILD));
  }

  @Test
  public void forwardWalkStampsOnlyTheForwardDirection() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "forward");
    store.publish(initialSnapshot(cacheKey, System.currentTimeMillis()), CacheStatus.ACTIVE);
    assertEquals(
        service.publishFullWalk(
            writeBack(
                GLOSSARY,
                TraversalDirection.FORWARD,
                Set.of(SEED),
                List.of(walkEdge(SEED, PARENT)),
                false,
                0L)),
        FullWalkPublishResult.PUBLISHED);

    EntityGraphSnapshot updated = store.getSnapshot(cacheKey);
    assertTrue(updated.getTraversalCoverage().isTrustedFullWalk(TraversalDirection.FORWARD));
    assertEquals(
        updated.getTraversalCoverage().getDirection(TraversalDirection.FORWARD).getExploredDepth(),
        1);
    assertFalse(updated.getTraversalCoverage().isTrustedFullWalk(TraversalDirection.REVERSE));
    assertTrue(hasEdge(updated, CHILD, SEED));
    assertTrue(hasEdge(updated, SEED, PARENT));

    Set<String> forward =
        vertices(
            service.expand(
                GLOSSARY,
                SOURCE,
                TraversalDirection.FORWARD,
                Set.of(SEED),
                100,
                EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
                ReadMode.CACHED,
                true));
    assertTrue(forward.contains(PARENT));
    assertFalse(forward.contains(CHILD));
    assertMiss(
        expand(service, true, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
        ReadMissReason.INSUFFICIENT_COVERAGE);
  }

  @Test
  public void newSeedJoinsTheExistingComponentOnlyWhenTheWalkContainsIt() {
    String extra = "urn:li:glossaryTerm:extra";
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "join");
    store.publish(
        snapshot(
            cacheKey,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(CHILD, SEED)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false)),
        CacheStatus.ACTIVE);

    assertEquals(
        service.publishFullWalk(
            writeBack(GLOSSARY, Set.of(SEED, extra), List.of(walkEdge(CHILD, SEED)), false, 0L)),
        FullWalkPublishResult.REJECTED_EMPTY_CREATE);
    assertEquals(store.getSnapshot(cacheKey).getGeneration(), 1L);
    assertFalse(hasEdge(store.getSnapshot(cacheKey), extra, SEED));

    assertEquals(
        service.publishFullWalk(
            writeBack(
                GLOSSARY,
                Set.of(SEED, extra),
                List.of(walkEdge(CHILD, SEED), walkEdge(extra, SEED)),
                false,
                0L)),
        FullWalkPublishResult.PUBLISHED);
    EntityGraphSnapshot updated = store.getSnapshot(cacheKey);
    assertEquals(updated.getGeneration(), 2L);
    assertTrue(hasEdge(updated, extra, SEED));
    assertTrue(hasEdge(updated, CHILD, SEED));
    assertTrue(
        vertices(
                service.expand(
                    GLOSSARY,
                    SOURCE,
                    TraversalDirection.REVERSE,
                    Set.of(SEED, extra),
                    100,
                    EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
                    ReadMode.CACHED,
                    true))
            .contains(extra));
  }

  @Test
  public void disabledCacheAndUnknownGraphRefuseWithoutWriting() {
    EntityGraphCacheService disabled =
        new EntityGraphCacheService(
            EntityGraphCacheProperties.builder().enabled(false).build(),
            registry,
            store,
            localViews,
            snapshotBuilder,
            TestOperationContexts.Builder.builder()
                .systemTelemetryContextSupplier(
                    () ->
                        SystemTelemetryContext.builder()
                            .metricUtils(MetricUtils.builder().registry(meterRegistry).build())
                            .tracer(OpenTelemetry.noop().getTracer("test"))
                            .build())
                .buildSystemContext(),
            rebuildExecutor);
    assertEquals(
        disabled.publishFullWalk(
            writeBack(GLOSSARY, Set.of(SEED), List.of(walkEdge(CHILD, SEED)), false, 0L)),
        FullWalkPublishResult.REJECTED_DISABLED);
    assertEquals(suppressed("rejected_disabled"), 1.0);
    assertMiss(
        disabled.expand(
            GLOSSARY,
            SOURCE,
            TraversalDirection.REVERSE,
            Set.of(SEED),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED,
            true),
        ReadMissReason.DISABLED);

    assertEquals(
        service.publishFullWalk(
            writeBack("no-such-graph", Set.of(SEED), List.of(walkEdge(CHILD, SEED)), false, 0L)),
        FullWalkPublishResult.REJECTED_NOT_PARTIAL);
    assertEquals(suppressed("rejected_not_partial"), 1.0);
    assertMiss(
        service.expand(
            "no-such-graph",
            SOURCE,
            TraversalDirection.REVERSE,
            Set.of(SEED),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED,
            true),
        ReadMissReason.INVALID_REQUEST);
    assertMiss(
        service.expand(
            GLOSSARY,
            SOURCE,
            TraversalDirection.REVERSE,
            Set.of(),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED,
            true),
        ReadMissReason.INVALID_REQUEST);
    verifyNoInteractions(snapshotBuilder);
  }

  @Test
  public void skipPublishFollowsFreshnessWhenTheTrustedStampIsKept() {
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "keep-stamp");
    store.publish(
        snapshot(
            cacheKey,
            GLOSSARY,
            0L,
            List.of(directed(CHILD, SEED), directed(GRANDCHILD, CHILD)),
            directionCoverage(TraversalDirection.REVERSE, 2, 1, true, true)),
        CacheStatus.ACTIVE);
    EntityGraphSnapshot stale = store.getSnapshot(cacheKey);
    assertTrue(
        store.shouldSkipPublish(
            cacheKey,
            copySnapshot(stale, directionCoverage(TraversalDirection.REVERSE, 2, 1, true, true))));
    assertFalse(store.shouldSkipPublish(cacheKey, copySnapshot(stale, null)));
    assertFalse(
        store.shouldSkipPublish(
            EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "never-published"),
            copySnapshot(stale, directionCoverage(TraversalDirection.REVERSE, 2, 1, true, true))));
  }

  @Test
  public void overlappingVertexJoinsTheExistingComponent() {
    String other = "urn:li:glossaryNode:other";
    String cacheKey = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "overlap");
    store.publish(
        snapshot(
            cacheKey,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(CHILD, SEED)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false)),
        CacheStatus.ACTIVE);

    assertEquals(
        service.publishFullWalk(
            writeBack(GLOSSARY, Set.of(other), List.of(walkEdge(CHILD, other)), false, 0L)),
        FullWalkPublishResult.PUBLISHED);
    assertEquals(
        store.findCacheKeyForSeeds(GLOSSARY, SOURCE, Set.of(other)).orElseThrow(), cacheKey);
    assertTrue(hasEdge(store.getSnapshot(cacheKey), CHILD, other));
    assertEquals(store.getSnapshot(cacheKey).getGeneration(), 2L);
  }

  @Test
  public void disconnectedOrCrossComponentWalkIsRejected() {
    assertEquals(
        service.publishFullWalk(
            writeBack(
                GLOSSARY,
                Set.of(SEED),
                List.of(walkEdge(CHILD, SEED), walkEdge(GRANDCHILD, PARENT)),
                false,
                0L)),
        FullWalkPublishResult.REJECTED_SPLIT_COMPONENTS);
    assertTrue(store.findCacheKeyForSeeds(GLOSSARY, SOURCE, Set.of(SEED)).isEmpty());

    String left = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "edge-left");
    String right = EntityGraphCacheKeys.componentCacheKey(GLOSSARY, SOURCE, "edge-right");
    store.publish(
        snapshot(
            left,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(CHILD, SEED)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false)),
        CacheStatus.ACTIVE);
    store.publish(
        snapshot(
            right,
            GLOSSARY,
            System.currentTimeMillis(),
            List.of(directed(GRANDCHILD, PARENT)),
            directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false)),
        CacheStatus.ACTIVE);
    assertEquals(
        service.publishFullWalk(
            writeBack(
                GLOSSARY,
                Set.of(SEED),
                List.of(walkEdge(CHILD, SEED), walkEdge(GRANDCHILD, SEED)),
                false,
                0L)),
        FullWalkPublishResult.REJECTED_SPLIT_COMPONENTS);
    assertEquals(store.getSnapshot(left).getGeneration(), 1L);
    assertEquals(store.getSnapshot(right).getGeneration(), 1L);
  }

  @Test
  public void mismatchedBuildSourceIsRejected() {
    assertEquals(
        service.publishFullWalk(
            FullWalkWriteBack.builder()
                .graphId(GLOSSARY)
                .source(GraphSnapshotSource.SEARCH)
                .direction(TraversalDirection.REVERSE)
                .seeds(Set.of(SEED))
                .edges(List.of(walkEdge(CHILD, SEED)))
                .build()),
        FullWalkPublishResult.REJECTED_SOURCE);
    assertEquals(suppressed("rejected_source"), 1.0);
  }

  private EntityGraphCacheService serviceOnFreshStore() {
    EntityGraphLocalViewCache otherViews = new EntityGraphLocalViewCache();
    EntityGraphDistributedStore otherStore =
        new EntityGraphDistributedStore(
            hazelcast, registry, cacheKey -> otherViews.evict(cacheKey));
    return new EntityGraphCacheService(
        EntityGraphCacheProperties.builder().enabled(true).build(),
        registry,
        otherStore,
        otherViews,
        snapshotBuilder,
        TestOperationContexts.Builder.builder().buildSystemContext(),
        rebuildExecutor);
  }

  private GraphReadResult expand(
      EntityGraphCacheService target, boolean requireFullPath, int maxDepth) {
    return expandRoots(target, Set.of(SEED), requireFullPath, maxDepth);
  }

  private GraphReadResult expandRoots(Set<String> roots, boolean requireFullPath, int maxDepth) {
    return expandRoots(service, roots, requireFullPath, maxDepth);
  }

  private static GraphReadResult expandRoots(
      EntityGraphCacheService target, Set<String> roots, boolean requireFullPath, int maxDepth) {
    return target.expand(
        GLOSSARY,
        SOURCE,
        TraversalDirection.REVERSE,
        roots,
        100,
        maxDepth,
        ReadMode.CACHED,
        requireFullPath);
  }

  private static void assertMiss(GraphReadResult result, ReadMissReason reason) {
    assertTrue(result.isMiss(), String.valueOf(result));
    assertEquals(((GraphReadResult.Miss) result).reason(), reason);
  }

  private static Set<String> vertices(GraphReadResult result) {
    assertTrue(result.isHit(), String.valueOf(result));
    return result.verticesOrEmpty();
  }

  private double suppressed(String reason) {
    return meterRegistry
        .get("entity.graph.cache.full_walk_publish_suppressed")
        .tag("reason", reason)
        .counter()
        .count();
  }

  private EntityGraphSnapshot initialSnapshot(String cacheKey, long builtAt) {
    return snapshot(
        cacheKey,
        GLOSSARY,
        builtAt,
        List.of(
            directed(SEED, PARENT),
            directed(BRANCH, PARENT),
            directed(CHILD, SEED),
            directed(GRANDCHILD, SEED)),
        directionCoverage(TraversalDirection.REVERSE, 1, 1, true, false),
        directionCoverage(TraversalDirection.FORWARD, 1, 1, true, false));
  }

  private static EntityGraphSnapshot candidate(
      EntityGraphSnapshot existing, long builtAt, boolean differentFingerprint) {
    return candidate(existing, builtAt, differentFingerprint, false);
  }

  private static EntityGraphSnapshot candidate(
      EntityGraphSnapshot existing, long builtAt, boolean differentFingerprint, boolean trusted) {
    return EntityGraphSnapshot.builder()
        .graphId(existing.getGraphId())
        .cacheKey(existing.getCacheKey())
        .generation(existing.getGeneration())
        .buildSource(existing.getBuildSource())
        .builtAtMillis(builtAt)
        .edges(existing.getEdges())
        .vertexCount(existing.getVertexCount())
        .edgeCount(existing.getEdgeCount())
        .topologyFingerprint(
            differentFingerprint
                ? existing.getTopologyFingerprint() + "-other"
                : existing.getTopologyFingerprint())
        .traversalCoverage(
            TraversalCoverage.builder()
                .direction(directionCoverage(TraversalDirection.REVERSE, 1, 1, true, trusted))
                .direction(existing.getTraversalCoverage().getDirection(TraversalDirection.FORWARD))
                .build())
        .cacheStatus(CacheStatus.ACTIVE.name())
        .build();
  }

  private static EntityGraphSnapshot snapshot(
      String cacheKey,
      String graphId,
      long builtAt,
      List<DirectedEdge> edges,
      DirectionCoverage... directions) {
    TraversalCoverage.TraversalCoverageBuilder coverage = TraversalCoverage.builder();
    for (DirectionCoverage direction : directions) {
      coverage.direction(direction);
    }
    return EntityGraphSnapshot.builder()
        .graphId(graphId)
        .cacheKey(cacheKey)
        .buildSource(SOURCE.name().toLowerCase(java.util.Locale.ROOT))
        .builtAtMillis(builtAt)
        .edges(edges)
        .vertexCount(4)
        .edgeCount(edges.size())
        .topologyFingerprint(cacheKey)
        .traversalCoverage(coverage.build())
        .build();
  }

  private static DirectionCoverage directionCoverage(
      TraversalDirection direction,
      int exploredDepth,
      int configuredMaxDepth,
      boolean complete,
      boolean trusted) {
    return DirectionCoverage.builder()
        .direction(direction)
        .explored(true)
        .exploredDepth(exploredDepth)
        .configuredMaxDepth(configuredMaxDepth)
        .complete(complete)
        .trustedSeeds(trusted ? List.of("urn:li:glossaryNode:trusted") : List.of())
        .build();
  }

  private static FullWalkWriteBack writeBack(
      String graphId,
      Set<String> seeds,
      List<FullWalkEdge> edges,
      boolean truncated,
      long generation) {
    return writeBack(graphId, TraversalDirection.REVERSE, seeds, edges, truncated, generation);
  }

  private static FullWalkWriteBack writeBack(
      String graphId,
      TraversalDirection direction,
      Set<String> seeds,
      List<FullWalkEdge> edges,
      boolean truncated,
      long generation) {
    return FullWalkWriteBack.builder()
        .graphId(graphId)
        .source(SOURCE)
        .direction(direction)
        .seeds(seeds)
        .edges(edges)
        .truncated(truncated)
        .invalidationGenerationAtStart(generation)
        .build();
  }

  private static EntityGraphSnapshot copySnapshot(
      EntityGraphSnapshot existing, DirectionCoverage direction) {
    TraversalCoverage coverage =
        direction == null ? null : TraversalCoverage.builder().direction(direction).build();
    return EntityGraphSnapshot.builder()
        .graphId(existing.getGraphId())
        .cacheKey(existing.getCacheKey())
        .generation(existing.getGeneration())
        .buildSource(existing.getBuildSource())
        .builtAtMillis(existing.getBuiltAtMillis())
        .edges(existing.getEdges())
        .vertexCount(existing.getVertexCount())
        .edgeCount(existing.getEdgeCount())
        .topologyFingerprint(existing.getTopologyFingerprint())
        .traversalCoverage(coverage)
        .cacheStatus(CacheStatus.ACTIVE.name())
        .build();
  }

  private static DirectedEdge directed(String source, String destination) {
    return DirectedEdge.builder()
        .sourceUrn(source)
        .destinationUrn(destination)
        .relationshipType("IsPartOf")
        .build();
  }

  private static FullWalkEdge walkEdge(String source, String destination) {
    return FullWalkEdge.builder()
        .sourceUrn(source)
        .destinationUrn(destination)
        .relationshipType("IsPartOf")
        .build();
  }

  private static boolean hasEdge(EntityGraphSnapshot snapshot, String source, String destination) {
    return snapshot.getEdges().stream()
        .anyMatch(
            edge ->
                source.equals(edge.getSourceUrn()) && destination.equals(edge.getDestinationUrn()));
  }

  private static EntityGraphDefinition partialDefinition(
      String graphId, int maxDepth, int maxVertices, int maxEdges) {
    return EntityGraphDefinition.builder()
        .graphId(graphId)
        .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(maxDepth).build())
        .bounds(
            GraphBounds.builder()
                .maxVertices(maxVertices)
                .maxEdges(OptionalInt.of(maxEdges))
                .build())
        .bindings(GraphBindings.builder().build())
        .populationIntervalSeconds(3600)
        .buildSource(SOURCE)
        .localEviction(localEviction())
        .enabled(true)
        .build();
  }

  private static LocalEvictionLimits localEviction() {
    return LocalEvictionLimits.builder().enabled(true).maxViews(8).maxEstimatedBytes(1024).build();
  }
}
