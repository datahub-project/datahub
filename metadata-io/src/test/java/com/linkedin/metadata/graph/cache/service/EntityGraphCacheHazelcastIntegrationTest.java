package com.linkedin.metadata.graph.cache.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBindings;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBounds;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.LocalEvictionLimits;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotSerializer;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import com.linkedin.metadata.graph.cache.store.EntityGraphOperationalStatus;
import com.linkedin.metadata.graph.cache.store.EntityGraphOperationalStatusSerializer;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityGraphCacheHazelcastIntegrationTest {

  private static final String FULL_GRAPH_ID = "domain";
  private static final String PARTIAL_GRAPH_ID = "domain-partial";
  private static final String ROOT = "urn:li:domain:root";
  private static final String CHILD = "urn:li:domain:child";
  private static final GraphSnapshotSource FULL_SOURCE = GraphSnapshotSource.SEARCH;
  private static final GraphSnapshotSource PARTIAL_SOURCE = GraphSnapshotSource.PRIMARY;
  private static final String FULL_CACHE_KEY =
      EntityGraphCacheKeys.fullCacheKey(FULL_GRAPH_ID, FULL_SOURCE);

  private HazelcastInstance hazelcast;
  private EntityGraphRegistry registry;
  private EntityGraphDistributedStore distributedStore;
  private EntityGraphLocalViewCache localViews;
  private EntityGraphSnapshotBuilder snapshotBuilder;
  private OperationContext systemOperationContext;
  private EntityGraphCacheService service;
  private EntityGraphDefinition fullDefinition;
  private EntityGraphDefinition partialDefinition;

  @BeforeMethod
  public void setUp() {
    Config config = new Config();
    config.setInstanceName("entity-graph-integration-" + java.util.UUID.randomUUID());
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

    fullDefinition = fullDefinition();
    partialDefinition = partialDefinition();

    registry = mock(EntityGraphRegistry.class);
    when(registry.hasFullScopeGraphs()).thenReturn(true);
    when(registry.getGraphsById())
        .thenReturn(Map.of(FULL_GRAPH_ID, fullDefinition, PARTIAL_GRAPH_ID, partialDefinition));
    when(registry.getDefinition(FULL_GRAPH_ID)).thenReturn(fullDefinition);
    when(registry.getDefinition(PARTIAL_GRAPH_ID)).thenReturn(partialDefinition);

    localViews = new EntityGraphLocalViewCache();
    distributedStore =
        new EntityGraphDistributedStore(
            hazelcast, registry, cacheKey -> localViews.evict(cacheKey));
    snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    systemOperationContext = TestOperationContexts.Builder.builder().buildSystemContext();

    EntityGraphCacheProperties properties =
        EntityGraphCacheProperties.builder().enabled(true).build();
    service =
        new EntityGraphCacheService(
            properties,
            registry,
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            Executors.newSingleThreadExecutor());
  }

  @AfterMethod
  public void tearDown() {
    if (hazelcast != null) {
      hazelcast.shutdown();
    }
  }

  @Test
  public void partialMarkerClearedOnEphemeralPublish() {
    String componentKey = "domain-partial@primary:componentfp";
    String markerKey =
        EntityGraphCacheKeys.partialFailureMarkerKey(PARTIAL_GRAPH_ID, PARTIAL_SOURCE, ROOT);
    distributedStore.markCooldown(markerKey);
    assertEquals(distributedStore.getStatus(markerKey), CacheStatus.COOLDOWN);

    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn(CHILD)
            .destinationUrn(ROOT)
            .relationshipType("IsPartOf")
            .build();
    TraversalCoverage coverage =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.REVERSE)
                    .explored(true)
                    .exploredDepth(1)
                    .configuredMaxDepth(15)
                    .complete(true)
                    .build())
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(PARTIAL_GRAPH_ID)
            .cacheKey(componentKey)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(coverage)
            .build();

    when(snapshotBuilder.buildPartial(
            eq(systemOperationContext),
            eq(partialDefinition),
            eq(PARTIAL_SOURCE),
            eq(Set.of(ROOT)),
            eq(TraversalDirection.REVERSE),
            isNull(),
            isNull(),
            isNull()))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());

    Set<String> expanded =
        expandEphemeral(
            service,
            PARTIAL_GRAPH_ID,
            PARTIAL_SOURCE,
            TraversalDirection.REVERSE,
            Set.of(ROOT),
            10);

    assertTrue(expanded.contains(ROOT));
    assertTrue(expanded.contains(CHILD));
    assertEquals(distributedStore.getStatus(componentKey), CacheStatus.ACTIVE);
    assertEquals(distributedStore.getStatus(markerKey), CacheStatus.ABSENT);
    assertEquals(distributedStore.getSnapshot(componentKey).getEdges().size(), 1);
  }

  @Test
  public void tombstoneExpandEmptyUntilCooldownExpiresAndRebuild() {
    EntityGraphSnapshot active = fullSnapshotWithEdge();
    distributedStore.publish(active, CacheStatus.ACTIVE);
    distributedStore.markCooldown(FULL_CACHE_KEY);
    assertEquals(distributedStore.getStatus(FULL_CACHE_KEY), CacheStatus.COOLDOWN);
    assertEquals(distributedStore.getSnapshot(FULL_CACHE_KEY).getEdges().size(), 0);

    Set<String> duringCooldown =
        expandCached(
            service, FULL_GRAPH_ID, FULL_SOURCE, TraversalDirection.REVERSE, Set.of(ROOT), 10);
    assertTrue(duringCooldown.isEmpty());
    Mockito.verify(snapshotBuilder, Mockito.never()).build(any(), any(), any(), any());

    expireCooldown(FULL_CACHE_KEY);

    EntityGraphSnapshot rebuilt = fullSnapshotWithEdge();
    when(snapshotBuilder.build(
            eq(systemOperationContext), eq(fullDefinition), eq(FULL_SOURCE), isNull()))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(rebuilt).build());

    Set<String> afterCooldown =
        expandCached(
            service, FULL_GRAPH_ID, FULL_SOURCE, TraversalDirection.REVERSE, Set.of(ROOT), 10);

    assertTrue(afterCooldown.contains(ROOT));
    assertTrue(afterCooldown.contains(CHILD));
    assertEquals(distributedStore.getStatus(FULL_CACHE_KEY), CacheStatus.ACTIVE);
    verify(snapshotBuilder)
        .build(eq(systemOperationContext), eq(fullDefinition), eq(FULL_SOURCE), isNull());
  }

  @Test
  public void fullExpandServesStaleActiveWhileBuilding() {
    EntityGraphSnapshot active = fullSnapshotWithEdge();
    distributedStore.publish(active, CacheStatus.ACTIVE);
    assertTrue(distributedStore.tryClaimRebuild(FULL_CACHE_KEY, 300_000L));
    assertEquals(distributedStore.getStatus(FULL_CACHE_KEY), CacheStatus.BUILDING);
    assertEquals(distributedStore.getSnapshot(FULL_CACHE_KEY).getEdges().size(), 1);

    Set<String> expanded =
        expandCached(
            service, FULL_GRAPH_ID, FULL_SOURCE, TraversalDirection.REVERSE, Set.of(ROOT), 10);

    assertTrue(expanded.contains(ROOT));
    assertTrue(expanded.contains(CHILD));
    Mockito.verify(snapshotBuilder, Mockito.never()).build(any(), any(), any(), any());
    assertEquals(distributedStore.getStatus(FULL_CACHE_KEY), CacheStatus.BUILDING);
  }

  @Test
  public void syncDeleteRemovesVertexDuringBuilding() {
    when(registry.getCandidateGraphIds("domain", "domainProperties"))
        .thenReturn(Set.of(FULL_GRAPH_ID));

    EntityGraphSnapshot active = fullSnapshotWithEdge();
    distributedStore.publish(active, CacheStatus.ACTIVE);
    assertTrue(distributedStore.tryClaimRebuild(FULL_CACHE_KEY, 300_000L));

    service.invalidateOnSyncBatch(
        com.linkedin.metadata.graph.cache.SyncGraphInvalidationBatch.builder()
            .delete(
                com.linkedin.metadata.graph.cache.SyncGraphInvalidationEntry.builder()
                    .entityUrn(CHILD)
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    EntityGraphSnapshot after = distributedStore.getSnapshot(FULL_CACHE_KEY);
    assertTrue(
        after == null
            || after.getEdges().stream().noneMatch(edge -> CHILD.equals(edge.getSourceUrn())),
        "Deleted vertex should be removed from snapshot during BUILDING");
  }

  @Test
  public void staleActiveExpandReturnsEmptyUnderBackgroundRebuild() {
    EntityGraphSnapshot active = staleFullSnapshot();
    distributedStore.publish(active, CacheStatus.ACTIVE);

    EntityGraphDefinition backgroundDefinition =
        EntityGraphDefinition.builder()
            .graphId(FULL_GRAPH_ID)
            .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .rebuildExecution(
                com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution
                    .BACKGROUND)
            .buildSource(FULL_SOURCE)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .enabled(true)
            .build();
    when(registry.getDefinition(FULL_GRAPH_ID)).thenReturn(backgroundDefinition);

    Set<String> expanded =
        expandCached(
            service, FULL_GRAPH_ID, FULL_SOURCE, TraversalDirection.REVERSE, Set.of(ROOT), 10);

    assertTrue(expanded.isEmpty());
  }

  @Test
  public void backgroundRebuildPublishesAfterColdExpand() throws Exception {
    EntityGraphDefinition backgroundDefinition =
        EntityGraphDefinition.builder()
            .graphId(FULL_GRAPH_ID)
            .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .rebuildExecution(
                com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution
                    .BACKGROUND)
            .buildSource(FULL_SOURCE)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .enabled(true)
            .build();
    when(registry.getDefinition(FULL_GRAPH_ID)).thenReturn(backgroundDefinition);

    EntityGraphSnapshot rebuilt = fullSnapshotWithEdge();
    when(snapshotBuilder.build(
            eq(systemOperationContext), eq(backgroundDefinition), eq(FULL_SOURCE), isNull()))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(rebuilt).build());

    DeferredExecutorService deferredExecutor = new DeferredExecutorService();
    EntityGraphCacheService backgroundService =
        new EntityGraphCacheService(
            EntityGraphCacheProperties.builder().enabled(true).build(),
            registry,
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            deferredExecutor);

    Set<String> cold =
        expandCached(
            backgroundService,
            FULL_GRAPH_ID,
            FULL_SOURCE,
            TraversalDirection.REVERSE,
            Set.of(ROOT),
            10);
    assertTrue(cold.isEmpty());

    deferredExecutor.drain();

    for (int i = 0;
        i < 50 && distributedStore.getStatus(FULL_CACHE_KEY) != CacheStatus.ACTIVE;
        i++) {
      Thread.sleep(100);
    }

    Set<String> warm =
        expandCached(
            backgroundService,
            FULL_GRAPH_ID,
            FULL_SOURCE,
            TraversalDirection.REVERSE,
            Set.of(ROOT),
            10);
    assertTrue(warm.contains(ROOT));
    assertTrue(warm.contains(CHILD));
    assertEquals(distributedStore.getStatus(FULL_CACHE_KEY), CacheStatus.ACTIVE);
  }

  private Set<String> expandCached(
      EntityGraphCacheService service,
      String graphId,
      GraphSnapshotSource source,
      TraversalDirection direction,
      Collection<String> roots,
      int limit) {
    return service
        .expand(
            graphId,
            source,
            direction,
            roots,
            limit,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED)
        .verticesOrEmpty();
  }

  private Set<String> expandEphemeral(
      EntityGraphCacheService service,
      String graphId,
      GraphSnapshotSource source,
      TraversalDirection direction,
      Collection<String> roots,
      int limit) {
    return service
        .expand(
            graphId,
            source,
            direction,
            roots,
            limit,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.EPHEMERAL)
        .verticesOrEmpty();
  }

  private void expireCooldown(String cacheKey) {
    IMap<String, EntityGraphOperationalStatus> statusMap =
        hazelcast.getMap(EntityGraphCacheProperties.STATUS_MAP);
    statusMap.put(
        cacheKey,
        EntityGraphOperationalStatus.of(
            CacheStatus.COOLDOWN, System.currentTimeMillis() - 400_000L));
  }

  private static EntityGraphSnapshot fullSnapshotWithEdge() {
    return EntityGraphSnapshot.builder()
        .graphId(FULL_GRAPH_ID)
        .cacheKey(FULL_CACHE_KEY)
        .generation(0L)
        .buildSource("search")
        .builtAtMillis(System.currentTimeMillis())
        .vertexCount(2)
        .edgeCount(1)
        .topologyFingerprint("fp")
        .traversalCoverage(TraversalCoverage.fullComplete())
        .edges(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(CHILD)
                    .destinationUrn(ROOT)
                    .relationshipType("IsPartOf")
                    .build()))
        .build();
  }

  private static EntityGraphSnapshot staleFullSnapshot() {
    return EntityGraphSnapshot.builder()
        .graphId(FULL_GRAPH_ID)
        .cacheKey(FULL_CACHE_KEY)
        .generation(0L)
        .buildSource("search")
        .builtAtMillis(System.currentTimeMillis() - 400_000L)
        .vertexCount(2)
        .edgeCount(1)
        .topologyFingerprint("fp")
        .traversalCoverage(TraversalCoverage.fullComplete())
        .edges(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(CHILD)
                    .destinationUrn(ROOT)
                    .relationshipType("IsPartOf")
                    .build()))
        .build();
  }

  private static EntityGraphDefinition fullDefinition() {
    return EntityGraphDefinition.builder()
        .graphId(FULL_GRAPH_ID)
        .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
        .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
        .bindings(GraphBindings.builder().build())
        .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
        .populationIntervalSeconds(300)
        .buildSource(FULL_SOURCE)
        .localEviction(
            LocalEvictionLimits.builder().enabled(true).maxViews(8).maxEstimatedBytes(1024).build())
        .enabled(true)
        .build();
  }

  private static EntityGraphDefinition partialDefinition() {
    return EntityGraphDefinition.builder()
        .graphId(PARTIAL_GRAPH_ID)
        .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
        .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
        .bindings(GraphBindings.builder().build())
        .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
        .populationIntervalSeconds(300)
        .buildSource(PARTIAL_SOURCE)
        .localEviction(
            LocalEvictionLimits.builder().enabled(true).maxViews(8).maxEstimatedBytes(1024).build())
        .enabled(true)
        .build();
  }

  /**
   * Queues submitted tasks until {@link #drain()} so background rebuild timing is deterministic in
   * tests.
   */
  private static final class DeferredExecutorService extends AbstractExecutorService {
    private final Queue<Runnable> pending = new ArrayDeque<>();
    private volatile boolean shutdown;

    @Override
    public void execute(Runnable command) {
      if (shutdown) {
        throw new RejectedExecutionException();
      }
      synchronized (pending) {
        pending.add(command);
      }
    }

    void drain() {
      Runnable task;
      while ((task = poll()) != null) {
        task.run();
      }
    }

    private Runnable poll() {
      synchronized (pending) {
        return pending.poll();
      }
    }

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown();
      synchronized (pending) {
        return List.copyOf(pending);
      }
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown && pending.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return pending.isEmpty();
    }
  }
}
