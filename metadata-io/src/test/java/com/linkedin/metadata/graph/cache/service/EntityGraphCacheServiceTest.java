package com.linkedin.metadata.graph.cache.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationBatch;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationEntry;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBindings;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBounds;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphEdgeTriplet;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.LocalEvictionLimits;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityGraphCacheServiceTest {

  private static final String GRAPH_ID = "domain";
  private static final GraphSnapshotSource SOURCE = GraphSnapshotSource.PRIMARY;
  private static final String CACHE_KEY =
      EntityGraphCacheKeys.fullCacheKey(GRAPH_ID, GraphSnapshotSource.PRIMARY);

  private EntityGraphCacheProperties properties;
  private EntityGraphRegistry registry;
  private EntityGraphDistributedStore distributedStore;
  private EntityGraphLocalViewCache localViews;
  private EntityGraphSnapshotBuilder snapshotBuilder;
  private OperationContext systemOperationContext;
  private EntityGraphCacheService service;
  private EntityGraphDefinition definition;
  private ExecutorService rebuildExecutor;

  @BeforeMethod
  public void setUp() {
    rebuildExecutor = Executors.newSingleThreadExecutor();
    properties = EntityGraphCacheProperties.builder().enabled(true).build();
    registry = mock(EntityGraphRegistry.class);
    distributedStore = mock(EntityGraphDistributedStore.class);
    localViews = mock(EntityGraphLocalViewCache.class);
    snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    systemOperationContext = TestOperationContexts.Builder.builder().buildSystemContext();

    definition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .buildSource(GraphSnapshotSource.PRIMARY)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .enabled(true)
            .build();

    when(registry.getDefinition(GRAPH_ID)).thenReturn(definition);
    when(distributedStore.tryClaimRebuild(any(), anyLong())).thenReturn(true);
    when(distributedStore.getInvalidationGeneration(any())).thenReturn(0L);

    service =
        new EntityGraphCacheService(
            properties,
            registry,
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            rebuildExecutor);
  }

  @Test
  public void expandSkipsRebuildWhenCooldownNotExpired() {
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.COOLDOWN);
    when(distributedStore.getCooldownRecordedAt(CACHE_KEY))
        .thenReturn(Optional.of(System.currentTimeMillis()));

    expandCached(
        service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of("urn:li:domain:root"), 10);

    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
  }

  @Test
  public void expandSkipsRebuildWhenClaimLost() {
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.BUILDING);
    when(distributedStore.tryClaimRebuild(eq(CACHE_KEY), anyLong())).thenReturn(false);

    expandCached(
        service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of("urn:li:domain:root"), 10);

    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
  }

  @Test
  public void rebuildPublishSuppressedWhenInvalidationGenerationChanges() {
    String root = "urn:li:domain:root";
    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ABSENT);
    when(distributedStore.getInvalidationGeneration(GRAPH_ID)).thenReturn(0L, 1L);
    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());

    expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    verify(distributedStore, never()).publish(any(), any());
    verify(distributedStore).releaseRebuildClaim(CACHE_KEY);
  }

  @Test
  public void expandRetriesRebuildWhenCooldownExpired() {
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.COOLDOWN);
    when(distributedStore.getCooldownRecordedAt(CACHE_KEY))
        .thenReturn(Optional.of(System.currentTimeMillis() - 400_000L));
    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.COOLDOWN)
                .failureReason("primary_failed")
                .build());

    expandCached(
        service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of("urn:li:domain:root"), 10);

    verify(snapshotBuilder).build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null));
  }

  @Test
  public void expandSkipsRebuildWhenOverLimit() {
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.OVER_LIMIT);

    expandCached(
        service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of("urn:li:domain:root"), 10);

    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
  }

  @Test
  public void expandSkipsRebuildWhenInvalid() {
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.INVALID);

    expandCached(
        service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of("urn:li:domain:root"), 10);

    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
  }

  @Test
  public void expandHonorsPerCallMaxDepthOnActiveSnapshot() {
    String root = "urn:li:domain:root";
    String child = "urn:li:domain:child";
    String grandchild = "urn:li:domain:grandchild";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(child)
                .destinationUrn(root)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(grandchild)
                .destinationUrn(child)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(1L)
            .buildSource("primary")
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(3)
            .edgeCount(2)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(1L);
    when(localViews.get(CACHE_KEY, 1L)).thenReturn(Optional.of(new EntityGraphView(edges)));

    Set<String> depthOne =
        expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 100, 1);
    assertTrue(depthOne.contains(root));
    assertTrue(depthOne.contains(child));
    assertFalse(depthOne.contains(grandchild));

    Set<String> depthTwo =
        expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 100, 2);
    assertTrue(depthTwo.contains(grandchild));
  }

  @Test
  public void expandReturnsHitForPresentSeedsWhenSomeSeedsMissingFromFullSnapshot() {
    String root = "urn:li:domain:root";
    String child = "urn:li:domain:child";
    String missing = "urn:li:domain:missing";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(child)
                .destinationUrn(root)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(1L)
            .buildSource("primary")
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(1L);
    when(localViews.get(CACHE_KEY, 1L)).thenReturn(Optional.of(new EntityGraphView(edges)));

    Set<String> expanded =
        expandCached(
            service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root, missing), 100);

    assertTrue(expanded.contains(root));
    assertTrue(expanded.contains(child));
    assertFalse(expanded.contains(missing));
  }

  @Test
  public void scheduledRebuildRunsWhenSnapshotActive() {
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY))
        .thenReturn(
            EntityGraphSnapshot.builder()
                .graphId(GRAPH_ID)
                .cacheKey(CACHE_KEY)
                .generation(1L)
                .buildSource("primary")
                .builtAtMillis(System.currentTimeMillis())
                .edges(List.of())
                .vertexCount(0)
                .edgeCount(0)
                .topologyFingerprint("fp")
                .traversalCoverage(TraversalCoverage.fullComplete())
                .cacheStatus(CacheStatus.ACTIVE.name())
                .build());
    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.COOLDOWN)
                .failureReason("search_failed")
                .build());

    service.scheduledRebuild(definition);

    verify(snapshotBuilder).build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null));
  }

  @Test
  public void scheduledRebuildSkipsWhenCooldownNotExpired() {
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.COOLDOWN);
    when(distributedStore.getCooldownRecordedAt(CACHE_KEY))
        .thenReturn(Optional.of(System.currentTimeMillis()));

    service.scheduledRebuild(definition);

    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
  }

  @Test
  public void invalidateOnSyncBatchCreateDropsFullGraph() {
    String childUrn = "urn:li:domain:child";
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GRAPH_ID, "domainProperties"))
        .thenReturn(Set.of("IsPartOf"));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .create(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(childUrn)
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore).dropGraph(GRAPH_ID);
    verify(localViews).evictGraph(GRAPH_ID);
  }

  @Test
  public void invalidateOnSyncBatchCreateDropsPartialGraph() {
    EntityGraphDefinition partialDefinition = partialDefinition();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialDefinition);
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GRAPH_ID, "domainProperties"))
        .thenReturn(Set.of("IsPartOf"));
    when(distributedStore.anySnapshotStatusForGraph(GRAPH_ID)).thenReturn(CacheStatus.ACTIVE);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .create(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn("urn:li:domain:child")
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore).dropPartialGraph(GRAPH_ID);
    verify(localViews).evictGraph(GRAPH_ID);
  }

  @Test
  public void invalidateOnSyncBatchCreateAlwaysDropsFullGraph() {
    String urn = "urn:li:domain:missing";
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GRAPH_ID, "domainProperties"))
        .thenReturn(Set.of("IsPartOf"));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .create(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(urn)
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore).dropGraph(GRAPH_ID);
    verify(localViews).evictGraph(GRAPH_ID);
  }

  @Test
  public void invalidateOnSyncBatchUpdateAlwaysDropsFullGraph() {
    String urn = "urn:li:domain:child";
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GRAPH_ID, "domainProperties"))
        .thenReturn(Set.of("IsPartOf"));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .update(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(urn)
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore).dropGraph(GRAPH_ID);
    verify(localViews).evictGraph(GRAPH_ID);
  }

  @Test
  public void invalidateOnSyncBatchUpdateDropsPartialGraph() {
    String urn = "urn:li:domain:child";
    EntityGraphDefinition partialDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(definition.getResolvedEdges())
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(2).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .buildSource(GraphSnapshotSource.PRIMARY)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .enabled(true)
            .build();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialDefinition);
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GRAPH_ID, "domainProperties"))
        .thenReturn(Set.of("IsPartOf"));
    when(distributedStore.anySnapshotStatusForGraph(GRAPH_ID)).thenReturn(CacheStatus.ACTIVE);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .update(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(urn)
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore).dropPartialGraph(GRAPH_ID);
    verify(localViews).evictGraph(GRAPH_ID);
    verify(distributedStore, never()).dropGraph(any());
  }

  @Test
  public void invalidateOnSyncBatchDeleteRemovesVertex() {
    String urn = "urn:li:domain:child";
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.removeVertexFromSnapshot(GRAPH_ID, urn, 100)).thenReturn(true);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .delete(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(urn)
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore).removeVertexFromSnapshot(GRAPH_ID, urn, 100);
    verify(localViews).evictGraph(GRAPH_ID);
    verify(distributedStore, never()).dropGraph(any());
  }

  @Test
  public void invalidateOnSyncBatchDeleteRemovesVertexDuringBuilding() {
    String urn = "urn:li:domain:child";
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.BUILDING);
    when(distributedStore.removeVertexFromSnapshot(GRAPH_ID, urn, 100)).thenReturn(true);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .delete(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(urn)
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore).removeVertexFromSnapshot(GRAPH_ID, urn, 100);
    verify(localViews).evictGraph(GRAPH_ID);
  }

  @Test
  public void invalidateOnSyncBatchDeleteDropsPartialGraph() {
    String urn = "urn:li:domain:child";
    EntityGraphDefinition partialDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(definition.getResolvedEdges())
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(2).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .buildSource(GraphSnapshotSource.PRIMARY)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .enabled(true)
            .build();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialDefinition);
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(distributedStore.anySnapshotStatusForGraph(GRAPH_ID)).thenReturn(CacheStatus.ACTIVE);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .delete(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(urn)
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore).dropPartialGraph(GRAPH_ID);
    verify(localViews).evictGraph(GRAPH_ID);
    verify(distributedStore, never()).removeVertexFromSnapshot(any(), any(), anyInt());
    verify(distributedStore, never()).dropGraph(any());
  }

  @Test
  public void invalidateOnSyncBatchDeleteRemovesVertexForEntityDelete() {
    String urn = "urn:li:domain:child";
    ResolvedGraphEdge edge =
        ResolvedGraphEdge.builder()
            .triplet(
                GraphEdgeTriplet.builder()
                    .sourceEntityType("domain")
                    .destinationEntityType("domain")
                    .relationshipType("IsPartOf")
                    .build())
            .aspectName("domainProperties")
            .build();
    EntityGraphDefinition domainDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(List.of(edge))
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .buildSource(GraphSnapshotSource.PRIMARY)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .enabled(true)
            .build();
    when(registry.getGraphsById()).thenReturn(Map.of(GRAPH_ID, domainDefinition));
    when(registry.getGraphIdsForEntityType("domain")).thenReturn(Set.of(GRAPH_ID));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.removeVertexFromSnapshot(GRAPH_ID, urn, 100)).thenReturn(true);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .delete(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(urn)
                    .entityType("domain")
                    .aspectName(null)
                    .build())
            .build());

    verify(distributedStore).removeVertexFromSnapshot(GRAPH_ID, urn, 100);
    verify(localViews).evictGraph(GRAPH_ID);
    verify(registry, never()).getCandidateGraphIds(any(), any());
  }

  @Test
  public void invalidateOnSyncBatchUpdateRemovesVertexForStatusAspect() {
    String urn = "urn:li:domain:child";
    ResolvedGraphEdge edge =
        ResolvedGraphEdge.builder()
            .triplet(
                GraphEdgeTriplet.builder()
                    .sourceEntityType("domain")
                    .destinationEntityType("domain")
                    .relationshipType("IsPartOf")
                    .build())
            .aspectName("domainProperties")
            .build();
    EntityGraphDefinition domainDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(List.of(edge))
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .buildSource(GraphSnapshotSource.PRIMARY)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .enabled(true)
            .build();
    when(registry.getGraphsById()).thenReturn(Map.of(GRAPH_ID, domainDefinition));
    when(registry.getDefinition(GRAPH_ID)).thenReturn(domainDefinition);
    when(registry.getGraphIdsForEntityType("domain")).thenReturn(Set.of(GRAPH_ID));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.removeVertexFromSnapshot(GRAPH_ID, urn, 100)).thenReturn(true);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .update(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn(urn)
                    .entityType("domain")
                    .aspectName("status")
                    .build())
            .build());

    verify(distributedStore).removeVertexFromSnapshot(GRAPH_ID, urn, 100);
    verify(localViews).evictGraph(GRAPH_ID);
    verify(registry, never()).getCandidateGraphIds(any(), any());
  }

  @Test
  public void invalidateOnSyncBatchCreateSkipsOverLimit() {
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GRAPH_ID, "domainProperties"))
        .thenReturn(Set.of("IsPartOf"));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.OVER_LIMIT);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .create(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn("urn:li:domain:new")
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore, never()).dropGraph(any());
  }

  @Test
  public void invalidateOnSyncBatchSkipsWhenAspectMapsToNoRelationships() {
    when(registry.getCandidateGraphIds("domain", "domainProperties")).thenReturn(Set.of(GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GRAPH_ID, "domainProperties")).thenReturn(Set.of());

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .create(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn("urn:li:domain:child")
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(distributedStore, never()).dropGraph(any());
    verify(localViews, never()).evictGraph(any());
  }

  @Test
  public void invalidateOnSyncBatchNoOpWhenCacheDisabled() {
    properties.setEnabled(false);

    service.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .create(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn("urn:li:domain:x")
                    .entityType("domain")
                    .aspectName("domainProperties")
                    .build())
            .build());

    verify(registry, never()).getCandidateGraphIds(any(), any());
    verify(distributedStore, never()).dropGraph(any());
  }

  @Test
  public void partialShouldSkipPublishReleasesMarkerLease() {
    String root = "urn:li:domain:root";
    String componentKey = "domain@primary:componentfp";
    String markerKey = EntityGraphCacheKeys.partialFailureMarkerKey(GRAPH_ID, SOURCE, root);
    EntityGraphDefinition partialDefinition = partialDefinition();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialDefinition);

    when(distributedStore.findCacheKeyForSeeds(eq(GRAPH_ID), eq(SOURCE), eq(Set.of(root)), any()))
        .thenReturn(Optional.empty());
    when(distributedStore.getStatus(markerKey)).thenReturn(CacheStatus.ABSENT);

    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(componentKey)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();

    when(snapshotBuilder.buildPartial(
            eq(systemOperationContext),
            eq(partialDefinition),
            eq(SOURCE),
            eq(Set.of(root)),
            eq(TraversalDirection.REVERSE),
            isNull(),
            isNull(),
            isNull()))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.shouldSkipPublish(componentKey, snapshot)).thenReturn(true);

    expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    verify(distributedStore).releaseRebuildClaim(markerKey);
    verify(distributedStore, never()).publish(any(), any());
  }

  @Test
  public void partialExpandFailsClosedWhenCoverageMissing() {
    String root = "urn:li:domain:root";
    String componentKey = "domain@primary:componentfp";
    EntityGraphDefinition partialDefinition = partialDefinition();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialDefinition);

    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(componentKey)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(null)
            .build();

    when(distributedStore.findCacheKeyForSeeds(eq(GRAPH_ID), eq(SOURCE), eq(Set.of(root)), any()))
        .thenReturn(Optional.of(componentKey));
    when(distributedStore.getStatus(componentKey)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(componentKey)).thenReturn(snapshot);
    when(distributedStore.getGeneration(componentKey)).thenReturn(1L);
    when(snapshotBuilder.buildPartial(
            eq(systemOperationContext),
            eq(partialDefinition),
            eq(SOURCE),
            eq(Set.of(root)),
            eq(TraversalDirection.FORWARD),
            eq(snapshot.getEdges()),
            isNull(),
            eq(componentKey)))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.COOLDOWN)
                .failureReason("scroll_incomplete")
                .build());
    when(localViews.get(componentKey, 1L))
        .thenReturn(Optional.of(new EntityGraphView(snapshot.getEdges())));

    Set<String> expanded =
        expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.FORWARD, Set.of(root), 10);

    assertTrue(expanded.isEmpty());
    verify(snapshotBuilder)
        .buildPartial(
            eq(systemOperationContext),
            eq(partialDefinition),
            eq(SOURCE),
            eq(Set.of(root)),
            eq(TraversalDirection.FORWARD),
            eq(snapshot.getEdges()),
            isNull(),
            eq(componentKey));
  }

  @Test
  public void partialExpandFailsClosedWhenCoverageStillIncompleteAfterRebuild() {
    String root = "urn:li:domain:root";
    String componentKey = "domain@primary:componentfp";
    EntityGraphDefinition partialDefinition = partialDefinition();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialDefinition);

    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    TraversalCoverage ancestorsOnly =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.FORWARD)
                    .explored(true)
                    .exploredDepth(1)
                    .configuredMaxDepth(15)
                    .complete(true)
                    .build())
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(componentKey)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(ancestorsOnly)
            .build();

    when(distributedStore.findCacheKeyForSeeds(eq(GRAPH_ID), eq(SOURCE), eq(Set.of(root)), any()))
        .thenReturn(Optional.of(componentKey));
    when(distributedStore.getStatus(componentKey)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(componentKey)).thenReturn(snapshot);
    when(distributedStore.getGeneration(componentKey)).thenReturn(1L);
    when(distributedStore.shouldSkipPublish(eq(componentKey), any())).thenReturn(false);
    when(snapshotBuilder.buildPartial(
            eq(systemOperationContext),
            eq(partialDefinition),
            eq(SOURCE),
            eq(Set.of(root)),
            eq(TraversalDirection.REVERSE),
            eq(snapshot.getEdges()),
            eq(ancestorsOnly),
            eq(componentKey)))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(localViews.get(componentKey, 1L))
        .thenReturn(Optional.of(new EntityGraphView(snapshot.getEdges())));

    Set<String> expanded =
        expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    assertTrue(expanded.isEmpty());
    verify(snapshotBuilder)
        .buildPartial(
            eq(systemOperationContext),
            eq(partialDefinition),
            eq(SOURCE),
            eq(Set.of(root)),
            eq(TraversalDirection.REVERSE),
            eq(snapshot.getEdges()),
            eq(ancestorsOnly),
            eq(componentKey));
  }

  @Test
  public void partialMissRebuildsInParallelWithoutWaiting() {
    String root = "urn:li:domain:root";
    String componentKey = "domain@primary:componentfp";
    String markerKey = EntityGraphCacheKeys.partialFailureMarkerKey(GRAPH_ID, SOURCE, root);
    EntityGraphDefinition partialDefinition = partialDefinition();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialDefinition);

    when(distributedStore.findCacheKeyForSeeds(eq(GRAPH_ID), eq(SOURCE), eq(Set.of(root)), any()))
        .thenReturn(Optional.empty());
    when(distributedStore.getStatus(markerKey)).thenReturn(CacheStatus.BUILDING);

    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
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
            .graphId(GRAPH_ID)
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
            eq(SOURCE),
            eq(Set.of(root)),
            eq(TraversalDirection.REVERSE),
            isNull(),
            isNull(),
            isNull()))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.shouldSkipPublish(componentKey, snapshot)).thenReturn(false);
    when(distributedStore.getStatus(componentKey)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(componentKey)).thenReturn(snapshot);
    when(distributedStore.getGeneration(componentKey)).thenReturn(1L);
    when(localViews.get(componentKey, 1L))
        .thenReturn(Optional.of(new EntityGraphView(snapshot.getEdges())));

    expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    verify(snapshotBuilder)
        .buildPartial(
            eq(systemOperationContext),
            eq(partialDefinition),
            eq(SOURCE),
            eq(Set.of(root)),
            eq(TraversalDirection.REVERSE),
            isNull(),
            isNull(),
            isNull());
    verify(distributedStore).publish(snapshot, CacheStatus.ACTIVE);
    verify(distributedStore).releaseRebuildClaim(markerKey);
    verify(distributedStore).dropFailureMarkerKeysForRoots(GRAPH_ID, SOURCE, Set.of(root));
  }

  @Test
  public void expandEphemeralNoOpWhenCacheDisabled() {
    properties.setEnabled(false);

    Set<String> expanded =
        expandEphemeral(
            service,
            GRAPH_ID,
            SOURCE,
            TraversalDirection.REVERSE,
            Set.of("urn:li:domain:root"),
            10);

    assertTrue(expanded.isEmpty());
    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
  }

  @Test
  public void expandEphemeralSkipsPublishWhenClaimLost() {
    String root = "urn:li:domain:root";
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:child")
                        .destinationUrn(root)
                        .relationshipType("IsPartOf")
                        .build()))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();

    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.BUILDING);
    when(distributedStore.tryClaimRebuild(eq(CACHE_KEY), anyLong())).thenReturn(false);

    expandEphemeral(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    verify(distributedStore, never()).publish(any(), any());
  }

  @Test
  public void expandEphemeralPublishesWhenCacheMissing() {
    String root = "urn:li:domain:root";
    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    TraversalCoverage coverage = TraversalCoverage.fullComplete();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(coverage)
            .build();

    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.BUILDING);
    when(distributedStore.shouldSkipPublish(CACHE_KEY, snapshot)).thenReturn(false);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(1L);

    Set<String> expanded =
        expandEphemeral(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    assertTrue(expanded.contains(root));
    assertTrue(expanded.contains("urn:li:domain:child"));
    verify(distributedStore).publish(snapshot, CacheStatus.ACTIVE);
  }

  @Test
  public void rebuildReleasesLeaseWhenBuildThrows() {
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ABSENT);
    when(distributedStore.tryClaimRebuild(eq(CACHE_KEY), anyLong())).thenReturn(true);
    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenThrow(new RuntimeException("build failed"));

    expectThrows(
        RuntimeException.class,
        () ->
            expandCached(
                service,
                GRAPH_ID,
                SOURCE,
                TraversalDirection.REVERSE,
                Set.of("urn:li:domain:root"),
                10));

    verify(distributedStore).markCooldown(CACHE_KEY);
  }

  @Test
  public void warmPublishReleasesLeaseWhenPublishThrows() {
    String root = "urn:li:domain:root";
    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();

    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ABSENT);
    when(distributedStore.tryClaimRebuild(eq(CACHE_KEY), anyLong())).thenReturn(true);
    doThrow(new RuntimeException("publish failed"))
        .when(distributedStore)
        .publish(any(), eq(CacheStatus.ACTIVE));

    expectThrows(
        RuntimeException.class,
        () ->
            expandEphemeral(
                service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10));

    verify(distributedStore).releaseRebuildClaim(CACHE_KEY);
  }

  @Test
  public void expandEphemeralSkipsPublishWhenCacheFresh() {
    String root = "urn:li:domain:root";
    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    TraversalCoverage coverage = TraversalCoverage.fullComplete();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(coverage)
            .build();
    EntityGraphSnapshot existing =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(coverage)
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(existing);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(0L);

    Set<String> expanded =
        expandEphemeral(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    assertTrue(expanded.contains(root));
    assertTrue(expanded.contains("urn:li:domain:child"));
    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
    verify(distributedStore, never()).publish(any(), any());
  }

  @Test
  public void expandEphemeralSkipsWarmPublishWhenCacheInCooldown() {
    String root = "urn:li:domain:root";
    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();

    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.COOLDOWN);
    when(distributedStore.getCooldownRecordedAt(CACHE_KEY))
        .thenReturn(Optional.of(System.currentTimeMillis()));

    Set<String> expanded =
        expandEphemeral(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    assertTrue(expanded.contains(root));
    assertTrue(expanded.contains("urn:li:domain:child"));
    verify(distributedStore, never()).publish(any(), any());
    verify(distributedStore, never()).tryClaimRebuild(any(), anyLong());
  }

  @Test
  public void expandEphemeralClearsPartialFailureMarkerOnPublish() {
    String root = "urn:li:domain:root";
    String componentKey = "domain@primary:componentfp";
    String markerKey = EntityGraphCacheKeys.partialFailureMarkerKey(GRAPH_ID, SOURCE, root);
    EntityGraphDefinition partialDefinition = partialDefinition();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialDefinition);
    when(distributedStore.getStatus(componentKey)).thenReturn(CacheStatus.BUILDING);

    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
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
            .graphId(GRAPH_ID)
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
            eq(SOURCE),
            eq(Set.of(root)),
            eq(TraversalDirection.REVERSE),
            isNull(),
            isNull(),
            isNull()))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.shouldSkipPublish(componentKey, snapshot)).thenReturn(false);
    when(distributedStore.getGeneration(componentKey)).thenReturn(1L);

    expandEphemeral(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    verify(distributedStore).publish(snapshot, CacheStatus.ACTIVE);
    verify(distributedStore).dropFailureMarkerKeysForRoots(GRAPH_ID, SOURCE, Set.of(root));
  }

  @Test
  public void expandEphemeralSkipsWarmPublishWhenBuildingLeaseHeld() {
    String root = "urn:li:domain:root";
    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis() - 400_000L)
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();
    EntityGraphSnapshot staleExisting =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("old-fp")
            .builtAtMillis(System.currentTimeMillis() - 400_000L)
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();

    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(staleExisting);
    when(distributedStore.isRebuildLeaseHeld(CACHE_KEY)).thenReturn(true);

    Set<String> expanded =
        expandEphemeral(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    assertTrue(expanded.contains(root));
    verify(distributedStore, never()).publish(any(), any());
    verify(distributedStore, never()).tryClaimRebuild(any(), anyLong());
  }

  @Test
  public void expandEphemeralSkipsWarmPublishWhenOverLimit() {
    String root = "urn:li:domain:root";
    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();

    when(snapshotBuilder.build(eq(systemOperationContext), eq(definition), eq(SOURCE), eq(null)))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.OVER_LIMIT);

    Set<String> expanded =
        expandEphemeral(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 10);

    assertTrue(expanded.contains(root));
    verify(distributedStore, never()).publish(any(), any());
    verify(distributedStore, never()).tryClaimRebuild(any(), anyLong());
  }

  @Test
  public void expandReturnsEmptyForUnsupportedSource() {
    Set<String> expanded =
        expandCached(
            service,
            GRAPH_ID,
            GraphSnapshotSource.SEARCH,
            TraversalDirection.REVERSE,
            Set.of("urn:li:domain:root"),
            10);

    assertTrue(expanded.isEmpty());
    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
  }

  @Test
  public void partialExpandUsesPerDirectionExploredDepthNotScopeMaxDepth() {
    String root = "urn:li:domain:root";
    String child = "urn:li:domain:child";
    String grandchild = "urn:li:domain:grandchild";
    String componentKey = "domain@graph:componentfp";
    GraphSnapshotSource graphSource = GraphSnapshotSource.GRAPH;
    EntityGraphDefinition partialGraphDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .buildSource(graphSource)
            .enabled(true)
            .build();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialGraphDefinition);

    DirectedEdge childToRoot =
        DirectedEdge.builder()
            .sourceUrn(child)
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    DirectedEdge grandchildToChild =
        DirectedEdge.builder()
            .sourceUrn(grandchild)
            .destinationUrn(child)
            .relationshipType("IsPartOf")
            .build();
    TraversalCoverage reverseOneHopOnly =
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
            .graphId(GRAPH_ID)
            .cacheKey(componentKey)
            .edges(List.of(childToRoot, grandchildToChild))
            .vertexCount(3)
            .edgeCount(2)
            .topologyFingerprint("fp")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(reverseOneHopOnly)
            .build();

    when(distributedStore.findCacheKeyForSeeds(
            eq(GRAPH_ID), eq(graphSource), eq(Set.of(root)), any()))
        .thenReturn(Optional.of(componentKey));
    when(distributedStore.getStatus(componentKey)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(componentKey)).thenReturn(snapshot);
    when(distributedStore.getGeneration(componentKey)).thenReturn(1L);
    when(localViews.get(componentKey, 1L))
        .thenReturn(Optional.of(new EntityGraphView(snapshot.getEdges())));

    Set<String> expanded =
        expandCached(service, GRAPH_ID, graphSource, TraversalDirection.REVERSE, Set.of(root), 100);

    assertTrue(expanded.contains(root));
    assertTrue(expanded.contains(child));
    assertFalse(expanded.contains(grandchild));
  }

  @Test
  public void stalePartialRebuildMergesExistingSnapshotAtSameKey() {
    String root = "urn:li:domain:root";
    String componentKey = "domain@graph:componentfp";
    GraphSnapshotSource graphSource = GraphSnapshotSource.GRAPH;
    EntityGraphDefinition partialGraphDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .buildSource(graphSource)
            .enabled(true)
            .build();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialGraphDefinition);

    DirectedEdge edge =
        DirectedEdge.builder()
            .sourceUrn("urn:li:domain:child")
            .destinationUrn(root)
            .relationshipType("IsPartOf")
            .build();
    TraversalCoverage forwardOnly =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.FORWARD)
                    .explored(true)
                    .exploredDepth(1)
                    .configuredMaxDepth(15)
                    .complete(true)
                    .build())
            .build();
    EntityGraphSnapshot staleSnapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(componentKey)
            .edges(List.of(edge))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .builtAtMillis(0L)
            .traversalCoverage(forwardOnly)
            .build();
    EntityGraphSnapshot refreshedSnapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(componentKey)
            .edges(staleSnapshot.getEdges())
            .vertexCount(staleSnapshot.getVertexCount())
            .edgeCount(staleSnapshot.getEdgeCount())
            .topologyFingerprint(staleSnapshot.getTopologyFingerprint())
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(forwardOnly)
            .build();

    when(distributedStore.findCacheKeyForSeeds(
            eq(GRAPH_ID), eq(graphSource), eq(Set.of(root)), any()))
        .thenReturn(Optional.of(componentKey));
    when(distributedStore.getStatus(componentKey)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(componentKey)).thenReturn(staleSnapshot);
    when(distributedStore.getGeneration(componentKey)).thenReturn(1L);
    when(snapshotBuilder.buildPartial(
            eq(systemOperationContext),
            eq(partialGraphDefinition),
            eq(graphSource),
            eq(Set.of(root)),
            eq(TraversalDirection.FORWARD),
            eq(staleSnapshot.getEdges()),
            eq(forwardOnly),
            eq(componentKey)))
        .thenReturn(
            BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(refreshedSnapshot).build());
    when(distributedStore.shouldSkipPublish(componentKey, refreshedSnapshot)).thenReturn(false);
    when(localViews.get(componentKey, 1L))
        .thenReturn(Optional.of(new EntityGraphView(refreshedSnapshot.getEdges())));

    expandCached(service, GRAPH_ID, graphSource, TraversalDirection.FORWARD, Set.of(root), 10);

    verify(snapshotBuilder)
        .buildPartial(
            eq(systemOperationContext),
            eq(partialGraphDefinition),
            eq(graphSource),
            eq(Set.of(root)),
            eq(TraversalDirection.FORWARD),
            eq(staleSnapshot.getEdges()),
            eq(forwardOnly),
            eq(componentKey));
  }

  @Test
  public void multiComponentPartialExpandUsesShallowestDirectionCoverage() {
    String root1 = "urn:li:domain:root1";
    String child1 = "urn:li:domain:child1";
    String root2 = "urn:li:domain:root2";
    String child2 = "urn:li:domain:child2";
    String grandchild2 = "urn:li:domain:grandchild2";
    String key1 = "domain@graph:fp1";
    String key2 = "domain@graph:fp2";
    GraphSnapshotSource graphSource = GraphSnapshotSource.GRAPH;
    EntityGraphDefinition partialGraphDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .buildSource(graphSource)
            .enabled(true)
            .build();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(partialGraphDefinition);

    TraversalCoverage shallowReverse =
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
    TraversalCoverage deepReverse =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.REVERSE)
                    .explored(true)
                    .exploredDepth(2)
                    .configuredMaxDepth(15)
                    .complete(true)
                    .build())
            .build();

    EntityGraphSnapshot snapshot1 =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(key1)
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn(child1)
                        .destinationUrn(root1)
                        .relationshipType("IsPartOf")
                        .build()))
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp1")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(shallowReverse)
            .build();
    EntityGraphSnapshot snapshot2 =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(key2)
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn(child2)
                        .destinationUrn(root2)
                        .relationshipType("IsPartOf")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn(grandchild2)
                        .destinationUrn(child2)
                        .relationshipType("IsPartOf")
                        .build()))
            .vertexCount(3)
            .edgeCount(2)
            .topologyFingerprint("fp2")
            .builtAtMillis(System.currentTimeMillis())
            .traversalCoverage(deepReverse)
            .build();

    when(distributedStore.findCacheKeyForSeeds(
            eq(GRAPH_ID), eq(graphSource), eq(Set.of(root1)), any()))
        .thenReturn(Optional.of(key1));
    when(distributedStore.findCacheKeyForSeeds(
            eq(GRAPH_ID), eq(graphSource), eq(Set.of(root2)), any()))
        .thenReturn(Optional.of(key2));
    when(distributedStore.getStatus(key1)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getStatus(key2)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(key1)).thenReturn(snapshot1);
    when(distributedStore.getSnapshot(key2)).thenReturn(snapshot2);
    when(distributedStore.getGeneration(key1)).thenReturn(1L);
    when(distributedStore.getGeneration(key2)).thenReturn(1L);
    when(localViews.get(key1, 1L))
        .thenReturn(Optional.of(new EntityGraphView(snapshot1.getEdges())));
    when(localViews.get(key2, 1L))
        .thenReturn(Optional.of(new EntityGraphView(snapshot2.getEdges())));

    Set<String> expanded =
        expandCached(
            service, GRAPH_ID, graphSource, TraversalDirection.REVERSE, Set.of(root1, root2), 100);

    assertTrue(expanded.contains(root1));
    assertTrue(expanded.contains(child1));
    assertTrue(expanded.contains(root2));
    assertTrue(expanded.contains(child2));
    assertFalse(expanded.contains(grandchild2));
  }

  @Test
  public void expandSucceedsAfterSurgicalVertexRemovalOnFullGraph() {
    String root = "urn:li:domain:root";
    String child = "urn:li:domain:child";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(child)
                .destinationUrn(root)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(2L)
            .buildSource("primary")
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp2")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(2L);
    when(localViews.get(CACHE_KEY, 2L)).thenReturn(Optional.of(new EntityGraphView(edges)));

    Set<String> expanded =
        expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 100);

    assertTrue(expanded.contains(root));
    assertTrue(expanded.contains(child));
  }

  @Test
  public void expandReturnsEmptyForSelfOnlyReverseExpand() {
    String root = "urn:li:domain:root";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(root)
                .destinationUrn("urn:li:domain:parent")
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(1L)
            .buildSource("primary")
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(1)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(1L);
    when(localViews.get(CACHE_KEY, 1L)).thenReturn(Optional.of(new EntityGraphView(edges)));

    Set<String> expanded =
        expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 100);

    assertTrue(expanded.isEmpty());
  }

  @Test
  public void expandReturnsEmptyWhenStaleActiveAndClaimLost() {
    String root = "urn:li:domain:root";
    String child = "urn:li:domain:child";
    long staleMillis = System.currentTimeMillis() - 400_000L;
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(child)
                .destinationUrn(root)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(1L)
            .buildSource("primary")
            .builtAtMillis(staleMillis)
            .edges(edges)
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.tryClaimRebuild(eq(CACHE_KEY), anyLong())).thenReturn(false);

    Set<String> expanded =
        expandCached(service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of(root), 100);

    assertTrue(expanded.isEmpty());
    verify(snapshotBuilder, never()).build(any(), any(), any(), any());
  }

  @Test
  public void walkOrderedForwardAncestorsEmptyWhenStaleActiveAndClaimLost() {
    String child = "urn:li:domain:child";
    String root = "urn:li:domain:root";
    long staleMillis = System.currentTimeMillis() - 400_000L;
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(child)
                .destinationUrn(root)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(1L)
            .buildSource("primary")
            .builtAtMillis(staleMillis)
            .edges(edges)
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.tryClaimRebuild(eq(CACHE_KEY), anyLong())).thenReturn(false);

    List<String> ancestors =
        walkOrderedForwardAncestorsCached(service, GRAPH_ID, SOURCE, child, 10);

    assertTrue(ancestors.isEmpty());
  }

  @Test
  public void expandReturnsTruncatedMissWhenPerCallLimitExceeded() {
    String root = "urn:li:domain:root";
    String child = "urn:li:domain:child";
    String grandchild = "urn:li:domain:grandchild";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(child)
                .destinationUrn(root)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(grandchild)
                .destinationUrn(child)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(1L)
            .buildSource("primary")
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(3)
            .edgeCount(2)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(1L);
    when(localViews.get(CACHE_KEY, 1L)).thenReturn(Optional.of(new EntityGraphView(edges)));

    GraphReadResult expanded =
        service.expand(
            GRAPH_ID,
            SOURCE,
            TraversalDirection.REVERSE,
            Set.of(root),
            2,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED);

    assertTrue(expanded.isMiss());
    assertEquals(ReadMissReason.TRUNCATED, ((GraphReadResult.Miss) expanded).reason());
    verify(distributedStore, never()).markOverLimit(CACHE_KEY);
  }

  @Test
  public void backgroundRebuildTransientFailureMarksCooldown() throws Exception {
    EntityGraphDefinition backgroundDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(definition.getResolvedEdges())
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .rebuildExecution(RebuildExecution.BACKGROUND)
            .buildSource(GraphSnapshotSource.PRIMARY)
            .localEviction(definition.getLocalEviction())
            .enabled(true)
            .build();
    when(registry.getDefinition(GRAPH_ID)).thenReturn(backgroundDefinition);
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ABSENT);
    when(snapshotBuilder.build(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("transient"));

    expandCached(
        service, GRAPH_ID, SOURCE, TraversalDirection.REVERSE, Set.of("urn:li:domain:root"), 10);

    verify(distributedStore, timeout(5000)).markCooldown(CACHE_KEY);
  }

  @Test
  public void walkOrderedForwardAncestorsEphemeralUsesLiveBuild() {
    String child = "urn:li:domain:child";
    String root = "urn:li:domain:root";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(child)
                .destinationUrn(root)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(1L)
            .buildSource("primary")
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ABSENT);
    when(snapshotBuilder.build(any(), any(), any(), isNull()))
        .thenReturn(BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build());

    List<String> ancestors =
        walkOrderedForwardAncestorsEphemeral(service, GRAPH_ID, SOURCE, child, 10);

    assertEquals(ancestors, List.of(root));
  }

  @Test
  public void listRelatedReturnsTypedNeighborsFromActiveSnapshot() {
    String graphId = "membership";
    GraphSnapshotSource source = GraphSnapshotSource.GRAPH;
    String cacheKey = EntityGraphCacheKeys.fullCacheKey(graphId, source);
    EntityGraphDefinition membershipDefinition =
        EntityGraphDefinition.builder()
            .graphId(graphId)
            .resolvedEdges(definition.getResolvedEdges())
            .scope(definition.getScope())
            .bindings(definition.getBindings())
            .bounds(definition.getBounds())
            .populationIntervalSeconds(definition.getPopulationIntervalSeconds())
            .localEviction(definition.getLocalEviction())
            .buildSource(source)
            .enabled(true)
            .build();
    when(registry.getDefinition(graphId)).thenReturn(membershipDefinition);

    String user = "urn:li:corpuser:alice";
    String group = "urn:li:corpGroup:eng";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(user)
                .destinationUrn(group)
                .relationshipType("IsMemberOfGroup")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(graphId)
            .cacheKey(cacheKey)
            .generation(1L)
            .buildSource("graph")
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(cacheKey)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(cacheKey)).thenReturn(snapshot);
    when(distributedStore.getGeneration(cacheKey)).thenReturn(1L);
    when(localViews.get(cacheKey, 1L)).thenReturn(Optional.of(new EntityGraphView(edges)));

    MembershipNeighborResult result =
        service.listRelated(
            graphId,
            source,
            group,
            TraversalDirection.REVERSE,
            Set.of("IsMemberOfGroup"),
            1,
            0,
            10,
            ReadMode.CACHED);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().size(), 1);
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), user);
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

  private Set<String> expandCached(
      EntityGraphCacheService service,
      String graphId,
      GraphSnapshotSource source,
      TraversalDirection direction,
      Collection<String> roots,
      int limit,
      int maxDepth) {
    return service
        .expand(graphId, source, direction, roots, limit, maxDepth, ReadMode.CACHED)
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

  private List<String> walkOrderedForwardAncestorsCached(
      EntityGraphCacheService service,
      String graphId,
      GraphSnapshotSource source,
      String seed,
      int maxDepth) {
    return service
        .walkOrderedForwardAncestors(graphId, source, seed, maxDepth, ReadMode.CACHED)
        .ancestorsOrEmpty();
  }

  private List<String> walkOrderedForwardAncestorsEphemeral(
      EntityGraphCacheService service,
      String graphId,
      GraphSnapshotSource source,
      String seed,
      int maxDepth) {
    return service
        .walkOrderedForwardAncestors(graphId, source, seed, maxDepth, ReadMode.EPHEMERAL)
        .ancestorsOrEmpty();
  }

  private static EntityGraphDefinition partialDefinition() {
    return EntityGraphDefinition.builder()
        .graphId(GRAPH_ID)
        .resolvedEdges(List.of(mock(ResolvedGraphEdge.class)))
        .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
        .bindings(GraphBindings.builder().build())
        .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
        .populationIntervalSeconds(300)
        .localEviction(
            LocalEvictionLimits.builder().enabled(true).maxViews(8).maxEstimatedBytes(1024).build())
        .buildSource(GraphSnapshotSource.PRIMARY)
        .enabled(true)
        .build();
  }
}
