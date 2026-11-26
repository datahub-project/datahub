package com.linkedin.metadata.graph.cache.service.invalidation;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.cache.visibility.IndexVisibilityEstimator;
import com.linkedin.metadata.cache.visibility.IndexVisibilityEstimator.IndexWriteSettings;
import com.linkedin.metadata.cache.visibility.IndexWriteFlusher;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.InvalidationVisibility;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.RefreshIntervals;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationBatch;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationEntry;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphEdgeTriplet;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphCacheInvalidatorTest {

  private static final String GLOSSARY_GRAPH_ID = "glossary";
  private static final String MEMBERSHIP_GRAPH_ID = "membership";

  private EntityGraphRegistry registry;
  private EntityGraphDistributedStore distributedStore;
  private EntityGraphLocalViewCache localViews;
  private GraphCacheInvalidator invalidator;

  @BeforeMethod
  public void setUp() {
    registry = mock(EntityGraphRegistry.class);
    distributedStore = mock(EntityGraphDistributedStore.class);
    localViews = mock(EntityGraphLocalViewCache.class);
    invalidator =
        new GraphCacheInvalidator(
            registry,
            distributedStore,
            localViews,
            TestOperationContexts.systemContextNoSearchAuthorization(),
            IndexVisibilityEstimator.DISABLED);
  }

  @Test
  public void graphIdsForEntityTypeDelegatesToRegistry() {
    when(registry.getGraphIdsForEntityType("domain")).thenReturn(Set.of("domain"));

    assertEquals(invalidator.graphIdsForEntityType("domain"), Set.of("domain"));
    verify(registry).getGraphIdsForEntityType("domain");
  }

  @Test
  public void glossaryNodeInfoUpdateDropsPartialGraph() {
    stubGlossaryPartialGraph();
    when(registry.getCandidateGraphIds("glossaryNode", GLOSSARY_NODE_INFO_ASPECT_NAME))
        .thenReturn(Set.of(GLOSSARY_GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GLOSSARY_GRAPH_ID, GLOSSARY_NODE_INFO_ASPECT_NAME))
        .thenReturn(Set.of("IsPartOf"));

    invalidator.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .update(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn("urn:li:glossaryNode:child")
                    .entityType("glossaryNode")
                    .aspectName(GLOSSARY_NODE_INFO_ASPECT_NAME)
                    .build())
            .build());

    verify(distributedStore).dropPartialGraph(GLOSSARY_GRAPH_ID);
    verify(localViews).evictGraph(GLOSSARY_GRAPH_ID);
  }

  @Test
  public void glossaryTermInfoUpdateDropsPartialGraph() {
    stubGlossaryPartialGraph();
    when(registry.getCandidateGraphIds("glossaryTerm", GLOSSARY_TERM_INFO_ASPECT_NAME))
        .thenReturn(Set.of(GLOSSARY_GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GLOSSARY_GRAPH_ID, GLOSSARY_TERM_INFO_ASPECT_NAME))
        .thenReturn(Set.of("IsPartOf"));

    invalidator.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .update(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn("urn:li:glossaryTerm:term")
                    .entityType("glossaryTerm")
                    .aspectName(GLOSSARY_TERM_INFO_ASPECT_NAME)
                    .build())
            .build());

    verify(distributedStore).dropPartialGraph(GLOSSARY_GRAPH_ID);
    verify(localViews).evictGraph(GLOSSARY_GRAPH_ID);
  }

  @Test
  public void nativeGroupMembershipUpdateOnAbsentCacheDropsFullMembershipGraph() {
    stubMembershipFullGraph();
    when(registry.getCandidateGraphIds("corpuser", NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))
        .thenReturn(Set.of(MEMBERSHIP_GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(
            MEMBERSHIP_GRAPH_ID, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))
        .thenReturn(Set.of("IsMemberOfNativeGroup", "IsMemberOfGroup"));

    invalidator.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .update(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn("urn:li:corpuser:datahub")
                    .entityType("corpuser")
                    .aspectName(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)
                    .build())
            .build());

    verify(distributedStore).dropGraph(MEMBERSHIP_GRAPH_ID);
    verify(localViews).evictGraph(MEMBERSHIP_GRAPH_ID);
  }

  @Test
  public void glossaryEntityDeleteDropsPartialGraph() {
    EntityGraphDefinition definition = stubGlossaryPartialGraph();
    when(registry.getGraphsById()).thenReturn(Map.of(GLOSSARY_GRAPH_ID, definition));
    when(registry.getGraphIdsForEntityType("glossaryNode")).thenReturn(Set.of(GLOSSARY_GRAPH_ID));

    invalidator.invalidateOnSyncBatch(
        SyncGraphInvalidationBatch.builder()
            .delete(
                SyncGraphInvalidationEntry.builder()
                    .entityUrn("urn:li:glossaryNode:child")
                    .entityType("glossaryNode")
                    .build())
            .build());

    verify(distributedStore).dropPartialGraph(GLOSSARY_GRAPH_ID);
    verify(localViews).evictGraph(GLOSSARY_GRAPH_ID);
  }

  @Test
  public void visibilityGraphKeepsActiveSnapshotAndRecordsWatermark() {
    stubGlossaryPartialGraph(InvalidationVisibility.SUBMIT_BOUND);
    stubGlossaryNodeInfoCandidate();

    long before = System.currentTimeMillis();
    visibilityInvalidator(IndexWriteFlusher.NONE).invalidateOnSyncBatch(glossaryNodeUpdate());

    ArgumentCaptor<Long> watermark = ArgumentCaptor.forClass(Long.class);
    verify(distributedStore)
        .recordVisibilityWatermark(
            org.mockito.ArgumentMatchers.eq(GLOSSARY_GRAPH_ID), watermark.capture());
    // flush 1s + graph refresh 2s + margin 1s
    assertTrue(watermark.getValue() >= before + 4_000L);
    verify(distributedStore, never()).dropPartialGraph(anyString());
    verify(localViews, never()).evictGraph(anyString());
  }

  @Test
  public void visibilityGraphDropsCooldownTombstoneNowAndStillRecordsWatermark() {
    stubGlossaryPartialGraph(InvalidationVisibility.SUBMIT_BOUND);
    stubGlossaryNodeInfoCandidate();
    when(distributedStore.anySnapshotStatusForGraph(GLOSSARY_GRAPH_ID))
        .thenReturn(CacheStatus.COOLDOWN);

    visibilityInvalidator(IndexWriteFlusher.NONE).invalidateOnSyncBatch(glossaryNodeUpdate());

    verify(distributedStore).dropPartialGraph(GLOSSARY_GRAPH_ID);
    verify(distributedStore)
        .recordVisibilityWatermark(org.mockito.ArgumentMatchers.eq(GLOSSARY_GRAPH_ID), anyLong());
  }

  @Test
  public void visibilityGraphDropsWhenNoBoundCanBeComputed() {
    stubGlossaryPartialGraph(InvalidationVisibility.SUBMIT_BOUND);
    stubGlossaryNodeInfoCandidate();

    new GraphCacheInvalidator(
            registry,
            distributedStore,
            localViews,
            TestOperationContexts.systemContextNoSearchAuthorization(),
            IndexVisibilityEstimator.DISABLED)
        .invalidateOnSyncBatch(glossaryNodeUpdate());

    verify(distributedStore).dropPartialGraph(GLOSSARY_GRAPH_ID);
    verify(distributedStore, never()).recordVisibilityWatermark(anyString(), anyLong());
  }

  @Test
  public void batchOfDeletesRecordsOneWatermarkPerGraph() {
    stubGlossaryPartialGraph(InvalidationVisibility.SUBMIT_BOUND);
    when(registry.getGraphIdsForEntityType("glossaryNode")).thenReturn(Set.of(GLOSSARY_GRAPH_ID));

    visibilityInvalidator(IndexWriteFlusher.NONE)
        .invalidateOnSyncBatch(
            SyncGraphInvalidationBatch.builder()
                .delete(
                    SyncGraphInvalidationEntry.builder()
                        .entityUrn("urn:li:glossaryNode:a")
                        .entityType("glossaryNode")
                        .build())
                .delete(
                    SyncGraphInvalidationEntry.builder()
                        .entityUrn("urn:li:glossaryNode:b")
                        .entityType("glossaryNode")
                        .build())
                .build());

    verify(distributedStore, times(1))
        .recordVisibilityWatermark(org.mockito.ArgumentMatchers.eq(GLOSSARY_GRAPH_ID), anyLong());
  }

  @Test
  public void invalidateImmediatelyIgnoresVisibility() {
    stubGlossaryPartialGraph(InvalidationVisibility.SUBMIT_BOUND);
    stubGlossaryNodeInfoCandidate();

    visibilityInvalidator(IndexWriteFlusher.NONE).invalidateImmediately(glossaryNodeUpdate());

    verify(distributedStore).dropPartialGraph(GLOSSARY_GRAPH_ID);
    verify(distributedStore, never()).recordVisibilityWatermark(anyString(), anyLong());
  }

  @Test
  public void flushAwaitFlushesOncePerBatchAcrossGraphsOfTheSameKind() {
    stubGlossaryPartialGraph(InvalidationVisibility.FLUSH_AWAIT);
    stubGlossaryNodeInfoCandidate();
    EntityGraphDefinition container = stubContainerPartialGraph(InvalidationVisibility.FLUSH_AWAIT);
    when(registry.getCandidateGraphIds("container", "containerProperties"))
        .thenReturn(Set.of(container.getGraphId()));
    when(registry.getRelationshipTypesForAspect(container.getGraphId(), "containerProperties"))
        .thenReturn(Set.of("IsPartOf"));
    AtomicInteger flushes = new AtomicInteger();

    visibilityInvalidator(
            kind -> {
              flushes.incrementAndGet();
              return true;
            })
        .invalidateOnSyncBatch(
            SyncGraphInvalidationBatch.builder()
                .update(
                    SyncGraphInvalidationEntry.builder()
                        .entityUrn("urn:li:glossaryNode:child")
                        .entityType("glossaryNode")
                        .aspectName(GLOSSARY_NODE_INFO_ASPECT_NAME)
                        .build())
                .update(
                    SyncGraphInvalidationEntry.builder()
                        .entityUrn("urn:li:container:child")
                        .entityType("container")
                        .aspectName("containerProperties")
                        .build())
                .build());

    assertEquals(flushes.get(), 1);
    verify(distributedStore)
        .recordVisibilityWatermark(org.mockito.ArgumentMatchers.eq(GLOSSARY_GRAPH_ID), anyLong());
    verify(distributedStore)
        .recordVisibilityWatermark(org.mockito.ArgumentMatchers.eq("container"), anyLong());
  }

  private IndexVisibilityEstimator estimatorFor(IndexWriteFlusher flusher) {
    IndexWriteSettings settings =
        new IndexWriteSettings(
            RefreshIntervals.builder().graphSeconds(2).build(),
            BulkProcessorConfiguration.builder()
                .flushPeriod(1)
                .numRetries(0)
                .retryInterval(0)
                .refreshPolicy("NONE")
                .build());
    return new IndexVisibilityEstimator(kind -> settings, flusher);
  }

  private GraphCacheInvalidator visibilityInvalidator(IndexWriteFlusher flusher) {
    return new GraphCacheInvalidator(
        registry,
        distributedStore,
        localViews,
        TestOperationContexts.systemContextNoSearchAuthorization(),
        estimatorFor(flusher));
  }

  private void stubGlossaryNodeInfoCandidate() {
    when(registry.getCandidateGraphIds("glossaryNode", GLOSSARY_NODE_INFO_ASPECT_NAME))
        .thenReturn(Set.of(GLOSSARY_GRAPH_ID));
    when(registry.getRelationshipTypesForAspect(GLOSSARY_GRAPH_ID, GLOSSARY_NODE_INFO_ASPECT_NAME))
        .thenReturn(Set.of("IsPartOf"));
  }

  private static SyncGraphInvalidationBatch glossaryNodeUpdate() {
    return SyncGraphInvalidationBatch.builder()
        .update(
            SyncGraphInvalidationEntry.builder()
                .entityUrn("urn:li:glossaryNode:child")
                .entityType("glossaryNode")
                .aspectName(GLOSSARY_NODE_INFO_ASPECT_NAME)
                .build())
        .build();
  }

  private EntityGraphDefinition stubContainerPartialGraph(InvalidationVisibility visibility) {
    EntityGraphDefinition definition =
        EntityGraphDefinition.builder()
            .graphId("container")
            .resolvedEdges(
                List.of(
                    ResolvedGraphEdge.builder()
                        .triplet(
                            GraphEdgeTriplet.builder()
                                .sourceEntityType("container")
                                .destinationEntityType("container")
                                .relationshipType("IsPartOf")
                                .build())
                        .build()))
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(12).build())
            .buildSource(GraphSnapshotSource.GRAPH)
            .invalidationVisibility(visibility)
            .invalidationMarginSeconds(1)
            .enabled(true)
            .build();
    when(registry.getDefinition("container")).thenReturn(definition);
    when(distributedStore.anySnapshotStatusForGraph("container")).thenReturn(CacheStatus.ACTIVE);
    return definition;
  }

  private EntityGraphDefinition stubGlossaryPartialGraph() {
    return stubGlossaryPartialGraph(null);
  }

  private EntityGraphDefinition stubGlossaryPartialGraph(InvalidationVisibility visibility) {
    EntityGraphDefinition definition =
        EntityGraphDefinition.builder()
            .graphId(GLOSSARY_GRAPH_ID)
            .resolvedEdges(
                List.of(
                    ResolvedGraphEdge.builder()
                        .triplet(
                            GraphEdgeTriplet.builder()
                                .sourceEntityType("glossaryNode")
                                .destinationEntityType("glossaryNode")
                                .relationshipType("IsPartOf")
                                .build())
                        .build(),
                    ResolvedGraphEdge.builder()
                        .triplet(
                            GraphEdgeTriplet.builder()
                                .sourceEntityType("glossaryTerm")
                                .destinationEntityType("glossaryNode")
                                .relationshipType("IsPartOf")
                                .build())
                        .build()))
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
            .buildSource(GraphSnapshotSource.GRAPH)
            .invalidationVisibility(visibility)
            .invalidationMarginSeconds(1)
            .enabled(true)
            .build();
    when(registry.getDefinition(GLOSSARY_GRAPH_ID)).thenReturn(definition);
    when(distributedStore.anySnapshotStatusForGraph(GLOSSARY_GRAPH_ID))
        .thenReturn(CacheStatus.ACTIVE);
    return definition;
  }

  private EntityGraphDefinition stubMembershipFullGraph() {
    EntityGraphDefinition definition =
        EntityGraphDefinition.builder()
            .graphId(MEMBERSHIP_GRAPH_ID)
            .resolvedEdges(
                List.of(
                    ResolvedGraphEdge.builder()
                        .triplet(
                            GraphEdgeTriplet.builder()
                                .sourceEntityType("corpuser")
                                .destinationEntityType("corpGroup")
                                .relationshipType("IsMemberOfNativeGroup")
                                .build())
                        .build()))
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
            .buildSource(GraphSnapshotSource.GRAPH)
            .enabled(true)
            .build();
    when(registry.getDefinition(MEMBERSHIP_GRAPH_ID)).thenReturn(definition);
    when(distributedStore.getStatus(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(CacheStatus.ABSENT);
    return definition;
  }
}
