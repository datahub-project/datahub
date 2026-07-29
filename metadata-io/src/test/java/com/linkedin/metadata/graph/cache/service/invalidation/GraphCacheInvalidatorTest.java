package com.linkedin.metadata.graph.cache.service.invalidation;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
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
            TestOperationContexts.systemContextNoSearchAuthorization());
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

  private EntityGraphDefinition stubGlossaryPartialGraph() {
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
