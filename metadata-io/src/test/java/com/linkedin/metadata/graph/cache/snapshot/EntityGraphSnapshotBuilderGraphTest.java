package com.linkedin.metadata.graph.cache.snapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBindings;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBounds;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphEdgeTriplet;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.LocalEvictionLimits;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityGraphSnapshotBuilderGraphTest {

  private static final String GRAPH_ID = "domain";

  private OperationContext opContext;
  private GraphRetriever graphRetriever;
  private EntityGraphSnapshotBuilder builder;
  private EntityGraphDefinition fullGraphDefinition;

  @BeforeMethod
  public void setUp() {
    EntityRegistry entityRegistry = new TestEntityRegistry();
    CachingAspectRetriever aspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);

    graphRetriever = spy(GraphRetriever.class);
    opContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> entityRegistry,
            () ->
                io.datahubproject.metadata.context.RetrieverContext.builder()
                    .aspectRetriever(aspectRetriever)
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(() -> entityRegistry))
                    .graphRetriever(graphRetriever)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .build(),
            null,
            null,
            null);

    GraphEdgeTriplet triplet =
        GraphEdgeTriplet.builder()
            .sourceEntityType("domain")
            .destinationEntityType("domain")
            .relationshipType("IsPartOf")
            .build();

    fullGraphDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(
                List.of(
                    ResolvedGraphEdge.builder()
                        .triplet(triplet)
                        .aspectName("domainProperties")
                        .searchable(false)
                        .graphDirection(RelationshipDirection.OUTGOING)
                        .build()))
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .buildSource(GraphSnapshotSource.GRAPH)
            .enabled(true)
            .build();

    builder = new EntityGraphSnapshotBuilder();
  }

  @Test
  public void fullGraphBuildFailsWhenScrollReturnsNull() {
    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(null);

    BuildResult result =
        builder.build(opContext, fullGraphDefinition, GraphSnapshotSource.GRAPH, null);

    assertEquals(result.getStatus(), CacheStatus.COOLDOWN);
    assertEquals(result.getFailureReason(), "scroll_incomplete");
  }

  @Test
  public void fullGraphBuildSucceedsFromGraphScroll() {
    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        "urn:li:domain:child",
                        "urn:li:domain:root",
                        RelationshipDirection.OUTGOING,
                        null))));

    BuildResult result =
        builder.build(opContext, fullGraphDefinition, GraphSnapshotSource.GRAPH, null);

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    assertNotNull(result.getSnapshot());
    assertEquals(result.getSnapshot().getEdges().size(), 1);
    assertEquals(
        result.getSnapshot().getCacheKey(),
        EntityGraphCacheKeys.fullCacheKey(GRAPH_ID, GraphSnapshotSource.GRAPH));
    assertTrue(
        result
            .getSnapshot()
            .getTraversalCoverage()
            .canSatisfy(com.linkedin.metadata.graph.cache.TraversalDirection.FORWARD));
  }

  @Test
  public void fullGraphBuildFailsOnEmptyScroll() {
    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, List.of()));

    BuildResult result =
        builder.build(opContext, fullGraphDefinition, GraphSnapshotSource.GRAPH, null);

    assertEquals(result.getStatus(), CacheStatus.COOLDOWN);
    assertEquals(result.getFailureReason(), "graph_failed");
  }

  @Test
  public void fullGraphBuildRejectsPrimarySource() {
    BuildResult result =
        builder.build(opContext, fullGraphDefinition, GraphSnapshotSource.PRIMARY, null);

    assertEquals(result.getStatus(), CacheStatus.INVALID);
    assertEquals(result.getFailureReason(), "primary_full_unsupported");
  }

  @Test
  public void fullGraphBuildFromSearchRequiresSearchableEdges() {
    BuildResult result =
        builder.build(opContext, fullGraphDefinition, GraphSnapshotSource.SEARCH, null);

    assertEquals(result.getStatus(), CacheStatus.INVALID);
    assertEquals(result.getFailureReason(), "search_not_configured");
  }

  @Test
  public void fullGraphBuildReturnsOverLimitWhenEdgeCapExceeded() {
    EntityGraphDefinition tightEdges =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(fullGraphDefinition.getResolvedEdges())
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(0)).build())
            .populationIntervalSeconds(300)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .buildSource(GraphSnapshotSource.GRAPH)
            .enabled(true)
            .build();

    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        "urn:li:domain:child",
                        "urn:li:domain:root",
                        RelationshipDirection.OUTGOING,
                        null))));

    BuildResult result = builder.build(opContext, tightEdges, GraphSnapshotSource.GRAPH, null);

    assertEquals(result.getStatus(), CacheStatus.OVER_LIMIT);
    assertEquals(result.getFailureReason(), "edge_limit");
  }

  @Test
  public void fullGraphBuildSucceedsFromSearchScroll() {
    EntityGraphDefinition searchableDefinition =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(
                List.of(
                    ResolvedGraphEdge.builder()
                        .triplet(
                            GraphEdgeTriplet.builder()
                                .sourceEntityType("domain")
                                .destinationEntityType("domain")
                                .relationshipType("IsPartOf")
                                .build())
                        .aspectName("domainProperties")
                        .searchField("parentDomain")
                        .searchable(true)
                        .graphDirection(RelationshipDirection.OUTGOING)
                        .build()))
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(15).build())
            .bindings(GraphBindings.builder().build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(150)).build())
            .populationIntervalSeconds(300)
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .buildSource(GraphSnapshotSource.SEARCH)
            .scrollBatchSize(100)
            .enabled(true)
            .build();

    SearchRetriever searchRetriever = Mockito.mock(SearchRetriever.class);
    SearchEntity entity =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn("urn:li:domain:child"))
            .setExtraFields(new StringMap(Map.of("parentDomain", "urn:li:domain:root")));
    when(searchRetriever.scroll(any(), any(), nullable(String.class), anyInt(), any(), any()))
        .thenReturn(new ScrollResult().setEntities(new SearchEntityArray(entity)));

    EntityRegistry entityRegistry = new TestEntityRegistry();
    CachingAspectRetriever aspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    OperationContext searchContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> entityRegistry,
            () ->
                io.datahubproject.metadata.context.RetrieverContext.builder()
                    .aspectRetriever(aspectRetriever)
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(() -> entityRegistry))
                    .graphRetriever(graphRetriever)
                    .searchRetriever(searchRetriever)
                    .build(),
            null,
            null,
            null);

    BuildResult result =
        builder.build(searchContext, searchableDefinition, GraphSnapshotSource.SEARCH, null);

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    assertNotNull(result.getSnapshot());
    assertEquals(result.getSnapshot().getEdges().size(), 1);
    assertEquals(
        result.getSnapshot().getCacheKey(),
        EntityGraphCacheKeys.fullCacheKey(GRAPH_ID, GraphSnapshotSource.SEARCH));
  }
}
