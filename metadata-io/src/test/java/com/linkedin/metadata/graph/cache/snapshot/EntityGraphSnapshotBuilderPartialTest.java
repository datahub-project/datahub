package com.linkedin.metadata.graph.cache.snapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBindings;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBounds;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphEdgeTriplet;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.LocalEvictionLimits;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityGraphSnapshotBuilderPartialTest {

  private static final String GRAPH_ID = "domain";
  private static final String ROOT = "urn:li:domain:root";
  private static final String CHILD = "urn:li:domain:child";
  private static final String GRANDCHILD = "urn:li:domain:grandchild";

  private OperationContext opContext;
  private CachingAspectRetriever aspectRetriever;
  private GraphRetriever graphRetriever;
  private EntityGraphSnapshotBuilder builder;
  private EntityGraphDefinition primaryDefinition;
  private EntityGraphDefinition searchDefinition;
  private List<ResolvedGraphEdge> resolvedEdges;
  private List<ResolvedGraphEdge> searchableResolvedEdges;

  @BeforeMethod
  public void setUp() {
    EntityRegistry entityRegistry = new TestEntityRegistry();
    aspectRetriever = mock(CachingAspectRetriever.class);
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

    resolvedEdges =
        List.of(
            ResolvedGraphEdge.builder()
                .triplet(triplet)
                .aspectName("domainProperties")
                .searchable(false)
                .graphDirection(RelationshipDirection.OUTGOING)
                .build());

    searchableResolvedEdges =
        List.of(
            ResolvedGraphEdge.builder()
                .triplet(triplet)
                .aspectName("domainProperties")
                .searchField("parentDomain")
                .searchable(true)
                .graphDirection(RelationshipDirection.OUTGOING)
                .build());

    primaryDefinition = partialDefinition(GraphSnapshotSource.PRIMARY, resolvedEdges, 100);
    searchDefinition = partialDefinition(GraphSnapshotSource.SEARCH, searchableResolvedEdges, 100);
    builder = new EntityGraphSnapshotBuilder();
  }

  @Test
  public void buildRejectsPartialScope() {
    expectThrows(
        IllegalStateException.class,
        () ->
            builder.build(opContext, primaryDefinition, GraphSnapshotSource.PRIMARY, Set.of(ROOT)));
  }

  @Test
  public void primaryReverseTraversalRejected() {
    BuildResult result =
        builder.buildPartial(
            opContext,
            primaryDefinition,
            GraphSnapshotSource.PRIMARY,
            Set.of(ROOT),
            TraversalDirection.REVERSE,
            null,
            null,
            null);

    assertEquals(result.getStatus(), CacheStatus.INVALID);
    assertEquals(result.getFailureReason(), "primary_reverse_unsupported");
  }

  @Test
  public void searchReverseTraversalRejected() {
    BuildResult result =
        builder.buildPartial(
            opContext,
            searchDefinition,
            GraphSnapshotSource.SEARCH,
            Set.of(ROOT),
            TraversalDirection.REVERSE,
            null,
            null,
            null);

    assertEquals(result.getStatus(), CacheStatus.INVALID);
    assertEquals(result.getFailureReason(), "search_reverse_unsupported");
  }

  @Test
  public void ancestorsMultiHopUsesSearchIndex() {
    SearchRetriever searchRetriever = mock(SearchRetriever.class);
    when(searchRetriever.scroll(any(), any(), nullable(String.class), anyInt(), any(), any()))
        .thenReturn(searchScrollResult(GRANDCHILD, CHILD))
        .thenReturn(searchScrollResult(CHILD, ROOT));

    BuildResult result =
        builder.buildPartial(
            opContextWithSearch(searchRetriever),
            searchDefinition,
            GraphSnapshotSource.SEARCH,
            Set.of(GRANDCHILD),
            TraversalDirection.FORWARD,
            null,
            null,
            null);

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    assertTrue(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.FORWARD));
    assertEquals(result.getSnapshot().getEdges().size(), 2);
  }

  @Test
  public void descendantsFromParentUsesGraphIncomingScroll() {
    EntityGraphDefinition graphDefinition =
        partialDefinition(GraphSnapshotSource.GRAPH, resolvedEdges, 100);

    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            any(),
            argThat(
                (RelationshipFilter filter) ->
                    filter.getDirection() == RelationshipDirection.INCOMING),
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
                        "IsPartOf", CHILD, ROOT, RelationshipDirection.INCOMING, null))));

    BuildResult result =
        builder.buildPartial(
            opContext,
            graphDefinition,
            GraphSnapshotSource.GRAPH,
            Set.of(ROOT),
            TraversalDirection.REVERSE,
            null,
            null,
            null);

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    assertNotNull(result.getSnapshot());
    assertTrue(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.REVERSE));
    assertEquals(result.getSnapshot().getEdges().size(), 1);
    assertEquals(result.getSnapshot().getEdges().get(0).getSourceUrn(), CHILD);
    assertEquals(result.getSnapshot().getEdges().get(0).getDestinationUrn(), ROOT);
  }

  @Test
  public void ancestorsMultiHopUsesPrimaryAspectLookup() {
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              if (urns.contains(UrnUtils.getUrn(GRANDCHILD))) {
                return aspectBatch(GRANDCHILD, CHILD);
              }
              if (urns.contains(UrnUtils.getUrn(CHILD))) {
                return aspectBatch(CHILD, ROOT);
              }
              return Map.of();
            });

    BuildResult result =
        builder.buildPartial(
            opContext,
            primaryDefinition,
            GraphSnapshotSource.PRIMARY,
            Set.of(GRANDCHILD),
            TraversalDirection.FORWARD,
            null,
            null,
            null);

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    assertTrue(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.FORWARD));
    assertEquals(result.getSnapshot().getEdges().size(), 2);
  }

  @Test
  public void vertexLimitMarksCoverageIncomplete() {
    EntityGraphDefinition tightBounds =
        partialDefinition(GraphSnapshotSource.GRAPH, resolvedEdges, 2);

    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            any(),
            argThat(
                (RelationshipFilter filter) ->
                    filter.getDirection() == RelationshipDirection.INCOMING),
            any(),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2,
                2,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf", CHILD, ROOT, RelationshipDirection.INCOMING, null),
                    new RelatedEntities(
                        "IsPartOf", GRANDCHILD, CHILD, RelationshipDirection.INCOMING, null))));

    BuildResult result =
        builder.buildPartial(
            opContext,
            tightBounds,
            GraphSnapshotSource.GRAPH,
            Set.of(ROOT),
            TraversalDirection.REVERSE,
            null,
            null,
            null);

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    assertFalse(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.REVERSE));
    assertEquals(
        result.getSnapshot().getTraversalCoverage().getDirections().get(0).getTruncationReason(),
        "vertex_limit");
  }

  @Test
  public void extendMergesCoverageForSecondDirectionUsingGraphScroll() {
    EntityGraphDefinition graphDefinition =
        partialDefinition(GraphSnapshotSource.GRAPH, resolvedEdges, 100);

    EntityGraphSnapshot.DirectedEdge edge =
        EntityGraphSnapshot.DirectedEdge.builder()
            .sourceUrn(CHILD)
            .destinationUrn(ROOT)
            .relationshipType("IsPartOf")
            .build();
    TraversalCoverage ancestorsOnly =
        TraversalCoverage.builder()
            .direction(
                TraversalCoverage.DirectionCoverage.builder()
                    .direction(TraversalDirection.FORWARD)
                    .explored(true)
                    .exploredDepth(1)
                    .configuredMaxDepth(2)
                    .complete(true)
                    .build())
            .build();

    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            any(),
            argThat(
                (RelationshipFilter filter) ->
                    filter.getDirection() == RelationshipDirection.INCOMING),
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
                        "IsPartOf", CHILD, ROOT, RelationshipDirection.INCOMING, null))));

    BuildResult result =
        builder.buildPartial(
            opContext,
            graphDefinition,
            GraphSnapshotSource.GRAPH,
            Set.of(ROOT),
            TraversalDirection.REVERSE,
            List.of(edge),
            ancestorsOnly,
            "domain@graph:stablekey");

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    assertEquals(result.getSnapshot().getCacheKey(), "domain@graph:stablekey");
    assertTrue(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.FORWARD));
    assertTrue(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.REVERSE));
    assertTrue(result.getSnapshot().getTraversalCoverage().isStrictImprovementOver(ancestorsOnly));
  }

  @Test
  public void searchNotConfiguredWhenNoSearchableEdges() {
    BuildResult result =
        builder.buildPartial(
            opContext,
            primaryDefinition,
            GraphSnapshotSource.SEARCH,
            Set.of(ROOT),
            TraversalDirection.FORWARD,
            null,
            null,
            null);

    assertEquals(result.getStatus(), CacheStatus.INVALID);
    assertEquals(result.getFailureReason(), "search_not_configured");
  }

  @Test
  public void edgeLimitMarksCoverageIncomplete() {
    EntityGraphDefinition tightEdges =
        EntityGraphDefinition.builder()
            .graphId(GRAPH_ID)
            .resolvedEdges(resolvedEdges)
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(2).build())
            .bounds(GraphBounds.builder().maxVertices(100).maxEdges(OptionalInt.of(0)).build())
            .bindings(GraphBindings.builder().build())
            .localEviction(
                LocalEvictionLimits.builder()
                    .enabled(true)
                    .maxViews(8)
                    .maxEstimatedBytes(1024)
                    .build())
            .buildSource(GraphSnapshotSource.GRAPH)
            .scrollBatchSize(100)
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
                        "IsPartOf", CHILD, ROOT, RelationshipDirection.INCOMING, null))));

    BuildResult result =
        builder.buildPartial(
            opContext,
            tightEdges,
            GraphSnapshotSource.GRAPH,
            Set.of(ROOT),
            TraversalDirection.REVERSE,
            null,
            null,
            null);

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    assertFalse(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.REVERSE));
    assertEquals(
        result.getSnapshot().getTraversalCoverage().getDirections().get(0).getTruncationReason(),
        "edge_limit");
  }

  private EntityGraphDefinition partialDefinition(
      GraphSnapshotSource buildSource, List<ResolvedGraphEdge> edges, int maxVertices) {
    return EntityGraphDefinition.builder()
        .graphId(GRAPH_ID)
        .resolvedEdges(edges)
        .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(2).build())
        .bindings(GraphBindings.builder().build())
        .bounds(
            GraphBounds.builder().maxVertices(maxVertices).maxEdges(OptionalInt.of(150)).build())
        .populationIntervalSeconds(300)
        .localEviction(
            LocalEvictionLimits.builder().enabled(true).maxViews(8).maxEstimatedBytes(1024).build())
        .buildSource(buildSource)
        .scrollBatchSize(100)
        .enabled(true)
        .build();
  }

  private OperationContext opContextWithSearch(SearchRetriever searchRetriever) {
    EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
    return TestOperationContexts.systemContext(
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
  }

  private static ScrollResult searchScrollResult(String sourceUrn, String destUrn) {
    SearchEntity entity =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn(sourceUrn))
            .setExtraFields(new StringMap(Map.of("parentDomain", destUrn)));
    return new ScrollResult().setEntities(new SearchEntityArray(entity));
  }

  private static Map<Urn, Map<String, Aspect>> aspectBatch(String sourceUrn, String destUrn) {
    DomainProperties properties = new DomainProperties().setParentDomain(UrnUtils.getUrn(destUrn));
    return Map.of(
        UrnUtils.getUrn(sourceUrn), Map.of("domainProperties", new Aspect(properties.data())));
  }
}
