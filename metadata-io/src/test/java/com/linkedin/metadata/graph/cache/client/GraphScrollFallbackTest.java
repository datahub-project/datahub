package com.linkedin.metadata.graph.cache.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class GraphScrollFallbackTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:domain:root");
  private static final Urn CHILD = UrnUtils.getUrn("urn:li:domain:child");

  @Test
  public void directChildrenScrollsOutgoingIsPartOfEdges() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            eq(Set.of("domain")),
            isNull(),
            eq(Set.of("domain")),
            any(),
            eq(Set.of("IsPartOf")),
            any(),
            eq(Edge.EDGE_SORT_CRITERION),
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
                        CHILD.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    OperationContext opContext = contextWithGraphRetriever(graphRetriever);
    DirectChildrenResult result =
        GraphScrollFallback.directChildren(
            opContext, HierarchyBindings.domainSpec(opContext), ROOT);

    assertEquals(result.getChildUrns(), Set.of(CHILD));
    assertFalse(result.isTruncated());
  }

  @Test
  public void directChildrenReturnsEmptyWhenGraphRetrieverEmpty() {
    OperationContext opContext = contextWithGraphRetriever(GraphRetriever.EMPTY);

    DirectChildrenResult result =
        GraphScrollFallback.directChildren(
            opContext, HierarchyBindings.domainSpec(opContext), ROOT);

    assertTrue(result.getChildUrns().isEmpty());
    assertFalse(result.isTruncated());
  }

  @Test
  public void allDescendantsCollectsNestedChildren() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            eq(Set.of("domain")),
            isNull(),
            eq(Set.of("domain")),
            any(),
            eq(Set.of("IsPartOf")),
            any(),
            eq(Edge.EDGE_SORT_CRITERION),
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
                        CHILD.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null))))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, List.of()));

    OperationContext opContext = contextWithGraphRetriever(graphRetriever);

    assertEquals(
        GraphScrollFallback.allDescendants(
            opContext, HierarchyBindings.domainSpec(opContext), ROOT),
        Set.of(CHILD));
  }

  @Test
  public void directChildrenMarksTruncatedOnScrollFailure() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenThrow(new RuntimeException("scroll failed"));

    OperationContext opContext = contextWithGraphRetriever(graphRetriever);

    DirectChildrenResult result =
        GraphScrollFallback.directChildren(
            opContext, HierarchyBindings.domainSpec(opContext), ROOT);

    assertTrue(result.isTruncated());
    assertTrue(result.getChildUrns().isEmpty());
  }

  private static OperationContext contextWithGraphRetriever(GraphRetriever graphRetriever) {
    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.DOMAIN))
        .thenReturn(
            Optional.of(
                EntityGraphBinding.builder()
                    .graphId("domain")
                    .source(GraphSnapshotSource.SEARCH)
                    .build()));
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(graphRetriever)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(mock(AspectRetriever.class))
            .entityGraphCache(entityGraphCache)
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }
}
