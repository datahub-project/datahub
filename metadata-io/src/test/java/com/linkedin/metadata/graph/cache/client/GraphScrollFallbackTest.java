package com.linkedin.metadata.graph.cache.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class GraphScrollFallbackTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:domain:root");
  private static final Urn CHILD_A = UrnUtils.getUrn("urn:li:domain:childA");
  private static final Urn CHILD_B = UrnUtils.getUrn("urn:li:domain:childB");
  private static final Urn GRANDCHILD_A = UrnUtils.getUrn("urn:li:domain:grandchildA");
  private static final Urn GRANDCHILD_B = UrnUtils.getUrn("urn:li:domain:grandchildB");

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
                        CHILD_A.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    OperationContext opContext = contextWithGraphRetriever(graphRetriever);
    DirectChildrenResult result =
        GraphScrollFallback.directChildren(
            opContext, HierarchyBindings.domainSpec(opContext), ROOT);

    assertEquals(result.getChildUrns(), Set.of(CHILD_A));
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
                        CHILD_A.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null))))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, List.of()));

    OperationContext opContext = contextWithGraphRetriever(graphRetriever);

    assertEquals(
        GraphScrollFallback.allDescendants(
            opContext, HierarchyBindings.domainSpec(opContext), ROOT),
        Set.of(CHILD_A));
  }

  @Test
  public void allDescendantsBatchesFrontierPerLevel() {
    // root -> {A, B}; A -> {grandchildA}; B -> {grandchildB}
    // Batched BFS: 3 scrolls (one per depth). Per-parent recursion would need 5.
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
                2,
                2,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        CHILD_A.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null),
                    new RelatedEntities(
                        "IsPartOf",
                        CHILD_B.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null))))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2,
                2,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        GRANDCHILD_A.toString(),
                        CHILD_A.toString(),
                        RelationshipDirection.OUTGOING,
                        null),
                    new RelatedEntities(
                        "IsPartOf",
                        GRANDCHILD_B.toString(),
                        CHILD_B.toString(),
                        RelationshipDirection.OUTGOING,
                        null))))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, List.of()));

    OperationContext opContext = contextWithGraphRetriever(graphRetriever);

    assertEquals(
        GraphScrollFallback.allDescendants(
            opContext, HierarchyBindings.domainSpec(opContext), ROOT),
        Set.of(CHILD_A, CHILD_B, GRANDCHILD_A, GRANDCHILD_B));

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(graphRetriever, times(3))
        .scrollRelatedEntities(
            eq(Set.of("domain")),
            isNull(),
            eq(Set.of("domain")),
            filterCaptor.capture(),
            eq(Set.of("IsPartOf")),
            any(),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull());

    List<Set<String>> parentsPerScroll =
        filterCaptor.getAllValues().stream()
            .map(GraphScrollFallbackTest::urnsInFilter)
            .collect(Collectors.toList());
    assertEquals(parentsPerScroll.get(0), Set.of(ROOT.toString()));
    assertEquals(parentsPerScroll.get(1), Set.of(CHILD_A.toString(), CHILD_B.toString()));
    assertEquals(parentsPerScroll.get(2), Set.of(GRANDCHILD_A.toString(), GRANDCHILD_B.toString()));
    // Wide frontiers must use one multi-value EQUAL criterion (termsQuery), not N OR clauses.
    assertEquals(filterCaptor.getAllValues().get(1).getOr().size(), 1);
    assertEquals(filterCaptor.getAllValues().get(1).getOr().get(0).getAnd().size(), 1);
    assertEquals(
        filterCaptor.getAllValues().get(1).getOr().get(0).getAnd().get(0).getValues().size(), 2);
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

  private static Set<String> urnsInFilter(Filter filter) {
    Set<String> urns = new HashSet<>();
    for (ConjunctiveCriterion orClause : filter.getOr()) {
      orClause.getAnd().stream()
          .filter(criterion -> "urn".equals(criterion.getField()))
          .forEach(criterion -> urns.addAll(criterion.getValues()));
    }
    return urns;
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
