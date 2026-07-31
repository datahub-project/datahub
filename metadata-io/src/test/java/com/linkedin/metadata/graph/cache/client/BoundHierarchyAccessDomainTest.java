package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BoundHierarchyAccessDomainTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:domain:root");
  private static final Urn CHILD = UrnUtils.getUrn("urn:li:domain:child");
  private static final Urn GRANDCHILD = UrnUtils.getUrn("urn:li:domain:grandchild");

  private EntityGraphCache entityGraphCache;
  private AspectRetriever aspectRetriever;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    entityGraphCache = mock(EntityGraphCache.class);
    aspectRetriever = mock(AspectRetriever.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("domain").source(GraphSnapshotSource.SEARCH).build();
    when(entityGraphCache.bindingForPolicyField("DOMAIN")).thenReturn(Optional.empty());
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.DOMAIN))
        .thenReturn(Optional.of(binding));

    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(aspectRetriever)
            .entityGraphCache(entityGraphCache)
            .build();
    opContext =
        base.toBuilder()
            .retrieverContext(retrieverContext)
            .build(base.getSessionAuthentication(), false);
  }

  private HierarchyReadSpec domainSpec(@Nonnull OperationContext context) {
    return HierarchyBindings.domainSpec(context);
  }

  @Test
  public void resolveOrderedParentUrnsUsesCacheEdgeWalkAfterExpand() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(GRANDCHILD.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(GRANDCHILD.toString(), CHILD.toString(), ROOT.toString())));
    when(entityGraphCache.walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(GRANDCHILD.toString()),
            eq(10),
            eq(ReadMode.CACHED)))
        .thenReturn(AncestorWalkResult.fromAncestors(List.of(CHILD.toString(), ROOT.toString())));

    List<Urn> parents =
        BoundHierarchyAccess.orderedParents(opContext, domainSpec(opContext), GRANDCHILD, 10);
    assertEquals(parents, List.of(CHILD, ROOT));
    verify(entityGraphCache)
        .walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(GRANDCHILD.toString()),
            eq(10),
            eq(ReadMode.CACHED));
    verify(aspectRetriever, atMost(1)).getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void orderedParentsUsesCacheWalkWithoutAspectCrossCheck() {
    Urn otherParent = UrnUtils.getUrn("urn:li:domain:other-parent");
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(GRANDCHILD.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(GRANDCHILD.toString(), CHILD.toString(), ROOT.toString())));
    when(entityGraphCache.walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(GRANDCHILD.toString()),
            eq(10),
            eq(ReadMode.CACHED)))
        .thenReturn(
            AncestorWalkResult.fromAncestors(List.of(otherParent.toString(), ROOT.toString())));

    List<Urn> parents =
        BoundHierarchyAccess.orderedParents(opContext, domainSpec(opContext), GRANDCHILD, 10);
    assertEquals(parents, List.of(otherParent, ROOT));
  }

  @Test
  public void orderedParentsFallsBackToAspectsWhenWalkMisses() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(GRANDCHILD.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(Set.of(GRANDCHILD.toString())));
    when(entityGraphCache.walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(GRANDCHILD.toString()),
            eq(10),
            eq(ReadMode.CACHED)))
        .thenReturn(AncestorWalkResult.miss(ReadMissReason.STALE_BLOCKED));

    Map<Urn, Urn> parentByDomain = new LinkedHashMap<>();
    parentByDomain.put(GRANDCHILD, CHILD);
    parentByDomain.put(CHILD, ROOT);
    parentByDomain.put(ROOT, null);
    mockDomainProperties(parentByDomain);

    List<Urn> parents =
        BoundHierarchyAccess.orderedParents(opContext, domainSpec(opContext), GRANDCHILD, 10);
    assertEquals(parents, List.of(CHILD, ROOT));
  }

  @Test
  public void resolveDirectChildDomainUrnsUsesMaxDepthOneReverseExpand() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(ROOT.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(Set.of(ROOT.toString(), CHILD.toString())));

    mockDomainProperties(parentMap(ROOT, null, CHILD, ROOT));

    Set<Urn> children =
        BoundHierarchyAccess.directChildUrns(opContext, domainSpec(opContext), ROOT);
    assertEquals(children, Set.of(CHILD));
  }

  @Test
  public void resolveDirectChildDomainsScrollsAllPagesWhenCacheMisses() {
    Urn child1 = UrnUtils.getUrn("urn:li:domain:child1");
    Urn child2 = UrnUtils.getUrn("urn:li:domain:child2");
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(ROOT.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.miss(ReadMissReason.STALE_BLOCKED));

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
                "page-2",
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        child1.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null))),
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        child2.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    mockDomainProperties(parentMap(ROOT, null, child1, ROOT, child2, ROOT));

    OperationContext scrollContext = contextWithGraphRetriever(graphRetriever);
    DirectChildrenResult result =
        BoundHierarchyAccess.directChildren(scrollContext, domainSpec(scrollContext), ROOT);
    assertEquals(result.getChildUrns(), Set.of(child1, child2));
    assertFalse(result.isTruncated());
  }

  @Test
  public void isDescendantDomainDetectsNestedDomain() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(ROOT.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(ROOT.toString(), CHILD.toString(), GRANDCHILD.toString())));
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(GRANDCHILD.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(Set.of(GRANDCHILD.toString())));

    mockDomainProperties(parentMap(ROOT, null, CHILD, ROOT, GRANDCHILD, CHILD));

    assertTrue(
        BoundHierarchyAccess.isDescendant(opContext, domainSpec(opContext), GRANDCHILD, ROOT));
    assertFalse(
        BoundHierarchyAccess.isDescendant(opContext, domainSpec(opContext), ROOT, GRANDCHILD));
  }

  @Test
  public void resolveDomainsWithAncestorsIncludesSelfAndParents() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(GRANDCHILD.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(GRANDCHILD.toString(), CHILD.toString(), ROOT.toString())));

    Set<Urn> domains =
        BoundHierarchyAccess.expandAncestors(opContext, domainSpec(opContext), Set.of(GRANDCHILD));
    assertEquals(domains, Set.of(GRANDCHILD, CHILD, ROOT));
    verify(aspectRetriever, never()).getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void resolveDomainsWithAncestorsFallsBackWhenExpandEmptyAfterOverLimit() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(GRANDCHILD.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.miss(ReadMissReason.STALE_BLOCKED));

    Map<Urn, Urn> parentByDomain = new LinkedHashMap<>();
    parentByDomain.put(GRANDCHILD, CHILD);
    parentByDomain.put(CHILD, ROOT);
    parentByDomain.put(ROOT, null);
    mockDomainProperties(parentByDomain);

    Set<Urn> domains =
        BoundHierarchyAccess.expandAncestors(opContext, domainSpec(opContext), Set.of(GRANDCHILD));
    assertEquals(domains, Set.of(GRANDCHILD, CHILD, ROOT));
  }

  @Test
  public void resolveDomainsWithAncestorsFallsBackOnEmptyExpand() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(GRANDCHILD.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.miss(ReadMissReason.STALE_BLOCKED));

    Map<Urn, Urn> parentByDomain = new LinkedHashMap<>();
    parentByDomain.put(GRANDCHILD, CHILD);
    parentByDomain.put(CHILD, ROOT);
    parentByDomain.put(ROOT, null);
    mockDomainProperties(parentByDomain);

    Set<Urn> domains =
        BoundHierarchyAccess.expandAncestors(opContext, domainSpec(opContext), Set.of(GRANDCHILD));
    assertEquals(domains, Set.of(GRANDCHILD, CHILD, ROOT));
  }

  @Test
  public void resolveDomainsWithAncestorsBypassesCacheWhenIncludeSoftDelete() {
    Map<Urn, Urn> parentByDomain = new LinkedHashMap<>();
    parentByDomain.put(GRANDCHILD, CHILD);
    parentByDomain.put(CHILD, ROOT);
    parentByDomain.put(ROOT, null);
    mockDomainProperties(parentByDomain);

    Set<Urn> domains =
        BoundHierarchyAccess.expandAncestors(
            opContext, domainSpec(opContext), Set.of(GRANDCHILD), true);
    assertEquals(domains, Set.of(GRANDCHILD, CHILD, ROOT));
    verify(entityGraphCache, never())
        .expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            any(),
            anyInt(),
            anyInt(),
            any(ReadMode.class));
  }

  @Test
  public void directChildrenReturnsEmptyWhenReverseExpandHasNoDescendants() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(ROOT.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(new GraphReadResult.EmptyHit(Set.of()));

    DirectChildrenResult result =
        BoundHierarchyAccess.directChildren(opContext, domainSpec(opContext), ROOT);
    assertTrue(result.getChildUrns().isEmpty());
    assertFalse(result.isTruncated());
  }

  @Test
  public void isDescendantDomainFallsBackWhenCandidateMissingFromExpand() {
    Urn other = UrnUtils.getUrn("urn:li:domain:other");
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(ROOT.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(Set.of(ROOT.toString(), other.toString())));

    Map<Urn, Urn> parentByDomain = new LinkedHashMap<>();
    parentByDomain.put(GRANDCHILD, CHILD);
    parentByDomain.put(CHILD, ROOT);
    parentByDomain.put(ROOT, null);
    parentByDomain.put(other, ROOT);
    mockDomainProperties(parentByDomain);

    assertTrue(
        BoundHierarchyAccess.isDescendant(opContext, domainSpec(opContext), GRANDCHILD, ROOT));
  }

  @Test
  public void resolveDomainsWithAncestorsUsesAspectsWhenCacheIncomplete() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(GRANDCHILD.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.miss(ReadMissReason.STALE_BLOCKED));

    Map<Urn, Urn> parentByDomain = new LinkedHashMap<>();
    parentByDomain.put(GRANDCHILD, CHILD);
    parentByDomain.put(CHILD, ROOT);
    parentByDomain.put(ROOT, null);
    mockDomainProperties(parentByDomain);

    Set<Urn> domains =
        BoundHierarchyAccess.expandAncestors(opContext, domainSpec(opContext), Set.of(GRANDCHILD));
    assertEquals(domains, Set.of(GRANDCHILD, CHILD, ROOT));
  }

  @Test
  public void resolveDirectChildDomainsTrustsCacheExpandResults() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(ROOT.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(Set.of(ROOT.toString(), CHILD.toString())));

    DirectChildrenResult result =
        BoundHierarchyAccess.directChildren(opContext, domainSpec(opContext), ROOT);
    assertEquals(result.getChildUrns(), Set.of(CHILD));
    assertFalse(result.isTruncated());
  }

  @Test
  public void resolveDirectChildDomainsBypassesCacheWhenIncludeSoftDeleteTrue() {
    Urn child = UrnUtils.getUrn("urn:li:domain:scroll-child");
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
                        child.toString(),
                        ROOT.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    mockDomainProperties(parentMap(ROOT, null, child, ROOT));

    OperationContext scrollContext = contextWithGraphRetriever(graphRetriever);
    DirectChildrenResult result =
        BoundHierarchyAccess.directChildren(scrollContext, domainSpec(scrollContext), ROOT, true);
    assertEquals(result.getChildUrns(), Set.of(child));
    verify(entityGraphCache, never())
        .expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            any(),
            anyInt(),
            anyInt(),
            any(ReadMode.class));
  }

  @Test
  public void scrollDirectChildDomainsHandlesNullEntities() {
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(ROOT.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.miss(ReadMissReason.ABSENT));

    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, null));

    OperationContext scrollContext = contextWithGraphRetriever(graphRetriever);
    DirectChildrenResult result =
        BoundHierarchyAccess.directChildren(scrollContext, domainSpec(scrollContext), ROOT);
    assertTrue(result.getChildUrns().isEmpty());
  }

  private OperationContext contextWithGraphRetriever(GraphRetriever graphRetriever) {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(graphRetriever)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(aspectRetriever)
            .entityGraphCache(entityGraphCache)
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }

  private static Map<Urn, Urn> parentMap(Object... urnAndParent) {
    Map<Urn, Urn> parentByDomain = new LinkedHashMap<>();
    for (int i = 0; i < urnAndParent.length; i += 2) {
      parentByDomain.put((Urn) urnAndParent[i], (Urn) urnAndParent[i + 1]);
    }
    return parentByDomain;
  }

  private void mockDomainProperties(Map<Urn, Urn> parentByDomain) {
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              @SuppressWarnings("unchecked")
              Set<String> aspectNames = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              for (Urn urn : urns) {
                if (!aspectNames.contains(DOMAIN_PROPERTIES_ASPECT_NAME)
                    || !parentByDomain.containsKey(urn)) {
                  continue;
                }
                Urn parent = parentByDomain.get(urn);
                DomainProperties properties = new DomainProperties();
                if (parent != null) {
                  properties.setParentDomain(parent);
                }
                result.put(
                    urn, Map.of(DOMAIN_PROPERTIES_ASPECT_NAME, new Aspect(properties.data())));
              }
              return result;
            });
  }
}
