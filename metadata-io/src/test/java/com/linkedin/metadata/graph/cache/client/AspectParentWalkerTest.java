package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectParentWalkerTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:domain:root");
  private static final Urn CHILD = UrnUtils.getUrn("urn:li:domain:child");
  private static final Urn GRANDCHILD = UrnUtils.getUrn("urn:li:domain:grandchild");

  private OperationContext opContext;
  private AspectRetriever aspectRetriever;
  private HierarchyReadSpec spec;

  @BeforeMethod
  public void setUp() {
    aspectRetriever = mock(AspectRetriever.class);
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
    spec = HierarchyBindings.domainSpec(opContext);
  }

  @Test
  public void orderedParentsWalksAspectChain() {
    Map<Urn, Urn> parentByUrn = new LinkedHashMap<>();
    parentByUrn.put(GRANDCHILD, CHILD);
    parentByUrn.put(CHILD, ROOT);
    parentByUrn.put(ROOT, null);
    mockParents(parentByUrn);

    List<Urn> parents = AspectParentWalker.orderedParents(opContext, spec, GRANDCHILD, 10);
    assertEquals(parents, List.of(CHILD, ROOT));
  }

  @Test
  public void isAncestorOfDetectsCycleSafeMatch() {
    Map<Urn, Urn> parentByUrn = new LinkedHashMap<>();
    parentByUrn.put(GRANDCHILD, CHILD);
    parentByUrn.put(CHILD, ROOT);
    parentByUrn.put(ROOT, null);
    mockParents(parentByUrn);

    assertTrue(AspectParentWalker.isAncestorOf(opContext, spec, GRANDCHILD, ROOT));
  }

  @Test
  public void orderedParentsStopsAtSoftDeletedParent() {
    Map<Urn, Urn> parentByUrn = new LinkedHashMap<>();
    parentByUrn.put(GRANDCHILD, CHILD);
    parentByUrn.put(CHILD, ROOT);
    parentByUrn.put(ROOT, null);
    mockParents(parentByUrn, Set.of(CHILD));

    // CHILD is soft-deleted, so the chain ends there -- ROOT is not reported even though it is
    // live, because it is only reachable through the deleted CHILD.
    List<Urn> parents = AspectParentWalker.orderedParents(opContext, spec, GRANDCHILD, 10);
    assertEquals(parents, List.of());
  }

  @Test
  public void orderedParentsTruncatesAtSoftDeletedAncestor() {
    Map<Urn, Urn> parentByUrn = new LinkedHashMap<>();
    parentByUrn.put(GRANDCHILD, CHILD);
    parentByUrn.put(CHILD, ROOT);
    parentByUrn.put(ROOT, null);
    mockParents(parentByUrn, Set.of(ROOT));

    List<Urn> parents = AspectParentWalker.orderedParents(opContext, spec, GRANDCHILD, 10);
    assertEquals(parents, List.of(CHILD));
  }

  @Test
  public void orderedParentsTreatsStatusReadFailureAsLive() {
    Map<Urn, Urn> parentByUrn = new LinkedHashMap<>();
    parentByUrn.put(GRANDCHILD, CHILD);
    parentByUrn.put(CHILD, null);
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<String> aspectNames = invocation.getArgument(2);
              if (aspectNames.contains(STATUS_ASPECT_NAME)) {
                throw new RuntimeException("status read failed");
              }
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              for (Urn urn : urns) {
                Urn parent = parentByUrn.get(urn);
                DomainProperties properties = new DomainProperties();
                if (parent != null) {
                  properties.setParentDomain(parent);
                }
                result.put(
                    urn, Map.of(DOMAIN_PROPERTIES_ASPECT_NAME, new Aspect(properties.data())));
              }
              return result;
            });

    // Failing open: a transient status read failure must not silently truncate the breadcrumb.
    List<Urn> parents = AspectParentWalker.orderedParents(opContext, spec, GRANDCHILD, 10);
    assertEquals(parents, List.of(CHILD));
  }

  private void mockParents(Map<Urn, Urn> parentByUrn) {
    mockParents(parentByUrn, Set.of());
  }

  /**
   * @param softDeleted urns whose {@code status} aspect reports {@code removed = true}
   */
  private void mockParents(Map<Urn, Urn> parentByUrn, Set<Urn> softDeleted) {
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              @SuppressWarnings("unchecked")
              Set<String> aspectNames = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              for (Urn urn : urns) {
                Map<String, Aspect> aspects = new LinkedHashMap<>();
                if (aspectNames.contains(DOMAIN_PROPERTIES_ASPECT_NAME)) {
                  Urn parent = parentByUrn.get(urn);
                  DomainProperties properties = new DomainProperties();
                  if (parent != null) {
                    properties.setParentDomain(parent);
                  }
                  aspects.put(DOMAIN_PROPERTIES_ASPECT_NAME, new Aspect(properties.data()));
                }
                if (aspectNames.contains(STATUS_ASPECT_NAME)) {
                  aspects.put(
                      STATUS_ASPECT_NAME,
                      new Aspect(new Status().setRemoved(softDeleted.contains(urn)).data()));
                }
                result.put(urn, aspects);
              }
              return result;
            });
  }
}
