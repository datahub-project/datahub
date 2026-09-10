package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

  private void mockParents(Map<Urn, Urn> parentByUrn) {
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
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
  }
}
