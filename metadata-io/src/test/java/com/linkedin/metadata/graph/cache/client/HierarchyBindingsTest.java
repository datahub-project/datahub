package com.linkedin.metadata.graph.cache.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
import java.util.Optional;
import org.testng.annotations.Test;

public class HierarchyBindingsTest {

  private static final EntityGraphBinding DOMAIN_BINDING =
      EntityGraphBinding.builder().graphId("domain").source(GraphSnapshotSource.SEARCH).build();

  private static final EntityGraphBinding GLOSSARY_BINDING =
      EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();

  private static final EntityGraphBinding CONTAINER_BINDING =
      EntityGraphBinding.builder().graphId("container").source(GraphSnapshotSource.GRAPH).build();

  @Test
  public void domainSpecUsesLiveBindingWhenPresent() {
    OperationContext opContext = contextWithCache(cacheWithDomainBinding());

    HierarchyReadSpec spec = HierarchyBindings.domainSpec(opContext);

    assertEquals(spec.getBinding().getGraphId(), "domain");
    assertEquals(spec.getRelationshipType(), "IsPartOf");
  }

  @Test
  public void glossarySpecUsesLiveBindingWhenPresent() {
    OperationContext opContext = contextWithCache(cacheWithGlossaryBinding());

    HierarchyReadSpec spec = HierarchyBindings.glossarySpec(opContext);

    assertEquals(spec.getBinding().getGraphId(), "glossary");
  }

  @Test
  public void containerSpecUsesLiveBindingWhenPresent() {
    OperationContext opContext = contextWithCache(cacheWithContainerBinding());

    HierarchyReadSpec spec = HierarchyBindings.containerSpec(opContext);

    assertEquals(spec.getBinding().getGraphId(), "container");
    assertEquals(spec.getRelationshipType(), "IsPartOf");
  }

  @Test
  public void containerSpecFallsBackWhenBindingMissing() {
    OperationContext opContext = contextWithCache(EntityGraphCache.NO_OP);

    HierarchyReadSpec spec = HierarchyBindings.containerSpec(opContext);

    assertEquals(spec.getBinding().getGraphId(), KnownEntityGraph.CONTAINER.getConfigKey());
    assertEquals(
        spec.getBinding().getSource(), KnownEntityGraph.CONTAINER.getExpectedBuildSource());
  }

  @Test
  public void resolveByFilterFieldWithFallbackUsesContainerKnownGraphBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForFilterField("container.keyword")).thenReturn(Optional.empty());
    when(cache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
        .thenReturn(Optional.of(CONTAINER_BINDING));
    OperationContext opContext = contextWithCache(cache);

    Optional<HierarchyReadSpec> spec =
        HierarchyBindings.resolveByFilterFieldWithFallback(
            opContext, "container.keyword", KnownEntityGraph.CONTAINER);

    assertTrue(spec.isPresent());
    assertEquals(spec.get().getBinding().getGraphId(), "container");
  }

  @Test
  public void domainSpecFallsBackWhenBindingMissing() {
    OperationContext opContext = contextWithCache(EntityGraphCache.NO_OP);

    HierarchyReadSpec spec = HierarchyBindings.domainSpec(opContext);

    assertEquals(spec.getBinding().getGraphId(), KnownEntityGraph.DOMAIN.getConfigKey());
    assertEquals(spec.getBinding().getSource(), KnownEntityGraph.DOMAIN.getExpectedBuildSource());
  }

  @Test
  public void resolveByPolicyFieldReturnsDomainSpec() {
    OperationContext opContext = contextWithCache(cacheWithPolicyBinding());

    Optional<HierarchyReadSpec> spec = HierarchyBindings.resolveByPolicyField(opContext, "DOMAIN");

    assertTrue(spec.isPresent());
    assertEquals(spec.get().getBinding().getGraphId(), "domain");
  }

  @Test
  public void resolveByPolicyFieldReturnsEmptyForUnknownField() {
    OperationContext opContext = contextWithCache(cacheWithPolicyBinding());

    assertFalse(HierarchyBindings.resolveByPolicyField(opContext, "UNKNOWN").isPresent());
  }

  @Test
  public void resolveByFilterFieldReturnsDomainSpec() {
    OperationContext opContext = contextWithCache(cacheWithFilterBinding());

    Optional<HierarchyReadSpec> spec =
        HierarchyBindings.resolveByFilterField(opContext, "domains.keyword");

    assertTrue(spec.isPresent());
    assertEquals(spec.get().getBinding().getGraphId(), "domain");
  }

  @Test
  public void resolveByFilterFieldWithFallbackUsesKnownGraphBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForFilterField("domains.keyword")).thenReturn(Optional.empty());
    when(cache.bindingForKnownGraph(KnownEntityGraph.DOMAIN))
        .thenReturn(Optional.of(DOMAIN_BINDING));
    OperationContext opContext = contextWithCache(cache);

    Optional<HierarchyReadSpec> spec =
        HierarchyBindings.resolveByFilterFieldWithFallback(
            opContext, "domains.keyword", KnownEntityGraph.DOMAIN);

    assertTrue(spec.isPresent());
    assertEquals(spec.get().getBinding().getGraphId(), "domain");
  }

  @Test
  public void resolveByPolicyFieldWithFallbackUsesKnownGraphBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForPolicyField("DOMAIN")).thenReturn(Optional.empty());
    when(cache.bindingForKnownGraph(KnownEntityGraph.DOMAIN))
        .thenReturn(Optional.of(DOMAIN_BINDING));
    OperationContext opContext = contextWithCache(cache);

    Optional<HierarchyReadSpec> spec =
        HierarchyBindings.resolveByPolicyFieldWithFallback(
            opContext, "DOMAIN", KnownEntityGraph.DOMAIN);

    assertTrue(spec.isPresent());
    assertEquals(spec.get().getBinding().getGraphId(), "domain");
  }

  private static EntityGraphCache cacheWithDomainBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForKnownGraph(KnownEntityGraph.DOMAIN))
        .thenReturn(Optional.of(DOMAIN_BINDING));
    return cache;
  }

  private static EntityGraphCache cacheWithGlossaryBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(GLOSSARY_BINDING));
    return cache;
  }

  private static EntityGraphCache cacheWithContainerBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
        .thenReturn(Optional.of(CONTAINER_BINDING));
    return cache;
  }

  private static EntityGraphCache cacheWithPolicyBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForPolicyField("DOMAIN")).thenReturn(Optional.of(DOMAIN_BINDING));
    return cache;
  }

  private static EntityGraphCache cacheWithFilterBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForFilterField("domains.keyword")).thenReturn(Optional.of(DOMAIN_BINDING));
    return cache;
  }

  private static OperationContext contextWithCache(EntityGraphCache cache) {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(mock(AspectRetriever.class))
            .entityGraphCache(cache)
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }
}
