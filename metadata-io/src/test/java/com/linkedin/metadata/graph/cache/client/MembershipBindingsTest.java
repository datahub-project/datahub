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

public class MembershipBindingsTest {

  private static final EntityGraphBinding MEMBERSHIP_BINDING =
      EntityGraphBinding.builder().graphId("membership").source(GraphSnapshotSource.GRAPH).build();

  @Test
  public void membershipSpecUsesLiveBindingWhenPresent() {
    OperationContext opContext = contextWithCache(cacheWithMembershipBinding());

    MembershipReadSpec spec = MembershipBindings.membershipSpec(opContext);

    assertEquals(spec.getBinding().getGraphId(), "membership");
    assertEquals(spec.getBinding().getSource(), GraphSnapshotSource.GRAPH);
    assertTrue(spec.getGroupRelationshipTypes().contains("IsMemberOfGroup"));
  }

  @Test
  public void membershipSpecFallsBackWhenBindingMissing() {
    OperationContext opContext = contextWithCache(EntityGraphCache.NO_OP);

    MembershipReadSpec spec = MembershipBindings.membershipSpec(opContext);

    assertEquals(spec.getBinding().getGraphId(), KnownEntityGraph.MEMBERSHIP.getConfigKey());
    assertEquals(
        spec.getBinding().getSource(), KnownEntityGraph.MEMBERSHIP.getExpectedBuildSource());
  }

  @Test
  public void resolveReturnsSpecWhenBindingPresent() {
    OperationContext opContext = contextWithCache(cacheWithMembershipBinding());

    Optional<MembershipReadSpec> spec = MembershipBindings.resolve(opContext);

    assertTrue(spec.isPresent());
    assertEquals(spec.get().getBinding().getGraphId(), "membership");
  }

  @Test
  public void resolveReturnsEmptyWhenBindingMissing() {
    OperationContext opContext = contextWithCache(EntityGraphCache.NO_OP);

    assertFalse(MembershipBindings.resolve(opContext).isPresent());
  }

  private static EntityGraphCache cacheWithMembershipBinding() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.bindingForKnownGraph(KnownEntityGraph.MEMBERSHIP))
        .thenReturn(Optional.of(MEMBERSHIP_BINDING));
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
