package com.linkedin.metadata.graph.cache.client;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class EntityGraphCacheClientsTest {

  private static final EntityGraphBinding DOMAIN_BINDING =
      EntityGraphBinding.builder().graphId("domain").source(GraphSnapshotSource.SEARCH).build();

  @Test
  public void walkOrderedForwardAncestorsUsesEphemeralWalkWhenSkipCache() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq("urn:li:domain:child"),
            eq(5),
            eq(ReadMode.EPHEMERAL)))
        .thenReturn(AncestorWalkResult.fromAncestors(List.of("urn:li:domain:root")));

    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization()
            .withSearchFlags(flags -> flags.setSkipCache(true));

    AncestorWalkResult ancestors =
        EntityGraphCacheClients.walkOrderedForwardAncestors(
            opContext, cache, DOMAIN_BINDING, "urn:li:domain:child", 5);

    assertEquals(ancestors.ancestorsOrEmpty(), List.of("urn:li:domain:root"));
    verify(cache)
        .walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq("urn:li:domain:child"),
            eq(5),
            eq(ReadMode.EPHEMERAL));
  }

  @Test
  public void expandReturnsEmptyHitForEmptyRootsWithoutCallingCache() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();

    GraphReadResult result =
        EntityGraphCacheClients.expand(
            GraphExpandRequest.builder()
                .opContext(opContext)
                .cache(cache)
                .binding(DOMAIN_BINDING)
                .direction(com.linkedin.metadata.graph.cache.TraversalDirection.REVERSE)
                .roots(Collections.emptyList())
                .limit(100)
                .build());

    assertTrue(result instanceof GraphReadResult.EmptyHit);
    verify(cache, never())
        .expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(com.linkedin.metadata.graph.cache.TraversalDirection.REVERSE),
            eq(Collections.emptyList()),
            eq(100),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED));
  }

  @Test
  public void expandUsesCachedModeWhenSkipCacheFalse() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(com.linkedin.metadata.graph.cache.TraversalDirection.FORWARD),
            eq(Set.of("urn:li:domain:root")),
            eq(50),
            eq(10),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(Set.of("urn:li:domain:root")));

    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();

    GraphReadResult result =
        EntityGraphCacheClients.expand(
            GraphExpandRequest.builder()
                .opContext(opContext)
                .cache(cache)
                .binding(DOMAIN_BINDING)
                .direction(com.linkedin.metadata.graph.cache.TraversalDirection.FORWARD)
                .roots(Set.of("urn:li:domain:root"))
                .limit(50)
                .maxDepth(10)
                .build());

    assertEquals(result.verticesOrEmpty(), Set.of("urn:li:domain:root"));
  }

  private static final EntityGraphBinding MEMBERSHIP_BINDING =
      EntityGraphBinding.builder().graphId("membership").source(GraphSnapshotSource.GRAPH).build();

  @Test
  public void listRelatedUsesCachedModeWhenSkipCacheFalse() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq("urn:li:corpuser:alice"),
            eq(TraversalDirection.FORWARD),
            eq(Set.of("IsMemberOfGroup")),
            eq(1),
            eq(0),
            eq(10),
            eq(ReadMode.CACHED)))
        .thenReturn(
            MembershipNeighborResult.fromNeighbors(
                List.of(
                    new MembershipNeighborResult.Neighbor(
                        "urn:li:corpGroup:eng", "IsMemberOfGroup")),
                1));

    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();

    MembershipNeighborResult result =
        EntityGraphCacheClients.listRelated(
            opContext,
            cache,
            MEMBERSHIP_BINDING,
            "urn:li:corpuser:alice",
            TraversalDirection.FORWARD,
            Set.of("IsMemberOfGroup"),
            1,
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().size(), 1);
  }

  @Test
  public void listRelatedUsesEphemeralModeWhenSkipCacheTrue() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    when(cache.listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq("urn:li:corpuser:alice"),
            eq(TraversalDirection.FORWARD),
            eq(Set.of("IsMemberOfGroup")),
            eq(1),
            eq(0),
            eq(10),
            eq(ReadMode.EPHEMERAL)))
        .thenReturn(MembershipNeighborResult.fromNeighbors(List.of(), 0));

    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization()
            .withSearchFlags(flags -> flags.setSkipCache(true));

    EntityGraphCacheClients.listRelated(
        opContext,
        cache,
        MEMBERSHIP_BINDING,
        "urn:li:corpuser:alice",
        TraversalDirection.FORWARD,
        Set.of("IsMemberOfGroup"),
        1,
        0,
        10);

    verify(cache)
        .listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq("urn:li:corpuser:alice"),
            eq(TraversalDirection.FORWARD),
            eq(Set.of("IsMemberOfGroup")),
            eq(1),
            eq(0),
            eq(10),
            eq(ReadMode.EPHEMERAL));
  }

  @Test
  public void walkOrderedForwardAncestorsReturnsEmptyForNonPositiveDepth() {
    EntityGraphCache cache = mock(EntityGraphCache.class);
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();

    AncestorWalkResult result =
        EntityGraphCacheClients.walkOrderedForwardAncestors(
            opContext, cache, DOMAIN_BINDING, "urn:li:domain:child", 0);

    assertEquals(result.ancestorsOrEmpty(), List.of());
    verify(cache, never())
        .walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq("urn:li:domain:child"),
            eq(0),
            eq(ReadMode.CACHED));
  }
}
