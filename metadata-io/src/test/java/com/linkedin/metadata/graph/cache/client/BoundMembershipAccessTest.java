package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME;
import static com.linkedin.metadata.Constants.IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.SessionActorIdentity;
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
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.ActorGroupMembershipService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BoundMembershipAccessTest {

  private static final Urn USER = UrnUtils.getUrn("urn:li:corpuser:alice");
  private static final Urn GROUP = UrnUtils.getUrn("urn:li:corpGroup:eng");
  private static final Urn ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Admin");
  private static final Urn INHERITED_ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Editor");

  private static final EntityGraphBinding MEMBERSHIP_BINDING =
      EntityGraphBinding.builder().graphId("membership").source(GraphSnapshotSource.GRAPH).build();

  private MembershipReadSpec spec;
  private EntityGraphCache entityGraphCache;

  @BeforeMethod
  public void setUp() {
    spec = MembershipReadSpecs.membership(MEMBERSHIP_BINDING);
    entityGraphCache = mock(EntityGraphCache.class);
  }

  @Test
  public void listRelatedReturnsEmptyForEmptyRelationshipTypes() {
    OperationContext opContext = contextWithCache(entityGraphCache);

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext, spec, USER, TraversalDirection.FORWARD, Set.of(), 0, 10);

    assertTrue(result.isHit());
    assertTrue(result.neighborsOrEmpty().isEmpty());
    verify(entityGraphCache, never())
        .listRelated(any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any());
  }

  @Test
  public void listRelatedReturnsEmptyForNonPositiveCount() {
    OperationContext opContext = contextWithCache(entityGraphCache);

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            USER,
            TraversalDirection.FORWARD,
            Set.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
            0,
            0);

    assertTrue(result.isHit());
    assertTrue(result.neighborsOrEmpty().isEmpty());
  }

  @Test
  public void listRelatedReturnsCacheHitWhenPresent() {
    when(entityGraphCache.listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq(USER.toString()),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
            eq(1),
            eq(0),
            eq(10),
            eq(ReadMode.CACHED)))
        .thenReturn(
            MembershipNeighborResult.fromNeighbors(
                List.of(
                    new MembershipNeighborResult.Neighbor(
                        GROUP.toString(), IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
                1));

    OperationContext opContext = contextWithCache(entityGraphCache);

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            USER,
            TraversalDirection.FORWARD,
            Set.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), GROUP.toString());
  }

  @Test
  public void listRelatedUsesAspectFallbackForCorpGroupMembership() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    SessionActorIdentity identity = new SessionActorIdentity(USER, List.of(GROUP), Set.of());
    OperationContext opContext =
        contextWithServices(
            entityGraphCache, GraphRetriever.EMPTY, membershipService, identity, Set.of());

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            USER,
            TraversalDirection.FORWARD,
            Set.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), GROUP.toString());
    assertEquals(
        result.neighborsOrEmpty().get(0).relationshipType(), IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME);
  }

  @Test
  public void listRelatedSkipsAspectFallbackForMultipleGroupRelationshipTypes() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, List.of()));

    OperationContext opContext = contextWithCache(entityGraphCache, graphRetriever);

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            USER,
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertTrue(result.neighborsOrEmpty().isEmpty());
  }

  @Test
  public void listRelatedUsesAspectFallbackForDirectUserRoles() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    SessionActorIdentity identity = new SessionActorIdentity(USER, List.of(), Set.of(ROLE));
    OperationContext opContext =
        contextWithServices(
            entityGraphCache, GraphRetriever.EMPTY, membershipService, identity, Set.of());

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            USER,
            TraversalDirection.FORWARD,
            spec.getRoleRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), ROLE.toString());
  }

  @Test
  public void listRelatedUsesAspectFallbackForGroupRoles() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    OperationContext opContext =
        contextWithServices(
            entityGraphCache,
            GraphRetriever.EMPTY,
            membershipService,
            SessionActorIdentity.empty(USER),
            Set.of(ROLE));

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            GROUP,
            TraversalDirection.FORWARD,
            spec.getRoleRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), ROLE.toString());
  }

  @Test
  public void listRelatedPaginatesAspectFallbackNeighbors() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    SessionActorIdentity identity =
        new SessionActorIdentity(
            USER, List.of(GROUP, UrnUtils.getUrn("urn:li:corpGroup:other")), Set.of());
    OperationContext opContext =
        contextWithServices(
            entityGraphCache, GraphRetriever.EMPTY, membershipService, identity, Set.of());

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            USER,
            TraversalDirection.FORWARD,
            Set.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
            1,
            1);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().size(), 1);
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), "urn:li:corpGroup:other");
  }

  @Test
  public void listRelatedFallsBackToScrollWhenAspectUnavailable() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        GROUP.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    OperationContext opContext = contextWithCache(entityGraphCache, graphRetriever);

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            USER,
            TraversalDirection.FORWARD,
            Set.of(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), GROUP.toString());
  }

  @Test
  public void listRelatedSkipsCacheWhenIncludeSoftDelete() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, List.of()));

    OperationContext opContext = contextWithCache(entityGraphCache, graphRetriever);

    BoundMembershipAccess.listRelated(
        opContext,
        spec,
        USER,
        TraversalDirection.FORWARD,
        Set.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
        0,
        10,
        true);

    verify(entityGraphCache, never())
        .listRelated(any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any());
  }

  @Test
  public void groupsForUserCollectsNeighborUrnsFromScrollFallback() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        GROUP.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    OperationContext opContext = contextWithCache(entityGraphCache, graphRetriever);

    assertEquals(BoundMembershipAccess.groupsForUser(opContext, spec, USER), Set.of(GROUP));
  }

  @Test
  public void directRolesForUserUsesAspectFallback() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    SessionActorIdentity identity = new SessionActorIdentity(USER, List.of(), Set.of(ROLE));
    OperationContext opContext =
        contextWithServices(
            entityGraphCache, GraphRetriever.EMPTY, membershipService, identity, Set.of());

    assertEquals(BoundMembershipAccess.directRolesForUser(opContext, spec, USER), Set.of(ROLE));
  }

  @Test
  public void rolesForGroupUsesAspectFallback() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    OperationContext opContext =
        contextWithServices(
            entityGraphCache,
            GraphRetriever.EMPTY,
            membershipService,
            SessionActorIdentity.empty(GROUP),
            Set.of(ROLE));

    assertEquals(BoundMembershipAccess.rolesForGroup(opContext, spec, GROUP), Set.of(ROLE));
  }

  @Test
  public void effectiveRolesForUserMergesDirectAndInheritedRoles() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    SessionActorIdentity identity = new SessionActorIdentity(USER, List.of(GROUP), Set.of(ROLE));

    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            eq(spec.getGroupRelationshipTypes()),
            any(),
            any(),
            any(),
            anyInt(),
            any(),
            any()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        GROUP.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));
    when(graphRetriever.scrollRelatedEntities(
            any(),
            any(),
            any(),
            any(),
            eq(spec.getRoleRelationshipTypes()),
            any(),
            any(),
            any(),
            anyInt(),
            any(),
            any()))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, List.of()));

    OperationContext opContext =
        contextWithServices(
            entityGraphCache, graphRetriever, membershipService, identity, Set.of(INHERITED_ROLE));

    Set<Urn> roles = BoundMembershipAccess.effectiveRolesForUser(opContext, spec, USER);

    assertTrue(roles.contains(ROLE));
    assertTrue(roles.contains(INHERITED_ROLE));
  }

  @Test
  public void effectiveRolesForUserUsesIdentityFallbackWhenGraphReadsEmpty() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    SessionActorIdentity identity = new SessionActorIdentity(USER, List.of(GROUP), Set.of());

    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(new RelatedEntitiesScrollResult(0, 0, null, List.of()));

    OperationContext opContext =
        contextWithServices(
            entityGraphCache, graphRetriever, membershipService, identity, Set.of(INHERITED_ROLE));

    assertEquals(
        BoundMembershipAccess.effectiveRolesForUser(opContext, spec, USER), Set.of(INHERITED_ROLE));
  }

  private static OperationContext contextWithCache(EntityGraphCache cache) {
    return contextWithCache(cache, GraphRetriever.EMPTY);
  }

  private static OperationContext contextWithCache(
      EntityGraphCache cache, GraphRetriever graphRetriever) {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(graphRetriever)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(mock(AspectRetriever.class))
            .entityGraphCache(cache)
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }

  private static OperationContext contextWithServices(
      EntityGraphCache cache,
      GraphRetriever graphRetriever,
      ActorGroupMembershipService membershipService,
      SessionActorIdentity identity,
      Set<Urn> rolesViaGroups) {
    when(membershipService.fetchUserIdentity(any(OperationContext.class), eq(USER)))
        .thenReturn(identity);
    when(membershipService.fetchRolesViaGroups(any(OperationContext.class), any()))
        .thenReturn(rolesViaGroups);

    ServicesRegistryContext servicesRegistryContext =
        ServicesRegistryContext.builder()
            .restrictedService(mock(RestrictedService.class))
            .actorGroupMembershipService(membershipService)
            .build();

    OperationContext base = contextWithCache(cache, graphRetriever);
    return base.toBuilder()
        .servicesRegistryContext(servicesRegistryContext)
        .build(base.getSessionAuthentication(), false);
  }

  @Test
  public void corpGroupIncomingMembersUseGraphScrollOnCacheMiss() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    Urn secondUser = UrnUtils.getUrn("urn:li:corpuser:bob");
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            eq(Set.of(CORP_USER_ENTITY_NAME)),
            isNull(),
            eq(Set.of(CORP_GROUP_ENTITY_NAME)),
            any(),
            eq(spec.getGroupRelationshipTypes()),
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
                        IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        GROUP.toString(),
                        RelationshipDirection.OUTGOING,
                        null),
                    new RelatedEntities(
                        IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME,
                        secondUser.toString(),
                        GROUP.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    OperationContext opContext = contextWithCache(entityGraphCache, graphRetriever);

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            GROUP,
            TraversalDirection.REVERSE,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().size(), 2);
    assertEquals(
        result.neighborsOrEmpty().get(0).relationshipType(),
        IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME);
  }

  @Test
  public void corpGroupIncomingMembersFailClosedOnScrollError() {
    when(entityGraphCache.listRelated(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(MembershipNeighborResult.miss(ReadMissReason.ABSENT));

    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenThrow(new RuntimeException("scroll failed"));

    OperationContext opContext = contextWithCache(entityGraphCache, graphRetriever);

    MembershipNeighborResult result =
        BoundMembershipAccess.listRelated(
            opContext,
            spec,
            GROUP,
            TraversalDirection.REVERSE,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isMiss());
  }
}
