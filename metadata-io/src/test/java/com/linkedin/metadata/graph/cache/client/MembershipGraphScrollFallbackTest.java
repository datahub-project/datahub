package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_ROLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME;
import static com.linkedin.metadata.Constants.IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
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
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MembershipGraphScrollFallbackTest {

  private static final Urn USER = UrnUtils.getUrn("urn:li:corpuser:alice");
  private static final Urn GROUP = UrnUtils.getUrn("urn:li:corpGroup:eng");
  private static final Urn ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Admin");

  private MembershipReadSpec spec;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    spec =
        MembershipReadSpecs.membership(
            EntityGraphBinding.builder()
                .graphId("membership")
                .source(GraphSnapshotSource.GRAPH)
                .build());
  }

  @Test
  public void listRelatedReturnsEmptyForEmptyGraphRetriever() {
    opContext = contextWithGraphRetriever(GraphRetriever.EMPTY);

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            USER.toString(),
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertTrue(result.neighborsOrEmpty().isEmpty());
  }

  @Test
  public void listRelatedReturnsEmptyForEmptyRelationshipTypes() {
    opContext = contextWithGraphRetriever(mock(GraphRetriever.class));

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext, spec, USER.toString(), TraversalDirection.FORWARD, Set.of(), 0, 10);

    assertTrue(result.isHit());
    assertTrue(result.neighborsOrEmpty().isEmpty());
  }

  @Test
  public void listRelatedReturnsEmptyForNonPositiveCount() {
    opContext = contextWithGraphRetriever(mock(GraphRetriever.class));

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            USER.toString(),
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            0,
            0);

    assertTrue(result.isHit());
    assertTrue(result.neighborsOrEmpty().isEmpty());
  }

  @Test
  public void listRelatedScrollsUserForwardToGroups() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            eq(Set.of(CORP_USER_ENTITY_NAME)),
            any(),
            eq(Set.of(CORP_GROUP_ENTITY_NAME)),
            isNull(),
            eq(spec.getGroupRelationshipTypes()),
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
                        IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        GROUP.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    opContext = contextWithGraphRetriever(graphRetriever);

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            USER.toString(),
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().size(), 1);
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), GROUP.toString());
  }

  @Test
  public void listRelatedScrollsGroupReverseToMembers() {
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

    opContext = contextWithGraphRetriever(graphRetriever);

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            GROUP.toString(),
            TraversalDirection.REVERSE,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), USER.toString());
  }

  @Test
  public void listRelatedScrollsGroupForwardToRoles() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            eq(Set.of(CORP_GROUP_ENTITY_NAME)),
            any(),
            eq(Set.of(DATAHUB_ROLE_ENTITY_NAME)),
            isNull(),
            eq(spec.getRoleRelationshipTypes()),
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
                        IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME,
                        GROUP.toString(),
                        ROLE.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    opContext = contextWithGraphRetriever(graphRetriever);

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            GROUP.toString(),
            TraversalDirection.FORWARD,
            spec.getRoleRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), ROLE.toString());
  }

  @Test
  public void listRelatedScrollsRoleReverseToUsers() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            eq(Set.of(CORP_USER_ENTITY_NAME)),
            isNull(),
            eq(Set.of(DATAHUB_ROLE_ENTITY_NAME)),
            any(),
            eq(spec.getRoleRelationshipTypes()),
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
                        IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME,
                        USER.toString(),
                        ROLE.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));

    opContext = contextWithGraphRetriever(graphRetriever);

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            ROLE.toString(),
            TraversalDirection.REVERSE,
            spec.getRoleRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), USER.toString());
  }

  @Test
  public void listRelatedReturnsEmptyForUnsupportedEntityType() {
    opContext = contextWithGraphRetriever(mock(GraphRetriever.class));

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertTrue(result.neighborsOrEmpty().isEmpty());
  }

  @Test
  public void listRelatedPaginatesNeighbors() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2,
                2,
                null,
                List.of(
                    new RelatedEntities(
                        IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        GROUP.toString(),
                        RelationshipDirection.OUTGOING,
                        null),
                    new RelatedEntities(
                        IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        "urn:li:corpGroup:other",
                        RelationshipDirection.OUTGOING,
                        null))));

    opContext = contextWithGraphRetriever(graphRetriever);

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            USER.toString(),
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            1,
            1);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().size(), 1);
    assertEquals(result.neighborsOrEmpty().get(0).neighborUrn(), "urn:li:corpGroup:other");
  }

  @Test
  public void listRelatedReturnsMissOnScrollFailure() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenThrow(new RuntimeException("scroll failed"));

    opContext = contextWithGraphRetriever(graphRetriever);

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            USER.toString(),
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isMiss());
  }

  @Test
  public void listRelatedScrollsMultiplePages() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    when(graphRetriever.scrollRelatedEntities(
            eq(Set.of(CORP_USER_ENTITY_NAME)),
            any(),
            eq(Set.of(CORP_GROUP_ENTITY_NAME)),
            isNull(),
            eq(spec.getGroupRelationshipTypes()),
            any(),
            eq(Edge.EDGE_SORT_CRITERION),
            isNull(),
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
                        IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        GROUP.toString(),
                        RelationshipDirection.OUTGOING,
                        null))));
    when(graphRetriever.scrollRelatedEntities(
            eq(Set.of(CORP_USER_ENTITY_NAME)),
            any(),
            eq(Set.of(CORP_GROUP_ENTITY_NAME)),
            isNull(),
            eq(spec.getGroupRelationshipTypes()),
            any(),
            eq(Edge.EDGE_SORT_CRITERION),
            eq("page-2"),
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
                        IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                        USER.toString(),
                        "urn:li:corpGroup:other",
                        RelationshipDirection.OUTGOING,
                        null))));

    opContext = contextWithGraphRetriever(graphRetriever);

    MembershipNeighborResult result =
        MembershipGraphScrollFallback.listRelated(
            opContext,
            spec,
            USER.toString(),
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            0,
            10);

    assertTrue(result.isHit());
    assertEquals(result.neighborsOrEmpty().size(), 2);
  }

  @Test
  public void neighborUrnsCollectsAllScrollResults() {
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

    opContext = contextWithGraphRetriever(graphRetriever);

    assertEquals(
        MembershipGraphScrollFallback.neighborUrns(
            opContext,
            spec,
            USER.toString(),
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes()),
        Set.of(GROUP));
  }

  private static OperationContext contextWithGraphRetriever(GraphRetriever graphRetriever) {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(graphRetriever)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(mock(AspectRetriever.class))
            .entityGraphCache(mock(EntityGraphCache.class))
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }
}
