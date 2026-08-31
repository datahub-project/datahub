package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryUtilsTest {

  private final String userUrn = "urn:li:corpuser:authorized";
  private final QueryContext mockContext = Mockito.mock(QueryContext.class);
  private final EntityClient mockClient = Mockito.mock(EntityClient.class);
  private final Urn parentNodeUrn = UrnUtils.getUrn("urn:li:glossaryNode:parent_node");
  private final Urn parentNodeUrn1 = UrnUtils.getUrn("urn:li:glossaryNode:parent_node1");
  private final Urn parentNodeUrn2 = UrnUtils.getUrn("urn:li:glossaryNode:parent_node2");
  private final Urn parentNodeUrn3 = UrnUtils.getUrn("urn:li:glossaryNode:parent_node3");

  private OperationContext mockOpContext;

  @BeforeMethod
  private void setUpTests() throws Exception {
    Mockito.when(mockContext.getActorUrn()).thenReturn(userUrn);
    when(mockContext.getMaxParentDepth()).thenReturn(50);
    mockOpContext = Mockito.spy(buildOpContext(EntityGraphCache.NO_OP, glossaryAspectRetriever()));
    when(mockContext.getOperationContext()).thenReturn(mockOpContext);

    GlossaryNodeInfo parentNode1 =
        new GlossaryNodeInfo()
            .setParentNode(GlossaryNodeUrn.createFromString("urn:li:glossaryNode:parent_node2"));
    GlossaryNodeInfo parentNode2 =
        new GlossaryNodeInfo()
            .setParentNode(GlossaryNodeUrn.createFromString("urn:li:glossaryNode:parent_node3"));

    Map<String, EnvelopedAspect> parentNode1Aspects = new HashMap<>();
    parentNode1Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new Aspect(
                    new GlossaryNodeInfo()
                        .setDefinition("node parent 1")
                        .setParentNode(parentNode1.getParentNode())
                        .data())));

    Map<String, EnvelopedAspect> parentNode2Aspects = new HashMap<>();
    parentNode2Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new Aspect(
                    new GlossaryNodeInfo()
                        .setDefinition("node parent 2")
                        .setParentNode(parentNode2.getParentNode())
                        .data())));

    Map<String, EnvelopedAspect> parentNode3Aspects = new HashMap<>();
    parentNode3Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new GlossaryNodeInfo().setDefinition("node parent 3").data())));

    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.GLOSSARY_NODE_ENTITY_NAME),
                eq(parentNodeUrn1),
                eq(ImmutableSet.of(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentNode1Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.GLOSSARY_NODE_ENTITY_NAME),
                eq(parentNodeUrn2),
                eq(ImmutableSet.of(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentNode2Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.GLOSSARY_NODE_ENTITY_NAME),
                eq(parentNodeUrn3),
                eq(ImmutableSet.of(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentNode3Aspects)));

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn3.toString());
    mockAuthRequest("MANAGE_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn2.toString());
    mockAuthRequest("MANAGE_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn1.toString());
    mockAuthRequest("MANAGE_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);
  }

  private AspectRetriever glossaryAspectRetriever() {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    Map<Urn, Urn> parentByNode = new HashMap<>();
    parentByNode.put(parentNodeUrn1, parentNodeUrn2);
    parentByNode.put(parentNodeUrn2, parentNodeUrn3);
    parentByNode.put(parentNodeUrn3, null);

    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              @SuppressWarnings("unchecked")
              Set<String> aspectNames = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                if (aspectNames.contains(GLOSSARY_NODE_INFO_ASPECT_NAME)
                    && parentByNode.containsKey(urn)) {
                  GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo();
                  Urn parent = parentByNode.get(urn);
                  if (parent != null) {
                    nodeInfo.setParentNode(GlossaryNodeUrn.createFromUrn(parent));
                  }
                  result.put(
                      urn, Map.of(GLOSSARY_NODE_INFO_ASPECT_NAME, new Aspect(nodeInfo.data())));
                }
              }
              return result;
            });
    return aspectRetriever;
  }

  private OperationContext buildOpContext(
      EntityGraphCache entityGraphCache, AspectRetriever aspectRetriever) {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(aspectRetriever)
            .entityGraphCache(entityGraphCache)
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }

  private void mockAuthRequest(
      String privilege, AuthorizationResult.Type allowOrDeny, EntitySpec resourceSpec) {
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(allowOrDeny);
    when(mockOpContext.authorize(eq(privilege), eq(resourceSpec), any())).thenReturn(result);
  }

  @Test
  public void testCanManageChildrenUsesEntityGraphCacheForAncestorWalk() throws Exception {
    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.walkOrderedForwardAncestors(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(parentNodeUrn1.toString()),
            eq(49),
            eq(ReadMode.CACHED)))
        .thenReturn(
            AncestorWalkResult.fromAncestors(
                List.of(parentNodeUrn2.toString(), parentNodeUrn3.toString())));

    OperationContext cacheOpContext = buildOpContext(entityGraphCache, mock(AspectRetriever.class));
    mockOpContext = Mockito.spy(cacheOpContext);
    when(mockContext.getOperationContext()).thenReturn(mockOpContext);

    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn1.toString());
    mockAuthRequest("MANAGE_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn3.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec3);

    assertTrue(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn1, mockClient));
    verify(mockClient, never())
        .getV2(
            any(),
            eq(Constants.GLOSSARY_NODE_ENTITY_NAME),
            eq(parentNodeUrn2),
            eq(ImmutableSet.of(GLOSSARY_NODE_INFO_ASPECT_NAME)));
  }

  @Test
  public void testCanManageGlossariesAuthorized() throws Exception {
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.ALLOW, null);

    assertTrue(GlossaryUtils.canManageGlossaries(mockContext));
  }

  @Test
  public void testCanManageGlossariesUnauthorized() throws Exception {
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    assertFalse(GlossaryUtils.canManageGlossaries(mockContext));
  }

  @Test
  public void testCanManageChildrenEntitiesWithManageGlossaries() throws Exception {
    // they have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.ALLOW, null);

    assertTrue(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn, mockClient));
  }

  @Test
  public void testCanManageChildrenEntitiesNoParentNode() throws Exception {
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    assertFalse(GlossaryUtils.canManageChildrenEntities(mockContext, null, mockClient));
  }

  @Test
  public void testCanManageChildrenEntitiesAuthorized() throws Exception {
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn.toString());
    mockAuthRequest("MANAGE_GLOSSARY_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec);

    assertTrue(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn, mockClient));
  }

  @Test
  public void testCanManageChildrenEntitiesUnauthorized() throws Exception {
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn.toString());
    mockAuthRequest("MANAGE_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec);
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec);

    assertFalse(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn, mockClient));
  }

  @Test
  public void testCanManageChildrenRecursivelyEntitiesAuthorized() throws Exception {
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn3.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn2.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn1.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);

    assertTrue(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn1, mockClient));
  }

  @Test
  public void testCanManageChildrenRecursivelyEntitiesUnauthorized() throws Exception {
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn3.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn2.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn1.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);

    assertFalse(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn1, mockClient));
  }

  @Test
  public void testCanManageChildrenRecursivelyEntitiesAuthorizedLevel2() throws Exception {
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn2.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec2);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn1.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);

    assertTrue(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn1, mockClient));
  }

  @Test
  public void testCanManageChildrenRecursivelyEntitiesUnauthorizedLevel2() throws Exception {
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn3.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn2.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    assertFalse(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn2, mockClient));
  }

  @Test
  public void testCanManageChildrenRecursivelyEntitiesNoLevel2() throws Exception {
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentNodeUrn.getEntityType(), parentNodeUrn3.toString());
    mockAuthRequest("MANAGE_ALL_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    assertFalse(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn3, mockClient));
  }
}
