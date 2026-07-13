package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.ParentNodesResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
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
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ParentNodesResolverTest {

  private static final GlossaryNodeUrn PARENT_NODE_1;
  private static final GlossaryNodeUrn PARENT_NODE_2;

  static {
    try {
      PARENT_NODE_1 =
          GlossaryNodeUrn.createFromString("urn:li:glossaryNode:11115397daf94708a8822b8106cfd451");
      PARENT_NODE_2 =
          GlossaryNodeUrn.createFromString("urn:li:glossaryNode:22225397daf94708a8822b8106cfd451");
    } catch (URISyntaxException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Test
  public void testGetSuccessForTerm() throws Exception {
    Urn termUrn = Urn.createFromString("urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451");

    GlossaryTermInfo termInfo = new GlossaryTermInfo().setParentNode(PARENT_NODE_1);
    GlossaryNodeInfo parent1Info =
        new GlossaryNodeInfo().setParentNode(PARENT_NODE_2).setDefinition("node parent 1");
    GlossaryNodeInfo parent2Info = new GlossaryNodeInfo().setDefinition("node parent 2");

    ParentNodesResolver resolver =
        new ParentNodesResolver(
            mockEntityClient(PARENT_NODE_1, PARENT_NODE_2, parent1Info, parent2Info));
    ParentNodesResult result = resolver.get(mockEnv(termUrn, true)).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getNodes().get(0).getUrn(), PARENT_NODE_1.toString());
    assertEquals(result.getNodes().get(1).getUrn(), PARENT_NODE_2.toString());
  }

  @Test
  public void testGetSuccessForNode() throws Exception {
    Urn nodeUrn = Urn.createFromString("urn:li:glossaryNode:00005397daf94708a8822b8106cfd451");

    GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo().setParentNode(PARENT_NODE_1);
    GlossaryNodeInfo parent1Info =
        new GlossaryNodeInfo().setParentNode(PARENT_NODE_2).setDefinition("node parent 1");
    GlossaryNodeInfo parent2Info = new GlossaryNodeInfo().setDefinition("node parent 2");

    ParentNodesResolver resolver =
        new ParentNodesResolver(
            mockEntityClient(PARENT_NODE_1, PARENT_NODE_2, parent1Info, parent2Info));
    ParentNodesResult result = resolver.get(mockEnv(nodeUrn, false)).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getNodes().get(0).getUrn(), PARENT_NODE_1.toString());
    assertEquals(result.getNodes().get(1).getUrn(), PARENT_NODE_2.toString());
  }

  @Test
  public void testGetSuccessUsesEntityGraphCacheWhenAvailable() throws Exception {
    Urn termUrn = Urn.createFromString("urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451");

    GlossaryNodeInfo parent1Info =
        new GlossaryNodeInfo().setParentNode(PARENT_NODE_2).setDefinition("node parent 1");
    GlossaryNodeInfo parent2Info = new GlossaryNodeInfo().setDefinition("node parent 2");

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.walkOrderedForwardAncestors(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(termUrn.toString()),
            eq(50),
            eq(ReadMode.CACHED)))
        .thenReturn(
            AncestorWalkResult.fromAncestors(
                List.of(PARENT_NODE_1.toString(), PARENT_NODE_2.toString())));

    ParentNodesResolver resolver =
        new ParentNodesResolver(
            mockEntityClient(PARENT_NODE_1, PARENT_NODE_2, parent1Info, parent2Info));
    ParentNodesResult result = resolver.get(mockEnv(termUrn, true, entityGraphCache)).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getNodes().get(0).getUrn(), PARENT_NODE_1.toString());
    assertEquals(result.getNodes().get(1).getUrn(), PARENT_NODE_2.toString());
  }

  private static DataFetchingEnvironment mockEnv(Urn sourceUrn, boolean term) {
    return mockEnv(sourceUrn, term, EntityGraphCache.NO_OP);
  }

  private static DataFetchingEnvironment mockEnv(
      Urn sourceUrn, boolean term, EntityGraphCache entityGraphCache) {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAuthentication()).thenReturn(mock(Authentication.class));
    when(mockContext.getMaxParentDepth()).thenReturn(50);
    OperationContext operationContext =
        operationContextWithAspects(sourceUrn, term, entityGraphCache);
    when(mockContext.getOperationContext()).thenReturn(operationContext);

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
    if (term) {
      GlossaryTerm termEntity = new GlossaryTerm();
      termEntity.setUrn(sourceUrn.toString());
      termEntity.setType(EntityType.GLOSSARY_TERM);
      when(mockEnv.getSource()).thenReturn(termEntity);
    } else {
      GlossaryNode nodeEntity = new GlossaryNode();
      nodeEntity.setUrn(sourceUrn.toString());
      nodeEntity.setType(EntityType.GLOSSARY_NODE);
      when(mockEnv.getSource()).thenReturn(nodeEntity);
    }
    return mockEnv;
  }

  private static OperationContext operationContextWithAspects(
      Urn sourceUrn, boolean term, EntityGraphCache entityGraphCache) {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              for (Urn urn : urns) {
                result.put(urn, aspectsFor(urn, sourceUrn, term));
              }
              return result;
            });

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

  private static Map<String, Aspect> aspectsFor(Urn urn, Urn sourceUrn, boolean term) {
    Map<String, Aspect> aspects = new HashMap<>();

    if (term && urn.equals(sourceUrn)) {
      aspects.put(
          GLOSSARY_TERM_INFO_ASPECT_NAME,
          new Aspect(new GlossaryTermInfo().setParentNode(PARENT_NODE_1).data()));
    } else if (urn.equals(PARENT_NODE_1)) {
      aspects.put(
          GLOSSARY_NODE_INFO_ASPECT_NAME,
          new Aspect(new GlossaryNodeInfo().setParentNode(PARENT_NODE_2).data()));
    } else if (urn.equals(PARENT_NODE_2)) {
      aspects.put(
          GLOSSARY_NODE_INFO_ASPECT_NAME,
          new Aspect(new GlossaryNodeInfo().setDefinition("node parent 2").data()));
    } else if (!term && urn.equals(sourceUrn)) {
      aspects.put(
          GLOSSARY_NODE_INFO_ASPECT_NAME,
          new Aspect(new GlossaryNodeInfo().setParentNode(PARENT_NODE_1).data()));
    }
    return aspects;
  }

  private static EntityClient mockEntityClient(
      GlossaryNodeUrn parentNode1,
      GlossaryNodeUrn parentNode2,
      GlossaryNodeInfo parent1Info,
      GlossaryNodeInfo parent2Info)
      throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    Map<String, EnvelopedAspect> parentNode1Aspects = new HashMap<>();
    parentNode1Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parent1Info.data())));

    Map<String, EnvelopedAspect> parentNode2Aspects = new HashMap<>();
    parentNode2Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parent2Info.data())));

    when(mockClient.getV2(
            any(), Mockito.eq(parentNode1.getEntityType()), Mockito.eq(parentNode1), any()))
        .thenReturn(
            new EntityResponse()
                .setEntityName(GLOSSARY_NODE_ENTITY_NAME)
                .setUrn(parentNode1)
                .setAspects(new EnvelopedAspectMap(parentNode1Aspects)));

    when(mockClient.getV2(
            any(), Mockito.eq(parentNode2.getEntityType()), Mockito.eq(parentNode2), any()))
        .thenReturn(
            new EntityResponse()
                .setEntityName(GLOSSARY_NODE_ENTITY_NAME)
                .setUrn(parentNode2)
                .setAspects(new EnvelopedAspectMap(parentNode2Aspects)));

    return mockClient;
  }
}
