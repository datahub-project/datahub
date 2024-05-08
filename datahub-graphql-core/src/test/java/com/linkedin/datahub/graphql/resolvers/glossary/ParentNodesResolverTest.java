package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

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
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ParentNodesResolverTest {
  @Test
  public void testGetSuccessForTerm() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Urn termUrn = Urn.createFromString("urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451");
    GlossaryTerm termEntity = new GlossaryTerm();
    termEntity.setUrn(termUrn.toString());
    termEntity.setType(EntityType.GLOSSARY_TERM);
    Mockito.when(mockEnv.getSource()).thenReturn(termEntity);

    final GlossaryTermInfo parentNode1 =
        new GlossaryTermInfo()
            .setParentNode(
                GlossaryNodeUrn.createFromString(
                    "urn:li:glossaryNode:11115397daf94708a8822b8106cfd451"))
            .setDefinition("test def");
    final GlossaryNodeInfo parentNode2 =
        new GlossaryNodeInfo()
            .setParentNode(
                GlossaryNodeUrn.createFromString(
                    "urn:li:glossaryNode:22225397daf94708a8822b8106cfd451"))
            .setDefinition("test def 2");

    Map<String, EnvelopedAspect> glossaryTermAspects = new HashMap<>();
    glossaryTermAspects.put(
        GLOSSARY_TERM_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentNode1.data())));

    Map<String, EnvelopedAspect> parentNode1Aspects = new HashMap<>();
    parentNode1Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new Aspect(
                    new GlossaryNodeInfo()
                        .setDefinition("node parent 1")
                        .setParentNode(parentNode2.getParentNode())
                        .data())));

    Map<String, EnvelopedAspect> parentNode2Aspects = new HashMap<>();
    parentNode2Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new GlossaryNodeInfo().setDefinition("node parent 2").data())));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(termUrn.getEntityType()),
                Mockito.eq(termUrn),
                Mockito.eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(glossaryTermAspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(parentNode1.getParentNode().getEntityType()),
                Mockito.eq(parentNode1.getParentNode()),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(GLOSSARY_NODE_ENTITY_NAME)
                .setUrn(parentNode1.getParentNode())
                .setAspects(new EnvelopedAspectMap(parentNode1Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(parentNode1.getParentNode().getEntityType()),
                Mockito.eq(parentNode1.getParentNode()),
                Mockito.eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentNode1Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(parentNode2.getParentNode().getEntityType()),
                Mockito.eq(parentNode2.getParentNode()),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(GLOSSARY_NODE_ENTITY_NAME)
                .setUrn(parentNode2.getParentNode())
                .setAspects(new EnvelopedAspectMap(parentNode2Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(parentNode2.getParentNode().getEntityType()),
                Mockito.eq(parentNode2.getParentNode()),
                Mockito.eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentNode2Aspects)));

    ParentNodesResolver resolver = new ParentNodesResolver(mockClient);
    ParentNodesResult result = resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(5))
        .getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertEquals(result.getCount(), 2);
    assertEquals(result.getNodes().get(0).getUrn(), parentNode1.getParentNode().toString());
    assertEquals(result.getNodes().get(1).getUrn(), parentNode2.getParentNode().toString());
  }

  @Test
  public void testGetSuccessForNode() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Urn nodeUrn = Urn.createFromString("urn:li:glossaryNode:00005397daf94708a8822b8106cfd451");
    GlossaryNode nodeEntity = new GlossaryNode();
    nodeEntity.setUrn(nodeUrn.toString());
    nodeEntity.setType(EntityType.GLOSSARY_NODE);
    Mockito.when(mockEnv.getSource()).thenReturn(nodeEntity);

    final GlossaryNodeInfo parentNode1 =
        new GlossaryNodeInfo()
            .setParentNode(
                GlossaryNodeUrn.createFromString(
                    "urn:li:glossaryNode:11115397daf94708a8822b8106cfd451"))
            .setDefinition("test def");
    final GlossaryNodeInfo parentNode2 =
        new GlossaryNodeInfo()
            .setParentNode(
                GlossaryNodeUrn.createFromString(
                    "urn:li:glossaryNode:22225397daf94708a8822b8106cfd451"))
            .setDefinition("test def 2");

    Map<String, EnvelopedAspect> glossaryNodeAspects = new HashMap<>();
    glossaryNodeAspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentNode1.data())));

    Map<String, EnvelopedAspect> parentNode1Aspects = new HashMap<>();
    parentNode1Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new Aspect(
                    new GlossaryNodeInfo()
                        .setDefinition("node parent 1")
                        .setParentNode(parentNode2.getParentNode())
                        .data())));

    Map<String, EnvelopedAspect> parentNode2Aspects = new HashMap<>();
    parentNode2Aspects.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new GlossaryNodeInfo().setDefinition("node parent 2").data())));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(nodeUrn.getEntityType()),
                Mockito.eq(nodeUrn),
                Mockito.eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(glossaryNodeAspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(parentNode1.getParentNode().getEntityType()),
                Mockito.eq(parentNode1.getParentNode()),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(GLOSSARY_NODE_ENTITY_NAME)
                .setUrn(parentNode1.getParentNode())
                .setAspects(new EnvelopedAspectMap(parentNode1Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(parentNode1.getParentNode().getEntityType()),
                Mockito.eq(parentNode1.getParentNode()),
                Mockito.eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentNode1Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(parentNode2.getParentNode().getEntityType()),
                Mockito.eq(parentNode2.getParentNode()),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(GLOSSARY_NODE_ENTITY_NAME)
                .setUrn(parentNode2.getParentNode())
                .setAspects(new EnvelopedAspectMap(parentNode2Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(parentNode2.getParentNode().getEntityType()),
                Mockito.eq(parentNode2.getParentNode()),
                Mockito.eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentNode2Aspects)));

    ParentNodesResolver resolver = new ParentNodesResolver(mockClient);
    ParentNodesResult result = resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(5))
        .getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertEquals(result.getCount(), 2);
    assertEquals(result.getNodes().get(0).getUrn(), parentNode1.getParentNode().toString());
    assertEquals(result.getNodes().get(1).getUrn(), parentNode2.getParentNode().toString());
  }
}
