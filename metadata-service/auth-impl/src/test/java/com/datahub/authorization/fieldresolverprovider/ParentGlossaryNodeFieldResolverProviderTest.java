package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParentGlossaryNodeFieldResolverProviderTest {

  private static final Urn TERM_URN = UrnUtils.getUrn("urn:li:glossaryTerm:term1");
  private static final Urn NODE_URN = UrnUtils.getUrn("urn:li:glossaryNode:node1");
  private static final Urn PARENT_NODE_URN = UrnUtils.getUrn("urn:li:glossaryNode:parent");
  private static final Urn GRANDPARENT_NODE_URN =
      UrnUtils.getUrn("urn:li:glossaryNode:grandparent");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,test.dataset,PROD)");

  private SystemEntityClient mockClient;
  private ParentGlossaryNodeFieldResolverProvider resolver;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockClient = mock(SystemEntityClient.class);
    resolver = new ParentGlossaryNodeFieldResolverProvider(mockClient);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testGetFieldTypes() {
    assertEquals(resolver.getFieldTypes().size(), 1);
    assertEquals(resolver.getFieldTypes().get(0), EntityFieldType.PARENT_GLOSSARY_NODE);
  }

  @Test
  public void testEmptyEntitySpec() throws Exception {
    EntitySpec emptySpec = new EntitySpec(GLOSSARY_NODE_ENTITY_NAME, "");
    Set<String> result =
        resolver.getFieldResolver(opContext, emptySpec).getFieldValuesFuture().join().getValues();
    assertTrue(result.isEmpty());
  }

  @Test
  public void testNonGlossaryEntity() throws Exception {
    EntitySpec datasetSpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, datasetSpec).getFieldValuesFuture().join().getValues();
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGlossaryTermWithNoParent() throws Exception {
    EntityResponse response = new EntityResponse();
    response.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(TERM_URN),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(response);

    EntitySpec termSpec = new EntitySpec(GLOSSARY_TERM_ENTITY_NAME, TERM_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, termSpec).getFieldValuesFuture().join().getValues();

    assertTrue(result.isEmpty());
  }

  @Test
  public void testGlossaryNodeWithNoParent() throws Exception {
    EntityResponse response = new EntityResponse();
    response.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(response);

    EntitySpec nodeSpec = new EntitySpec(GLOSSARY_NODE_ENTITY_NAME, NODE_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, nodeSpec).getFieldValuesFuture().join().getValues();

    assertTrue(result.isEmpty());
  }

  @Test
  public void testGlossaryTermWithSingleParent() throws Exception {
    GlossaryTermInfo termInfo = new GlossaryTermInfo();
    termInfo.setParentNode(GlossaryNodeUrn.createFromUrn(PARENT_NODE_URN));

    EntityResponse termResponse = new EntityResponse();
    EnvelopedAspectMap termAspectMap = new EnvelopedAspectMap();
    termAspectMap.put(
        GLOSSARY_TERM_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(termInfo.data())));
    termResponse.setAspects(termAspectMap);

    EntityResponse parentResponse = new EntityResponse();
    parentResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(TERM_URN),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(termResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(PARENT_NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(parentResponse);

    EntitySpec termSpec = new EntitySpec(GLOSSARY_TERM_ENTITY_NAME, TERM_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, termSpec).getFieldValuesFuture().join().getValues();

    assertEquals(result.size(), 1);
    assertTrue(
        result.contains(PARENT_NODE_URN.toString()),
        "Result should contain: " + PARENT_NODE_URN.toString() + " but got: " + result);
  }

  @Test
  public void testGlossaryNodeWithSingleParent() throws Exception {
    GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo();
    nodeInfo.setParentNode(GlossaryNodeUrn.createFromUrn(PARENT_NODE_URN));

    EntityResponse nodeResponse = new EntityResponse();
    EnvelopedAspectMap nodeAspectMap = new EnvelopedAspectMap();
    nodeAspectMap.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(nodeInfo.data())));
    nodeResponse.setAspects(nodeAspectMap);

    EntityResponse parentResponse = new EntityResponse();
    parentResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(nodeResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(PARENT_NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(parentResponse);

    EntitySpec nodeSpec = new EntitySpec(GLOSSARY_NODE_ENTITY_NAME, NODE_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, nodeSpec).getFieldValuesFuture().join().getValues();

    assertEquals(result.size(), 1);
    assertTrue(result.contains(PARENT_NODE_URN.toString()));
  }

  @Test
  public void testGlossaryTermWithMultipleParents() throws Exception {
    GlossaryTermInfo termInfo = new GlossaryTermInfo();
    termInfo.setParentNode(GlossaryNodeUrn.createFromUrn(PARENT_NODE_URN));

    EntityResponse termResponse = new EntityResponse();
    EnvelopedAspectMap termAspectMap = new EnvelopedAspectMap();
    termAspectMap.put(
        GLOSSARY_TERM_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(termInfo.data())));
    termResponse.setAspects(termAspectMap);

    GlossaryNodeInfo parentInfo = new GlossaryNodeInfo();
    parentInfo.setParentNode(GlossaryNodeUrn.createFromUrn(GRANDPARENT_NODE_URN));

    EntityResponse parentResponse = new EntityResponse();
    EnvelopedAspectMap parentAspectMap = new EnvelopedAspectMap();
    parentAspectMap.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentInfo.data())));
    parentResponse.setAspects(parentAspectMap);

    EntityResponse grandparentResponse = new EntityResponse();
    grandparentResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(TERM_URN),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(termResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(PARENT_NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(parentResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(GRANDPARENT_NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(grandparentResponse);

    EntitySpec termSpec = new EntitySpec(GLOSSARY_TERM_ENTITY_NAME, TERM_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, termSpec).getFieldValuesFuture().join().getValues();

    assertEquals(result.size(), 2);
    assertTrue(result.contains(PARENT_NODE_URN.toString()));
    assertTrue(result.contains(GRANDPARENT_NODE_URN.toString()));
  }

  @Test
  public void testCycleDetection() throws Exception {
    GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo();
    nodeInfo.setParentNode(GlossaryNodeUrn.createFromUrn(PARENT_NODE_URN));

    EntityResponse nodeResponse = new EntityResponse();
    EnvelopedAspectMap nodeAspectMap = new EnvelopedAspectMap();
    nodeAspectMap.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(nodeInfo.data())));
    nodeResponse.setAspects(nodeAspectMap);

    GlossaryNodeInfo parentInfo = new GlossaryNodeInfo();
    parentInfo.setParentNode(GlossaryNodeUrn.createFromUrn(NODE_URN));

    EntityResponse parentResponse = new EntityResponse();
    EnvelopedAspectMap parentAspectMap = new EnvelopedAspectMap();
    parentAspectMap.put(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentInfo.data())));
    parentResponse.setAspects(parentAspectMap);

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(nodeResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(PARENT_NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(parentResponse);

    EntitySpec nodeSpec = new EntitySpec(GLOSSARY_NODE_ENTITY_NAME, NODE_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, nodeSpec).getFieldValuesFuture().join().getValues();

    assertEquals(result.size(), 2);
    assertTrue(result.contains(PARENT_NODE_URN.toString()));
    assertTrue(result.contains(NODE_URN.toString()));
  }

  @Test
  public void testErrorHandlingForTerm() throws Exception {
    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(TERM_URN),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenThrow(new RuntimeException("Test exception"));

    EntitySpec termSpec = new EntitySpec(GLOSSARY_TERM_ENTITY_NAME, TERM_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, termSpec).getFieldValuesFuture().join().getValues();

    assertTrue(result.isEmpty());
  }

  @Test
  public void testErrorHandlingForNode() throws Exception {
    when(mockClient.getV2(
            eq(opContext),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(NODE_URN),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenThrow(new RuntimeException("Test exception"));

    EntitySpec nodeSpec = new EntitySpec(GLOSSARY_NODE_ENTITY_NAME, NODE_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, nodeSpec).getFieldValuesFuture().join().getValues();

    assertTrue(result.isEmpty());
  }
}
