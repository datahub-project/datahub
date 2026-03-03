package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link GlossaryFieldResolverProvider}.
 *
 * <p>Tests cover three main scenarios: 1. Glossary term entities resolving to themselves + parent
 * nodes 2. Glossary node entities resolving to themselves + parent nodes 3. Regular entities
 * (datasets, etc.) resolving their applied glossary terms + parent nodes
 */
public class GlossaryFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<GlossaryFieldResolverProvider> {

  private static final Urn PARENT_NODE_URN = UrnUtils.getUrn("urn:li:glossaryNode:parentNode");
  private static final Urn CHILD_NODE_URN = UrnUtils.getUrn("urn:li:glossaryNode:childNode");
  private static final Urn GRANDCHILD_NODE_URN =
      UrnUtils.getUrn("urn:li:glossaryNode:grandchildNode");

  private static final Urn TERM_A_URN = UrnUtils.getUrn("urn:li:glossaryTerm:termA");
  private static final Urn TERM_B_URN = UrnUtils.getUrn("urn:li:glossaryTerm:termB");
  private static final Urn TERM_C_URN = UrnUtils.getUrn("urn:li:glossaryTerm:termC");

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  private SystemEntityClient mockEntityClient;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Override
  protected GlossaryFieldResolverProvider buildFieldResolverProvider() {
    return new GlossaryFieldResolverProvider(mock(SystemEntityClient.class));
  }

  // ===== Tests for Glossary Term Entities =====

  @Test
  public void testGlossaryTermWithParentNodes()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: termA -> childNode -> parentNode
    setupTermWithNodeHierarchy();

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(GLOSSARY_TERM_ENTITY_NAME, TERM_A_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 3, "Should resolve term itself + child node + parent node");
    assertTrue(values.contains(TERM_A_URN.toString()));
    assertTrue(values.contains(CHILD_NODE_URN.toString()));
    assertTrue(values.contains(PARENT_NODE_URN.toString()));
  }

  @Test
  public void testGlossaryTermWithNoParentNode()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: termA with no parent node (root-level term)
    final Map<Urn, EntityResponse> termResponse = new HashMap<>();
    termResponse.put(TERM_A_URN, createGlossaryTermInfoResponse(TERM_A_URN, null));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(Set.of(TERM_A_URN)),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(termResponse);

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(GLOSSARY_TERM_ENTITY_NAME, TERM_A_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 1, "Should resolve only the term itself");
    assertTrue(values.contains(TERM_A_URN.toString()));
  }

  @Test
  public void testGlossaryTermWithDeepHierarchy()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: termA -> grandchildNode -> childNode -> parentNode
    setupTermWithDeepNodeHierarchy();

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(GLOSSARY_TERM_ENTITY_NAME, TERM_A_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 4, "Should resolve term + all 3 levels of parent nodes");
    assertTrue(values.contains(TERM_A_URN.toString()));
    assertTrue(values.contains(GRANDCHILD_NODE_URN.toString()));
    assertTrue(values.contains(CHILD_NODE_URN.toString()));
    assertTrue(values.contains(PARENT_NODE_URN.toString()));
  }

  // ===== Tests for Glossary Node Entities =====

  @Test
  public void testGlossaryNodeWithParentNodes()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: grandchildNode -> childNode -> parentNode
    setupNodeHierarchy();

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec =
        new EntitySpec(GLOSSARY_NODE_ENTITY_NAME, GRANDCHILD_NODE_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 3, "Should resolve grandchild node, child node, and parent node");
    assertTrue(values.contains(GRANDCHILD_NODE_URN.toString()));
    assertTrue(values.contains(CHILD_NODE_URN.toString()));
    assertTrue(values.contains(PARENT_NODE_URN.toString()));
  }

  @Test
  public void testGlossaryNodeWithNoParent()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: parentNode with no parent (root-level node)
    final Map<Urn, EntityResponse> nodeResponse = new HashMap<>();
    nodeResponse.put(
        PARENT_NODE_URN, createGlossaryNodeInfoResponse(PARENT_NODE_URN, null /* no parent */));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(Set.of(PARENT_NODE_URN)),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(nodeResponse);

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec =
        new EntitySpec(GLOSSARY_NODE_ENTITY_NAME, PARENT_NODE_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 1, "Should resolve only the node itself");
    assertTrue(values.contains(PARENT_NODE_URN.toString()));
  }

  @Test
  public void testGlossaryNodeHierarchyWithCyclePrevention()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: Test that we don't infinitely loop if there's a cycle
    // childNode -> parentNode -> childNode (cycle)
    final Map<Urn, EntityResponse> childResponse = new HashMap<>();
    childResponse.put(
        CHILD_NODE_URN, createGlossaryNodeInfoResponse(CHILD_NODE_URN, PARENT_NODE_URN));

    final Map<Urn, EntityResponse> parentResponse = new HashMap<>();
    parentResponse.put(
        PARENT_NODE_URN, createGlossaryNodeInfoResponse(PARENT_NODE_URN, CHILD_NODE_URN));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(Set.of(CHILD_NODE_URN)),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(childResponse);

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(Set.of(PARENT_NODE_URN)),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(parentResponse);

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec =
        new EntitySpec(GLOSSARY_NODE_ENTITY_NAME, CHILD_NODE_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(
        values.size(), 2, "Should resolve both nodes despite cycle, but not infinitely loop");
    assertTrue(values.contains(CHILD_NODE_URN.toString()));
    assertTrue(values.contains(PARENT_NODE_URN.toString()));
  }

  // ===== Tests for Non-Glossary Entities (Datasets, etc.) =====

  @Test
  public void testDatasetWithGlossaryTerms()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: dataset with termA (termA -> childNode -> parentNode)
    setupDatasetWithGlossaryTerms();

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 3, "Should resolve applied term + its parent nodes");
    assertTrue(values.contains(TERM_A_URN.toString()));
    assertTrue(values.contains(CHILD_NODE_URN.toString()));
    assertTrue(values.contains(PARENT_NODE_URN.toString()));
  }

  @Test
  public void testDatasetWithMultipleGlossaryTerms()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: dataset with termA and termB, both under different node hierarchies
    setupDatasetWithMultipleGlossaryTerms();

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    // termA + childNode + parentNode + termB + grandchildNode = 5
    assertEquals(values.size(), 5, "Should resolve all applied terms + their parent nodes");
    assertTrue(values.contains(TERM_A_URN.toString()));
    assertTrue(values.contains(TERM_B_URN.toString()));
    assertTrue(values.contains(CHILD_NODE_URN.toString()));
    assertTrue(values.contains(PARENT_NODE_URN.toString()));
    assertTrue(values.contains(GRANDCHILD_NODE_URN.toString()));
  }

  @Test
  public void testDatasetWithNoGlossaryTerms()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: dataset with no glossary terms
    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(null);

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 0, "Should return empty set for entity with no glossary terms");
  }

  @Test
  public void testDatasetWithRootLevelGlossaryTerm()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: dataset with termA that has no parent node (root-level term)
    final EntityResponse datasetResponse = new EntityResponse();
    datasetResponse.setUrn(DATASET_URN);
    datasetResponse.setEntityName(DATASET_ENTITY_NAME);

    final GlossaryTerms glossaryTerms = new GlossaryTerms();
    final GlossaryTermAssociation association = new GlossaryTermAssociation();
    try {
      association.setUrn(GlossaryTermUrn.createFromUrn(TERM_A_URN));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create GlossaryTermUrn", e);
    }
    glossaryTerms.setTerms(
        new GlossaryTermAssociationArray(Collections.singletonList(association)));

    final EnvelopedAspect glossaryTermsAspect = new EnvelopedAspect();
    glossaryTermsAspect.setName(GLOSSARY_TERMS_ASPECT_NAME);
    glossaryTermsAspect.setValue(new Aspect(glossaryTerms.data()));

    datasetResponse.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(GLOSSARY_TERMS_ASPECT_NAME, glossaryTermsAspect)));

    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(datasetResponse);

    // termA has no parent node
    final Map<Urn, EntityResponse> termResponse = new HashMap<>();
    termResponse.put(TERM_A_URN, createGlossaryTermInfoResponse(TERM_A_URN, null));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(Set.of(TERM_A_URN)),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(termResponse);

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 1, "Should resolve only the root-level term with no parent nodes");
    assertTrue(values.contains(TERM_A_URN.toString()));
  }

  // ===== Helper Methods =====

  private void setupTermWithNodeHierarchy() throws RemoteInvocationException, URISyntaxException {
    // termA -> childNode -> parentNode
    final Map<Urn, EntityResponse> termResponse = new HashMap<>();
    termResponse.put(TERM_A_URN, createGlossaryTermInfoResponse(TERM_A_URN, CHILD_NODE_URN));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(Set.of(TERM_A_URN)),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(termResponse);

    // Mock node hierarchy: childNode -> parentNode -> null
    final Map<Urn, EntityResponse> childNodeResponse = new HashMap<>();
    childNodeResponse.put(
        CHILD_NODE_URN, createGlossaryNodeInfoResponse(CHILD_NODE_URN, PARENT_NODE_URN));

    final Map<Urn, EntityResponse> parentNodeResponse = new HashMap<>();
    parentNodeResponse.put(PARENT_NODE_URN, createGlossaryNodeInfoResponse(PARENT_NODE_URN, null));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(Set.of(CHILD_NODE_URN)),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(childNodeResponse);

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(Set.of(PARENT_NODE_URN)),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(parentNodeResponse);
  }

  private void setupTermWithDeepNodeHierarchy()
      throws RemoteInvocationException, URISyntaxException {
    // termA -> grandchildNode -> childNode -> parentNode
    final Map<Urn, EntityResponse> termResponse = new HashMap<>();
    termResponse.put(TERM_A_URN, createGlossaryTermInfoResponse(TERM_A_URN, GRANDCHILD_NODE_URN));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(Set.of(TERM_A_URN)),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(termResponse);

    // Setup 3-level node hierarchy: grandchild -> child -> parent
    setupNodeHierarchy();
  }

  private void setupNodeHierarchy() throws RemoteInvocationException, URISyntaxException {
    // Mock 3-level hierarchy: grandchildNode -> childNode -> parentNode -> null
    final Map<Urn, EntityResponse> grandchildNodeResponse = new HashMap<>();
    grandchildNodeResponse.put(
        GRANDCHILD_NODE_URN, createGlossaryNodeInfoResponse(GRANDCHILD_NODE_URN, CHILD_NODE_URN));

    final Map<Urn, EntityResponse> childNodeResponse = new HashMap<>();
    childNodeResponse.put(
        CHILD_NODE_URN, createGlossaryNodeInfoResponse(CHILD_NODE_URN, PARENT_NODE_URN));

    final Map<Urn, EntityResponse> parentNodeResponse = new HashMap<>();
    parentNodeResponse.put(PARENT_NODE_URN, createGlossaryNodeInfoResponse(PARENT_NODE_URN, null));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(Set.of(GRANDCHILD_NODE_URN)),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(grandchildNodeResponse);

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(Set.of(CHILD_NODE_URN)),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(childNodeResponse);

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(Set.of(PARENT_NODE_URN)),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenReturn(parentNodeResponse);
  }

  private void setupDatasetWithGlossaryTerms()
      throws RemoteInvocationException, URISyntaxException {
    // Dataset has termA
    final EntityResponse datasetResponse = new EntityResponse();
    datasetResponse.setUrn(DATASET_URN);
    datasetResponse.setEntityName(DATASET_ENTITY_NAME);

    final GlossaryTerms glossaryTerms = new GlossaryTerms();
    final GlossaryTermAssociation association = new GlossaryTermAssociation();
    try {
      association.setUrn(GlossaryTermUrn.createFromUrn(TERM_A_URN));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create GlossaryTermUrn", e);
    }
    glossaryTerms.setTerms(
        new GlossaryTermAssociationArray(Collections.singletonList(association)));

    final EnvelopedAspect glossaryTermsAspect = new EnvelopedAspect();
    glossaryTermsAspect.setName(GLOSSARY_TERMS_ASPECT_NAME);
    glossaryTermsAspect.setValue(new Aspect(glossaryTerms.data()));

    datasetResponse.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(GLOSSARY_TERMS_ASPECT_NAME, glossaryTermsAspect)));

    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(datasetResponse);

    // termA -> childNode -> parentNode
    setupTermWithNodeHierarchy();
  }

  private void setupDatasetWithMultipleGlossaryTerms()
      throws RemoteInvocationException, URISyntaxException {
    // Dataset has termA and termB
    final EntityResponse datasetResponse = new EntityResponse();
    datasetResponse.setUrn(DATASET_URN);
    datasetResponse.setEntityName(DATASET_ENTITY_NAME);

    final GlossaryTerms glossaryTerms = new GlossaryTerms();
    final GlossaryTermAssociation assocA = new GlossaryTermAssociation();
    final GlossaryTermAssociation assocB = new GlossaryTermAssociation();
    try {
      assocA.setUrn(GlossaryTermUrn.createFromUrn(TERM_A_URN));
      assocB.setUrn(GlossaryTermUrn.createFromUrn(TERM_B_URN));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create GlossaryTermUrn", e);
    }
    glossaryTerms.setTerms(new GlossaryTermAssociationArray(Arrays.asList(assocA, assocB)));

    final EnvelopedAspect glossaryTermsAspect = new EnvelopedAspect();
    glossaryTermsAspect.setName(GLOSSARY_TERMS_ASPECT_NAME);
    glossaryTermsAspect.setValue(new Aspect(glossaryTerms.data()));

    datasetResponse.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(GLOSSARY_TERMS_ASPECT_NAME, glossaryTermsAspect)));

    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(datasetResponse);

    // termA -> childNode, termB -> grandchildNode (no parent for variety)
    final Map<Urn, EntityResponse> termResponses = new HashMap<>();
    termResponses.put(TERM_A_URN, createGlossaryTermInfoResponse(TERM_A_URN, CHILD_NODE_URN));
    termResponses.put(TERM_B_URN, createGlossaryTermInfoResponse(TERM_B_URN, GRANDCHILD_NODE_URN));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            anySet(),
            eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              final Set<Urn> urns = invocation.getArgument(2);
              final Map<Urn, EntityResponse> responses = new HashMap<>();
              for (Urn urn : urns) {
                if (urn.equals(TERM_A_URN)) {
                  responses.put(
                      TERM_A_URN, createGlossaryTermInfoResponse(TERM_A_URN, CHILD_NODE_URN));
                } else if (urn.equals(TERM_B_URN)) {
                  responses.put(
                      TERM_B_URN, createGlossaryTermInfoResponse(TERM_B_URN, GRANDCHILD_NODE_URN));
                }
              }
              return responses;
            });

    // Node hierarchy: childNode -> parentNode, grandchildNode -> null
    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            anySet(),
            eq(Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              final Set<Urn> urns = invocation.getArgument(2);
              final Map<Urn, EntityResponse> responses = new HashMap<>();
              for (Urn urn : urns) {
                if (urn.equals(CHILD_NODE_URN)) {
                  responses.put(
                      CHILD_NODE_URN,
                      createGlossaryNodeInfoResponse(CHILD_NODE_URN, PARENT_NODE_URN));
                } else if (urn.equals(PARENT_NODE_URN)) {
                  responses.put(
                      PARENT_NODE_URN, createGlossaryNodeInfoResponse(PARENT_NODE_URN, null));
                } else if (urn.equals(GRANDCHILD_NODE_URN)) {
                  responses.put(
                      GRANDCHILD_NODE_URN,
                      createGlossaryNodeInfoResponse(GRANDCHILD_NODE_URN, null));
                }
              }
              return responses;
            });
  }

  private EntityResponse createGlossaryTermInfoResponse(
      final Urn termUrn, final Urn parentNodeUrn) {
    final EntityResponse response = new EntityResponse();
    response.setUrn(termUrn);
    response.setEntityName(GLOSSARY_TERM_ENTITY_NAME);

    final GlossaryTermInfo termInfo = new GlossaryTermInfo();
    termInfo.setName(termUrn.getId());
    if (parentNodeUrn != null) {
      try {
        termInfo.setParentNode(GlossaryNodeUrn.createFromUrn(parentNodeUrn));
      } catch (Exception e) {
        throw new RuntimeException("Failed to create GlossaryNodeUrn from " + parentNodeUrn, e);
      }
    }

    final EnvelopedAspect termInfoAspect = new EnvelopedAspect();
    termInfoAspect.setName(GLOSSARY_TERM_INFO_ASPECT_NAME);
    termInfoAspect.setValue(new Aspect(termInfo.data()));

    response.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(GLOSSARY_TERM_INFO_ASPECT_NAME, termInfoAspect)));

    return response;
  }

  private EntityResponse createGlossaryNodeInfoResponse(
      final Urn nodeUrn, final Urn parentNodeUrn) {
    final EntityResponse response = new EntityResponse();
    response.setUrn(nodeUrn);
    response.setEntityName(GLOSSARY_NODE_ENTITY_NAME);

    final GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo();
    nodeInfo.setName(nodeUrn.getId());
    if (parentNodeUrn != null) {
      try {
        nodeInfo.setParentNode(GlossaryNodeUrn.createFromUrn(parentNodeUrn));
      } catch (Exception e) {
        throw new RuntimeException("Failed to create GlossaryNodeUrn from " + parentNodeUrn, e);
      }
    }

    final EnvelopedAspect nodeInfoAspect = new EnvelopedAspect();
    nodeInfoAspect.setName(GLOSSARY_NODE_INFO_ASPECT_NAME);
    nodeInfoAspect.setValue(new Aspect(nodeInfo.data()));

    response.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(GLOSSARY_NODE_INFO_ASPECT_NAME, nodeInfoAspect)));

    return response;
  }
}
