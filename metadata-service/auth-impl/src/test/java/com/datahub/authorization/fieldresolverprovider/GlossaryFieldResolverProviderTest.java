package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
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

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  private SystemEntityClient mockEntityClient;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    opContext = operationContextWithGlossaryParents(Map.of(), Map.of());
  }

  @Override
  protected GlossaryFieldResolverProvider buildFieldResolverProvider() {
    return new GlossaryFieldResolverProvider(mock(SystemEntityClient.class));
  }

  @Test
  public void testGlossaryTermWithParentNodes()
      throws ExecutionException, InterruptedException, URISyntaxException {
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
      throws ExecutionException, InterruptedException, URISyntaxException {
    Map<Urn, Urn> parentByTerm = new HashMap<>();
    parentByTerm.put(TERM_A_URN, null);
    opContext = operationContextWithGlossaryParents(parentByTerm, Map.of());

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
      throws ExecutionException, InterruptedException, URISyntaxException {
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

  @Test
  public void testGlossaryNodeWithParentNodes()
      throws ExecutionException, InterruptedException, URISyntaxException {
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
      throws ExecutionException, InterruptedException, URISyntaxException {
    Map<Urn, Urn> parentByNode = new HashMap<>();
    parentByNode.put(PARENT_NODE_URN, null);
    opContext = operationContextWithGlossaryParents(Map.of(), parentByNode);

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
      throws ExecutionException, InterruptedException, URISyntaxException {
    Map<Urn, Urn> parentByNode = new HashMap<>();
    parentByNode.put(CHILD_NODE_URN, PARENT_NODE_URN);
    parentByNode.put(PARENT_NODE_URN, CHILD_NODE_URN);
    opContext = operationContextWithGlossaryParents(Map.of(), parentByNode);

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

  @Test
  public void testGlossaryTermUsesEntityGraphCacheWhenExpandSucceeds()
      throws ExecutionException, InterruptedException, URISyntaxException {
    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(TERM_A_URN.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(
                    TERM_A_URN.toString(), CHILD_NODE_URN.toString(), PARENT_NODE_URN.toString())));

    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    opContext =
        operationContextWithGlossaryParents(Map.of(), Map.of(), entityGraphCache, aspectRetriever);

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(GLOSSARY_TERM_ENTITY_NAME, TERM_A_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    Set<String> values = result.getValues();
    assertEquals(values.size(), 3);
    verify(aspectRetriever, never()).getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void testDatasetWithGlossaryTerms()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
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
    setupDatasetWithMultipleGlossaryTerms();

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
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
    final EntityResponse datasetResponse = new EntityResponse();
    datasetResponse.setUrn(DATASET_URN);
    datasetResponse.setEntityName(DATASET_ENTITY_NAME);

    final GlossaryTerms glossaryTerms = new GlossaryTerms();
    final GlossaryTermAssociation association = new GlossaryTermAssociation();
    association.setUrn(GlossaryTermUrn.createFromUrn(TERM_A_URN));
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

    Map<Urn, Urn> parentByTerm = new HashMap<>();
    parentByTerm.put(TERM_A_URN, null);
    opContext = operationContextWithGlossaryParents(parentByTerm, Map.of());

    final GlossaryFieldResolverProvider provider =
        new GlossaryFieldResolverProvider(mockEntityClient);
    final EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());

    final FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    final FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    final Set<String> values = result.getValues();
    assertEquals(values.size(), 1, "Should resolve only the root-level term with no parent nodes");
    assertTrue(values.contains(TERM_A_URN.toString()));
  }

  private OperationContext operationContextWithGlossaryParents(
      Map<Urn, Urn> parentByTerm, Map<Urn, Urn> parentByNode) {
    return operationContextWithGlossaryParents(
        parentByTerm, parentByNode, EntityGraphCache.NO_OP, mock(AspectRetriever.class));
  }

  private OperationContext operationContextWithGlossaryParents(
      Map<Urn, Urn> parentByTerm,
      Map<Urn, Urn> parentByNode,
      EntityGraphCache entityGraphCache,
      AspectRetriever aspectRetriever) {
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              @SuppressWarnings("unchecked")
              Set<String> aspectNames = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                if (aspectNames.contains(GLOSSARY_TERM_INFO_ASPECT_NAME)
                    && parentByTerm.containsKey(urn)) {
                  GlossaryTermInfo termInfo = new GlossaryTermInfo();
                  Urn parent = parentByTerm.get(urn);
                  if (parent != null) {
                    termInfo.setParentNode(GlossaryNodeUrn.createFromUrn(parent));
                  }
                  result.put(
                      urn, Map.of(GLOSSARY_TERM_INFO_ASPECT_NAME, new Aspect(termInfo.data())));
                }
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

  private void setupTermWithNodeHierarchy() {
    Map<Urn, Urn> parentByTerm = Map.of(TERM_A_URN, CHILD_NODE_URN);
    Map<Urn, Urn> parentByNode = new HashMap<>();
    parentByNode.put(CHILD_NODE_URN, PARENT_NODE_URN);
    parentByNode.put(PARENT_NODE_URN, null);
    opContext = operationContextWithGlossaryParents(parentByTerm, parentByNode);
  }

  private void setupTermWithDeepNodeHierarchy() {
    Map<Urn, Urn> parentByTerm = Map.of(TERM_A_URN, GRANDCHILD_NODE_URN);
    Map<Urn, Urn> parentByNode = new HashMap<>();
    parentByNode.put(GRANDCHILD_NODE_URN, CHILD_NODE_URN);
    parentByNode.put(CHILD_NODE_URN, PARENT_NODE_URN);
    parentByNode.put(PARENT_NODE_URN, null);
    opContext = operationContextWithGlossaryParents(parentByTerm, parentByNode);
  }

  private void setupNodeHierarchy() {
    Map<Urn, Urn> parentByNode = new HashMap<>();
    parentByNode.put(GRANDCHILD_NODE_URN, CHILD_NODE_URN);
    parentByNode.put(CHILD_NODE_URN, PARENT_NODE_URN);
    parentByNode.put(PARENT_NODE_URN, null);
    opContext = operationContextWithGlossaryParents(Map.of(), parentByNode);
  }

  private void setupDatasetWithGlossaryTerms()
      throws RemoteInvocationException, URISyntaxException {
    final EntityResponse datasetResponse = new EntityResponse();
    datasetResponse.setUrn(DATASET_URN);
    datasetResponse.setEntityName(DATASET_ENTITY_NAME);

    final GlossaryTerms glossaryTerms = new GlossaryTerms();
    final GlossaryTermAssociation association = new GlossaryTermAssociation();
    association.setUrn(GlossaryTermUrn.createFromUrn(TERM_A_URN));
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

    setupTermWithNodeHierarchy();
  }

  private void setupDatasetWithMultipleGlossaryTerms()
      throws RemoteInvocationException, URISyntaxException {
    final EntityResponse datasetResponse = new EntityResponse();
    datasetResponse.setUrn(DATASET_URN);
    datasetResponse.setEntityName(DATASET_ENTITY_NAME);

    final GlossaryTerms glossaryTerms = new GlossaryTerms();
    final GlossaryTermAssociation assocA = new GlossaryTermAssociation();
    final GlossaryTermAssociation assocB = new GlossaryTermAssociation();
    assocA.setUrn(GlossaryTermUrn.createFromUrn(TERM_A_URN));
    assocB.setUrn(GlossaryTermUrn.createFromUrn(TERM_B_URN));
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

    Map<Urn, Urn> parentByTerm = new HashMap<>();
    parentByTerm.put(TERM_A_URN, CHILD_NODE_URN);
    parentByTerm.put(TERM_B_URN, GRANDCHILD_NODE_URN);
    Map<Urn, Urn> parentByNode = new HashMap<>();
    parentByNode.put(CHILD_NODE_URN, PARENT_NODE_URN);
    parentByNode.put(PARENT_NODE_URN, null);
    parentByNode.put(GRANDCHILD_NODE_URN, null);
    opContext = operationContextWithGlossaryParents(parentByTerm, parentByNode);
  }
}
