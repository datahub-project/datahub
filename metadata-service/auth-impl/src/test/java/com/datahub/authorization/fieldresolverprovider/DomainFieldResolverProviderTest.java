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
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<DomainFieldResolverProvider> {

  private static final Urn PARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:parent");
  private static final Urn CHILD_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:child");
  private static final Urn GRANDCHILD_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:grandchild");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  private SystemEntityClient mockEntityClient;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    opContext = operationContextWithDomainParents(Map.of());
  }

  private OperationContext operationContextWithDomainParents(Map<Urn, Urn> parentByDomain) {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(
            any(), any(), eq(Set.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                if (parentByDomain.containsKey(urn)) {
                  DomainProperties properties = new DomainProperties();
                  Urn parent = parentByDomain.get(urn);
                  if (parent != null) {
                    properties.setParentDomain(parent);
                  }
                  result.put(
                      urn, Map.of(DOMAIN_PROPERTIES_ASPECT_NAME, new Aspect(properties.data())));
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
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }

  @Override
  protected DomainFieldResolverProvider buildFieldResolverProvider() {
    return new DomainFieldResolverProvider(mock(SystemEntityClient.class));
  }

  @Test
  public void testDomainEntityResolvesWithParents()
      throws ExecutionException, InterruptedException, URISyntaxException {
    // Setup: grandchild -> child -> parent
    setupDomainHierarchy();

    DomainFieldResolverProvider provider = new DomainFieldResolverProvider(mockEntityClient);
    EntitySpec entitySpec = new EntitySpec(DOMAIN_ENTITY_NAME, GRANDCHILD_DOMAIN_URN.toString());

    FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    Set<String> values = result.getValues();
    assertEquals(values.size(), 3, "Should resolve grandchild, child, and parent domains");
    assertTrue(values.contains(GRANDCHILD_DOMAIN_URN.toString()));
    assertTrue(values.contains(CHILD_DOMAIN_URN.toString()));
    assertTrue(values.contains(PARENT_DOMAIN_URN.toString()));
  }

  @Test
  public void testDomainEntityWithNoParent()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    Map<Urn, Urn> parentByDomain = new HashMap<>();
    parentByDomain.put(PARENT_DOMAIN_URN, null);
    opContext = operationContextWithDomainParents(parentByDomain);

    DomainFieldResolverProvider provider = new DomainFieldResolverProvider(mockEntityClient);
    EntitySpec entitySpec = new EntitySpec(DOMAIN_ENTITY_NAME, PARENT_DOMAIN_URN.toString());

    FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    Set<String> values = result.getValues();
    assertEquals(values.size(), 1, "Should resolve only the parent domain itself");
    assertTrue(values.contains(PARENT_DOMAIN_URN.toString()));
  }

  @Test
  public void testNonDomainEntityWithDomainAssignment()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: dataset with child domain assignment, child -> parent hierarchy
    setupDatasetWithDomain();

    DomainFieldResolverProvider provider = new DomainFieldResolverProvider(mockEntityClient);
    EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());

    FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    Set<String> values = result.getValues();
    assertEquals(values.size(), 2, "Should resolve child domain and its parent");
    assertTrue(values.contains(CHILD_DOMAIN_URN.toString()));
    assertTrue(values.contains(PARENT_DOMAIN_URN.toString()));
  }

  @Test
  public void testNonDomainEntityWithNoDomainAssignment()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    // Setup: dataset with no domain assignment
    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(null);

    DomainFieldResolverProvider provider = new DomainFieldResolverProvider(mockEntityClient);
    EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());

    FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    Set<String> values = result.getValues();
    assertEquals(values.size(), 0, "Should return empty set for entity with no domain");
  }

  @Test
  public void testDomainHierarchyWithCyclePrevention()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    Map<Urn, Urn> parentByDomain = new HashMap<>();
    parentByDomain.put(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN);
    parentByDomain.put(PARENT_DOMAIN_URN, CHILD_DOMAIN_URN);
    opContext = operationContextWithDomainParents(parentByDomain);

    DomainFieldResolverProvider provider = new DomainFieldResolverProvider(mockEntityClient);
    EntitySpec entitySpec = new EntitySpec(DOMAIN_ENTITY_NAME, CHILD_DOMAIN_URN.toString());

    FieldResolver resolver = provider.getFieldResolver(opContext, entitySpec);
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    Set<String> values = result.getValues();
    assertEquals(
        values.size(), 2, "Should resolve both domains despite cycle, but not infinitely loop");
    assertTrue(values.contains(CHILD_DOMAIN_URN.toString()));
    assertTrue(values.contains(PARENT_DOMAIN_URN.toString()));
  }

  @Test
  public void testDomainEntityUsesEntityGraphCacheWhenExpandSucceeds()
      throws ExecutionException, InterruptedException, URISyntaxException {
    com.linkedin.metadata.graph.cache.EntityGraphCache entityGraphCache =
        mock(com.linkedin.metadata.graph.cache.EntityGraphCache.class);
    com.linkedin.metadata.graph.cache.EntityGraphBinding binding =
        com.linkedin.metadata.graph.cache.EntityGraphBinding.builder()
            .graphId("domain")
            .source(com.linkedin.metadata.graph.cache.GraphSnapshotSource.SEARCH)
            .build();
    when(entityGraphCache.bindingForPolicyField("DOMAIN")).thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("domain"),
            eq(com.linkedin.metadata.graph.cache.GraphSnapshotSource.SEARCH),
            eq(com.linkedin.metadata.graph.cache.TraversalDirection.FORWARD),
            eq(Set.of(GRANDCHILD_DOMAIN_URN.toString())),
            anyInt(),
            eq(com.linkedin.metadata.graph.cache.EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(com.linkedin.metadata.graph.cache.ReadMode.CACHED)))
        .thenReturn(
            com.linkedin.metadata.graph.cache.GraphReadResult.fromVertices(
                Set.of(
                    GRANDCHILD_DOMAIN_URN.toString(),
                    CHILD_DOMAIN_URN.toString(),
                    PARENT_DOMAIN_URN.toString())));

    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(aspectRetriever)
            .entityGraphCache(entityGraphCache)
            .build();
    OperationContext cacheContext =
        TestOperationContexts.systemContextNoSearchAuthorization().toBuilder()
            .retrieverContext(retrieverContext)
            .build(
                TestOperationContexts.systemContextNoSearchAuthorization()
                    .getSessionAuthentication(),
                false);

    DomainFieldResolverProvider provider = new DomainFieldResolverProvider(mockEntityClient);
    EntitySpec entitySpec = new EntitySpec(DOMAIN_ENTITY_NAME, GRANDCHILD_DOMAIN_URN.toString());

    FieldResolver resolver = provider.getFieldResolver(cacheContext, entitySpec);
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().get();

    Set<String> values = result.getValues();
    assertEquals(values.size(), 3);
    verify(aspectRetriever, never()).getLatestAspectObjects(any(), any(), any());
  }

  // Helper methods

  private void setupDomainHierarchy() {
    Map<Urn, Urn> parentByDomain = new HashMap<>();
    parentByDomain.put(GRANDCHILD_DOMAIN_URN, CHILD_DOMAIN_URN);
    parentByDomain.put(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN);
    parentByDomain.put(PARENT_DOMAIN_URN, null);
    opContext = operationContextWithDomainParents(parentByDomain);
  }

  private void setupDatasetWithDomain() throws RemoteInvocationException, URISyntaxException {
    // Dataset has child domain assignment
    EntityResponse datasetResponse = new EntityResponse();
    datasetResponse.setUrn(DATASET_URN);
    datasetResponse.setEntityName(DATASET_ENTITY_NAME);

    Domains domains = new Domains();
    domains.setDomains(
        new com.linkedin.common.UrnArray(Collections.singletonList(CHILD_DOMAIN_URN)));

    EnvelopedAspect domainsAspect = new EnvelopedAspect();
    domainsAspect.setName(DOMAINS_ASPECT_NAME);
    domainsAspect.setValue(new Aspect(domains.data()));

    datasetResponse.setAspects(
        new EnvelopedAspectMap(Collections.singletonMap(DOMAINS_ASPECT_NAME, domainsAspect)));

    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(datasetResponse);

    Map<Urn, Urn> parentByDomain = new HashMap<>();
    parentByDomain.put(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN);
    parentByDomain.put(PARENT_DOMAIN_URN, null);
    opContext = operationContextWithDomainParents(parentByDomain);
  }
}
