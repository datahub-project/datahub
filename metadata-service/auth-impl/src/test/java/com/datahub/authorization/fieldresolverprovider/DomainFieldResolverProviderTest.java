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
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
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
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Override
  protected DomainFieldResolverProvider buildFieldResolverProvider() {
    return new DomainFieldResolverProvider(mock(SystemEntityClient.class));
  }

  @Test
  public void testDomainEntityResolvesWithParents()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
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
    // Setup: parent domain with no parent
    Map<Urn, EntityResponse> parentResponse = new HashMap<>();
    parentResponse.put(
        PARENT_DOMAIN_URN, createDomainPropertiesResponse(PARENT_DOMAIN_URN, null /* no parent */));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(PARENT_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(parentResponse);

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
    // Setup: Test that we don't infinitely loop if there's a cycle
    // In practice this shouldn't happen, but we should handle it gracefully
    Map<Urn, EntityResponse> childResponse = new HashMap<>();
    childResponse.put(
        CHILD_DOMAIN_URN, createDomainPropertiesResponse(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN));

    Map<Urn, EntityResponse> parentResponse = new HashMap<>();
    parentResponse.put(
        PARENT_DOMAIN_URN, createDomainPropertiesResponse(PARENT_DOMAIN_URN, CHILD_DOMAIN_URN));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(CHILD_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(childResponse);

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(PARENT_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(parentResponse);

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

  // Helper methods

  private void setupDomainHierarchy() throws RemoteInvocationException, URISyntaxException {
    // grandchild -> child
    Map<Urn, EntityResponse> grandchildResponse = new HashMap<>();
    grandchildResponse.put(
        GRANDCHILD_DOMAIN_URN,
        createDomainPropertiesResponse(GRANDCHILD_DOMAIN_URN, CHILD_DOMAIN_URN));

    // child -> parent
    Map<Urn, EntityResponse> childResponse = new HashMap<>();
    childResponse.put(
        CHILD_DOMAIN_URN, createDomainPropertiesResponse(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN));

    // parent -> null
    Map<Urn, EntityResponse> parentResponse = new HashMap<>();
    parentResponse.put(PARENT_DOMAIN_URN, createDomainPropertiesResponse(PARENT_DOMAIN_URN, null));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(GRANDCHILD_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(grandchildResponse);

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(CHILD_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(childResponse);

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(PARENT_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(parentResponse);
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

    // child -> parent hierarchy
    Map<Urn, EntityResponse> childResponse = new HashMap<>();
    childResponse.put(
        CHILD_DOMAIN_URN, createDomainPropertiesResponse(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN));

    Map<Urn, EntityResponse> parentResponse = new HashMap<>();
    parentResponse.put(PARENT_DOMAIN_URN, createDomainPropertiesResponse(PARENT_DOMAIN_URN, null));

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(CHILD_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(childResponse);

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(PARENT_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(parentResponse);

    // Handle anySet() for batching
    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(DOMAIN_ENTITY_NAME),
            anySet(),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(2);
              Map<Urn, EntityResponse> responses = new HashMap<>();
              if (urns.contains(CHILD_DOMAIN_URN)) {
                responses.put(
                    CHILD_DOMAIN_URN,
                    createDomainPropertiesResponse(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN));
              }
              if (urns.contains(PARENT_DOMAIN_URN)) {
                responses.put(
                    PARENT_DOMAIN_URN, createDomainPropertiesResponse(PARENT_DOMAIN_URN, null));
              }
              return responses;
            });
  }

  private EntityResponse createDomainPropertiesResponse(Urn domainUrn, Urn parentDomainUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(domainUrn);
    response.setEntityName(DOMAIN_ENTITY_NAME);

    DomainProperties properties = new DomainProperties();
    properties.setName(domainUrn.getId());
    if (parentDomainUrn != null) {
      properties.setParentDomain(parentDomainUrn);
    }

    EnvelopedAspect propertiesAspect = new EnvelopedAspect();
    propertiesAspect.setName(DOMAIN_PROPERTIES_ASPECT_NAME);
    propertiesAspect.setValue(new Aspect(properties.data()));

    response.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(DOMAIN_PROPERTIES_ASPECT_NAME, propertiesAspect)));

    return response;
  }
}
