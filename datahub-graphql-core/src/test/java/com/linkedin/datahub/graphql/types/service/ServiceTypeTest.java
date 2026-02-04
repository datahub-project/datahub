package com.linkedin.datahub.graphql.types.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Service;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.service.ServiceProperties;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ServiceTypeTest {

  private static final String SERVICE_URN_1 = "urn:li:service:service1";
  private static final String SERVICE_URN_2 = "urn:li:service:service2";

  @Mock private EntityClient entityClient;
  @Mock private QueryContext queryContext;
  @Mock private OperationContext operationContext;

  private ServiceType serviceType;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    serviceType = new ServiceType(entityClient);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Basic Accessor Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testType() {
    assertEquals(serviceType.type(), EntityType.SERVICE);
  }

  @Test
  public void testObjectClass() {
    assertEquals(serviceType.objectClass(), Service.class);
  }

  @Test
  public void testGetKeyProvider() {
    Service service = new Service();
    service.setUrn(SERVICE_URN_1);
    assertEquals(serviceType.getKeyProvider().apply(service), SERVICE_URN_1);
  }

  @Test
  public void testAspectsToFetchContainsRequiredAspects() {
    assertTrue(ServiceType.ASPECTS_TO_FETCH.contains(Constants.SERVICE_PROPERTIES_ASPECT_NAME));
    assertTrue(ServiceType.ASPECTS_TO_FETCH.contains(Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME));
    assertTrue(ServiceType.ASPECTS_TO_FETCH.contains(Constants.SUB_TYPES_ASPECT_NAME));
    assertTrue(ServiceType.ASPECTS_TO_FETCH.contains(Constants.OWNERSHIP_ASPECT_NAME));
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Constructor Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testConstructorNullEntityClient() {
    assertThrows(NullPointerException.class, () -> new ServiceType(null));
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // BatchLoad Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testBatchLoadSingleService() throws Exception {
    List<String> urns = ImmutableList.of(SERVICE_URN_1);
    Urn serviceUrn = UrnUtils.getUrn(SERVICE_URN_1);

    ServiceProperties props = new ServiceProperties();
    props.setDisplayName("Service 1");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            eq(ServiceType.ASPECTS_TO_FETCH)))
        .thenReturn(ImmutableMap.of(serviceUrn, entityResponse));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<Service>> results =
        serviceType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertNotNull(results.get(0));
    assertNotNull(results.get(0).getData());
    assertEquals(results.get(0).getData().getUrn(), SERVICE_URN_1);
    assertEquals(results.get(0).getData().getProperties().getDisplayName(), "Service 1");
  }

  @Test
  public void testBatchLoadMultipleServices() throws Exception {
    List<String> urns = ImmutableList.of(SERVICE_URN_1, SERVICE_URN_2);
    Urn serviceUrn1 = UrnUtils.getUrn(SERVICE_URN_1);
    Urn serviceUrn2 = UrnUtils.getUrn(SERVICE_URN_2);

    ServiceProperties props1 = new ServiceProperties();
    props1.setDisplayName("Service 1");
    ServiceProperties props2 = new ServiceProperties();
    props2.setDisplayName("Service 2");

    EntityResponse response1 = createEntityResponse(serviceUrn1, props1);
    EntityResponse response2 = createEntityResponse(serviceUrn2, props2);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            eq(ServiceType.ASPECTS_TO_FETCH)))
        .thenReturn(ImmutableMap.of(serviceUrn1, response1, serviceUrn2, response2));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<Service>> results =
        serviceType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    // Results should be in the same order as URNs
    assertEquals(results.get(0).getData().getUrn(), SERVICE_URN_1);
    assertEquals(results.get(1).getData().getUrn(), SERVICE_URN_2);
  }

  @Test
  public void testBatchLoadEmptyList() throws Exception {
    List<String> urns = Collections.emptyList();

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            eq(ServiceType.ASPECTS_TO_FETCH)))
        .thenReturn(Collections.emptyMap());

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<Service>> results =
        serviceType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 0);
  }

  @Test
  public void testBatchLoadServiceNotFound() throws Exception {
    List<String> urns = ImmutableList.of(SERVICE_URN_1);

    // Return empty map (service doesn't exist)
    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            eq(ServiceType.ASPECTS_TO_FETCH)))
        .thenReturn(Collections.emptyMap());

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<Service>> results =
        serviceType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    // Result should be null for missing entity
    assertNull(results.get(0));
  }

  @Test
  public void testBatchLoadPartialResults() throws Exception {
    List<String> urns = ImmutableList.of(SERVICE_URN_1, SERVICE_URN_2);
    Urn serviceUrn1 = UrnUtils.getUrn(SERVICE_URN_1);

    ServiceProperties props1 = new ServiceProperties();
    props1.setDisplayName("Service 1");
    EntityResponse response1 = createEntityResponse(serviceUrn1, props1);

    // Only return service1, not service2
    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            eq(ServiceType.ASPECTS_TO_FETCH)))
        .thenReturn(ImmutableMap.of(serviceUrn1, response1));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<Service>> results =
        serviceType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    assertNotNull(results.get(0));
    assertNotNull(results.get(0).getData());
    assertNull(results.get(1)); // Service 2 not found
  }

  @Test
  public void testBatchLoadThrowsException() throws Exception {
    List<String> urns = ImmutableList.of(SERVICE_URN_1);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            eq(ServiceType.ASPECTS_TO_FETCH)))
        .thenThrow(new RemoteInvocationException("Batch get failed"));

    // Use batchLoadWithoutAuthorization to test exception handling
    assertThrows(
        RuntimeException.class,
        () -> serviceType.batchLoadWithoutAuthorization(urns, queryContext));
  }

  @Test
  public void testBatchLoadPreservesOrder() throws Exception {
    // Request in reverse order
    List<String> urns = ImmutableList.of(SERVICE_URN_2, SERVICE_URN_1);
    Urn serviceUrn1 = UrnUtils.getUrn(SERVICE_URN_1);
    Urn serviceUrn2 = UrnUtils.getUrn(SERVICE_URN_2);

    ServiceProperties props1 = new ServiceProperties();
    props1.setDisplayName("Service 1");
    ServiceProperties props2 = new ServiceProperties();
    props2.setDisplayName("Service 2");

    EntityResponse response1 = createEntityResponse(serviceUrn1, props1);
    EntityResponse response2 = createEntityResponse(serviceUrn2, props2);

    // Map order doesn't matter - batchLoad should preserve input order
    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            eq(ServiceType.ASPECTS_TO_FETCH)))
        .thenReturn(ImmutableMap.of(serviceUrn1, response1, serviceUrn2, response2));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<Service>> results =
        serviceType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    // Should be in same order as requested
    assertEquals(results.get(0).getData().getUrn(), SERVICE_URN_2);
    assertEquals(results.get(1).getData().getUrn(), SERVICE_URN_1);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════════════

  private EntityResponse createEntityResponse(Urn urn, ServiceProperties props) {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(Constants.SERVICE_ENTITY_NAME);
    response.setAspects(aspects);
    return response;
  }
}
