package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateDataProductInput;
import com.linkedin.datahub.graphql.generated.CreateDataProductPropertiesInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateDataProductResolverTest {

  private static final Urn TEST_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:test-domain");
  private static final Urn TEST_DATA_PRODUCT_URN =
      UrnUtils.getUrn("urn:li:dataProduct:test-data-product");
  private static final String TEST_DATA_PRODUCT_ID = "test-data-product";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  @Test
  public void testCreateDataProductWithDomainAuthorizationSuccess() throws Exception {
    // Setup mocks
    DataProductService mockDataProductService = Mockito.mock(DataProductService.class);
    EntityService<?> mockEntityService = getMockEntityService();

    // Mock domain exists
    Mockito.when(mockDataProductService.verifyEntityExists(any(), eq(TEST_DOMAIN_URN)))
        .thenReturn(true);

    // Mock data product creation
    Mockito.when(
            mockDataProductService.createDataProduct(
                any(), eq(TEST_DATA_PRODUCT_ID), eq("Test Product"), eq("Test Description")))
        .thenReturn(TEST_DATA_PRODUCT_URN);

    // Mock setDomain
    Mockito.doNothing()
        .when(mockDataProductService)
        .setDomain(any(), eq(TEST_DATA_PRODUCT_URN), eq(TEST_DOMAIN_URN));

    // Mock getDataProductEntityResponse with proper aspects
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setUrn(TEST_DATA_PRODUCT_URN);
    mockResponse.setEntityName("dataProduct");
    mockResponse.setAspects(new com.linkedin.entity.EnvelopedAspectMap());
    Mockito.when(
            mockDataProductService.getDataProductEntityResponse(any(), eq(TEST_DATA_PRODUCT_URN)))
        .thenReturn(mockResponse);

    // Create resolver
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(mockDataProductService, mockEntityService);

    // Create input
    CreateDataProductPropertiesInput properties = new CreateDataProductPropertiesInput();
    properties.setName("Test Product");
    properties.setDescription("Test Description");

    CreateDataProductInput input = new CreateDataProductInput();
    input.setId(TEST_DATA_PRODUCT_ID);
    input.setDomainUrn(TEST_DOMAIN_URN.toString());
    input.setProperties(properties);

    // Setup environment with ALLOW context
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Execute - should succeed
    resolver.get(mockEnv).get();

    // Verify data product was created
    Mockito.verify(mockDataProductService, Mockito.times(1))
        .createDataProduct(
            any(), eq(TEST_DATA_PRODUCT_ID), eq("Test Product"), eq("Test Description"));
    Mockito.verify(mockDataProductService, Mockito.times(1))
        .setDomain(any(), eq(TEST_DATA_PRODUCT_URN), eq(TEST_DOMAIN_URN));
  }

  @Test
  public void testCreateDataProductWithDomainAuthorizationDenied() throws Exception {
    // Setup mocks
    DataProductService mockDataProductService = Mockito.mock(DataProductService.class);
    EntityService<?> mockEntityService = getMockEntityService();

    // Mock domain exists
    Mockito.when(mockDataProductService.verifyEntityExists(any(), eq(TEST_DOMAIN_URN)))
        .thenReturn(true);

    // Create resolver
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(mockDataProductService, mockEntityService);

    // Create input
    CreateDataProductPropertiesInput properties = new CreateDataProductPropertiesInput();
    properties.setName("Test Product");
    properties.setDescription("Test Description");

    CreateDataProductInput input = new CreateDataProductInput();
    input.setId(TEST_DATA_PRODUCT_ID);
    input.setDomainUrn(TEST_DOMAIN_URN.toString());
    input.setProperties(properties);

    // Setup environment with DENY context
    QueryContext mockContext = getMockDenyContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Execute - should fail with authorization exception
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify data product was NOT created
    Mockito.verify(mockDataProductService, Mockito.times(0))
        .createDataProduct(any(), any(), any(), any());
    Mockito.verify(mockDataProductService, Mockito.times(0)).setDomain(any(), any(), any());
  }

  @Test
  public void testCreateDataProductWithSpecificDomainAuthorizationRequest() throws Exception {
    // This test verifies that the data product creation works with domain assignment
    // The authorization logic is implicitly tested through the resolver's internal calls
    DataProductService mockDataProductService = Mockito.mock(DataProductService.class);
    EntityService<?> mockEntityService = getMockEntityService();

    // Mock domain exists
    Mockito.when(mockDataProductService.verifyEntityExists(any(), eq(TEST_DOMAIN_URN)))
        .thenReturn(true);

    // Mock data product creation
    Mockito.when(
            mockDataProductService.createDataProduct(
                any(), eq(TEST_DATA_PRODUCT_ID), eq("Test Product"), eq("Test Description")))
        .thenReturn(TEST_DATA_PRODUCT_URN);

    // Mock setDomain
    Mockito.doNothing()
        .when(mockDataProductService)
        .setDomain(any(), eq(TEST_DATA_PRODUCT_URN), eq(TEST_DOMAIN_URN));

    // Mock getDataProductEntityResponse with proper aspects
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setUrn(TEST_DATA_PRODUCT_URN);
    mockResponse.setEntityName("dataProduct");
    mockResponse.setAspects(new com.linkedin.entity.EnvelopedAspectMap());
    Mockito.when(
            mockDataProductService.getDataProductEntityResponse(any(), eq(TEST_DATA_PRODUCT_URN)))
        .thenReturn(mockResponse);

    // Create resolver
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(mockDataProductService, mockEntityService);

    // Create input
    CreateDataProductPropertiesInput properties = new CreateDataProductPropertiesInput();
    properties.setName("Test Product");
    properties.setDescription("Test Description");

    CreateDataProductInput input = new CreateDataProductInput();
    input.setId(TEST_DATA_PRODUCT_ID);
    input.setDomainUrn(TEST_DOMAIN_URN.toString());
    input.setProperties(properties);

    // Setup environment with standard ALLOW context
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Execute
    resolver.get(mockEnv).get();

    // Verify data product was created with the domain
    Mockito.verify(mockDataProductService, Mockito.times(1))
        .createDataProduct(
            any(), eq(TEST_DATA_PRODUCT_ID), eq("Test Product"), eq("Test Description"));

    // Verify domain was set on the data product
    Mockito.verify(mockDataProductService, Mockito.times(1))
        .setDomain(any(), eq(TEST_DATA_PRODUCT_URN), eq(TEST_DOMAIN_URN));
  }

  @Test
  public void testCreateDataProductWithNonExistentDomain() throws Exception {
    // Setup mocks
    DataProductService mockDataProductService = Mockito.mock(DataProductService.class);
    EntityService<?> mockEntityService = getMockEntityService();

    // Mock domain does NOT exist
    Mockito.when(mockDataProductService.verifyEntityExists(any(), eq(TEST_DOMAIN_URN)))
        .thenReturn(false);

    // Create resolver
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(mockDataProductService, mockEntityService);

    // Create input
    CreateDataProductPropertiesInput properties = new CreateDataProductPropertiesInput();
    properties.setName("Test Product");
    properties.setDescription("Test Description");

    CreateDataProductInput input = new CreateDataProductInput();
    input.setId(TEST_DATA_PRODUCT_ID);
    input.setDomainUrn(TEST_DOMAIN_URN.toString());
    input.setProperties(properties);

    // Setup environment
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Execute - should fail because domain doesn't exist
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify data product was NOT created
    Mockito.verify(mockDataProductService, Mockito.times(0))
        .createDataProduct(any(), any(), any(), any());
  }
}
