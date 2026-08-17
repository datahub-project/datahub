package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchSetDataProductInput;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchSetDataProductResolverTest {

  private static final String TEST_RESOURCE_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_RESOURCE_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_DATA_PRODUCT_URN = "urn:li:dataProduct:test-product";
  private static final String TEST_DOMAIN_URN = "urn:li:domain:test-domain";

  private static void mockExistingUrns(DataProductService mockService, Urn... existingUrns) {
    final Set<Urn> existing = new HashSet<>(List.of(existingUrns));
    Mockito.when(mockService.filterExistingUrns(any(), anyCollection()))
        .thenAnswer(
            invocation -> {
              Collection<Urn> requestedUrns = invocation.getArgument(1);
              return requestedUrns.stream().filter(existing::contains).collect(Collectors.toSet());
            });
  }

  private static void mockDataProductDomain(DataProductService mockService) {
    Mockito.when(mockService.getDataProductDomains(any(), any()))
        .thenReturn(new Domains().setDomains(new UrnArray(UrnUtils.getUrn(TEST_DOMAIN_URN))));
  }

  @Test
  public void testGetSuccessWithDataProductUrn() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    mockExistingUrns(
        mockService, UrnUtils.getUrn(TEST_RESOURCE_URN_1), UrnUtils.getUrn(TEST_RESOURCE_URN_2));
    mockDataProductDomain(mockService);
    Mockito.when(mockService.verifyEntityExists(any(), eq(UrnUtils.getUrn(TEST_DATA_PRODUCT_URN))))
        .thenReturn(true);

    BatchSetDataProductResolver resolver = new BatchSetDataProductResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductInput input =
        new BatchSetDataProductInput(
            TEST_DATA_PRODUCT_URN, List.of(TEST_RESOURCE_URN_1, TEST_RESOURCE_URN_2));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .batchSetDataProduct(any(), eq(UrnUtils.getUrn(TEST_DATA_PRODUCT_URN)), anyList());
  }

  @Test
  public void testGetSuccessWithoutDataProductUrn() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    mockExistingUrns(mockService, UrnUtils.getUrn(TEST_RESOURCE_URN_1));

    BatchSetDataProductResolver resolver = new BatchSetDataProductResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductInput input =
        new BatchSetDataProductInput(null, List.of(TEST_RESOURCE_URN_1));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).batchUnsetDataProduct(any(), anyList());
  }

  @Test
  public void testGetFailureResourceDoesNotExistWithDataProductUrn() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    mockExistingUrns(mockService); // Resource does not exist
    mockDataProductDomain(mockService);
    Mockito.when(mockService.verifyEntityExists(any(), eq(UrnUtils.getUrn(TEST_DATA_PRODUCT_URN))))
        .thenReturn(true);

    BatchSetDataProductResolver resolver = new BatchSetDataProductResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductInput input =
        new BatchSetDataProductInput(TEST_DATA_PRODUCT_URN, List.of(TEST_RESOURCE_URN_1));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).batchSetDataProduct(any(), any(), any());
  }

  @Test
  public void testGetFailureResourceDoesNotExistWithoutDataProductUrn() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    mockExistingUrns(mockService); // Resource does not exist

    BatchSetDataProductResolver resolver = new BatchSetDataProductResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductInput input =
        new BatchSetDataProductInput(null, List.of(TEST_RESOURCE_URN_1));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).batchUnsetDataProduct(any(), any());
  }

  @Test
  public void testGetFailureUnauthorizedProductSideMembershipChange() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    mockExistingUrns(mockService, UrnUtils.getUrn(TEST_RESOURCE_URN_1));
    Mockito.when(mockService.verifyEntityExists(any(), eq(UrnUtils.getUrn(TEST_DATA_PRODUCT_URN))))
        .thenReturn(true);
    // No domains associated with the data product, so no domain grants MANAGE_DATA_PRODUCTS.
    Mockito.when(mockService.getDataProductDomains(any(), any())).thenReturn(new Domains());

    BatchSetDataProductResolver resolver = new BatchSetDataProductResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductInput input =
        new BatchSetDataProductInput(TEST_DATA_PRODUCT_URN, List.of(TEST_RESOURCE_URN_1));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).batchSetDataProduct(any(), any(), any());
  }
}
