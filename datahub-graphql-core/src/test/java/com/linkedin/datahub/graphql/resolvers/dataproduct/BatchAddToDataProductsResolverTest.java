package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.BatchSetDataProductsInput;
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

public class BatchAddToDataProductsResolverTest {

  private static final String TEST_RESOURCE_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_RESOURCE_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_DATA_PRODUCT_URN_1 = "urn:li:dataProduct:test-product-1";
  private static final String TEST_DATA_PRODUCT_URN_2 = "urn:li:dataProduct:test-product-2";

  private static void mockExistingUrns(DataProductService mockService, Urn... existingUrns) {
    final Set<Urn> existing = new HashSet<>(List.of(existingUrns));
    Mockito.when(mockService.filterExistingUrns(any(), anyCollection()))
        .thenAnswer(
            invocation -> {
              Collection<Urn> requestedUrns = invocation.getArgument(1);
              return requestedUrns.stream().filter(existing::contains).collect(Collectors.toSet());
            });
  }

  @Test
  public void testGetSuccessMultipleDataProducts() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFeatureFlags.isMultipleDataProductsPerAsset()).thenReturn(true);
    mockExistingUrns(
        mockService,
        UrnUtils.getUrn(TEST_RESOURCE_URN_1),
        UrnUtils.getUrn(TEST_RESOURCE_URN_2),
        UrnUtils.getUrn(TEST_DATA_PRODUCT_URN_1),
        UrnUtils.getUrn(TEST_DATA_PRODUCT_URN_2));

    BatchAddToDataProductsResolver resolver =
        new BatchAddToDataProductsResolver(mockService, mockFeatureFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductsInput input = new BatchSetDataProductsInput();
    input.setResourceUrns(List.of(TEST_RESOURCE_URN_1, TEST_RESOURCE_URN_2));
    input.setDataProductUrns(List.of(TEST_DATA_PRODUCT_URN_1, TEST_DATA_PRODUCT_URN_2));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(2))
        .filterExistingUrns(any(), any()); // 1 batched call for resources + 1 for data products
    Mockito.verify(mockService, Mockito.times(2))
        .batchAddToDataProduct(any(), any(Urn.class), anyList()); // 2 data products
  }

  @Test
  public void testGetSuccessSingleResourceSingleDataProduct() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFeatureFlags.isMultipleDataProductsPerAsset()).thenReturn(true);
    mockExistingUrns(
        mockService,
        UrnUtils.getUrn(TEST_RESOURCE_URN_1),
        UrnUtils.getUrn(TEST_DATA_PRODUCT_URN_1));

    BatchAddToDataProductsResolver resolver =
        new BatchAddToDataProductsResolver(mockService, mockFeatureFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductsInput input = new BatchSetDataProductsInput();
    input.setResourceUrns(List.of(TEST_RESOURCE_URN_1));
    input.setDataProductUrns(List.of(TEST_DATA_PRODUCT_URN_1));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(2))
        .filterExistingUrns(any(), any()); // 1 batched call for resources + 1 for data products
    Mockito.verify(mockService, Mockito.times(1))
        .batchAddToDataProduct(any(), eq(UrnUtils.getUrn(TEST_DATA_PRODUCT_URN_1)), anyList());
  }

  @Test
  public void testGetFailureFeatureFlagDisabled() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFeatureFlags.isMultipleDataProductsPerAsset()).thenReturn(false);

    BatchAddToDataProductsResolver resolver =
        new BatchAddToDataProductsResolver(mockService, mockFeatureFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductsInput input = new BatchSetDataProductsInput();
    input.setResourceUrns(List.of(TEST_RESOURCE_URN_1));
    input.setDataProductUrns(List.of(TEST_DATA_PRODUCT_URN_1));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(Exception.class, () -> resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(0)).batchAddToDataProduct(any(), any(), any());
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFeatureFlags.isMultipleDataProductsPerAsset()).thenReturn(true);
    mockExistingUrns(mockService); // Resource does not exist

    BatchAddToDataProductsResolver resolver =
        new BatchAddToDataProductsResolver(mockService, mockFeatureFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductsInput input = new BatchSetDataProductsInput();
    input.setResourceUrns(List.of(TEST_RESOURCE_URN_1));
    input.setDataProductUrns(List.of(TEST_DATA_PRODUCT_URN_1));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).batchAddToDataProduct(any(), any(), any());
  }

  @Test
  public void testGetFailureDataProductDoesNotExist() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFeatureFlags.isMultipleDataProductsPerAsset()).thenReturn(true);
    mockExistingUrns(mockService, UrnUtils.getUrn(TEST_RESOURCE_URN_1)); // Data product missing

    BatchAddToDataProductsResolver resolver =
        new BatchAddToDataProductsResolver(mockService, mockFeatureFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDataProductsInput input = new BatchSetDataProductsInput();
    input.setResourceUrns(List.of(TEST_RESOURCE_URN_1));
    input.setDataProductUrns(List.of(TEST_DATA_PRODUCT_URN_1));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).batchAddToDataProduct(any(), any(), any());
  }
}
