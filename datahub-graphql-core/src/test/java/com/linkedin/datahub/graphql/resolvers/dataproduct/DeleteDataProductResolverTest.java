package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteDataProductResolverTest {

  private static final String TEST_DATA_PRODUCT_URN = "urn:li:dataProduct:test-product";
  private static final Urn TEST_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:test-domain");

  private DataFetchingEnvironment setupMockEnv(QueryContext context) {
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_DATA_PRODUCT_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(context);
    return mockEnv;
  }

  private static Domains domainsWith(Urn domainUrn) {
    return new Domains().setDomains(new UrnArray(ImmutableList.of(domainUrn)));
  }

  @Test
  public void testDeleteWithoutDomainAsPrivilegedUser() throws Exception {
    // Regression test: a data product with no domain must remain deletable by
    // a user holding the DELETE privilege on the entity (e.g. an admin).
    // The domain-based authorization check alone fails closed for everyone
    // when the product has no domains.
    DataProductService mockService = Mockito.mock(DataProductService.class);
    Mockito.when(mockService.verifyEntityExists(any(), any())).thenReturn(true);
    Mockito.when(mockService.getDataProductDomains(any(), any())).thenReturn(null);

    DeleteDataProductResolver resolver = new DeleteDataProductResolver(mockService);
    DataFetchingEnvironment mockEnv = setupMockEnv(getMockAllowContext());

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1))
        .deleteDataProduct(any(), Mockito.eq(UrnUtils.getUrn(TEST_DATA_PRODUCT_URN)));
  }

  @Test
  public void testDeleteWithoutDomainUnauthorized() throws Exception {
    // Fail-closed is preserved for users without any delete privilege.
    DataProductService mockService = Mockito.mock(DataProductService.class);
    Mockito.when(mockService.verifyEntityExists(any(), any())).thenReturn(true);
    Mockito.when(mockService.getDataProductDomains(any(), any())).thenReturn(null);

    DeleteDataProductResolver resolver = new DeleteDataProductResolver(mockService);
    DataFetchingEnvironment mockEnv = setupMockEnv(getMockDenyContext());

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).deleteDataProduct(any(), any());
  }

  @Test
  public void testDeleteWithDomainAuthorized() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    Mockito.when(mockService.verifyEntityExists(any(), any())).thenReturn(true);
    Mockito.when(mockService.getDataProductDomains(any(), any()))
        .thenReturn(domainsWith(TEST_DOMAIN_URN));

    DeleteDataProductResolver resolver = new DeleteDataProductResolver(mockService);
    DataFetchingEnvironment mockEnv = setupMockEnv(getMockAllowContext());

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1))
        .deleteDataProduct(any(), Mockito.eq(UrnUtils.getUrn(TEST_DATA_PRODUCT_URN)));
  }

  @Test
  public void testDeleteWithDomainUnauthorized() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    Mockito.when(mockService.verifyEntityExists(any(), any())).thenReturn(true);
    Mockito.when(mockService.getDataProductDomains(any(), any()))
        .thenReturn(domainsWith(TEST_DOMAIN_URN));

    DeleteDataProductResolver resolver = new DeleteDataProductResolver(mockService);
    DataFetchingEnvironment mockEnv = setupMockEnv(getMockDenyContext());

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).deleteDataProduct(any(), any());
  }

  @Test
  public void testDeleteNonExistentDataProduct() throws Exception {
    DataProductService mockService = Mockito.mock(DataProductService.class);
    Mockito.when(mockService.verifyEntityExists(any(), any())).thenReturn(false);

    DeleteDataProductResolver resolver = new DeleteDataProductResolver(mockService);
    DataFetchingEnvironment mockEnv = setupMockEnv(getMockAllowContext());

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).deleteDataProduct(any(), any());
  }
}
