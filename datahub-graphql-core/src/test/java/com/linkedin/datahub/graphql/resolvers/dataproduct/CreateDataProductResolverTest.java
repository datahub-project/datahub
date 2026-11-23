package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;

import com.datahub.authorization.AuthorizationResult;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateDataProductInput;
import com.linkedin.datahub.graphql.generated.CreateDataProductPropertiesInput;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateDataProductResolverTest {

  private static final Urn DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:product");
  private static final Urn DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:finance");
  private static final Urn PARENT_URN = UrnUtils.getUrn("urn:li:dataProduct:parent");
  private static final Urn CHILD_URN = UrnUtils.getUrn("urn:li:dataProduct:child");

  private DataProductService dataProductService;
  private CreateDataProductResolver resolver;

  @BeforeMethod
  public void setUp() {
    dataProductService = mock(DataProductService.class);
    EntityService<?> entityService = mock(EntityService.class);
    resolver = new CreateDataProductResolver(dataProductService, entityService);
    when(dataProductService.verifyEntityExists(any(), eq(DOMAIN_URN))).thenReturn(true);
    when(dataProductService.createDataProduct(any(), any(), any(), any(), any()))
        .thenReturn(DATA_PRODUCT_URN);
    when(dataProductService.getDataProductEntityResponse(any(), eq(DATA_PRODUCT_URN)))
        .thenReturn(
            new EntityResponse()
                .setUrn(DATA_PRODUCT_URN)
                .setEntityName("dataProduct")
                .setAspects(new EnvelopedAspectMap()));
  }

  @Test
  public void testRejectsNonDomainUrnBeforeWriting() {
    // The domain privilege check accepts EDIT_ENTITY on whatever URN it is given, and a non-domain
    // URN is only rejected by setDomain, after dataProductProperties has been committed.
    DataFetchingEnvironment environment =
        environment("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)", null);

    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }

  @Test
  public void testCreatesWithParentInSameWrite() throws Exception {
    when(dataProductService.verifyEntityExists(any(), eq(PARENT_URN))).thenReturn(true);
    when(dataProductService.createDataProduct(any(), any(), any(), any(), eq(PARENT_URN)))
        .thenReturn(CHILD_URN);

    resolver.get(environment(DOMAIN_URN.toString(), PARENT_URN.toString())).join();

    verify(dataProductService)
        .createDataProduct(any(), any(), eq("Product"), any(), eq(PARENT_URN));
  }

  @Test
  public void testRejectsMissingParentBeforeWriting() {
    when(dataProductService.verifyEntityExists(any(), eq(PARENT_URN))).thenReturn(false);
    DataFetchingEnvironment environment = environment(DOMAIN_URN.toString(), PARENT_URN.toString());

    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  @Test
  public void testRejectsNonDataProductParentBeforeWriting() {
    DataFetchingEnvironment environment = environment(DOMAIN_URN.toString(), DOMAIN_URN.toString());

    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  @Test
  public void testCreatesWithoutDomain() throws Exception {
    DataProduct result = resolver.get(environment(null, null)).join();

    assertNotNull(result);
    verify(dataProductService, times(1)).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }

  @Test
  public void testRejectsWithoutDomainWhenUnauthorized() {
    DataFetchingEnvironment environment = environment(null, null, getMockDenyContext());

    assertHasAuthorizationCause(
        expectThrows(CompletionException.class, () -> resolver.get(environment).join()));
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  @Test
  public void testRejectsWithoutDomainWithOnlyManageDomains() {
    DataFetchingEnvironment environment =
        environment(null, null, contextGranting(PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE.getType()));

    assertHasAuthorizationCause(
        expectThrows(CompletionException.class, () -> resolver.get(environment).join()));
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  @Test
  public void testCreatesWithoutDomainWithOnlyManageAllDataProducts() throws Exception {
    // The mocked context cannot run the mapper's view check, so skip mapping the result.
    when(dataProductService.getDataProductEntityResponse(any(), any())).thenReturn(null);

    resolver
        .get(
            environment(
                null,
                null,
                contextGranting(PoliciesConfig.MANAGE_ALL_DATA_PRODUCTS_PRIVILEGE.getType())))
        .join();

    verify(dataProductService, times(1)).createDataProduct(any(), any(), any(), any(), any());
  }

  @Test
  public void testBlankDomainUrnIsTreatedAsNoDomain() throws Exception {
    resolver.get(environment("  ", null)).join();

    verify(dataProductService, never()).verifyEntityExists(any(), any());
    verify(dataProductService, times(1)).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }

  @Test
  public void testBlankDomainUrnStillRequiresManageAllDataProducts() {
    // A blank domainUrn takes the no-Domain path, so it must not skip the privilege check.
    for (String blankDomainUrn : new String[] {"", "  "}) {
      DataFetchingEnvironment environment = environment(blankDomainUrn, null, getMockDenyContext());

      assertHasAuthorizationCause(
          expectThrows(CompletionException.class, () -> resolver.get(environment).join()));
    }
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }

  @Test
  public void testCreatesWithDomain() throws Exception {
    DataProduct result = resolver.get(environment(DOMAIN_URN.toString(), null)).join();

    assertNotNull(result);
    verify(dataProductService, times(1)).setDomain(any(), eq(DATA_PRODUCT_URN), eq(DOMAIN_URN));
  }

  @Test
  public void testCreatesWithDomainWithOnlyManageAllDataProducts() throws Exception {
    // The mocked context cannot run the mapper's view check, so skip mapping the result.
    when(dataProductService.getDataProductEntityResponse(any(), any())).thenReturn(null);

    resolver
        .get(
            environment(
                DOMAIN_URN.toString(),
                null,
                contextGranting(PoliciesConfig.MANAGE_ALL_DATA_PRODUCTS_PRIVILEGE.getType())))
        .join();

    verify(dataProductService, times(1)).setDomain(any(), eq(DATA_PRODUCT_URN), eq(DOMAIN_URN));
  }

  @Test
  public void testRejectsMissingDomainBeforeWriting() {
    when(dataProductService.verifyEntityExists(any(), eq(DOMAIN_URN))).thenReturn(false);
    DataFetchingEnvironment environment = environment(DOMAIN_URN.toString(), null);

    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }

  @Test
  public void testRejectsWithDomainWhenUnauthorized() {
    DataFetchingEnvironment environment =
        environment(DOMAIN_URN.toString(), null, getMockDenyContext());

    assertHasAuthorizationCause(
        expectThrows(CompletionException.class, () -> resolver.get(environment).join()));
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  private static void assertHasAuthorizationCause(Throwable exception) {
    Throwable cause = exception.getCause();
    while (cause != null && !(cause instanceof AuthorizationException)) {
      cause = cause.getCause();
    }
    assertNotNull(cause, "Expected an AuthorizationException in the cause chain");
  }

  private static QueryContext contextGranting(String privilege) {
    QueryContext context = mock(QueryContext.class);
    OperationContext operationContext = mock(OperationContext.class);
    when(context.getOperationContext()).thenReturn(operationContext);
    when(context.getActorUrn()).thenReturn("urn:li:corpuser:test");
    when(operationContext.authorize(any(), any(), any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));
    when(operationContext.authorize(eq(privilege), any(), any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
    return context;
  }

  private static DataFetchingEnvironment environment(
      @Nullable String domainUrn, @Nullable String parentDataProduct) {
    return environment(domainUrn, parentDataProduct, getMockAllowContext());
  }

  private static DataFetchingEnvironment environment(
      @Nullable String domainUrn, @Nullable String parentDataProduct, QueryContext context) {
    CreateDataProductPropertiesInput properties = new CreateDataProductPropertiesInput();
    properties.setName("Product");
    properties.setParentDataProduct(parentDataProduct);
    CreateDataProductInput input = new CreateDataProductInput();
    input.setProperties(properties);
    input.setDomainUrn(domainUrn);

    DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getArgument(eq("input"))).thenReturn(input);
    when(environment.getContext()).thenReturn(context);
    return environment;
  }
}
