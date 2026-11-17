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
  public void testCreatesWithoutDomain() throws Exception {
    DataProductService dataProductService = mock(DataProductService.class);
    EntityService<?> entityService = mock(EntityService.class);
    Urn dataProductUrn = UrnUtils.getUrn("urn:li:dataProduct:product");
    when(dataProductService.createDataProduct(any(), any(), any(), any(), any()))
        .thenReturn(dataProductUrn);
    when(dataProductService.getDataProductEntityResponse(any(), eq(dataProductUrn)))
        .thenReturn(
            new EntityResponse()
                .setUrn(dataProductUrn)
                .setEntityName("dataProduct")
                .setAspects(new EnvelopedAspectMap()));
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, entityService);

    DataFetchingEnvironment environment =
        mockEnvironment(createInputWithoutDomain(), getMockAllowContext());

    DataProduct result = resolver.get(environment).join();

    assertNotNull(result);
    verify(dataProductService, times(1)).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }

  @Test
  public void testRejectsWithoutDomainWhenUnauthorized() {
    DataProductService dataProductService = mock(DataProductService.class);
    EntityService<?> entityService = mock(EntityService.class);
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, entityService);

    DataFetchingEnvironment environment =
        mockEnvironment(createInputWithoutDomain(), getMockDenyContext());

    CompletionException exception =
        expectThrows(CompletionException.class, () -> resolver.get(environment).join());
    Throwable cause = exception.getCause();
    while (cause != null && !(cause instanceof AuthorizationException)) {
      cause = cause.getCause();
    }
    assertNotNull(cause, "Expected an AuthorizationException in the cause chain");
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  @Test
  public void testRejectsNonDataProductParentBeforeWriting() {
    DataFetchingEnvironment environment = environment(DOMAIN_URN.toString(), DOMAIN_URN.toString());

    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  private static DataFetchingEnvironment environment(
      String domainUrn, @Nullable String parentDataProduct) {
    CreateDataProductPropertiesInput properties = new CreateDataProductPropertiesInput();
    properties.setName("Product");
    properties.setParentDataProduct(parentDataProduct);
    CreateDataProductInput input = new CreateDataProductInput();
    input.setProperties(properties);
    input.setDomainUrn(domainUrn);

    QueryContext context = getMockAllowContext();
    DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getArgument(eq("input"))).thenReturn(input);
    when(environment.getContext()).thenReturn(context);
    return environment;
  }

  @Test
  public void testRejectsWithoutDomainWithOnlyManageDomains() {
    DataProductService dataProductService = mock(DataProductService.class);
    EntityService<?> entityService = mock(EntityService.class);
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, entityService);

    DataFetchingEnvironment environment =
        mockEnvironment(
            createInputWithoutDomain(),
            contextGranting(PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE.getType()));

    CompletionException exception =
        expectThrows(CompletionException.class, () -> resolver.get(environment).join());
    Throwable cause = exception.getCause();
    while (cause != null && !(cause instanceof AuthorizationException)) {
      cause = cause.getCause();
    }
    assertNotNull(cause, "Expected an AuthorizationException in the cause chain");
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  @Test
  public void testCreatesWithoutDomainWithOnlyManageAllDataProducts() throws Exception {
    DataProductService dataProductService = mock(DataProductService.class);
    EntityService<?> entityService = mock(EntityService.class);
    Urn dataProductUrn = UrnUtils.getUrn("urn:li:dataProduct:product");
    when(dataProductService.createDataProduct(any(), any(), any(), any(), any()))
        .thenReturn(dataProductUrn);
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, entityService);

    DataFetchingEnvironment environment =
        mockEnvironment(
            createInputWithoutDomain(),
            contextGranting(PoliciesConfig.MANAGE_ALL_DATA_PRODUCTS_PRIVILEGE.getType()));

    resolver.get(environment).join();
    verify(dataProductService, times(1)).createDataProduct(any(), any(), any(), any(), any());
  }

  @Test
  public void testBlankDomainUrnIsTreatedAsNoDomain() throws Exception {
    DataProductService dataProductService = mockDataProductService();
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, mock(EntityService.class));
    CreateDataProductInput input = createInputWithoutDomain();
    input.setDomainUrn("  ");

    resolver.get(mockEnvironment(input, getMockAllowContext())).join();

    verify(dataProductService, never()).verifyEntityExists(any(), any());
    verify(dataProductService, times(1)).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }

  @Test
  public void testCreatesWithDomain() throws Exception {
    DataProductService dataProductService = mockDataProductService();
    when(dataProductService.verifyEntityExists(any(), eq(DOMAIN_URN))).thenReturn(true);
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, mock(EntityService.class));

    DataProduct result =
        resolver.get(mockEnvironment(createInputWithDomain(), getMockAllowContext())).join();

    assertNotNull(result);
    verify(dataProductService, times(1)).setDomain(any(), eq(DATA_PRODUCT_URN), eq(DOMAIN_URN));
  }

  @Test
  public void testCreatesWithDomainWithOnlyManageAllDataProducts() throws Exception {
    DataProductService dataProductService = mockDataProductService();
    // The mocked context cannot run the mapper's view check, so skip mapping the result.
    when(dataProductService.getDataProductEntityResponse(any(), any())).thenReturn(null);
    when(dataProductService.verifyEntityExists(any(), eq(DOMAIN_URN))).thenReturn(true);
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, mock(EntityService.class));

    resolver
        .get(
            mockEnvironment(
                createInputWithDomain(),
                contextGranting(PoliciesConfig.MANAGE_ALL_DATA_PRODUCTS_PRIVILEGE.getType())))
        .join();

    verify(dataProductService, times(1)).setDomain(any(), eq(DATA_PRODUCT_URN), eq(DOMAIN_URN));
  }

  @Test
  public void testRejectsMissingDomainBeforeWriting() {
    DataProductService dataProductService = mockDataProductService();
    when(dataProductService.verifyEntityExists(any(), eq(DOMAIN_URN))).thenReturn(false);
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, mock(EntityService.class));

    assertThrows(
        CompletionException.class,
        () -> resolver.get(mockEnvironment(createInputWithDomain(), getMockAllowContext())).join());
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }

  @Test
  public void testRejectsWithDomainWhenUnauthorized() {
    DataProductService dataProductService = mockDataProductService();
    when(dataProductService.verifyEntityExists(any(), eq(DOMAIN_URN))).thenReturn(true);
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, mock(EntityService.class));

    CompletionException exception =
        expectThrows(
            CompletionException.class,
            () ->
                resolver
                    .get(mockEnvironment(createInputWithDomain(), getMockDenyContext()))
                    .join());
    assertHasAuthorizationCause(exception);
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
  }

  private static DataProductService mockDataProductService() {
    DataProductService dataProductService = mock(DataProductService.class);
    when(dataProductService.createDataProduct(any(), any(), any(), any(), any()))
        .thenReturn(DATA_PRODUCT_URN);
    when(dataProductService.getDataProductEntityResponse(any(), eq(DATA_PRODUCT_URN)))
        .thenReturn(
            new EntityResponse()
                .setUrn(DATA_PRODUCT_URN)
                .setEntityName("dataProduct")
                .setAspects(new EnvelopedAspectMap()));
    return dataProductService;
  }

  private static void assertHasAuthorizationCause(Throwable exception) {
    Throwable cause = exception.getCause();
    while (cause != null && !(cause instanceof AuthorizationException)) {
      cause = cause.getCause();
    }
    assertNotNull(cause, "Expected an AuthorizationException in the cause chain");
  }

  private static CreateDataProductInput createInputWithDomain() {
    CreateDataProductInput input = createInputWithoutDomain();
    input.setDomainUrn(DOMAIN_URN.toString());
    return input;
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

  private static CreateDataProductInput createInputWithoutDomain() {
    CreateDataProductPropertiesInput properties = new CreateDataProductPropertiesInput();
    properties.setName("Product");
    CreateDataProductInput input = new CreateDataProductInput();
    input.setProperties(properties);
    return input;
  }

  private static DataFetchingEnvironment mockEnvironment(
      CreateDataProductInput input, QueryContext context) {
    DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getArgument(eq("input"))).thenReturn(input);
    when(environment.getContext()).thenReturn(context);
    return environment;
  }
}
