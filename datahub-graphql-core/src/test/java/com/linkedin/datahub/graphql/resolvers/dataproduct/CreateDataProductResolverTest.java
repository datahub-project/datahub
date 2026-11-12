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

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateDataProductInput;
import com.linkedin.datahub.graphql.generated.CreateDataProductPropertiesInput;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateDataProductResolverTest {

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
