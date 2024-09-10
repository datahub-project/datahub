package com.linkedin.datahub.graphql.resolvers.businessattribute;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockEntityService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.google.common.collect.ImmutableList;
import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.urn.BusinessAttributeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddBusinessAttributeInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AddBusinessAttributeResolverTest {
  private static final String BUSINESS_ATTRIBUTE_URN =
      "urn:li:businessAttribute:7d0c4283-de02-4043-aaf2-698b04274658";
  private static final String RESOURCE_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_bar)";
  private EntityService<?> mockService;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;

  private void init() {
    mockService = getMockEntityService();
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
  }

  private void setupAllowContext() {
    mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testSuccess() throws Exception {
    init();
    setupAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(addBusinessAttributeInput());
    Mockito.when(
            mockService.exists(
                any(OperationContext.class),
                eq(Urn.createFromString((BUSINESS_ATTRIBUTE_URN))),
                eq(true)))
        .thenReturn(true);

    Mockito.when(
            mockService.getAspect(
                any(OperationContext.class),
                eq(Urn.createFromString(RESOURCE_URN)),
                eq(Constants.BUSINESS_ATTRIBUTE_ASPECT),
                eq(0L)))
        .thenReturn(new BusinessAttributes());

    AddBusinessAttributeResolver addBusinessAttributeResolver =
        new AddBusinessAttributeResolver(mockService);
    addBusinessAttributeResolver.get(mockEnv).get();

    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(any(OperationContext.class), any(AspectsBatchImpl.class), eq(false));
  }

  @Test
  public void testBusinessAttributeAlreadyAdded() throws Exception {
    init();
    setupAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(addBusinessAttributeInput());
    Mockito.when(
            mockService.exists(
                any(OperationContext.class),
                eq(Urn.createFromString((BUSINESS_ATTRIBUTE_URN))),
                eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.getAspect(
                any(OperationContext.class),
                eq(Urn.createFromString(RESOURCE_URN)),
                eq(Constants.BUSINESS_ATTRIBUTE_ASPECT),
                eq(0L)))
        .thenReturn(businessAttributes());

    AddBusinessAttributeResolver addBusinessAttributeResolver =
        new AddBusinessAttributeResolver(mockService);
    addBusinessAttributeResolver.get(mockEnv).get();

    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(any(OperationContext.class), any(AspectsBatchImpl.class), eq(false));
  }

  @Test
  public void testBusinessAttributeNotExists() throws Exception {
    init();
    setupAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(addBusinessAttributeInput());
    Mockito.when(
            mockService.exists(
                any(OperationContext.class),
                eq(Urn.createFromString((BUSINESS_ATTRIBUTE_URN))),
                eq(true)))
        .thenReturn(false);
    Mockito.when(
            mockService.exists(
                any(OperationContext.class), eq(Urn.createFromString(RESOURCE_URN)), eq(true)))
        .thenReturn(true);

    AddBusinessAttributeResolver addBusinessAttributeResolver =
        new AddBusinessAttributeResolver(mockService);
    RuntimeException exception =
        expectThrows(RuntimeException.class, () -> addBusinessAttributeResolver.get(mockEnv).get());
    assertTrue(
        exception
            .getMessage()
            .equals(String.format("This urn does not exist: %s", BUSINESS_ATTRIBUTE_URN)));
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any(AspectsBatchImpl.class), eq(false));
  }

  public AddBusinessAttributeInput addBusinessAttributeInput() {
    AddBusinessAttributeInput addBusinessAttributeInput = new AddBusinessAttributeInput();
    addBusinessAttributeInput.setBusinessAttributeUrn(BUSINESS_ATTRIBUTE_URN);
    addBusinessAttributeInput.setResourceUrn(resourceRefInput());
    return addBusinessAttributeInput;
  }

  private ImmutableList<ResourceRefInput> resourceRefInput() {
    ResourceRefInput resourceRefInput = new ResourceRefInput();
    resourceRefInput.setResourceUrn(RESOURCE_URN);
    return ImmutableList.of(resourceRefInput);
  }

  private BusinessAttributes businessAttributes() throws URISyntaxException {
    BusinessAttributes businessAttributes = new BusinessAttributes();
    BusinessAttributeAssociation businessAttributeAssociation = new BusinessAttributeAssociation();
    businessAttributeAssociation.setBusinessAttributeUrn(
        BusinessAttributeUrn.createFromString(BUSINESS_ATTRIBUTE_URN));
    businessAttributes.setBusinessAttribute(businessAttributeAssociation);
    return businessAttributes;
  }
}
