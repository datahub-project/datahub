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
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RemoveBusinessAttributeResolverTest {
  private static final String BUSINESS_ATTRIBUTE_URN =
      "urn:li:businessAttribute:7d0c4283-de02-4043-aaf2-698b04274658";
  private static final String RESOURCE_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_bar)";
  private EntityService<?> mockService;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
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
    setupAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(addBusinessAttributeInput());

    Mockito.when(
            mockService.getAspect(
                any(OperationContext.class),
                eq(Urn.createFromString(RESOURCE_URN)),
                eq(Constants.BUSINESS_ATTRIBUTE_ASPECT),
                eq(0L)))
        .thenReturn(businessAttributes());

    RemoveBusinessAttributeResolver resolver = new RemoveBusinessAttributeResolver(mockService);
    resolver.get(mockEnv).get();

    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), Mockito.any(AspectsBatchImpl.class), eq(false));
  }

  @Test
  public void testBusinessAttributeNotAdded() throws Exception {
    setupAllowContext();
    AddBusinessAttributeInput input = addBusinessAttributeInput();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(
            mockService.getAspect(
                any(OperationContext.class),
                eq(Urn.createFromString(RESOURCE_URN)),
                eq(Constants.BUSINESS_ATTRIBUTE_ASPECT),
                eq(0L)))
        .thenReturn(new BusinessAttributes());

    RemoveBusinessAttributeResolver resolver = new RemoveBusinessAttributeResolver(mockService);
    ExecutionException actualException =
        expectThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    assertTrue(
        actualException
            .getCause()
            .getMessage()
            .equals(
                String.format(
                    "Failed to remove Business Attribute with urn %s from resources %s",
                    input.getBusinessAttributeUrn(), input.getResourceUrn())));

    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), Mockito.any(AspectsBatchImpl.class), eq(false));
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
