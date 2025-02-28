package com.linkedin.datahub.graphql.resolvers.businessattribute;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.SchemaFieldDataType;
import com.linkedin.datahub.graphql.generated.UpdateBusinessAttributeInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.BusinessAttributeUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.BusinessAttributeService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.BooleanType;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateBusinessAttributeResolverTest {
  private static final String TEST_BUSINESS_ATTRIBUTE_NAME = "test-business-attribute";
  private static final String TEST_BUSINESS_ATTRIBUTE_DESCRIPTION = "test-description";
  private static final String TEST_BUSINESS_ATTRIBUTE_NAME_UPDATED =
      "test-business-attribute-updated";
  private static final String TEST_BUSINESS_ATTRIBUTE_DESCRIPTION_UPDATED =
      "test-description-updated";
  private static final String TEST_BUSINESS_ATTRIBUTE_URN =
      "urn:li:businessAttribute:7d0c4283-de02-4043-aaf2-698b04274658";
  private static final Urn TEST_BUSINESS_ATTRIBUTE_URN_OBJ =
      UrnUtils.getUrn(TEST_BUSINESS_ATTRIBUTE_URN);
  private EntityClient mockClient;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;
  private BusinessAttributeService businessAttributeService;
  private Authentication mockAuthentication;
  private SearchResult searchResult;

  private void init() {
    mockClient = Mockito.mock(EntityClient.class);
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    businessAttributeService = Mockito.mock(BusinessAttributeService.class);
    mockAuthentication = Mockito.mock(Authentication.class);
    searchResult = Mockito.mock(SearchResult.class);
  }

  @Test
  public void testSuccess() throws Exception {
    init();
    setupAllowContext();
    final UpdateBusinessAttributeInput testInput =
        new UpdateBusinessAttributeInput(
            TEST_BUSINESS_ATTRIBUTE_NAME_UPDATED,
            TEST_BUSINESS_ATTRIBUTE_DESCRIPTION_UPDATED,
            SchemaFieldDataType.NUMBER);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getArgument("urn")).thenReturn(TEST_BUSINESS_ATTRIBUTE_URN);
    Mockito.when(
            mockClient.exists(any(OperationContext.class), eq(TEST_BUSINESS_ATTRIBUTE_URN_OBJ)))
        .thenReturn(true);
    Mockito.when(
            businessAttributeService.getBusinessAttributeEntityResponse(
                any(OperationContext.class), eq(TEST_BUSINESS_ATTRIBUTE_URN_OBJ)))
        .thenReturn(getBusinessAttributeEntityResponse());
    Mockito.when(
            mockClient.filter(
                Mockito.any(OperationContext.class),
                Mockito.any(String.class),
                Mockito.any(Filter.class),
                Mockito.isNull(),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(searchResult);
    Mockito.when(searchResult.getNumEntities()).thenReturn(0);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.any(MetadataChangeProposal.class)))
        .thenReturn(TEST_BUSINESS_ATTRIBUTE_URN);

    UpdateBusinessAttributeResolver resolver =
        new UpdateBusinessAttributeResolver(mockClient, businessAttributeService);
    resolver.get(mockEnv).get();

    // verify
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                new CreateBusinessAttributeProposalMatcher(updatedMetadataChangeProposal())));
  }

  @Test
  public void testNotExists() throws Exception {
    init();
    setupAllowContext();
    final UpdateBusinessAttributeInput testInput =
        new UpdateBusinessAttributeInput(
            TEST_BUSINESS_ATTRIBUTE_NAME_UPDATED,
            TEST_BUSINESS_ATTRIBUTE_DESCRIPTION_UPDATED,
            SchemaFieldDataType.NUMBER);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getArgument("urn")).thenReturn(TEST_BUSINESS_ATTRIBUTE_URN);
    Mockito.when(
            mockClient.exists(any(OperationContext.class), eq(TEST_BUSINESS_ATTRIBUTE_URN_OBJ)))
        .thenReturn(false);

    UpdateBusinessAttributeResolver resolver =
        new UpdateBusinessAttributeResolver(mockClient, businessAttributeService);
    RuntimeException expectedException =
        expectThrows(RuntimeException.class, () -> resolver.get(mockEnv));
    assertTrue(
        expectedException
            .getMessage()
            .equals(String.format("This urn does not exist: %s", TEST_BUSINESS_ATTRIBUTE_URN)));

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), Mockito.any(MetadataChangeProposal.class));
  }

  @Test
  public void testNameConflict() throws Exception {
    init();
    setupAllowContext();
    final UpdateBusinessAttributeInput testInput =
        new UpdateBusinessAttributeInput(
            TEST_BUSINESS_ATTRIBUTE_NAME,
            TEST_BUSINESS_ATTRIBUTE_DESCRIPTION_UPDATED,
            SchemaFieldDataType.NUMBER);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getArgument("urn")).thenReturn(TEST_BUSINESS_ATTRIBUTE_URN);
    Mockito.when(
            mockClient.exists(any(OperationContext.class), eq(TEST_BUSINESS_ATTRIBUTE_URN_OBJ)))
        .thenReturn(true);
    Mockito.when(
            businessAttributeService.getBusinessAttributeEntityResponse(
                any(OperationContext.class), eq(TEST_BUSINESS_ATTRIBUTE_URN_OBJ)))
        .thenReturn(getBusinessAttributeEntityResponse());
    Mockito.when(
            mockClient.filter(
                Mockito.any(OperationContext.class),
                Mockito.any(String.class),
                Mockito.any(Filter.class),
                Mockito.isNull(),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(searchResult);
    Mockito.when(searchResult.getNumEntities()).thenReturn(1);

    UpdateBusinessAttributeResolver resolver =
        new UpdateBusinessAttributeResolver(mockClient, businessAttributeService);

    ExecutionException exception =
        expectThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());

    // Verify
    assertTrue(
        exception
            .getCause()
            .getMessage()
            .equals(
                "\"test-business-attribute\" already exists as Business Attribute. Please pick a unique name."));
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), Mockito.any(MetadataChangeProposal.class));
  }

  @Test
  public void testNotAuthorized() throws Exception {
    init();
    setupDenyContext();
    final UpdateBusinessAttributeInput testInput =
        new UpdateBusinessAttributeInput(
            TEST_BUSINESS_ATTRIBUTE_NAME,
            TEST_BUSINESS_ATTRIBUTE_DESCRIPTION_UPDATED,
            SchemaFieldDataType.NUMBER);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getArgument("urn")).thenReturn(TEST_BUSINESS_ATTRIBUTE_URN);

    UpdateBusinessAttributeResolver resolver =
        new UpdateBusinessAttributeResolver(mockClient, businessAttributeService);
    AuthorizationException exception =
        expectThrows(AuthorizationException.class, () -> resolver.get(mockEnv));

    assertTrue(
        exception
            .getMessage()
            .equals(
                "Unauthorized to perform this action. Please contact your DataHub administrator."));
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), Mockito.any(MetadataChangeProposal.class));
  }

  private EntityResponse getBusinessAttributeEntityResponse() throws Exception {
    Map<Urn, EntityResponse> result = new HashMap<>();
    EnvelopedAspectMap map = new EnvelopedAspectMap();
    BusinessAttributeInfo businessAttributeInfo = businessAttributeInfo();
    map.put(
        BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(businessAttributeInfo.data())));
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(map);
    entityResponse.setUrn(Urn.createFromString(TEST_BUSINESS_ATTRIBUTE_URN));
    return entityResponse;
  }

  private MetadataChangeProposal updatedMetadataChangeProposal() {
    BusinessAttributeInfo info = new BusinessAttributeInfo();
    info.setFieldPath(TEST_BUSINESS_ATTRIBUTE_NAME_UPDATED);
    info.setName(TEST_BUSINESS_ATTRIBUTE_NAME_UPDATED);
    info.setDescription(TEST_BUSINESS_ATTRIBUTE_DESCRIPTION_UPDATED);
    info.setType(
        BusinessAttributeUtils.mapSchemaFieldDataType(SchemaFieldDataType.BOOLEAN),
        SetMode.IGNORE_NULL);
    return AspectUtils.buildMetadataChangeProposal(
        TEST_BUSINESS_ATTRIBUTE_URN_OBJ, BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME, info);
  }

  private void setupAllowContext() {
    mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(mockAuthentication);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  private void setupDenyContext() {
    mockContext = getMockDenyContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(mockAuthentication);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  private BusinessAttributeInfo businessAttributeInfo() {
    BusinessAttributeInfo businessAttributeInfo = new BusinessAttributeInfo();
    businessAttributeInfo.setName(TEST_BUSINESS_ATTRIBUTE_NAME);
    businessAttributeInfo.setFieldPath(TEST_BUSINESS_ATTRIBUTE_NAME);
    businessAttributeInfo.setDescription(TEST_BUSINESS_ATTRIBUTE_DESCRIPTION);
    com.linkedin.schema.SchemaFieldDataType schemaFieldDataType =
        new com.linkedin.schema.SchemaFieldDataType();
    schemaFieldDataType.setType(
        com.linkedin.schema.SchemaFieldDataType.Type.create(new BooleanType()));
    businessAttributeInfo.setType(schemaFieldDataType);
    return businessAttributeInfo;
  }
}
