package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.OrganizationUpdateInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.organization.OrganizationProperties;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateOrganizationResolverTest {

  private static final String ORGANIZATION_URN = "urn:li:organization:test-org";

  private EntityClient _entityClient;
  private UpdateOrganizationResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _resolver = new UpdateOrganizationResolver(_entityClient);
  }

  @Test
  public void testUpdateOrganizationSuccess() throws Exception {
    OrganizationUpdateInput input = new OrganizationUpdateInput();
    input.setName("Updated Name");
    input.setDescription("Updated Description");

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    OrganizationProperties existingProps = new OrganizationProperties();
    existingProps.setName("Original Name");
    existingProps.setDescription("Original Description");

    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(
        new com.linkedin.entity.Aspect(GenericRecordUtils.serializeAspect(existingProps).data()));
    aspects.put(ORGANIZATION_PROPERTIES_ASPECT_NAME, aspect);
    response.setAspects(aspects);

    Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    batchResponse.put(Urn.createFromString(ORGANIZATION_URN), response);

    Mockito.when(_entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(batchResponse);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false))).thenReturn(null);

    Boolean result = _resolver.get(_dataFetchingEnvironment).get();

    assertTrue(result);
    ArgumentCaptor<com.linkedin.mxe.MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(com.linkedin.mxe.MetadataChangeProposal.class);
    Mockito.verify(_entityClient, Mockito.times(1))
        .ingestProposal(any(), proposalCaptor.capture(), eq(false));

    OrganizationProperties updatedProps =
        GenericRecordUtils.deserializeAspect(
            proposalCaptor.getValue().getAspect().getValue(),
            proposalCaptor.getValue().getAspect().getContentType(),
            OrganizationProperties.class);
    assertEquals(updatedProps.getName(), "Updated Name");
    assertEquals(updatedProps.getDescription(), "Updated Description");
  }

  @Test
  public void testUpdateOrganizationWithParent() throws Exception {
    OrganizationUpdateInput input = new OrganizationUpdateInput();
    input.setName("Updated Name");
    input.setParentUrn("urn:li:organization:parent-org");

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    OrganizationProperties existingProps = new OrganizationProperties();
    existingProps.setName("Original Name");

    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(
        new com.linkedin.entity.Aspect(GenericRecordUtils.serializeAspect(existingProps).data()));
    aspects.put(ORGANIZATION_PROPERTIES_ASPECT_NAME, aspect);
    response.setAspects(aspects);

    Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    batchResponse.put(Urn.createFromString(ORGANIZATION_URN), response);

    Mockito.when(_entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(batchResponse);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false))).thenReturn(null);

    Boolean result = _resolver.get(_dataFetchingEnvironment).get();

    assertTrue(result);
    Mockito.verify(_entityClient, Mockito.times(2)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testUpdateOrganizationUnauthorized() {
    OrganizationUpdateInput input = new OrganizationUpdateInput();
    input.setName("Updated Name");

    QueryContext mockContext = getMockDenyContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateOrganizationWithLogoUrl() throws Exception {
    OrganizationUpdateInput input = new OrganizationUpdateInput();
    input.setName("Updated Name");
    input.setLogoUrl("https://example.com/logo.png");

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    OrganizationProperties existingProps = new OrganizationProperties();
    existingProps.setName("Original Name");

    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(
        new com.linkedin.entity.Aspect(GenericRecordUtils.serializeAspect(existingProps).data()));
    aspects.put(ORGANIZATION_PROPERTIES_ASPECT_NAME, aspect);
    response.setAspects(aspects);

    Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    batchResponse.put(Urn.createFromString(ORGANIZATION_URN), response);

    Mockito.when(_entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(batchResponse);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false))).thenReturn(null);

    Boolean result = _resolver.get(_dataFetchingEnvironment).get();

    assertTrue(result);
    ArgumentCaptor<com.linkedin.mxe.MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(com.linkedin.mxe.MetadataChangeProposal.class);
    Mockito.verify(_entityClient, Mockito.times(1))
        .ingestProposal(any(), proposalCaptor.capture(), eq(false));

    OrganizationProperties updatedProps =
        GenericRecordUtils.deserializeAspect(
            proposalCaptor.getValue().getAspect().getValue(),
            proposalCaptor.getValue().getAspect().getContentType(),
            OrganizationProperties.class);
    assertEquals(updatedProps.getLogoUrl(), "https://example.com/logo.png");
  }

  @Test
  public void testUpdateOrganizationNotFound() throws Exception {
    OrganizationUpdateInput input = new OrganizationUpdateInput();
    input.setName("Updated Name");

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    Mockito.when(_entityClient.batchGetV2(any(), any(), any(), any()))
        .thenReturn(Collections.emptyMap());

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }
}
