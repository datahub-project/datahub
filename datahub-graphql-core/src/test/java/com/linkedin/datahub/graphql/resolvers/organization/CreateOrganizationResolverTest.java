package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateOrganizationInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.key.OrganizationKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.organization.OrganizationProperties;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateOrganizationResolverTest {

  private EntityClient _entityClient;
  private CreateOrganizationResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _resolver = new CreateOrganizationResolver(_entityClient);
  }

  @Test
  public void testCreateOrganizationSuccess() throws Exception {
    CreateOrganizationInput input = new CreateOrganizationInput();
    input.setId("test-org");
    input.setName("Test Organization");
    input.setDescription("Test Description");

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    Mockito.when(_entityClient.exists(any(), any(Urn.class))).thenReturn(false);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false)))
        .thenReturn("urn:li:organization:test-org");

    String result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result, "urn:li:organization:test-org");

    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(_entityClient, Mockito.times(3))
        .ingestProposal(any(), proposalCaptor.capture(), eq(false));

    MetadataChangeProposal propertiesProposal = proposalCaptor.getAllValues().get(0);
    assertEquals(propertiesProposal.getEntityType(), ORGANIZATION_ENTITY_NAME);
    assertEquals(propertiesProposal.getAspectName(), ORGANIZATION_PROPERTIES_ASPECT_NAME);
    assertEquals(propertiesProposal.getChangeType(), ChangeType.UPSERT);

    OrganizationProperties props =
        GenericRecordUtils.deserializeAspect(
            propertiesProposal.getAspect().getValue(),
            propertiesProposal.getAspect().getContentType(),
            OrganizationProperties.class);
    assertEquals(props.getName(), "Test Organization");
    assertEquals(props.getDescription(), "Test Description");
  }

  @Test
  public void testCreateOrganizationWithGeneratedId() throws Exception {
    CreateOrganizationInput input = new CreateOrganizationInput();
    input.setName("Test Organization");
    input.setId(null);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    Mockito.when(_entityClient.exists(any(), any(Urn.class))).thenReturn(false);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false)))
        .thenAnswer(
            invocation -> {
              MetadataChangeProposal proposal = invocation.getArgument(1);
              OrganizationKey key =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getEntityKeyAspect().getValue(),
                      proposal.getEntityKeyAspect().getContentType(),
                      OrganizationKey.class);
              return EntityKeyUtils.convertEntityKeyToUrn(key, ORGANIZATION_ENTITY_NAME).toString();
            });

    String result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertTrue(result.startsWith("urn:li:organization:"));
  }

  @Test
  public void testCreateOrganizationWithParent() throws Exception {
    CreateOrganizationInput input = new CreateOrganizationInput();
    input.setId("child-org");
    input.setName("Child Organization");
    input.setParentUrn("urn:li:organization:parent-org");

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    Mockito.when(_entityClient.exists(any(), any(Urn.class))).thenReturn(false);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false)))
        .thenReturn("urn:li:organization:child-org");

    String result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(_entityClient, Mockito.times(4))
        .ingestProposal(any(), proposalCaptor.capture(), eq(false));
  }

  @Test
  public void testCreateOrganizationUnauthorized() throws Exception {
    CreateOrganizationInput input = new CreateOrganizationInput();
    input.setId("test-org");
    input.setName("Test Organization");

    QueryContext mockContext = getMockDenyContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
    Mockito.verify(_entityClient, Mockito.times(0)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testCreateOrganizationAlreadyExists() throws Exception {
    CreateOrganizationInput input = new CreateOrganizationInput();
    input.setId("existing-org");
    input.setName("Existing Organization");

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    Mockito.when(_entityClient.exists(any(), any(Urn.class))).thenReturn(true);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testCreateOrganizationWithLogoUrl() throws Exception {
    CreateOrganizationInput input = new CreateOrganizationInput();
    input.setId("test-org");
    input.setName("Test Organization");
    input.setLogoUrl("https://example.com/logo.png");

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    Mockito.when(_entityClient.exists(any(), any(Urn.class))).thenReturn(false);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false)))
        .thenReturn("urn:li:organization:test-org");

    String result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(_entityClient, Mockito.times(4))
        .ingestProposal(any(), proposalCaptor.capture(), eq(false));

    OrganizationProperties props =
        GenericRecordUtils.deserializeAspect(
            proposalCaptor.getAllValues().get(0).getAspect().getValue(),
            proposalCaptor.getAllValues().get(0).getAspect().getContentType(),
            OrganizationProperties.class);
    assertEquals(props.getLogoUrl(), "https://example.com/logo.png");
  }
}
