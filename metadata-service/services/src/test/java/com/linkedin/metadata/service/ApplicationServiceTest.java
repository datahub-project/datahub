package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.application.ApplicationKey;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.application.Applications;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ApplicationServiceTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_APPLICATION_URN =
      UrnUtils.getUrn("urn:li:application:" + UUID.randomUUID());
  private static final Urn TEST_APPLICATION_URN_2 =
      UrnUtils.getUrn("urn:li:application:" + UUID.randomUUID());
  private static final Urn TEST_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:" + UUID.randomUUID());
  private static final Urn TEST_ASSET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final Urn TEST_ASSET_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:s3,test2,PROD)");

  private EntityClient _entityClient;
  private ApplicationService _applicationService;
  private OperationContext _opContext;
  private EntityRegistry _mockEntityRegistry;

  @BeforeMethod
  public void setUp() {
    _entityClient = mock(EntityClient.class);
    _mockEntityRegistry = mock(EntityRegistry.class); // Mock EntityRegistry
    _opContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN, _mockEntityRegistry);
    _applicationService = new ApplicationService(_entityClient);
  }

  @Test
  public void testCreateApplication() throws Exception {
    // Test case 1: Create with ID, name, and description
    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);

    String id = "test-app-id";
    String name = "Test Application";
    String description = "This is a test application.";
    ApplicationKey key = new ApplicationKey().setId(id);
    Urn expectedUrn = EntityKeyUtils.convertEntityKeyToUrn(key, Constants.APPLICATION_ENTITY_NAME);

    when(_entityClient.exists(eq(_opContext), eq(expectedUrn))).thenReturn(false);
    when(_entityClient.ingestProposal(eq(_opContext), any(MetadataChangeProposal.class), eq(false)))
        .thenReturn(expectedUrn.toString());

    Urn actualUrn = _applicationService.createApplication(_opContext, id, name, description);
    assertEquals(actualUrn, expectedUrn);

    verify(_entityClient, times(1)).ingestProposal(eq(_opContext), mcpCaptor.capture(), eq(false));
    MetadataChangeProposal mcp = mcpCaptor.getValue();
    assertEquals(mcp.getEntityType(), Constants.APPLICATION_ENTITY_NAME);
    assertEquals(mcp.getAspectName(), Constants.APPLICATION_PROPERTIES_ASPECT_NAME);
    assertEquals(mcp.getChangeType(), ChangeType.UPSERT);
    ApplicationProperties props =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            ApplicationProperties.class);
    assertEquals(props.getName(), name);
    assertEquals(props.getDescription(), description);

    // Test case 2: Create with null ID (auto-generated)
    Mockito.reset(_entityClient); // Reset mock for new interaction
    String name2 = "Another App";
    when(_entityClient.exists(eq(_opContext), any(Urn.class))).thenReturn(false);
    when(_entityClient.ingestProposal(eq(_opContext), any(MetadataChangeProposal.class), eq(false)))
        .thenAnswer(
            invocation -> {
              MetadataChangeProposal proposal = invocation.getArgument(1);
              return proposal.getEntityUrn().toString();
            });

    Urn actualUrn2 = _applicationService.createApplication(_opContext, null, name2, null);
    assertNotNull(actualUrn2);
    assertTrue(actualUrn2.getId().length() > 0); // Check if ID was generated

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor2 =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(1)).ingestProposal(eq(_opContext), mcpCaptor2.capture(), eq(false));
    MetadataChangeProposal mcp2 = mcpCaptor2.getValue();
    ApplicationProperties props2 =
        GenericRecordUtils.deserializeAspect(
            mcp2.getAspect().getValue(),
            mcp2.getAspect().getContentType(),
            ApplicationProperties.class);
    assertEquals(props2.getName(), name2);
    assertNull(props2.getDescription());

    // Test case 3: Application already exists
    Mockito.reset(_entityClient);
    when(_entityClient.exists(eq(_opContext), eq(expectedUrn))).thenReturn(true);
    assertThrows(
        IllegalArgumentException.class,
        () -> _applicationService.createApplication(_opContext, id, name, description));
  }

  @Test
  public void testGetApplicationProperties() throws Exception {
    ApplicationProperties props = new ApplicationProperties().setName("Test");
    EntityResponse response = new EntityResponse();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(props.data()));
    response.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(
                Constants.APPLICATION_PROPERTIES_ASPECT_NAME, envelopedAspect)));
    response.setEntityName(Constants.APPLICATION_ENTITY_NAME);
    response.setUrn(TEST_APPLICATION_URN);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(response);

    ApplicationProperties actualProps =
        _applicationService.getApplicationProperties(_opContext, TEST_APPLICATION_URN);
    assertNotNull(actualProps);
    assertEquals(actualProps.getName(), "Test");

    // Test case: Not found
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN_2),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(null);
    assertNull(_applicationService.getApplicationProperties(_opContext, TEST_APPLICATION_URN_2));
  }

  @Test
  public void testGetApplicationEntityResponse() throws Exception {
    EntityResponse expectedResponse = new EntityResponse();
    expectedResponse.setUrn(TEST_APPLICATION_URN);
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(expectedResponse);

    EntityResponse actualResponse =
        _applicationService.getApplicationEntityResponse(_opContext, TEST_APPLICATION_URN);
    assertEquals(actualResponse, expectedResponse);

    // Test case: Exception
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN_2),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenThrow(new RuntimeException("Failed to fetch"));
    assertNull(
        _applicationService.getApplicationEntityResponse(_opContext, TEST_APPLICATION_URN_2));
  }

  @Test
  public void testDeleteApplication() throws Exception {
    _applicationService.deleteApplication(_opContext, TEST_APPLICATION_URN);
    verify(_entityClient, times(1)).deleteEntity(eq(_opContext), eq(TEST_APPLICATION_URN));
  }

  @Test
  public void testSetDomain() throws Exception {
    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);

    _applicationService.setDomain(_opContext, TEST_APPLICATION_URN, TEST_DOMAIN_URN);

    verify(_entityClient, times(1)).ingestProposal(eq(_opContext), mcpCaptor.capture(), eq(false));
    MetadataChangeProposal mcp = mcpCaptor.getValue();
    assertEquals(mcp.getEntityType(), Constants.APPLICATION_ENTITY_NAME);
    assertEquals(mcp.getEntityUrn(), TEST_APPLICATION_URN);
    assertEquals(mcp.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Domains domains =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(), mcp.getAspect().getContentType(), Domains.class);
    assertEquals(domains.getDomains().size(), 1);
    assertEquals(domains.getDomains().get(0), TEST_DOMAIN_URN);
  }

  @Test
  public void testBatchSetApplicationAssets() throws Exception {
    List<Urn> assetUrns = ImmutableList.of(TEST_ASSET_URN, TEST_ASSET_URN_2);
    _applicationService.batchSetApplicationAssets(
        _opContext, TEST_APPLICATION_URN, assetUrns, TEST_USER_URN);

    ArgumentCaptor<List<MetadataChangeProposal>> mcpListCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(_entityClient, times(1))
        .batchIngestProposals(eq(_opContext), mcpListCaptor.capture(), eq(false));

    List<MetadataChangeProposal> mcps = mcpListCaptor.getValue();
    assertEquals(mcps.size(), 2);

    // Verify first proposal
    MetadataChangeProposal mcp1 = mcps.get(0);
    assertEquals(mcp1.getEntityUrn(), TEST_ASSET_URN);
    assertEquals(mcp1.getEntityType(), TEST_ASSET_URN.getEntityType());
    assertEquals(mcp1.getAspectName(), Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME);
    Applications apps1 =
        GenericRecordUtils.deserializeAspect(
            mcp1.getAspect().getValue(), mcp1.getAspect().getContentType(), Applications.class);
    assertEquals(apps1.getApplications().size(), 1);
    assertEquals(apps1.getApplications().get(0), TEST_APPLICATION_URN);

    // Verify second proposal
    MetadataChangeProposal mcp2 = mcps.get(1);
    assertEquals(mcp2.getEntityUrn(), TEST_ASSET_URN_2);
    assertEquals(mcp2.getEntityType(), TEST_ASSET_URN_2.getEntityType());
    assertEquals(mcp2.getAspectName(), Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME);
    Applications apps2 =
        GenericRecordUtils.deserializeAspect(
            mcp2.getAspect().getValue(), mcp2.getAspect().getContentType(), Applications.class);
    assertEquals(apps2.getApplications().size(), 1);
    assertEquals(apps2.getApplications().get(0), TEST_APPLICATION_URN);
  }

  @Test
  public void testBatchRemoveApplicationAssets() throws Exception {
    List<Urn> assetUrnsToRemove = ImmutableList.of(TEST_ASSET_URN);
    _applicationService.batchRemoveApplicationAssets(
        _opContext, TEST_APPLICATION_URN, assetUrnsToRemove, TEST_USER_URN);

    ArgumentCaptor<List<MetadataChangeProposal>> mcpListCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(_entityClient, times(1))
        .batchIngestProposals(eq(_opContext), mcpListCaptor.capture(), eq(false));

    List<MetadataChangeProposal> mcps = mcpListCaptor.getValue();
    assertEquals(mcps.size(), 1);

    MetadataChangeProposal mcp = mcps.get(0);
    assertEquals(mcp.getEntityUrn(), TEST_ASSET_URN);
    assertEquals(mcp.getEntityType(), TEST_ASSET_URN.getEntityType());
    assertEquals(mcp.getAspectName(), Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME);
    Applications apps =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(), mcp.getAspect().getContentType(), Applications.class);
    assertNotNull(apps.getApplications());
    assertTrue(apps.getApplications().isEmpty());
  }

  @Test
  public void testUnsetApplication() throws Exception {
    _applicationService.unsetApplication(_opContext, TEST_ASSET_URN, TEST_USER_URN);

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(1)).ingestProposal(eq(_opContext), mcpCaptor.capture(), eq(false));

    MetadataChangeProposal mcp = mcpCaptor.getValue();
    assertEquals(mcp.getEntityUrn(), TEST_ASSET_URN);
    assertEquals(mcp.getEntityType(), TEST_ASSET_URN.getEntityType());
    assertEquals(mcp.getAspectName(), Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME);

    Applications apps =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(), mcp.getAspect().getContentType(), Applications.class);
    assertNotNull(apps.getApplications());
    assertTrue(apps.getApplications().isEmpty());
  }

  @Test
  public void testVerifyEntityExists() throws Exception {
    when(_entityClient.exists(eq(_opContext), eq(TEST_APPLICATION_URN))).thenReturn(true);
    assertTrue(_applicationService.verifyEntityExists(_opContext, TEST_APPLICATION_URN));

    when(_entityClient.exists(eq(_opContext), eq(TEST_APPLICATION_URN_2))).thenReturn(false);
    assertFalse(_applicationService.verifyEntityExists(_opContext, TEST_APPLICATION_URN_2));

    when(_entityClient.exists(eq(_opContext), eq(TEST_DOMAIN_URN)))
        .thenThrow(new RuntimeException("Client error"));
    assertThrows(
        RuntimeException.class,
        () -> _applicationService.verifyEntityExists(_opContext, TEST_DOMAIN_URN));
  }
}
