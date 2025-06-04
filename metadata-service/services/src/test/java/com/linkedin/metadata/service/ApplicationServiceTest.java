package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.application.ApplicationAssociation;
import com.linkedin.application.ApplicationAssociationArray;
import com.linkedin.application.ApplicationKey;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private EntityClient _entityClient;
  private GraphClient _graphClient;
  private ApplicationService _applicationService;
  private OperationContext _opContext;
  private EntityRegistry _mockEntityRegistry;

  @BeforeMethod
  public void setUp() {
    _entityClient = mock(EntityClient.class);
    _graphClient = mock(GraphClient.class);
    _mockEntityRegistry = mock(EntityRegistry.class); // Mock EntityRegistry
    _opContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN, _mockEntityRegistry);
    _applicationService = new ApplicationService(_entityClient, _graphClient);
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
    assertNotNull(props.getAssets()); // Assets should be initialized
    assertTrue(props.getAssets().isEmpty());

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
    ApplicationProperties props =
        new ApplicationProperties().setName("Test").setAssets(new ApplicationAssociationArray());
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

  private EntityResponse mockApplicationPropertiesResponse(
      Urn appUrn, ApplicationProperties properties) {
    EntityResponse response = new EntityResponse();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(properties.data()));
    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(Constants.APPLICATION_PROPERTIES_ASPECT_NAME, envelopedAspect);
    response.setAspects(new EnvelopedAspectMap(aspects));
    response.setEntityName(Constants.APPLICATION_ENTITY_NAME);
    response.setUrn(appUrn);
    return response;
  }

  @Test
  public void testDeleteApplication() throws Exception {
    // Case 1: No assets (properties or graph) - successful deletion
    ApplicationProperties emptyProps =
        new ApplicationProperties().setAssets(new ApplicationAssociationArray());
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockApplicationPropertiesResponse(TEST_APPLICATION_URN, emptyProps));

    // Ensure graph client returns null for this case explicitly
    when(_graphClient.getRelatedEntities(
            eq(TEST_APPLICATION_URN.toString()),
            eq(ImmutableSet.of("ApplicationContains")),
            eq(RelationshipDirection.OUTGOING),
            eq(0),
            eq(1),
            eq(_opContext.getAuthentication().getActor().toUrnStr())))
        .thenReturn(null); // Explicitly return null

    _applicationService.deleteApplication(_opContext, TEST_APPLICATION_URN);
    verify(_entityClient, times(1)).deleteEntity(eq(_opContext), eq(TEST_APPLICATION_URN));
    Mockito.reset(_entityClient, _graphClient); // Reset after Case 1

    // Case 2: Assets in properties - should throw exception
    ApplicationProperties propsWithAssets =
        new ApplicationProperties()
            .setAssets(
                new ApplicationAssociationArray(ImmutableList.of(new ApplicationAssociation())));
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockApplicationPropertiesResponse(TEST_APPLICATION_URN, propsWithAssets));
    assertThrows(
        RuntimeException.class,
        () -> _applicationService.deleteApplication(_opContext, TEST_APPLICATION_URN));
    verify(_entityClient, Mockito.never()).deleteEntity(any(), any());
    Mockito.reset(_entityClient, _graphClient); // Reset after Case 2
  }

  @Test
  public void testBatchSetApplicationAssets() throws Exception {
    ApplicationProperties initialProps =
        new ApplicationProperties().setAssets(new ApplicationAssociationArray());
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockApplicationPropertiesResponse(TEST_APPLICATION_URN, initialProps));

    List<Urn> assetUrns = ImmutableList.of(TEST_ASSET_URN);
    _applicationService.batchSetApplicationAssets(
        _opContext, TEST_APPLICATION_URN, assetUrns, TEST_USER_URN);

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(1)).ingestProposal(eq(_opContext), mcpCaptor.capture(), eq(false));
    MetadataChangeProposal mcp = mcpCaptor.getValue();
    ApplicationProperties capturedProps =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            ApplicationProperties.class);
    assertEquals(capturedProps.getAssets().size(), 1);
    assertEquals(capturedProps.getAssets().get(0).getDestinationUrn(), TEST_ASSET_URN);
    // TODO: Add more specific asset checks once ApplicationAssociation structure is clear

    // Test case: Application properties not found
    Mockito.reset(_entityClient);
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN_2),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(null);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            _applicationService.batchSetApplicationAssets(
                _opContext, TEST_APPLICATION_URN_2, assetUrns, TEST_USER_URN));
  }

  @Test
  public void testBatchRemoveApplicationAssets() throws Exception {
    // === Case 1: Successfully remove an existing asset from an app with multiple assets ===
    Urn assetUrnToRemove = TEST_ASSET_URN;
    Urn remainingAssetUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:s3,remainingAsset,PROD)");

    ApplicationAssociation associationToRemove =
        new ApplicationAssociation().setDestinationUrn(assetUrnToRemove);
    ApplicationAssociation associationToRemain =
        new ApplicationAssociation().setDestinationUrn(remainingAssetUrn);

    ApplicationProperties propsWithTwoAssets =
        new ApplicationProperties()
            .setAssets(
                new ApplicationAssociationArray(
                    ImmutableList.of(associationToRemove, associationToRemain)));

    when(_entityClient.getV2(
            eq(_opContext),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN),
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockApplicationPropertiesResponse(TEST_APPLICATION_URN, propsWithTwoAssets));

    _applicationService.batchRemoveApplicationAssets(
        _opContext, TEST_APPLICATION_URN, ImmutableList.of(assetUrnToRemove), TEST_USER_URN);

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(1)).ingestProposal(eq(_opContext), mcpCaptor.capture(), eq(false));
    MetadataChangeProposal mcp = mcpCaptor.getValue();
    ApplicationProperties capturedProps =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            ApplicationProperties.class);
    assertNotNull(capturedProps.getAssets());
    assertEquals(capturedProps.getAssets().size(), 1, "One asset should remain");
    assertEquals(
        capturedProps.getAssets().get(0).getDestinationUrn(),
        remainingAssetUrn,
        "The correct asset should remain");

    Mockito.reset(_entityClient);

    // === Case 2: Application properties not found ===
    when(_entityClient.getV2(
            eq(_opContext),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN_2), // Use a different URN for this distinct case
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(null);
    _applicationService.batchRemoveApplicationAssets(
        _opContext,
        TEST_APPLICATION_URN_2,
        ImmutableList.of(TEST_ASSET_URN),
        TEST_USER_URN); // Should do nothing as properties are not found
    verify(_entityClient, Mockito.never()).ingestProposal(any(), any(), anyBoolean());

    // No need to reset _entityClient if the next case sets its own mocks or resets.

    // === Case 3: Application has no assets initially ===
    Mockito.reset(_entityClient); // Reset for clarity for this case
    ApplicationProperties emptyProps =
        new ApplicationProperties().setAssets(new ApplicationAssociationArray());
    when(_entityClient.getV2(
            eq(_opContext),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN), // Can reuse TEST_APPLICATION_URN for this scenario
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockApplicationPropertiesResponse(TEST_APPLICATION_URN, emptyProps));
    _applicationService.batchRemoveApplicationAssets(
        _opContext,
        TEST_APPLICATION_URN,
        ImmutableList.of(TEST_ASSET_URN), // Attempt to remove an asset not present in empty list
        TEST_USER_URN);
    verify(_entityClient, Mockito.never())
        .ingestProposal(any(), any(), anyBoolean()); // No change, so no proposal
  }

  @Test
  public void testUnsetApplication() throws Exception {
    // Case 1: Resource is part of an application
    EntityRelationship graphRelationship = new EntityRelationship();
    graphRelationship.setEntity(TEST_APPLICATION_URN); // Application is the source
    graphRelationship.setType("ApplicationContains");

    EntityRelationships relationships = new EntityRelationships();
    relationships.setRelationships(
        new EntityRelationshipArray(ImmutableList.of(graphRelationship)));

    when(_graphClient.getRelatedEntities(
            eq(TEST_ASSET_URN.toString()),
            eq(ImmutableSet.of("ApplicationContains")),
            eq(RelationshipDirection.INCOMING),
            eq(0),
            eq(1),
            eq(_opContext.getAuthentication().getActor().toUrnStr())))
        .thenReturn(relationships);

    // Mock getApplicationProperties for the subsequent batchRemoveApplicationAssets call
    ApplicationAssociation assetAssociation = new ApplicationAssociation();
    assetAssociation.setDestinationUrn(
        TEST_ASSET_URN); // Corrected: Use setDestinationUrn with the asset being unset
    ApplicationProperties appProps =
        new ApplicationProperties()
            .setAssets(new ApplicationAssociationArray(ImmutableList.of(assetAssociation)));
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.APPLICATION_ENTITY_NAME),
            eq(TEST_APPLICATION_URN), // This is the URN found by the graph client
            eq(ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockApplicationPropertiesResponse(TEST_APPLICATION_URN, appProps));

    _applicationService.unsetApplication(_opContext, TEST_ASSET_URN, TEST_USER_URN);

    // Verify that batchRemoveApplicationAssets was effectively called
    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(1))
        .ingestProposal(eq(_opContext), mcpCaptor.capture(), eq(false)); // batchRemove called this

    MetadataChangeProposal capturedMcp = mcpCaptor.getValue();
    ApplicationProperties capturedProps =
        GenericRecordUtils.deserializeAspect(
            capturedMcp.getAspect().getValue(),
            capturedMcp.getAspect().getContentType(),
            ApplicationProperties.class);
    assertNotNull(capturedProps.getAssets());
    assertTrue(
        capturedProps.getAssets().isEmpty(),
        "Asset TEST_ASSET_URN should have been removed from application assets via unsetApplication");

    // Case 2: Resource is not part of any application
    Mockito.reset(_graphClient, _entityClient);
    when(_graphClient.getRelatedEntities(
            eq(TEST_ASSET_URN.toString()),
            eq(ImmutableSet.of("ApplicationContains")),
            eq(RelationshipDirection.INCOMING),
            eq(0),
            eq(1),
            eq(_opContext.getAuthentication().getActor().toUrnStr())))
        .thenReturn(
            new EntityRelationships()
                .setRelationships(new EntityRelationshipArray())); // No relationships

    _applicationService.unsetApplication(_opContext, TEST_ASSET_URN, TEST_USER_URN);
    verify(_entityClient, Mockito.never())
        .ingestProposal(
            eq(_opContext), any(MetadataChangeProposal.class), anyBoolean()); // No removal attempt
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
