package com.linkedin.metadata.connection;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubConnectionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConnectionServiceTest {

  private EntityClient entityClient;
  private Authentication systemAuthentication;
  private ConnectionService connectionService;

  @BeforeMethod
  public void setUp() {
    entityClient = Mockito.mock(EntityClient.class);
    systemAuthentication = Mockito.mock(Authentication.class);
    connectionService = new ConnectionService(entityClient);
  }

  @Test
  public void testUpsertConnection() throws Exception {
    final String id = "testId";
    final Urn platformUrn = UrnUtils.getUrn("urn:li:dataPlatform:slack");
    final DataHubConnectionDetailsType type = DataHubConnectionDetailsType.JSON;
    final DataHubJsonConnection json = new DataHubJsonConnection().setEncryptedBlob("blob");
    final Authentication authentication = Mockito.mock(Authentication.class);
    final DataHubConnectionKey key = new DataHubConnectionKey().setId(id);
    final Urn connectionUrn =
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_CONNECTION_ENTITY_NAME);

    // Execute and assert
    Urn result =
        connectionService.upsertConnection(
            mock(OperationContext.class), id, platformUrn, type, json, null);

    DataHubConnectionDetails expectedDetails = mockConnectionDetails(id);
    DataPlatformInstance expectedDataPlatformInstance = mockPlatformInstance(platformUrn);

    verify(entityClient)
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.eq(
                ImmutableList.of(
                    AspectUtils.buildMetadataChangeProposal(
                        connectionUrn,
                        Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                        expectedDetails),
                    AspectUtils.buildMetadataChangeProposal(
                        connectionUrn,
                        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                        expectedDataPlatformInstance))),
            Mockito.eq(false));
    assertEquals(result, connectionUrn);
  }

  @Test
  public void testGetConnectionDetails() throws Exception {
    final Urn connectionUrn = Mockito.mock(Urn.class);

    final DataHubConnectionDetails connectionDetails = mockConnectionDetails("testId");
    final DataPlatformInstance platformInstance =
        mockPlatformInstance(UrnUtils.getUrn("urn:li:dataPlatform:slack"));

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATAHUB_CONNECTION_ENTITY_NAME)
            .setUrn(connectionUrn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setName(Constants.DATAHUB_CONNECTION_ENTITY_NAME)
                            .setValue(new Aspect(connectionDetails.data())),
                        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setName(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                            .setValue(new Aspect(platformInstance.data())))));
    when(entityClient.getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.DATAHUB_CONNECTION_ENTITY_NAME),
            Mockito.eq(connectionUrn),
            Mockito.eq(
                ImmutableSet.of(
                    Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                    Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME))))
        .thenReturn(response);

    // Execute and assert
    DataHubConnectionDetails details =
        connectionService.getConnectionDetails(mock(OperationContext.class), connectionUrn);
    assertEquals(details, connectionDetails);
  }

  @Test
  public void testGetConnectionEntityResponse() throws Exception {
    final Urn connectionUrn = Mockito.mock(Urn.class);
    EntityResponse response = Mockito.mock(EntityResponse.class);
    when(entityClient.getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.DATAHUB_CONNECTION_ENTITY_NAME),
            Mockito.eq(connectionUrn),
            Mockito.eq(
                ImmutableSet.of(
                    Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                    Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME))))
        .thenReturn(response);
    // Execute and assert
    assertEquals(
        connectionService.getConnectionEntityResponse(mock(OperationContext.class), connectionUrn),
        response);
  }

  private DataHubConnectionDetails mockConnectionDetails(String id) {
    return new DataHubConnectionDetails()
        .setType(DataHubConnectionDetailsType.JSON)
        .setName(id)
        .setJson(new DataHubJsonConnection().setEncryptedBlob("blob"));
  }

  private DataPlatformInstance mockPlatformInstance(Urn platformUrn) {
    return new DataPlatformInstance().setPlatform(platformUrn);
  }
}
