package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.ownership.OwnershipTypeInfo;
import com.linkedin.r2.RemoteInvocationException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OwnershipTypeServiceTest {

  private static final Urn TEST_OWNERSHIP_TYPE_URN = UrnUtils.getUrn("urn:li:ownershipType:test");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  private void testCreateOwnershipTypeSuccess() throws Exception {

    final EntityClient mockClient = createOwnershipTypeMockEntityClient();
    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    // Case 1: With description
    Urn urn =
        service.createOwnershipType(
            "test OwnershipType", "my description", mockAuthentication(), 0L);

    Assert.assertEquals(urn, TEST_OWNERSHIP_TYPE_URN);
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    // Case 2: Without description
    urn = service.createOwnershipType("test OwnershipType", null, mockAuthentication(), 0L);

    Assert.assertEquals(urn, TEST_OWNERSHIP_TYPE_URN);
    Mockito.verify(mockClient, Mockito.times(2))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(Authentication.class),
            Mockito.eq(false));
  }

  @Test
  private void testCreateOwnershipTypeErrorMissingInputs() throws Exception {
    final EntityClient mockClient = createOwnershipTypeMockEntityClient();
    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    // Only case: missing OwnershipType Name
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.createOwnershipType(null, "my description", mockAuthentication(), 0L));
  }

  @Test
  private void testCreateOwnershipTypeError() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.createOwnershipType("new name", "my description", mockAuthentication(), 1L));
  }

  @Test
  private void testUpdateOwnershipTypeSuccess() throws Exception {
    final String oldName = "old name";
    final String oldDescription = "old description";
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    resetUpdateOwnershipTypeMockEntityClient(
        mockClient, TEST_OWNERSHIP_TYPE_URN, oldName, oldDescription, TEST_USER_URN, 0L, 0L);

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));
    final String newName = "new name";
    final String newDescription = "new description";

    // Case 1: Update name only
    service.updateOwnershipType(TEST_OWNERSHIP_TYPE_URN, newName, null, mockAuthentication(), 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.eq(
                buildUpdateOwnershipTypeProposal(
                    TEST_OWNERSHIP_TYPE_URN, newName, oldDescription, 0L, 1L)),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    resetUpdateOwnershipTypeMockEntityClient(
        mockClient, TEST_OWNERSHIP_TYPE_URN, oldName, oldDescription, TEST_USER_URN, 0L, 0L);

    // Case 2: Update description only
    service.updateOwnershipType(
        TEST_OWNERSHIP_TYPE_URN, null, newDescription, mockAuthentication(), 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.eq(
                buildUpdateOwnershipTypeProposal(
                    TEST_OWNERSHIP_TYPE_URN, oldName, newDescription, 0L, 1L)),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    resetUpdateOwnershipTypeMockEntityClient(
        mockClient, TEST_OWNERSHIP_TYPE_URN, oldName, oldDescription, TEST_USER_URN, 0L, 0L);

    // Case 3: Update all fields at once
    service.updateOwnershipType(
        TEST_OWNERSHIP_TYPE_URN, newName, newDescription, mockAuthentication(), 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.eq(
                buildUpdateOwnershipTypeProposal(
                    TEST_OWNERSHIP_TYPE_URN, newName, newDescription, 0L, 1L)),
            Mockito.any(Authentication.class),
            Mockito.eq(false));
  }

  @Test
  private void testUpdateOwnershipTypeMissingOwnershipType() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(TEST_OWNERSHIP_TYPE_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(null);

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    final String newName = "new name";

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.updateOwnershipType(
                TEST_OWNERSHIP_TYPE_URN, newName, null, mockAuthentication(), 1L));
  }

  @Test
  private void testUpdateOwnershipTypeError() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
            Mockito.eq(TEST_OWNERSHIP_TYPE_URN),
            Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME)),
            Mockito.any(Authentication.class));

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.updateOwnershipType(
                TEST_OWNERSHIP_TYPE_URN, "new name", null, mockAuthentication(), 1L));
  }

  @Test
  private void testDeleteOwnershipTypeSuccess() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    service.deleteOwnershipType(TEST_OWNERSHIP_TYPE_URN, true, mockAuthentication());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(Mockito.eq(TEST_OWNERSHIP_TYPE_URN), Mockito.any(Authentication.class));

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntityReferences(
            Mockito.eq(TEST_OWNERSHIP_TYPE_URN), Mockito.any(Authentication.class));
  }

  @Test
  private void testDeleteOwnershipTypeError() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .deleteEntity(Mockito.eq(TEST_OWNERSHIP_TYPE_URN), Mockito.any(Authentication.class));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.deleteOwnershipType(TEST_OWNERSHIP_TYPE_URN, false, mockAuthentication()));
  }

  @Test
  private void testGetOwnershipTypeInfoSuccess() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final String name = "name";
    final String description = "description";

    resetGetOwnershipTypeInfoMockEntityClient(
        mockClient, TEST_OWNERSHIP_TYPE_URN, name, description, TEST_USER_URN, 0L, 1L);

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    final OwnershipTypeInfo info =
        service.getOwnershipTypeInfo(TEST_OWNERSHIP_TYPE_URN, mockAuthentication());

    // Assert that the info is correct.
    Assert.assertEquals((long) info.getCreated().getTime(), 0L);
    Assert.assertEquals((long) info.getLastModified().getTime(), 1L);
    Assert.assertEquals(info.getName(), name);
    Assert.assertEquals(info.getDescription(), description);
    Assert.assertEquals(info.getCreated().getActor(), TEST_USER_URN);
  }

  @Test
  private void testGetOwnershipTypeInfoNoOwnershipTypeExists() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(TEST_OWNERSHIP_TYPE_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(null);

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    Assert.assertNull(service.getOwnershipTypeInfo(TEST_OWNERSHIP_TYPE_URN, mockAuthentication()));
  }

  @Test
  private void testGetOwnershipTypeInfoError() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
            Mockito.eq(TEST_OWNERSHIP_TYPE_URN),
            Mockito.eq(
                ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME, Constants.STATUS_ASPECT_NAME)),
            Mockito.any(Authentication.class));

    final OwnershipTypeService service =
        new OwnershipTypeService(mockClient, Mockito.mock(Authentication.class));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.getOwnershipTypeInfo(TEST_OWNERSHIP_TYPE_URN, mockAuthentication()));
  }

  private static MetadataChangeProposal buildUpdateOwnershipTypeProposal(
      final Urn urn,
      final String newName,
      final String newDescription,
      final long createdAtMs,
      final long updatedAtMs) {

    OwnershipTypeInfo info = new OwnershipTypeInfo();
    info.setName(newName);
    info.setDescription(newDescription);
    info.setCreated(new AuditStamp().setActor(TEST_USER_URN).setTime(createdAtMs));
    info.setLastModified(new AuditStamp().setActor(TEST_USER_URN).setTime(updatedAtMs));

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(OWNERSHIP_TYPE_ENTITY_NAME);
    mcp.setAspectName(OWNERSHIP_TYPE_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(info));
    return mcp;
  }

  private static EntityClient createOwnershipTypeMockEntityClient() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                Mockito.any(MetadataChangeProposal.class),
                Mockito.any(Authentication.class),
                Mockito.eq(false)))
        .thenReturn(TEST_OWNERSHIP_TYPE_URN.toString());
    return mockClient;
  }

  private static void resetUpdateOwnershipTypeMockEntityClient(
      final EntityClient mockClient,
      final Urn ownershipTypeUrn,
      final String existingName,
      final String existingDescription,
      final Urn existingOwner,
      final long existingCreatedAt,
      final long existingUpdatedAt)
      throws Exception {

    Mockito.reset(mockClient);

    Mockito.when(
            mockClient.ingestProposal(
                Mockito.any(MetadataChangeProposal.class),
                Mockito.any(Authentication.class),
                Mockito.eq(false)))
        .thenReturn(ownershipTypeUrn.toString());

    final OwnershipTypeInfo existingInfo =
        new OwnershipTypeInfo()
            .setName(existingName)
            .setDescription(existingDescription)
            .setCreated(new AuditStamp().setActor(existingOwner).setTime(existingCreatedAt))
            .setLastModified(new AuditStamp().setActor(existingOwner).setTime(existingUpdatedAt));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(ownershipTypeUrn),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME, STATUS_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse()
                .setUrn(ownershipTypeUrn)
                .setEntityName(OWNERSHIP_TYPE_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            OWNERSHIP_TYPE_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(existingInfo.data()))))));
  }

  private static void resetGetOwnershipTypeInfoMockEntityClient(
      final EntityClient mockClient,
      final Urn ownershipTypeUrn,
      final String existingName,
      final String existingDescription,
      final Urn existingOwner,
      final long existingCreatedAt,
      final long existingUpdatedAt)
      throws Exception {

    Mockito.reset(mockClient);

    final OwnershipTypeInfo existingInfo =
        new OwnershipTypeInfo()
            .setName(existingName)
            .setDescription(existingDescription)
            .setCreated(new AuditStamp().setActor(existingOwner).setTime(existingCreatedAt))
            .setLastModified(new AuditStamp().setActor(existingOwner).setTime(existingUpdatedAt));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(ownershipTypeUrn),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME, STATUS_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse()
                .setUrn(ownershipTypeUrn)
                .setEntityName(OWNERSHIP_TYPE_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            OWNERSHIP_TYPE_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(existingInfo.data()))))));
  }

  private static Authentication mockAuthentication() {
    Authentication mockAuth = Mockito.mock(Authentication.class);
    Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, TEST_USER_URN.getId()));
    return mockAuth;
  }
}
