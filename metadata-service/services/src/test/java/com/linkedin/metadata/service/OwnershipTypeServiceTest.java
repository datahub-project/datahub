package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

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
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.ownership.OwnershipTypeInfo;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OwnershipTypeServiceTest {

  private static final Urn TEST_OWNERSHIP_TYPE_URN = UrnUtils.getUrn("urn:li:ownershipType:test");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

  @Test
  private void testCreateOwnershipTypeSuccess() throws Exception {

    final SystemEntityClient mockClient = createOwnershipTypeMockEntityClient();
    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    // Case 1: With description
    Urn urn = service.createOwnershipType(opContext, "test OwnershipType", "my description", 0L);

    Assert.assertEquals(urn, TEST_OWNERSHIP_TYPE_URN);
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));

    // Case 2: Without description
    urn = service.createOwnershipType(opContext, "test OwnershipType", null, 0L);

    Assert.assertEquals(urn, TEST_OWNERSHIP_TYPE_URN);
    Mockito.verify(mockClient, Mockito.times(2))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  private void testCreateOwnershipTypeErrorMissingInputs() throws Exception {
    final SystemEntityClient mockClient = createOwnershipTypeMockEntityClient();
    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    // Only case: missing OwnershipType Name
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.createOwnershipType(opContext, null, "my description", 0L));
  }

  @Test
  private void testCreateOwnershipTypeError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.createOwnershipType(opContext, "new name", "my description", 1L));
  }

  @Test
  private void testUpdateOwnershipTypeSuccess() throws Exception {
    final String oldName = "old name";
    final String oldDescription = "old description";
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    resetUpdateOwnershipTypeMockEntityClient(
        mockClient, TEST_OWNERSHIP_TYPE_URN, oldName, oldDescription, TEST_USER_URN, 0L, 0L);

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);
    final String newName = "new name";
    final String newDescription = "new description";

    // Case 1: Update name only
    service.updateOwnershipType(opContext, TEST_OWNERSHIP_TYPE_URN, newName, null, 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.eq(
                buildUpdateOwnershipTypeProposal(
                    TEST_OWNERSHIP_TYPE_URN, newName, oldDescription, 0L, 1L)),
            Mockito.eq(false));

    resetUpdateOwnershipTypeMockEntityClient(
        mockClient, TEST_OWNERSHIP_TYPE_URN, oldName, oldDescription, TEST_USER_URN, 0L, 0L);

    // Case 2: Update description only
    service.updateOwnershipType(opContext, TEST_OWNERSHIP_TYPE_URN, null, newDescription, 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.eq(
                buildUpdateOwnershipTypeProposal(
                    TEST_OWNERSHIP_TYPE_URN, oldName, newDescription, 0L, 1L)),
            Mockito.eq(false));

    resetUpdateOwnershipTypeMockEntityClient(
        mockClient, TEST_OWNERSHIP_TYPE_URN, oldName, oldDescription, TEST_USER_URN, 0L, 0L);

    // Case 3: Update all fields at once
    service.updateOwnershipType(opContext, TEST_OWNERSHIP_TYPE_URN, newName, newDescription, 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.eq(
                buildUpdateOwnershipTypeProposal(
                    TEST_OWNERSHIP_TYPE_URN, newName, newDescription, 0L, 1L)),
            Mockito.eq(false));
  }

  @Test
  private void testUpdateOwnershipTypeMissingOwnershipType() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(TEST_OWNERSHIP_TYPE_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME))))
        .thenReturn(null);

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    final String newName = "new name";

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.updateOwnershipType(opContext, TEST_OWNERSHIP_TYPE_URN, newName, null, 1L));
  }

  @Test
  private void testUpdateOwnershipTypeError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            any(OperationContext.class),
            Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
            Mockito.eq(TEST_OWNERSHIP_TYPE_URN),
            Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME)));

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.updateOwnershipType(opContext, TEST_OWNERSHIP_TYPE_URN, "new name", null, 1L));
  }

  @Test
  private void testDeleteOwnershipTypeSuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    service.deleteOwnershipType(opContext, TEST_OWNERSHIP_TYPE_URN, true);

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_OWNERSHIP_TYPE_URN));

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntityReferences(any(OperationContext.class), Mockito.eq(TEST_OWNERSHIP_TYPE_URN));
  }

  @Test
  private void testDeleteOwnershipTypeError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_OWNERSHIP_TYPE_URN));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.deleteOwnershipType(opContext, TEST_OWNERSHIP_TYPE_URN, false));
  }

  @Test
  private void testGetOwnershipTypeInfoSuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final String name = "name";
    final String description = "description";

    resetGetOwnershipTypeInfoMockEntityClient(
        mockClient, TEST_OWNERSHIP_TYPE_URN, name, description, TEST_USER_URN, 0L, 1L);

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    final OwnershipTypeInfo info = service.getOwnershipTypeInfo(opContext, TEST_OWNERSHIP_TYPE_URN);

    // Assert that the info is correct.
    Assert.assertEquals((long) info.getCreated().getTime(), 0L);
    Assert.assertEquals((long) info.getLastModified().getTime(), 1L);
    Assert.assertEquals(info.getName(), name);
    Assert.assertEquals(info.getDescription(), description);
    Assert.assertEquals(info.getCreated().getActor(), TEST_USER_URN);
  }

  @Test
  private void testGetOwnershipTypeInfoNoOwnershipTypeExists() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(TEST_OWNERSHIP_TYPE_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME))))
        .thenReturn(null);

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    Assert.assertNull(service.getOwnershipTypeInfo(opContext, TEST_OWNERSHIP_TYPE_URN));
  }

  @Test
  private void testGetOwnershipTypeInfoError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            any(OperationContext.class),
            Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
            Mockito.eq(TEST_OWNERSHIP_TYPE_URN),
            Mockito.eq(
                ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME, Constants.STATUS_ASPECT_NAME)));

    final OwnershipTypeService service = new OwnershipTypeService(mockClient);

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.getOwnershipTypeInfo(opContext, TEST_OWNERSHIP_TYPE_URN));
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

  private static SystemEntityClient createOwnershipTypeMockEntityClient() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false)))
        .thenReturn(TEST_OWNERSHIP_TYPE_URN.toString());
    return mockClient;
  }

  private static void resetUpdateOwnershipTypeMockEntityClient(
      final SystemEntityClient mockClient,
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
                any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false)))
        .thenReturn(ownershipTypeUrn.toString());

    final OwnershipTypeInfo existingInfo =
        new OwnershipTypeInfo()
            .setName(existingName)
            .setDescription(existingDescription)
            .setCreated(new AuditStamp().setActor(existingOwner).setTime(existingCreatedAt))
            .setLastModified(new AuditStamp().setActor(existingOwner).setTime(existingUpdatedAt));

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(ownershipTypeUrn),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME, STATUS_ASPECT_NAME))))
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
      final SystemEntityClient mockClient,
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
                any(OperationContext.class),
                Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                Mockito.eq(ownershipTypeUrn),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME, STATUS_ASPECT_NAME))))
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
    Authentication mockAuth = mock(Authentication.class);
    Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, TEST_USER_URN.getId()));
    return mockAuth;
  }
}
