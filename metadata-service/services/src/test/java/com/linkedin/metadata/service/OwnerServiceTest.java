package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.service.OwnerService.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OwnerServiceTest {

  private static final Urn TEST_OWNER_URN_1 = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_OWNER_URN_2 = UrnUtils.getUrn("urn:li:corpuser:test2");

  private static final Urn TEST_ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final Urn TEST_ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");
  private static OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);

  @Test
  private void testAddOwnersExistingOwner() throws Exception {
    Ownership existingOwnership = new Ownership();
    existingOwnership.setOwners(
        new OwnerArray(
            ImmutableList.of(new Owner().setOwner(TEST_OWNER_URN_1).setType(OwnershipType.NONE))));
    SystemEntityClient mockClient = createMockOwnersClient(existingOwnership);

    final OwnerService service = new OwnerService(mockClient);

    Urn newOwnerUrn = UrnUtils.getUrn("urn:li:corpuser:newTag");
    List<MetadataChangeProposal> events =
        service.buildAddOwnersProposals(
            opContext,
            ImmutableList.of(newOwnerUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            OwnershipType.NONE);

    OwnerArray expected =
        new OwnerArray(
            ImmutableList.of(
                new Owner().setOwner(TEST_OWNER_URN_1).setType(OwnershipType.NONE),
                new Owner()
                    .setOwner(newOwnerUrn)
                    .setType(OwnershipType.NONE)
                    .setTypeUrn(mapOwnershipTypeToEntity(OwnershipType.NONE.toString()))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.OWNERSHIP_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    Ownership ownerAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Ownership.class);
    Assert.assertEquals(ownerAspect1.getOwners(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.OWNERSHIP_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    Ownership ownerAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Ownership.class);
    Assert.assertEquals(ownerAspect2.getOwners(), expected);
  }

  @Test
  private void testAddOwnersNoExistingOwners() throws Exception {
    SystemEntityClient mockClient = createMockOwnersClient(null);

    final OwnerService service = new OwnerService(mockClient);

    Urn newOwnerUrn = UrnUtils.getUrn("urn:li:corpuser:newOwner");
    List<MetadataChangeProposal> events =
        service.buildAddOwnersProposals(
            opContext,
            ImmutableList.of(newOwnerUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            OwnershipType.NONE);

    OwnerArray expectedOwners =
        new OwnerArray(
            ImmutableList.of(
                new Owner()
                    .setOwner(newOwnerUrn)
                    .setType(OwnershipType.NONE)
                    .setTypeUrn(mapOwnershipTypeToEntity(OwnershipType.NONE.toString()))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.OWNERSHIP_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    Ownership ownerAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Ownership.class);
    Assert.assertEquals(ownerAspect1.getOwners(), expectedOwners);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.OWNERSHIP_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    Ownership ownerAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Ownership.class);
    Assert.assertEquals(ownerAspect2.getOwners(), expectedOwners);
  }

  @Test
  private void testRemoveOwnerExistingOwners() throws Exception {
    Ownership existingOwnership = new Ownership();
    existingOwnership.setOwners(
        new OwnerArray(
            ImmutableList.of(
                new Owner().setOwner(TEST_OWNER_URN_1).setType(OwnershipType.TECHNICAL_OWNER),
                new Owner().setOwner(TEST_OWNER_URN_2).setType(OwnershipType.DATA_STEWARD))));
    SystemEntityClient mockClient = createMockOwnersClient(existingOwnership);

    final OwnerService service = new OwnerService(mockClient);

    List<MetadataChangeProposal> events =
        service.buildRemoveOwnersProposals(
            opContext,
            ImmutableList.of(TEST_OWNER_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    Ownership expected =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    ImmutableList.of(
                        new Owner()
                            .setOwner(TEST_OWNER_URN_2)
                            .setType(OwnershipType.DATA_STEWARD))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.OWNERSHIP_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate ownersAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Ownership.class);
    Assert.assertEquals(ownersAspect1, expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.OWNERSHIP_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate ownersAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Ownership.class);
    Assert.assertEquals(ownersAspect2, expected);
  }

  @Test
  private void testRemoveOwnerNoExistingOwners() throws Exception {
    SystemEntityClient mockClient = createMockOwnersClient(null);

    final OwnerService service = new OwnerService(mockClient);

    Urn newTagUrn = UrnUtils.getUrn("urn:li:corpuser:newOwner");
    List<MetadataChangeProposal> events =
        service.buildRemoveOwnersProposals(
            opContext,
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    OwnerArray expected = new OwnerArray(ImmutableList.of());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.OWNERSHIP_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    Ownership ownersAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Ownership.class);
    Assert.assertEquals(ownersAspect1.getOwners(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.OWNERSHIP_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    Ownership ownersAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Ownership.class);
    Assert.assertEquals(ownersAspect2.getOwners(), expected);
  }

  @Test
  private void testGetEntityOwners() throws Exception {
    final Ownership existingOwnership = new Ownership();
    existingOwnership.setOwners(
        new OwnerArray(
            ImmutableList.of(
                new Owner().setOwner(TEST_OWNER_URN_1).setType(OwnershipType.TECHNICAL_OWNER),
                new Owner().setOwner(TEST_OWNER_URN_2).setType(OwnershipType.DATA_STEWARD))));

    final OwnerService service = createMockOwnersService(null);
    final EntityResponse entityResponse = new EntityResponse();

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                OWNERSHIP_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(existingOwnership.data())))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponse);

    final List<Owner> owners = service.getEntityOwners(opContext, TEST_ENTITY_URN_1);
    Assert.assertEquals(
        owners.get(0),
        new Owner().setOwner(TEST_OWNER_URN_1).setType(OwnershipType.TECHNICAL_OWNER));
    Assert.assertEquals(
        owners.get(1), new Owner().setOwner(TEST_OWNER_URN_2).setType(OwnershipType.DATA_STEWARD));
  }

  @Test
  private void testGetEntityOwnersNullDomains() throws Exception {
    final OwnerService service = createMockOwnersService(null);
    final EntityResponse entityResponse = new EntityResponse();

    entityResponse.setAspects(new EnvelopedAspectMap(Collections.emptyMap()));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponse);

    final List<Owner> owners = service.getEntityOwners(opContext, TEST_ENTITY_URN_1);
    Assert.assertEquals(owners.size(), 0);
  }

  @Test(
      expectedExceptions = RemoteInvocationException.class,
      expectedExceptionsMessageRegExp = "Failed to connect to downstream service")
  private void testGetEntityOwnersRemoteInvocationException() throws Exception {
    final OwnerService service = createMockOwnersService(null);
    final EntityResponse entityResponse = new EntityResponse();

    entityResponse.setAspects(new EnvelopedAspectMap(Collections.emptyMap()));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException("Failed to connect to downstream service"));

    service.getEntityOwners(opContext, TEST_ENTITY_URN_1);

    // Throws expected exception - Decide whether the caller should handle this explicitly.
  }

  private static OwnerService createMockOwnersService(@Nullable Ownership existingOwnership) {
    setMockAspectRetriever(existingOwnership);
    return new OwnerService(mock(SystemEntityClient.class));
  }

  private static SystemEntityClient createMockOwnersClient(@Nullable Ownership existingOwnership)
      throws Exception {
    return createMockEntityClient(existingOwnership, Constants.OWNERSHIP_ASPECT_NAME);
  }

  private static SystemEntityClient createMockEntityClient(
      @Nullable RecordTemplate aspect, String aspectName) throws Exception {
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2)),
                Mockito.eq(ImmutableSet.of(aspectName))))
        .thenReturn(
            aspect != null
                ? ImmutableMap.of(
                    TEST_ENTITY_URN_1,
                    new EntityResponse()
                        .setUrn(TEST_ENTITY_URN_1)
                        .setEntityName(Constants.DATASET_ENTITY_NAME)
                        .setAspects(
                            new EnvelopedAspectMap(
                                ImmutableMap.of(
                                    aspectName,
                                    new EnvelopedAspect().setValue(new Aspect(aspect.data()))))),
                    TEST_ENTITY_URN_2,
                    new EntityResponse()
                        .setUrn(TEST_ENTITY_URN_2)
                        .setEntityName(Constants.DATASET_ENTITY_NAME)
                        .setAspects(
                            new EnvelopedAspectMap(
                                ImmutableMap.of(
                                    aspectName,
                                    new EnvelopedAspect().setValue(new Aspect(aspect.data()))))))
                : Collections.emptyMap());
    return mockClient;
  }

  private static void setMockAspectRetriever(@Nullable Ownership existingOwnership) {
    reset(mockAspectRetriever);

    if (existingOwnership != null) {
      Mockito.when(
              mockAspectRetriever.getLatestAspectObjects(
                  eq(Set.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2)),
                  eq(Set.of(OWNERSHIP_ASPECT_NAME))))
          .thenReturn(
              ImmutableMap.of(
                  TEST_ENTITY_URN_1,
                  Map.of(OWNERSHIP_ASPECT_NAME, new Aspect(existingOwnership.data())),
                  TEST_ENTITY_URN_2,
                  Map.of(OWNERSHIP_ASPECT_NAME, new Aspect(existingOwnership.data()))));
    } else {
      Mockito.when(mockAspectRetriever.getLatestAspectObjects(anySet(), anySet()))
          .thenReturn(Collections.emptyMap());
    }
  }
}
