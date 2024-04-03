package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.OwnerService.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
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
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Collections;
import java.util.List;
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

  @Test
  private void testAddOwnersExistingOwner() throws Exception {
    Ownership existingOwnership = new Ownership();
    existingOwnership.setOwners(
        new OwnerArray(
            ImmutableList.of(new Owner().setOwner(TEST_OWNER_URN_1).setType(OwnershipType.NONE))));
    EntityClient mockClient = createMockOwnersClient(existingOwnership);

    final OwnerService service = new OwnerService(mockClient, Mockito.mock(Authentication.class));

    Urn newOwnerUrn = UrnUtils.getUrn("urn:li:corpuser:newTag");
    List<MetadataChangeProposal> events =
        service.buildAddOwnersProposals(
            ImmutableList.of(newOwnerUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            OwnershipType.NONE,
            mockAuthentication());

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
    EntityClient mockClient = createMockOwnersClient(null);

    final OwnerService service = new OwnerService(mockClient, Mockito.mock(Authentication.class));

    Urn newOwnerUrn = UrnUtils.getUrn("urn:li:corpuser:newOwner");
    List<MetadataChangeProposal> events =
        service.buildAddOwnersProposals(
            ImmutableList.of(newOwnerUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            OwnershipType.NONE,
            mockAuthentication());

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
    EntityClient mockClient = createMockOwnersClient(existingOwnership);

    final OwnerService service = new OwnerService(mockClient, Mockito.mock(Authentication.class));

    List<MetadataChangeProposal> events =
        service.buildRemoveOwnersProposals(
            ImmutableList.of(TEST_OWNER_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

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
    EntityClient mockClient = createMockOwnersClient(null);

    final OwnerService service = new OwnerService(mockClient, Mockito.mock(Authentication.class));

    Urn newTagUrn = UrnUtils.getUrn("urn:li:corpuser:newOwner");
    List<MetadataChangeProposal> events =
        service.buildRemoveOwnersProposals(
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

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

  private static EntityClient createMockOwnersClient(@Nullable Ownership existingOwnership)
      throws Exception {
    return createMockEntityClient(existingOwnership, Constants.OWNERSHIP_ASPECT_NAME);
  }

  private static EntityClient createMockEntityClient(
      @Nullable RecordTemplate aspect, String aspectName) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.batchGetV2(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2)),
                Mockito.eq(ImmutableSet.of(aspectName)),
                Mockito.any(Authentication.class)))
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

  private static Authentication mockAuthentication() {
    Authentication mockAuth = Mockito.mock(Authentication.class);
    Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, Constants.SYSTEM_ACTOR));
    return mockAuth;
  }
}
