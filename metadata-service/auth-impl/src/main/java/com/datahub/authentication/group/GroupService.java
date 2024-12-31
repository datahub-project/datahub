package com.datahub.authentication.group;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class GroupService {
  private final EntityClient _entityClient;
  private final EntityService<?> _entityService;
  private final GraphClient _graphClient;

  public GroupService(
      @Nonnull EntityClient entityClient,
      @Nonnull EntityService<?> entityService,
      @Nonnull GraphClient graphClient) {
    Objects.requireNonNull(entityClient, "entityClient must not be null!");
    Objects.requireNonNull(entityService, "entityService must not be null!");
    Objects.requireNonNull(graphClient, "secretService must not be null!");

    _entityClient = entityClient;
    _entityService = entityService;
    _graphClient = graphClient;
  }

  public boolean groupExists(@Nonnull OperationContext opContext, @Nonnull Urn groupUrn) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    return _entityService.exists(opContext, groupUrn, true);
  }

  public Origin getGroupOrigin(@Nonnull OperationContext opContext, @Nonnull final Urn groupUrn) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    return (Origin) _entityService.getLatestAspect(opContext, groupUrn, ORIGIN_ASPECT_NAME);
  }

  public void addUserToNativeGroup(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn userUrn,
      @Nonnull final Urn groupUrn) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    // Verify the user exists
    if (!_entityService.exists(opContext, userUrn, true)) {
      throw new RuntimeException("Failed to add member to group. User does not exist.");
    }

    try {
      // First, fetch user's group membership aspect.
      NativeGroupMembership nativeGroupMembership =
          getExistingNativeGroupMembership(opContext, userUrn);
      // Handle the duplicate case.
      nativeGroupMembership.getNativeGroups().remove(groupUrn);
      nativeGroupMembership.getNativeGroups().add(groupUrn);

      // Finally, create the MetadataChangeProposal.
      final MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(userUrn);
      proposal.setEntityType(CORP_USER_ENTITY_NAME);
      proposal.setAspectName(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(nativeGroupMembership));
      proposal.setChangeType(ChangeType.UPSERT);
      _entityClient.ingestProposal(opContext, proposal);
    } catch (Exception e) {
      throw new RuntimeException("Failed to add member to group", e);
    }
  }

  public String createNativeGroup(
      @Nonnull OperationContext opContext,
      @Nonnull final CorpGroupKey corpGroupKey,
      @Nonnull final String groupName,
      @Nonnull final String groupDescription)
      throws Exception {
    Objects.requireNonNull(corpGroupKey, "corpGroupKey must not be null");
    Objects.requireNonNull(groupName, "groupName must not be null");
    Objects.requireNonNull(groupDescription, "groupDescription must not be null");

    Urn corpGroupUrn =
        EntityKeyUtils.convertEntityKeyToUrn(corpGroupKey, Constants.CORP_GROUP_ENTITY_NAME);
    if (groupExists(opContext, corpGroupUrn)) {
      throw new IllegalArgumentException("This Group already exists!");
    }

    String groupInfo = createGroupInfo(opContext, corpGroupKey, groupName, groupDescription);
    createNativeGroupOrigin(opContext, corpGroupUrn);
    return groupInfo;
  }

  public void removeExistingNativeGroupMembers(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final List<Urn> userUrnList)
      throws Exception {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    Objects.requireNonNull(userUrnList, "userUrnList must not be null");

    final Set<Urn> userUrns = new HashSet<>(userUrnList);
    for (Urn userUrn : userUrns) {
      final Map<Urn, EntityResponse> entityResponseMap =
          _entityClient.batchGetV2(
              opContext,
              CORP_USER_ENTITY_NAME,
              userUrns,
              Collections.singleton(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME));
      EntityResponse entityResponse = entityResponseMap.get(userUrn);
      if (entityResponse == null) {
        continue;
      }

      final NativeGroupMembership nativeGroupMembership =
          new NativeGroupMembership(
              entityResponse
                  .getAspects()
                  .get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)
                  .getValue()
                  .data());
      if (nativeGroupMembership.getNativeGroups().remove(groupUrn)) {
        // Finally, create the MetadataChangeProposal.
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityUrn(userUrn);
        proposal.setEntityType(CORP_USER_ENTITY_NAME);
        proposal.setAspectName(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(nativeGroupMembership));
        proposal.setChangeType(ChangeType.UPSERT);
        _entityClient.ingestProposal(opContext, proposal);
      }
    }
  }

  public void migrateGroupMembershipToNativeGroupMembership(
      @Nonnull OperationContext opContext, @Nonnull final Urn groupUrn, final String actorUrnStr)
      throws Exception {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    // Get the existing set of users
    final List<Urn> userUrnList = getExistingGroupMembers(groupUrn, actorUrnStr);
    // Remove the existing group membership for each user in the group
    removeExistingGroupMembers(opContext, groupUrn, userUrnList);
    // Mark the group as a native group
    createNativeGroupOrigin(opContext, groupUrn);
    // Add each user as a native group member to the group
    userUrnList.forEach(userUrn -> addUserToNativeGroup(opContext, userUrn, groupUrn));
  }

  public List<Urn> getGroupsForUser(@Nonnull OperationContext opContext, @Nonnull final Urn userUrn)
      throws Exception {
    final NativeGroupMembership nativeGroupMembership =
        getExistingNativeGroupMembership(opContext, userUrn);
    final GroupMembership groupMembership = getExistingGroupMembership(opContext, userUrn);
    final List<Urn> allGroups = new ArrayList<>();
    allGroups.addAll(nativeGroupMembership.getNativeGroups());
    allGroups.addAll(groupMembership.getGroups());
    return allGroups;
  }

  NativeGroupMembership getExistingNativeGroupMembership(
      @Nonnull OperationContext opContext, @Nonnull final Urn userUrn) throws Exception {
    final EntityResponse entityResponse =
        _entityClient
            .batchGetV2(
                opContext,
                CORP_USER_ENTITY_NAME,
                Collections.singleton(userUrn),
                Collections.singleton(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))
            .get(userUrn);

    final NativeGroupMembership nativeGroupMembership;
    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)) {
      // If the user doesn't have the NativeGroupMembership aspect, create one.
      nativeGroupMembership = new NativeGroupMembership();
      nativeGroupMembership.setNativeGroups(new UrnArray());
    } else {
      nativeGroupMembership =
          new NativeGroupMembership(
              entityResponse
                  .getAspects()
                  .get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)
                  .getValue()
                  .data());
    }
    return nativeGroupMembership;
  }

  GroupMembership getExistingGroupMembership(
      @Nonnull OperationContext opContext, @Nonnull final Urn userUrn)
      throws RemoteInvocationException, URISyntaxException {
    final EntityResponse entityResponse =
        _entityClient
            .batchGetV2(
                opContext,
                CORP_USER_ENTITY_NAME,
                Collections.singleton(userUrn),
                Collections.singleton(GROUP_MEMBERSHIP_ASPECT_NAME))
            .get(userUrn);

    final GroupMembership groupMembership;
    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(GROUP_MEMBERSHIP_ASPECT_NAME)) {
      // If the user doesn't have the GroupMembership aspect, create one.
      groupMembership = new GroupMembership();
      groupMembership.setGroups(new UrnArray());
    } else {
      groupMembership =
          new GroupMembership(
              entityResponse.getAspects().get(GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
    }
    return groupMembership;
  }

  String createGroupInfo(
      @Nonnull OperationContext opContext,
      @Nonnull final CorpGroupKey corpGroupKey,
      @Nonnull final String groupName,
      @Nonnull final String groupDescription)
      throws Exception {
    Objects.requireNonNull(corpGroupKey, "corpGroupKey must not be null");
    Objects.requireNonNull(groupName, "groupName must not be null");
    Objects.requireNonNull(groupDescription, "groupDescription must not be null");

    // Create the Group info.
    final CorpGroupInfo corpGroupInfo = new CorpGroupInfo();
    corpGroupInfo.setDisplayName(groupName);
    corpGroupInfo.setDescription(groupDescription);
    corpGroupInfo.setGroups(new CorpGroupUrnArray());
    corpGroupInfo.setMembers(new CorpuserUrnArray());
    corpGroupInfo.setAdmins(new CorpuserUrnArray());
    corpGroupInfo.setCreated(
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr())));

    // Finally, create the MetadataChangeProposal.
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(corpGroupKey));
    proposal.setEntityType(Constants.CORP_GROUP_ENTITY_NAME);
    proposal.setAspectName(Constants.CORP_GROUP_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(corpGroupInfo));
    proposal.setChangeType(ChangeType.UPSERT);
    return _entityClient.ingestProposal(opContext, proposal);
  }

  void createNativeGroupOrigin(@Nonnull OperationContext opContext, @Nonnull final Urn groupUrn)
      throws Exception {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    // Create the Group info.
    final Origin groupOrigin = new Origin();
    groupOrigin.setType(OriginType.NATIVE);

    // Finally, create the MetadataChangeProposal.
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(groupUrn);
    proposal.setEntityType(Constants.CORP_GROUP_ENTITY_NAME);
    proposal.setAspectName(ORIGIN_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(groupOrigin));
    proposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(opContext, proposal);
  }

  List<Urn> getExistingGroupMembers(@Nonnull final Urn groupUrn, final String actorUrnStr) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    final EntityRelationships relationships =
        _graphClient.getRelatedEntities(
            groupUrn.toString(),
            ImmutableList.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
            RelationshipDirection.INCOMING,
            0,
            500,
            actorUrnStr);
    return relationships.getRelationships().stream()
        .map(EntityRelationship::getEntity)
        .collect(Collectors.toList());
  }

  void removeExistingGroupMembers(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final List<Urn> userUrnList)
      throws Exception {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    Objects.requireNonNull(userUrnList, "userUrnList must not be null");

    final Set<Urn> userUrns = new HashSet<>(userUrnList);
    for (Urn userUrn : userUrns) {
      final Map<Urn, EntityResponse> entityResponseMap =
          _entityClient.batchGetV2(
              opContext,
              CORP_USER_ENTITY_NAME,
              userUrns,
              Collections.singleton(GROUP_MEMBERSHIP_ASPECT_NAME));
      EntityResponse entityResponse = entityResponseMap.get(userUrn);
      if (entityResponse == null) {
        continue;
      }

      final GroupMembership groupMembership =
          new GroupMembership(
              entityResponse.getAspects().get(GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
      if (groupMembership.getGroups().remove(groupUrn)) {
        // Finally, create the MetadataChangeProposal.
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityUrn(userUrn);
        proposal.setEntityType(CORP_USER_ENTITY_NAME);
        proposal.setAspectName(GROUP_MEMBERSHIP_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(groupMembership));
        proposal.setChangeType(ChangeType.UPSERT);
        _entityClient.ingestProposal(opContext, proposal);
      }
    }
  }
}
