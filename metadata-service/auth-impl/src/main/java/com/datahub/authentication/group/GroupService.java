package com.datahub.authentication.group;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.buildSynchronousMetadataChangeProposal;

import com.datahub.authorization.SessionActorIdentity;
import com.google.common.collect.ImmutableSet;
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
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.ActorGroupMembershipService;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GroupService implements ActorGroupMembershipService {

  private static final ImmutableSet<String> USER_MEMBERSHIP_ASPECTS =
      ImmutableSet.of(
          GROUP_MEMBERSHIP_ASPECT_NAME,
          NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
          ROLE_MEMBERSHIP_ASPECT_NAME);

  private final SystemEntityClient _entityClient;
  private final EntityService<?> _entityService;
  private final GraphClient _graphClient;

  public GroupService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull EntityService<?> entityService,
      @Nonnull GraphClient graphClient) {
    Objects.requireNonNull(entityClient, "entityClient must not be null!");
    Objects.requireNonNull(entityService, "entityService must not be null!");
    Objects.requireNonNull(graphClient, "graphClient must not be null!");

    _entityClient = entityClient;
    _entityService = entityService;
    _graphClient = graphClient;
  }

  @Override
  @Nonnull
  public SessionActorIdentity fetchUserIdentity(
      @Nonnull final OperationContext opContext, @Nonnull final Urn userUrn) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    try {
      final EntityResponse entityResponse =
          _entityClient
              .batchGetV2(
                  opContext, CORP_USER_ENTITY_NAME, Set.of(userUrn), USER_MEMBERSHIP_ASPECTS)
              .get(userUrn);

      if (entityResponse == null || !entityResponse.hasAspects()) {
        return SessionActorIdentity.empty(userUrn);
      }

      final List<Urn> corpGroups = new ArrayList<>();
      if (entityResponse.getAspects().containsKey(GROUP_MEMBERSHIP_ASPECT_NAME)) {
        final GroupMembership groupMembership =
            new GroupMembership(
                entityResponse.getAspects().get(GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
        if (groupMembership.hasGroups()) {
          corpGroups.addAll(groupMembership.getGroups());
        }
      }

      final List<Urn> nativeGroups = new ArrayList<>();
      if (entityResponse.getAspects().containsKey(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)) {
        final NativeGroupMembership nativeGroupMembership =
            new NativeGroupMembership(
                entityResponse
                    .getAspects()
                    .get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)
                    .getValue()
                    .data());
        if (nativeGroupMembership.hasNativeGroups()) {
          nativeGroups.addAll(nativeGroupMembership.getNativeGroups());
        }
      }

      final Set<Urn> directRoles = new HashSet<>();
      if (entityResponse.getAspects().containsKey(ROLE_MEMBERSHIP_ASPECT_NAME)) {
        final RoleMembership roleMembership =
            new RoleMembership(
                entityResponse.getAspects().get(ROLE_MEMBERSHIP_ASPECT_NAME).getValue().data());
        if (roleMembership.hasRoles()) {
          directRoles.addAll(roleMembership.getRoles());
        }
      }

      return new SessionActorIdentity(userUrn, corpGroups, nativeGroups, directRoles);
    } catch (Exception e) {
      log.error("Failed to fetch group membership for urn {}", userUrn, e);
      return SessionActorIdentity.empty(userUrn);
    }
  }

  @Override
  @Nonnull
  public List<Urn> getGroupsForUser(
      @Nonnull OperationContext opContext, @Nonnull final Urn userUrn) {
    if (userUrn.equals(opContext.getSessionActorContext().getActorUrn())) {
      return new ArrayList<>(opContext.getSessionActorContext().getGroupMembership());
    }
    return new ArrayList<>(fetchUserIdentity(opContext, userUrn).getGroups());
  }

  @Override
  @Nonnull
  public Set<Urn> fetchRolesViaGroups(
      @Nonnull final OperationContext opContext, @Nonnull final Collection<Urn> groups) {
    if (groups.isEmpty()) {
      return Collections.emptySet();
    }
    final HashSet<Urn> groupUrns = new HashSet<>(groups);
    try {
      final Map<Urn, EntityResponse> responseMap =
          _entityClient.batchGetV2(
              opContext,
              CORP_GROUP_ENTITY_NAME,
              groupUrns,
              ImmutableSet.of(ROLE_MEMBERSHIP_ASPECT_NAME));

      return responseMap.keySet().stream()
          .filter(Objects::nonNull)
          .filter(key -> responseMap.get(key) != null)
          .filter(key -> responseMap.get(key).hasAspects())
          .map(key -> responseMap.get(key).getAspects())
          .filter(aspectMap -> aspectMap.containsKey(ROLE_MEMBERSHIP_ASPECT_NAME))
          .map(
              aspectMap ->
                  new RoleMembership(aspectMap.get(ROLE_MEMBERSHIP_ASPECT_NAME).getValue().data()))
          .filter(RoleMembership::hasRoles)
          .map(RoleMembership::getRoles)
          .flatMap(List::stream)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      log.error("Failed to fetch {} for urns {}", ROLE_MEMBERSHIP_ASPECT_NAME, groupUrns, e);
      return Collections.emptySet();
    }
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
    addUsersToNativeGroup(opContext, Collections.singletonList(userUrn), groupUrn);
  }

  /**
   * Adds every supplied user to the group in a fixed number of round trips — one existence check,
   * one aspect read and one batched write — rather than three per user.
   */
  public void addUsersToNativeGroup(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> userUrnList,
      @Nonnull final Urn groupUrn) {
    addUsersToNativeGroup(opContext, userUrnList, groupUrn, true);
  }

  /**
   * @param requireUsersExist when true, a user that does not exist fails the whole call before
   *     anything is written. The migration path passes false: its member list comes from graph
   *     edges that can outlive a hard-deleted user, and failing there would leave the group
   *     permanently unable to complete migration.
   */
  private void addUsersToNativeGroup(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> userUrnList,
      @Nonnull final Urn groupUrn,
      final boolean requireUsersExist) {
    Objects.requireNonNull(userUrnList, "userUrnList must not be null");
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    final Set<Urn> userUrns = new LinkedHashSet<>(userUrnList);
    if (userUrns.isEmpty()) {
      return;
    }

    final Set<Urn> existingUsers = _entityService.exists(opContext, userUrns, true);
    if (requireUsersExist && !existingUsers.containsAll(userUrns)) {
      final Set<Urn> missingUsers = new LinkedHashSet<>(userUrns);
      missingUsers.removeAll(existingUsers);
      throw new RuntimeException(
          String.format("Failed to add members to group. Users do not exist: %s", missingUsers));
    }

    final Set<Urn> targetUsers =
        userUrns.stream()
            .filter(existingUsers::contains)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (targetUsers.isEmpty()) {
      log.warn("No existing users to add to group {}; skipping", groupUrn);
      return;
    }

    try {
      final Map<Urn, EntityResponse> entityResponses =
          batchGetUserAspectsNoCache(
              opContext, targetUsers, Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME));

      final List<MetadataChangeProposal> proposals = new ArrayList<>(targetUsers.size());
      for (Urn userUrn : targetUsers) {
        final NativeGroupMembership nativeGroupMembership =
            toNativeGroupMembership(entityResponses.get(userUrn));
        // Handle the duplicate case.
        nativeGroupMembership.getNativeGroups().remove(groupUrn);
        nativeGroupMembership.getNativeGroups().add(groupUrn);
        proposals.add(
            buildSynchronousMetadataChangeProposal(
                userUrn, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, nativeGroupMembership));
      }
      _entityClient.batchIngestProposals(opContext, proposals, false);
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

    final Set<Urn> userUrns = new LinkedHashSet<>(userUrnList);
    if (userUrns.isEmpty()) {
      return;
    }

    // Both aspects grant membership (they are merged in SessionActorIdentity and queried together
    // by the UI), and a group that never finished migrating still has members holding only the
    // legacy one. Stripping only the native aspect would report success while leaving those
    // members in the group with their access intact.
    final Map<Urn, EntityResponse> entityResponses =
        batchGetUserAspectsNoCache(
            opContext,
            userUrns,
            Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, GROUP_MEMBERSHIP_ASPECT_NAME));

    final List<MetadataChangeProposal> proposals = new ArrayList<>();
    for (Urn userUrn : userUrns) {
      final EntityResponse entityResponse = entityResponses.get(userUrn);

      final NativeGroupMembership nativeGroupMembership = toNativeGroupMembership(entityResponse);
      if (nativeGroupMembership.getNativeGroups().remove(groupUrn)) {
        proposals.add(
            buildSynchronousMetadataChangeProposal(
                userUrn, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, nativeGroupMembership));
      }

      final GroupMembership groupMembership = toGroupMembership(entityResponse);
      if (groupMembership.getGroups().remove(groupUrn)) {
        proposals.add(
            buildSynchronousMetadataChangeProposal(
                userUrn, GROUP_MEMBERSHIP_ASPECT_NAME, groupMembership));
      }
    }
    if (!proposals.isEmpty()) {
      _entityClient.batchIngestProposals(opContext, proposals, false);
    }
  }

  public void migrateGroupMembershipToNativeGroupMembership(
      @Nonnull OperationContext opContext, @Nonnull final Urn groupUrn, final String actorUrnStr)
      throws Exception {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    // This runs inline in the Add/RemoveGroupMembers resolvers (required by the DataHub Cloud
    // upgrade path), so it can be interrupted at any point by a request timeout or client
    // disconnect. Step order is what makes an interrupted run recoverable rather than lossy:
    //
    //  1. The member list comes from the graph, which is derived from the GroupMembership aspect
    //     (see the IsMemberOfGroup relationship on GroupMembership.pdl). Removing that aspect
    //     first would destroy the only source a retry could re-read the members from.
    //  2. Both membership writes are idempotent, so granting native membership before revoking
    //     the old one is safe and leaves members reachable by either aspect in between.
    //  3. Origin is written last because a set Origin permanently disables the migration guard in
    //     the resolvers. Until it lands, an interrupted run simply re-migrates on the next call.
    final List<Urn> userUrnList = getExistingGroupMembers(groupUrn, actorUrnStr);
    addUsersToNativeGroup(opContext, userUrnList, groupUrn, false);
    removeExistingGroupMembers(opContext, groupUrn, userUrnList);
    createNativeGroupOrigin(opContext, groupUrn);
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

    return toNativeGroupMembership(entityResponse);
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

    return toGroupMembership(entityResponse);
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

    final MetadataChangeProposal proposal =
        buildSynchronousMetadataChangeProposal(
            Constants.CORP_GROUP_ENTITY_NAME,
            corpGroupKey,
            Constants.CORP_GROUP_INFO_ASPECT_NAME,
            corpGroupInfo);
    return _entityClient.ingestProposal(opContext, proposal);
  }

  void createNativeGroupOrigin(@Nonnull OperationContext opContext, @Nonnull final Urn groupUrn)
      throws Exception {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    // Create the Group info.
    final Origin groupOrigin = new Origin();
    groupOrigin.setType(OriginType.NATIVE);

    final MetadataChangeProposal proposal =
        buildSynchronousMetadataChangeProposal(groupUrn, ORIGIN_ASPECT_NAME, groupOrigin);
    _entityClient.ingestProposal(opContext, proposal);
  }

  List<Urn> getExistingGroupMembers(@Nonnull final Urn groupUrn, final String actorUrnStr) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    final EntityRelationships relationships =
        _graphClient.getRelatedEntities(
            groupUrn.toString(),
            ImmutableSet.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
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

    final Set<Urn> userUrns = new LinkedHashSet<>(userUrnList);
    if (userUrns.isEmpty()) {
      return;
    }

    final Map<Urn, EntityResponse> entityResponses =
        batchGetUserAspectsNoCache(opContext, userUrns, Set.of(GROUP_MEMBERSHIP_ASPECT_NAME));

    final List<MetadataChangeProposal> proposals = new ArrayList<>();
    for (Urn userUrn : userUrns) {
      final GroupMembership groupMembership = toGroupMembership(entityResponses.get(userUrn));
      if (groupMembership.getGroups().remove(groupUrn)) {
        proposals.add(
            buildSynchronousMetadataChangeProposal(
                userUrn, GROUP_MEMBERSHIP_ASPECT_NAME, groupMembership));
      }
    }
    if (!proposals.isEmpty()) {
      _entityClient.batchIngestProposals(opContext, proposals, false);
    }
  }

  private NativeGroupMembership toNativeGroupMembership(@Nullable EntityResponse entityResponse) {
    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)) {
      final NativeGroupMembership nativeGroupMembership = new NativeGroupMembership();
      nativeGroupMembership.setNativeGroups(new UrnArray());
      return nativeGroupMembership;
    }
    return new NativeGroupMembership(
        entityResponse.getAspects().get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
  }

  private GroupMembership toGroupMembership(@Nullable EntityResponse entityResponse) {
    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(GROUP_MEMBERSHIP_ASPECT_NAME)) {
      final GroupMembership groupMembership = new GroupMembership();
      groupMembership.setGroups(new UrnArray());
      return groupMembership;
    }
    return new GroupMembership(
        entityResponse.getAspects().get(GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
  }

  /** Bypasses {@link SystemEntityClient}'s aspect cache for read-modify-write mutations only. */
  private Map<Urn, EntityResponse> batchGetUserAspectsNoCache(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> userUrns,
      @Nonnull Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return _entityClient.batchGetV2NoCache(opContext, CORP_USER_ENTITY_NAME, userUrns, aspectNames);
  }
}
