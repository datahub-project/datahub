package com.datahub.authentication.group;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.buildSynchronousMetadataChangeProposal;

import com.datahub.authorization.SessionActorIdentity;
import com.google.common.annotations.VisibleForTesting;
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
  private static final int GROUP_MEMBER_PAGE_SIZE = 1000;
  private static final int RESTORE_INDICES_BATCH_SIZE = 100;
  private static final String GROUP_MEMBERSHIP_REPAIR_METRIC =
      "auth.group.native_group_membership_edge_repair";
  private static final String GROUP_MEMBERSHIP_REPAIR_FAILED_METRIC =
      "auth.group.native_group_membership_edge_repair_failed";

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
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    addUsersToNativeGroup(opContext, List.of(userUrn), groupUrn);
  }

  public void addUsersToNativeGroup(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> userUrns,
      @Nonnull final Urn groupUrn) {
    Objects.requireNonNull(userUrns, "userUrns must not be null");
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    final Set<Urn> requested = new LinkedHashSet<>(userUrns);
    if (requested.isEmpty()) {
      return;
    }

    final Set<Urn> found = _entityService.exists(opContext, requested, true);
    final Set<Urn> absent =
        requested.stream().filter(urn -> !found.contains(urn)).collect(Collectors.toSet());
    if (!absent.isEmpty()) {
      throw new RuntimeException(
          String.format("Failed to add members to group. Users do not exist: %s", absent));
    }

    try {
      final Map<Urn, EntityResponse> responses =
          batchGetUserAspectsNoCache(
              opContext, requested, Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME));

      final Set<Urn> alreadyMembers = new LinkedHashSet<>();
      for (Urn userUrn : requested) {
        final NativeGroupMembership nativeGroupMembership =
            toNativeGroupMembership(responses.get(userUrn));
        if (nativeGroupMembership.getNativeGroups().contains(groupUrn)) {
          alreadyMembers.add(userUrn);
        } else {
          nativeGroupMembership.getNativeGroups().add(groupUrn);
        }
        // Issued for already-members too: content is unchanged so the MCL is suppressed, but a
        // fresh lastObserved still updates the row, preserving actor/APP_SOURCE provenance.
        final MetadataChangeProposal proposal =
            buildSynchronousMetadataChangeProposal(
                userUrn, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, nativeGroupMembership);
        _entityClient.ingestProposal(opContext, proposal);
      }

      if (!alreadyMembers.isEmpty()) {
        repairMissingNativeGroupEdges(opContext, alreadyMembers, groupUrn);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to add members to group", e);
    }
  }

  /**
   * Rebuilds {@code IsMemberOfNativeGroup} edges for users whose aspect already names the group but
   * whose edge is absent.
   *
   * <p>An UPSERT cannot do this: content-identical writes are suppressed both at MCL emission
   * ({@code EntityServiceImpl#conditionallyProduceMCLAsync}) and, were an MCL emitted, by graph
   * diff mode computing an empty diff. Either gate strands the edge whenever it was reaped
   * independently of the aspect — notably by a group hard-delete, which reaps incoming edges it
   * does not own. {@code restoreIndices} re-emits the aspect with forceIndexing so the graph is
   * rebuilt from aspect content rather than from a diff.
   */
  private void repairMissingNativeGroupEdges(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> alreadyMembers,
      @Nonnull final Urn groupUrn) {
    final Set<Urn> divergent = findMembersMissingEdges(opContext, alreadyMembers, groupUrn);
    if (divergent.isEmpty()) {
      return;
    }

    // INFO, not WARN: graph writes are not immediately visible (bulk flush plus the ~3s ES
    // refresh interval), so this branch is routinely taken for a few seconds after a member is
    // added, well before anything is actually wrong. A WARN that fires in healthy operation
    // trains operators to ignore it.
    log.info(
        "Rebuilding group membership edges: {} user(s) reference group {} in "
            + "nativeGroupMembership without a corresponding {} edge yet. This is expected "
            + "briefly after a member is added, since the graph index refreshes asynchronously.",
        divergent.size(),
        groupUrn,
        IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME);
    // This counter includes the transient post-write cases described above, not just genuine
    // divergence - a nonzero value alone is not proof of corruption.
    opContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                metricUtils.incrementMicrometer(GROUP_MEMBERSHIP_REPAIR_METRIC, divergent.size()));

    try {
      _entityService.restoreIndices(
          opContext,
          divergent,
          Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME),
          RESTORE_INDICES_BATCH_SIZE,
          false);
    } catch (Exception e) {
      // Deliberately not rethrown. The membership aspect — the source of truth for authorization
      // — was written successfully above; only the derived index is still stale, and a retry of
      // this mutation re-attempts the repair. Failing here would report the whole add as failed.
      log.error("Failed to rebuild group membership edges for group {}", groupUrn, e);
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.incrementMicrometer(
                      GROUP_MEMBERSHIP_REPAIR_FAILED_METRIC, divergent.size()));
    }
  }

  private Set<Urn> findMembersMissingEdges(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> alreadyMembers,
      @Nonnull final Urn groupUrn) {
    try {
      final Set<Urn> withEdge =
          new HashSet<>(
              getNativeGroupMembers(
                  groupUrn, opContext.getSessionActorContext().getActorUrn().toString()));
      return alreadyMembers.stream()
          .filter(urn -> !withEdge.contains(urn))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      // Fail toward repair: over-reindexing is correct but costly, whereas under-reindexing
      // leaves the aspect and the graph silently diverged. Bounded by the size of this request.
      log.warn(
          "Failed to read current members of group {}; treating all {} existing member(s) as"
              + " divergent.",
          groupUrn,
          alreadyMembers.size(),
          e);
      return alreadyMembers;
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
      final NativeGroupMembership nativeGroupMembership =
          loadNativeGroupMembershipForUpdate(opContext, userUrn);
      if (nativeGroupMembership.getNativeGroups().remove(groupUrn)) {
        final MetadataChangeProposal proposal =
            buildSynchronousMetadataChangeProposal(
                userUrn, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, nativeGroupMembership);
        _entityClient.ingestProposal(opContext, proposal);
      }
    }
  }

  public void migrateGroupMembershipToNativeGroupMembership(
      @Nonnull OperationContext opContext, @Nonnull final Urn groupUrn, final String actorUrnStr)
      throws Exception {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");

    // Get the existing set of users. This list is graph-derived, so it may still name users who
    // no longer exist in SQL (e.g. a deleted corpuser whose IsMemberOfGroup edge is stale).
    // addUsersToNativeGroup rejects the whole batch if any URN is absent, so those stale URNs
    // must be filtered out before removeExistingGroupMembers commits anything - otherwise one
    // stale edge would empty the group's membership entirely.
    final List<Urn> graphDerivedUserUrnList = getExistingGroupMembers(groupUrn, actorUrnStr);
    final Set<Urn> existingUserUrns =
        _entityService.exists(opContext, new LinkedHashSet<>(graphDerivedUserUrnList), true);
    final List<Urn> userUrnList =
        graphDerivedUserUrnList.stream()
            .filter(existingUserUrns::contains)
            .collect(Collectors.toList());

    final int staleCount = graphDerivedUserUrnList.size() - userUrnList.size();
    if (staleCount > 0) {
      final List<Urn> staleUserUrns =
          graphDerivedUserUrnList.stream()
              .filter(urn -> !existingUserUrns.contains(urn))
              .collect(Collectors.toList());
      log.warn(
          "Dropping {} stale member(s) of group {} during native group migration; referenced by a"
              + " graph edge but no longer present as an entity: {}",
          staleCount,
          groupUrn,
          staleUserUrns);
    }

    // Remove the existing group membership for each (still-existing) user in the group
    removeExistingGroupMembers(opContext, groupUrn, userUrnList);
    // Mark the group as a native group
    createNativeGroupOrigin(opContext, groupUrn);
    // Add each user as a native group member to the group
    addUsersToNativeGroup(opContext, userUrnList, groupUrn);
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

  /**
   * Members whose {@code nativeGroupMembership} has materialized an {@code IsMemberOfNativeGroup}
   * edge to this group. Reads the graph index, so it reflects derived state rather than the
   * authoritative aspect — the two can disagree, which is precisely what callers check for.
   */
  public List<Urn> getNativeGroupMembers(
      @Nonnull final Urn groupUrn, @Nonnull final String actorUrnStr) {
    return getNativeGroupMembers(groupUrn, actorUrnStr, GROUP_MEMBER_PAGE_SIZE);
  }

  @VisibleForTesting
  List<Urn> getNativeGroupMembers(
      @Nonnull final Urn groupUrn, @Nonnull final String actorUrnStr, final int pageSize) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    Objects.requireNonNull(actorUrnStr, "actorUrnStr must not be null");

    final List<Urn> members = new ArrayList<>();
    int start = 0;
    int total = 0;
    do {
      final EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              groupUrn.toString(),
              ImmutableSet.of(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              start,
              pageSize,
              actorUrnStr);
      if (relationships == null || relationships.getRelationships() == null) {
        break;
      }
      relationships
          .getRelationships()
          .forEach(relationship -> members.add(relationship.getEntity()));
      total = relationships.getTotal();
      start += pageSize;
    } while (start < total);

    return members;
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
      final GroupMembership groupMembership = loadGroupMembershipForUpdate(opContext, userUrn);
      if (groupMembership.getGroups().remove(groupUrn)) {
        final MetadataChangeProposal proposal =
            buildSynchronousMetadataChangeProposal(
                userUrn, GROUP_MEMBERSHIP_ASPECT_NAME, groupMembership);
        _entityClient.ingestProposal(opContext, proposal);
      }
    }
  }

  private NativeGroupMembership loadNativeGroupMembershipForUpdate(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn) throws Exception {
    final EntityResponse entityResponse =
        batchGetUserAspectsNoCache(
                opContext,
                Collections.singleton(userUrn),
                Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))
            .get(userUrn);
    return toNativeGroupMembership(entityResponse);
  }

  private GroupMembership loadGroupMembershipForUpdate(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn)
      throws RemoteInvocationException, URISyntaxException {
    final EntityResponse entityResponse =
        batchGetUserAspectsNoCache(
                opContext, Collections.singleton(userUrn), Set.of(GROUP_MEMBERSHIP_ASPECT_NAME))
            .get(userUrn);
    return toGroupMembership(entityResponse);
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
