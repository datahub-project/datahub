package com.datahub.authentication.group;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.buildMetadataChangeProposal;
import static com.linkedin.metadata.entity.AspectUtils.buildSynchronousMetadataChangeProposal;

import com.datahub.authorization.SessionActorIdentity;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
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
import java.util.LinkedHashMap;
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
  private static final String GROUP_MEMBER_SCROLL_KEEP_ALIVE = "5m";
  // Members are looked up in chunks so the edge query stays a bounded terms lookup regardless of
  // how many users a single request names.
  private static final int MEMBER_EDGE_LOOKUP_CHUNK_SIZE = 500;
  private static final int RESTORE_INDICES_BATCH_SIZE = 100;
  private static final int MEMBERSHIP_CLEANUP_BATCH_SIZE = 100;
  private static final String GROUP_MEMBERSHIP_REPAIR_METRIC =
      "auth.group.native_group_membership_edge_repair";
  private static final String GROUP_MEMBERSHIP_REPAIR_FAILED_METRIC =
      "auth.group.native_group_membership_edge_repair_failed";
  private static final String MEMBERSHIP_CLEANUP_FAILED_METRIC =
      "auth.group.native_group_membership_cleanup_failed";

  private final SystemEntityClient _entityClient;
  private final EntityService<?> _entityService;
  private final GraphClient _graphClient;
  private final GraphService _graphService;

  public GroupService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull EntityService<?> entityService,
      @Nonnull GraphClient graphClient,
      @Nonnull GraphService graphService) {
    Objects.requireNonNull(entityClient, "entityClient must not be null!");
    Objects.requireNonNull(entityService, "entityService must not be null!");
    Objects.requireNonNull(graphClient, "graphClient must not be null!");
    Objects.requireNonNull(graphService, "graphService must not be null!");

    _entityClient = entityClient;
    _entityService = entityService;
    _graphClient = graphClient;
    _graphService = graphService;
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
      final List<MetadataChangeProposal> proposals = new ArrayList<>(requested.size());
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
        proposals.add(
            buildSynchronousMetadataChangeProposal(
                userUrn, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, nativeGroupMembership));
      }
      // One batched call rather than one write per member. The client still partitions it at
      // entityClient.restli.ingest.batchSize, so a large add spans several transactions rather
      // than one. These stay synchronous proposals - APP_SOURCE=ui keeps UpdateIndicesService
      // inline - because a caller adding members reads the relationship index straight afterwards
      // and must not be told the members are absent; that read is what the Terraform provider
      // polls.
      _entityClient.batchIngestProposals(opContext, proposals, false);

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
   *
   * <p>That re-emission is asynchronous - a RESTATE MCL over Kafka - so unlike the write it
   * follows, the rebuilt edge only appears once the consumer catches up. A caller polling for the
   * member list has to tolerate that lag on this path.
   */
  private void repairMissingNativeGroupEdges(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> alreadyMembers,
      @Nonnull final Urn groupUrn) {
    final Set<Urn> divergent = findMembersMissingEdges(opContext, alreadyMembers, groupUrn);
    if (divergent.isEmpty()) {
      return;
    }

    // INFO, not WARN: graph writes are not immediately visible, so this branch is routinely
    // taken for a few seconds after a member is added, well before anything is actually wrong.
    log.info(
        "Rebuilding group membership edges: {} user(s) reference group {} in "
            + "nativeGroupMembership without a corresponding {} edge yet. This is expected "
            + "briefly after a member is added, since the graph index refreshes asynchronously.",
        divergent.size(),
        groupUrn,
        IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME);
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
      final Set<Urn> withEdge = findMembersWithEdges(opContext, alreadyMembers, groupUrn);
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

  /**
   * Strips {@code groupUrn} from each listed user's {@code nativeGroupMembership}.
   *
   * <p>Best-effort by contract: one aspect read plus one synchronously indexed write per user,
   * sequentially, with no batching, retry, or durable record of what remains. A caller that runs
   * this outside the request thread — {@code RemoveGroupResolver} does, after a group delete — can
   * lose the remainder of the list to a GMS restart mid-loop. The backstop is the repair in {@link
   * #addUsersToNativeGroup}: a member left with a stale reference has it cleaned up the next time
   * they are added to a group with that urn. Making the sweep resumable is worthwhile but out of
   * scope here.
   *
   * <p>This is the explicit "remove these members" path, which applies the list unconditionally.
   * Cleanup after a group delete goes through {@link #removeStaleNativeGroupMembership} instead,
   * because there the list predates the delete and may since have been legitimately restored.
   */
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

  /**
   * Strips {@code groupUrn} from the {@code nativeGroupMembership} of every captured member.
   *
   * <p>The captured list predates the delete, so a member re-added to a group recreated under the
   * same id in the window between the two loses that add. That is deliberate. The obvious guard —
   * {@code If-Unmodified-Since: deletedAtMs} on each write — cannot actually catch it: a re-add
   * arriving while the aspect still names the group writes byte-identical content, which does not
   * advance the audit stamp the precondition reads, so the write is not rejected. All such a guard
   * ever blocks is an unrelated write to the same aspect, leaving a stale reference exactly where
   * it claimed to protect a membership.
   *
   * <p>Left uncovered, that race costs an admin one repeated add, and it errs toward less access.
   * The alternative — skipping cleanup whenever the group exists again — abandons every member
   * nobody re-added, and because authorization reads this aspect rather than the graph, each of
   * them keeps the recreated group's privileges while appearing in no member list at all.
   *
   * <p>Best-effort by contract, like {@link #removeExistingNativeGroupMembers}: the caller has
   * already reported the delete itself as successful, so no failure here is surfaced. Unlike that
   * method, aspect reads and writes are batched, and one failing batch does not abandon the rest of
   * the captured list.
   */
  public void removeStaleNativeGroupMembership(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final List<Urn> capturedMembers) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    Objects.requireNonNull(capturedMembers, "capturedMembers must not be null");

    final List<Urn> deduped = new ArrayList<>(new LinkedHashSet<>(capturedMembers));
    for (List<Urn> batch : Lists.partition(deduped, MEMBERSHIP_CLEANUP_BATCH_SIZE)) {
      removeStaleGroupReferences(opContext, groupUrn, batch);
    }
  }

  /** One aspect read for the whole batch, then one write covering everyone still referencing it. */
  private void removeStaleGroupReferences(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final List<Urn> batch) {
    final Map<Urn, MetadataChangeProposal> proposals =
        buildStaleReferenceProposals(opContext, groupUrn, batch);
    if (proposals == null || proposals.isEmpty()) {
      return;
    }

    try {
      _entityClient.batchIngestProposals(opContext, proposals.values(), false);
    } catch (Exception e) {
      // Nothing here is conditional, so a rejection means storage, not contention — replaying the
      // batch member by member would only hit the same failure. The references are left for the
      // next add to this urn to repair.
      log.error(
          "Failed to clear references to deleted group {} from {} member(s); leaving them in"
              + " place.",
          groupUrn,
          proposals.size(),
          e);
      incrementCleanupMetric(opContext, MEMBERSHIP_CLEANUP_FAILED_METRIC, proposals.size());
    }
  }

  /**
   * @return the write to issue per member that still references the group, or null if the batch
   *     could not be read at all
   */
  @Nullable
  private Map<Urn, MetadataChangeProposal> buildStaleReferenceProposals(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final List<Urn> batch) {
    final Map<Urn, EntityResponse> responses;
    try {
      responses =
          batchGetUserAspectsNoCache(
              opContext, new LinkedHashSet<>(batch), Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME));
    } catch (Exception e) {
      log.error(
          "Failed to read nativeGroupMembership for {} member(s) of deleted group {}; leaving this"
              + " batch untouched.",
          batch.size(),
          groupUrn,
          e);
      incrementCleanupMetric(opContext, MEMBERSHIP_CLEANUP_FAILED_METRIC, batch.size());
      return null;
    }

    final Map<Urn, MetadataChangeProposal> proposals = new LinkedHashMap<>();
    for (Urn userUrn : batch) {
      final NativeGroupMembership nativeGroupMembership =
          toNativeGroupMembership(responses.get(userUrn));
      if (!nativeGroupMembership.getNativeGroups().remove(groupUrn)) {
        continue;
      }
      proposals.put(
          userUrn,
          buildMetadataChangeProposal(
              userUrn, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, nativeGroupMembership));
    }
    return proposals;
  }

  private static void incrementCleanupMetric(
      @Nonnull OperationContext opContext, @Nonnull final String metricName, final int count) {
    if (count == 0) {
      return;
    }
    opContext
        .getMetricUtils()
        .ifPresent(metricUtils -> metricUtils.incrementMicrometer(metricName, count));
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
   *
   * <p>Scrolls with search_after rather than from/size offsets. A group's membership is unbounded,
   * and offset paging is rejected once {@code from + size} passes {@code index.max_result_window}
   * (10k by default) — which fails the read outright rather than merely truncating it, so a caller
   * treating failure as "no members" would silently lose the whole list for exactly the largest
   * groups. {@code deleteEntityReferences} scrolls the same edges for the same reason.
   */
  public List<Urn> getNativeGroupMembers(
      @Nonnull OperationContext opContext, @Nonnull final Urn groupUrn) {
    return getNativeGroupMembers(opContext, groupUrn, GROUP_MEMBER_PAGE_SIZE);
  }

  @VisibleForTesting
  List<Urn> getNativeGroupMembers(
      @Nonnull OperationContext opContext, @Nonnull final Urn groupUrn, final int pageSize) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    return scrollNativeGroupMemberEdges(opContext, groupUrn, null, pageSize);
  }

  /**
   * Members of {@code groupUrn} drawn from {@code candidates} that have an {@code
   * IsMemberOfNativeGroup} edge. Both endpoints are filtered in the query, so the cost tracks the
   * number of candidates rather than the size of the group.
   */
  private Set<Urn> findMembersWithEdges(
      @Nonnull OperationContext opContext,
      @Nonnull final Collection<Urn> candidates,
      @Nonnull final Urn groupUrn) {
    final List<Urn> candidateList = new ArrayList<>(candidates);
    final Set<Urn> withEdge = new HashSet<>();
    for (int i = 0; i < candidateList.size(); i += MEMBER_EDGE_LOOKUP_CHUNK_SIZE) {
      final List<Urn> chunk =
          candidateList.subList(
              i, Math.min(i + MEMBER_EDGE_LOOKUP_CHUNK_SIZE, candidateList.size()));
      withEdge.addAll(
          scrollNativeGroupMemberEdges(opContext, groupUrn, chunk, MEMBER_EDGE_LOOKUP_CHUNK_SIZE));
    }
    return withEdge;
  }

  /**
   * @param candidates when non-null, restricts the far side of the edge to these users; when null,
   *     every member of the group is returned
   */
  private List<Urn> scrollNativeGroupMemberEdges(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nullable final Collection<Urn> candidates,
      final int pageSize) {
    final Filter memberFilter;
    if (candidates == null) {
      memberFilter = QueryUtils.EMPTY_FILTER;
    } else {
      final Criterion criterion =
          QueryUtils.newCriterion(
              "urn", candidates.stream().map(Urn::toString).collect(Collectors.toList()));
      if (criterion == null) {
        return List.of();
      }
      memberFilter = QueryUtils.newFilter(criterion);
    }

    final GraphFilters graphFilters =
        new GraphFilters(
            QueryUtils.newFilter("urn", groupUrn.toString()),
            memberFilter,
            null,
            null,
            Set.of(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME),
            QueryUtils.newRelationshipFilter(
                QueryUtils.EMPTY_FILTER, RelationshipDirection.INCOMING));

    final List<Urn> members = new ArrayList<>();
    String scrollId = null;
    do {
      final RelatedEntitiesScrollResult result =
          _graphService.scrollRelatedEntities(
              opContext,
              graphFilters,
              Edge.EDGE_SORT_CRITERION,
              scrollId,
              GROUP_MEMBER_SCROLL_KEEP_ALIVE,
              pageSize,
              null,
              null);
      // An empty page also terminates the loop: with search_after there is nothing beyond it, and
      // trusting the scrollId alone would spin forever on a backend that always returns one.
      if (result == null || result.getEntities() == null || result.getEntities().isEmpty()) {
        break;
      }
      // Direction is INCOMING, so the related urn is the member rather than the group.
      result.getEntities().stream()
          .map(RelatedEntities::getUrn)
          .map(UrnUtils::getUrn)
          .forEach(members::add);
      scrollId = result.getScrollId();
    } while (scrollId != null);

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
