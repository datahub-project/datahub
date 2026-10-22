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
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.validation.ConditionalWriteValidator;
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
import io.datahubproject.metadata.context.ReadPreference;
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
  // Legacy IsMemberOfGroup edges come from GraphClient, which offers offset paging only.
  private static final int LEGACY_GROUP_MEMBER_PAGE_SIZE = 500;
  // Elasticsearch caps both max_result_window and exact hit counts at 10k by default.
  private static final int MAX_GROUP_MEMBERS_TO_MIGRATE = 10_000;
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

  /** Used when conditional membership cleanup is off, where the limit does not apply. */
  private static final int DEFAULT_CONDITIONAL_WRITE_LIMIT = 3;

  private final SystemEntityClient _entityClient;
  private final EntityService<?> _entityService;
  private final GraphClient _graphClient;
  private final GraphService _graphService;
  private final boolean _conditionalMembershipCleanup;
  private final int _conditionalWriteLimit;

  public GroupService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull EntityService<?> entityService,
      @Nonnull GraphClient graphClient,
      @Nonnull GraphService graphService) {
    this(
        entityClient,
        entityService,
        graphClient,
        graphService,
        false,
        DEFAULT_CONDITIONAL_WRITE_LIMIT);
  }

  /**
   * @param conditionalMembershipCleanup when true, {@link #removeStaleNativeGroupMembership} writes
   *     each member's {@code nativeGroupMembership} conditioned on the version read. If the aspect
   *     moved meanwhile, it is read again and the group removed from the current value, up to
   *     {@code conditionalWriteLimit} writes, so a group added meanwhile is kept
   * @param conditionalWriteLimit writes of one member's cleanup, the first included, before its
   *     failure is counted; at least 1
   */
  public GroupService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull EntityService<?> entityService,
      @Nonnull GraphClient graphClient,
      @Nonnull GraphService graphService,
      final boolean conditionalMembershipCleanup,
      final int conditionalWriteLimit) {
    Objects.requireNonNull(entityClient, "entityClient must not be null!");
    Objects.requireNonNull(entityService, "entityService must not be null!");
    Objects.requireNonNull(graphClient, "graphClient must not be null!");
    Objects.requireNonNull(graphService, "graphService must not be null!");
    if (conditionalWriteLimit < 1) {
      throw new IllegalArgumentException(
          "conditionalWriteLimit must be at least 1, got " + conditionalWriteLimit);
    }

    _entityClient = entityClient;
    _entityService = entityService;
    _graphClient = graphClient;
    _graphService = graphService;
    _conditionalMembershipCleanup = conditionalMembershipCleanup;
    _conditionalWriteLimit = conditionalWriteLimit;
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
   * Revokes membership held through <em>either</em> aspect. Members of an unmigrated group hold
   * only {@code groupMembership}, so stripping just {@code nativeGroupMembership} would report
   * success and leave them in the group.
   *
   * <p>Best-effort: one batched read and one batched synchronously indexed write, with no retry.
   * The entity client partitions a large batch, so a failure part-way through leaves earlier
   * members removed. The backstop is the repair in {@link #addUsersToNativeGroup}.
   *
   * <p>This is the explicit "remove these members" path, which applies the list unconditionally.
   * Cleanup after a group delete goes through {@link #removeStaleNativeGroupMembership} instead,
   * because there the list predates the delete and may since have been legitimately restored.
   * {@link #removeExistingGroupMembers} is migration-internal and revokes only the legacy aspect.
   */
  public void removeGroupMembers(
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
   * <p>With conditional membership cleanup on, the first write is one batch of per-member
   * proposals, each with {@code If-Version-Match}. An aspect changed since it was read, such as by
   * another group being added, is read again and the group removed from what is there now, so that
   * change is kept. A byte-identical re-add does not change the aspect, so the race above is not
   * caught this way either.
   *
   * <p>Best-effort by contract, like {@link #removeGroupMembers}: the caller has already reported
   * the delete itself as successful, so no failure here is surfaced. Unlike that method, the
   * captured list is partitioned into fixed-size batches and one failing batch does not abandon the
   * rest.
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

  /**
   * One aspect read for the whole batch. Unconditionally, that is followed by one write covering
   * everyone still referencing the group. With conditional cleanup on, it is instead one batch of
   * per-member conditional writes (see {@link #removeStaleGroupReferencesIfUnchanged}).
   */
  private void removeStaleGroupReferences(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final List<Urn> batch) {
    if (_conditionalMembershipCleanup) {
      removeStaleGroupReferencesIfUnchanged(opContext, groupUrn, batch);
      return;
    }
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
    final Map<Urn, EntityResponse> responses =
        readNativeGroupMemberships(opContext, groupUrn, batch);
    if (responses == null) {
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

  /**
   * @return the batch's aspects, or null if they could not be read (logged and counted)
   */
  @Nullable
  private Map<Urn, EntityResponse> readNativeGroupMemberships(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final List<Urn> batch) {
    try {
      // A lagging replica can hide the deleted group from a member, so the conditional cleanup
      // would skip them; the per-member re-read is pinned to primary for the same reason.
      return batchGetUserAspectsNoCache(
          _conditionalMembershipCleanup
              ? opContext.withReadPreference(ReadPreference.PRIMARY)
              : opContext,
          new LinkedHashSet<>(batch),
          Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME));
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
  }

  /**
   * One aspect read for the whole batch, then one {@code batchIngestProposals} of per-member
   * conditional writes, each with its own version. The entity client splits the batch into chunks
   * of {@code batchIngestSize}, so a thrown batch may follow chunks that committed; every member of
   * it is re-read from primary, as is a failed slot. A member already without the group is done. A
   * moved aspect is retried one at a time, counting the batch as write 1. An unmoved member of a
   * thrown multi-member batch was not written, so it is retried alone.
   */
  private void removeStaleGroupReferencesIfUnchanged(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final List<Urn> batch) {
    final Map<Urn, EntityResponse> responses =
        readNativeGroupMemberships(opContext, groupUrn, batch);
    if (responses == null) {
      return;
    }

    final Map<Urn, MetadataChangeProposal> proposals = new LinkedHashMap<>();
    final Map<Urn, Long> versions = new LinkedHashMap<>();
    for (Urn userUrn : batch) {
      final EnvelopedAspect aspect = nativeGroupMembershipAspect(responses.get(userUrn));
      final NativeGroupMembership remaining = withoutGroup(aspect, groupUrn);
      if (remaining == null) {
        continue;
      }
      final long version = ConditionalWriteValidator.resolveAspectVersion(aspect);
      final MetadataChangeProposal proposal =
          buildMetadataChangeProposal(userUrn, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, remaining);
      proposal.setHeaders(
          new StringMap(
              Map.of(
                  ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH,
                  String.valueOf(version))));
      proposals.put(userUrn, proposal);
      versions.put(userUrn, version);
    }
    if (proposals.isEmpty()) {
      return;
    }

    final List<Urn> proposalOrder = new ArrayList<>(proposals.keySet());
    final List<Urn> failedSlots = new ArrayList<>();
    Exception batchRejection = null;
    try {
      final List<String> results =
          _entityClient.batchIngestProposals(opContext, new ArrayList<>(proposals.values()), false);
      for (int i = 0; i < proposalOrder.size(); i++) {
        final String result = i < results.size() ? results.get(i) : null;
        if (result == null) {
          failedSlots.add(proposalOrder.get(i));
        }
      }
    } catch (Exception e) {
      // Chunks of batchIngestSize before the one that threw may have committed; the per-member
      // re-read below tells which.
      batchRejection = e;
      failedSlots.addAll(proposalOrder);
    }

    int failed = 0;
    final boolean thrownBatch = batchRejection != null;
    final int batchSize = proposalOrder.size();
    final Map<Urn, EnvelopedAspect> latestByMember = new LinkedHashMap<>();
    if (!failedSlots.isEmpty()) {
      try {
        final Map<Urn, EntityResponse> latestResponses =
            batchGetUserAspectsNoCache(
                opContext.withReadPreference(ReadPreference.PRIMARY),
                new LinkedHashSet<>(failedSlots),
                Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME));
        for (Urn userUrn : failedSlots) {
          latestByMember.put(userUrn, nativeGroupMembershipAspect(latestResponses.get(userUrn)));
        }
      } catch (Exception e) {
        if (batchRejection != null) {
          e.addSuppressed(batchRejection);
        }
        log.error(
            "Failed to re-read nativeGroupMembership for {} member(s) of deleted group {}; leaving"
                + " them in place.",
            failedSlots.size(),
            groupUrn,
            e);
        failed += failedSlots.size();
      }
    }
    for (Urn userUrn : failedSlots) {
      if (!latestByMember.containsKey(userUrn)) {
        continue;
      }
      boolean cleared;
      try {
        cleared =
            settleMemberAfterBatch(
                opContext,
                groupUrn,
                userUrn,
                latestByMember.get(userUrn),
                versions.get(userUrn),
                thrownBatch,
                batchSize,
                batchRejection);
      } catch (Exception e) {
        if (batchRejection != null) {
          e.addSuppressed(batchRejection);
        }
        log.error(
            "Failed to clear the reference to deleted group {} from {}; leaving it in place.",
            groupUrn,
            userUrn,
            e);
        cleared = false;
      }
      if (!cleared) {
        failed++;
      }
    }
    incrementCleanupMetric(opContext, MEMBERSHIP_CLEANUP_FAILED_METRIC, failed);
  }

  /**
   * After the batch attempt: the group already gone is success. A moved aspect is a conflict and
   * the batch counts as write 1. An unmoved aspect on a one-member batch is a storage failure and
   * is not written again. An unmoved member of a thrown multi-member batch was not written (its
   * chunk did not commit), so it is retried alone from write 1.
   */
  private boolean settleMemberAfterBatch(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final Urn userUrn,
      @Nullable final EnvelopedAspect latest,
      final long sentVersion,
      final boolean thrownBatch,
      final int batchSize,
      @Nullable final Exception batchRejection)
      throws Exception {
    if (withoutGroup(latest, groupUrn) == null) {
      return true;
    }
    if (ConditionalWriteValidator.resolveAspectVersion(latest) == sentVersion) {
      if (thrownBatch && batchSize > 1) {
        return removeGroupIfUnchanged(opContext, groupUrn, userUrn, latest, 0);
      }
      log.error(
          "Failed to clear the reference to deleted group {} from {} after {} write(s); leaving"
              + " it in place.",
          groupUrn,
          userUrn,
          1,
          batchRejection);
      return false;
    }
    return removeGroupIfUnchanged(opContext, groupUrn, userUrn, latest, 1);
  }

  /**
   * Removes {@code groupUrn} from one member's {@code nativeGroupMembership} only if the aspect is
   * still at the version read. When it has moved on, it is read again and the group removed from
   * what is there now, up to the configured number of writes in all. {@code writesAlreadyDone}
   * counts writes already issued for this member, including a batch attempt.
   *
   * @return false when the reference was left in place (logged)
   */
  private boolean removeGroupIfUnchanged(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final Urn userUrn,
      @Nullable final EnvelopedAspect readAspect,
      final int writesAlreadyDone)
      throws Exception {
    EnvelopedAspect current = readAspect;
    for (int writes = writesAlreadyDone + 1; ; writes++) {
      final NativeGroupMembership remaining = withoutGroup(current, groupUrn);
      if (remaining == null) {
        return true;
      }
      final long version = ConditionalWriteValidator.resolveAspectVersion(current);
      final MetadataChangeProposal proposal =
          buildMetadataChangeProposal(userUrn, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME, remaining);
      proposal.setHeaders(
          new StringMap(
              Map.of(
                  ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH,
                  String.valueOf(version))));
      Exception rejection = null;
      try {
        if (_entityClient.ingestProposal(opContext, proposal, false) != null) {
          return true;
        }
      } catch (Exception e) {
        // A version mismatch is thrown inside a request and dropped outside one; the read below
        // tells it from any other failure.
        rejection = e;
      }

      final EnvelopedAspect latest;
      try {
        latest =
            nativeGroupMembershipAspect(
                batchGetUserAspectsNoCache(
                        opContext.withReadPreference(ReadPreference.PRIMARY),
                        Set.of(userUrn),
                        Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))
                    .get(userUrn));
      } catch (Exception e) {
        if (rejection != null) {
          e.addSuppressed(rejection);
        }
        throw e;
      }
      if (withoutGroup(latest, groupUrn) == null) {
        return true;
      }
      if (ConditionalWriteValidator.resolveAspectVersion(latest) == version
          || writes >= _conditionalWriteLimit) {
        log.error(
            "Failed to clear the reference to deleted group {} from {} after {} write(s); leaving"
                + " it in place.",
            groupUrn,
            userUrn,
            writes,
            rejection);
        return false;
      }
      current = latest;
    }
  }

  /**
   * @return the membership without {@code groupUrn}, or null when the aspect is absent or does not
   *     name the group
   */
  @Nullable
  private static NativeGroupMembership withoutGroup(
      @Nullable final EnvelopedAspect aspect, @Nonnull final Urn groupUrn) {
    if (aspect == null) {
      return null;
    }
    final List<Urn> groups = new NativeGroupMembership(aspect.getValue().data()).getNativeGroups();
    if (!groups.contains(groupUrn)) {
      return null;
    }
    final List<Urn> remaining =
        groups.stream().filter(group -> !group.equals(groupUrn)).collect(Collectors.toList());
    return new NativeGroupMembership().setNativeGroups(new UrnArray(remaining));
  }

  @Nullable
  private static EnvelopedAspect nativeGroupMembershipAspect(
      @Nullable final EntityResponse entityResponse) {
    if (entityResponse == null || !entityResponse.hasAspects()) {
      return null;
    }
    return entityResponse.getAspects().get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME);
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

    // Runs inline in the resolvers, so a timeout or disconnect can interrupt it. Step order keeps
    // that recoverable: the member list comes from graph edges derived from GroupMembership, so
    // grant native membership before revoking it, and write Origin last - a set Origin disables
    // the resolvers' retry guard for good.
    //
    // Graph edges outlive a hard-deleted user, and addUsersToNativeGroup rejects the whole batch
    // if any URN is absent, so filter stale URNs out before committing anything.
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

    addUsersToNativeGroup(opContext, userUrnList, groupUrn);
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

    final List<Urn> memberUrns = new ArrayList<>();
    int start = 0;
    // Guard on the end of the window, not its start: Elasticsearch rejects a request whose
    // from + size exceeds max_result_window outright, so a page size that does not divide the
    // ceiling would fail the migration instead of truncating it.
    while (start + LEGACY_GROUP_MEMBER_PAGE_SIZE <= MAX_GROUP_MEMBERS_TO_MIGRATE) {
      final EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              groupUrn.toString(),
              ImmutableSet.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              start,
              LEGACY_GROUP_MEMBER_PAGE_SIZE,
              actorUrnStr);

      final List<Urn> page =
          relationships.getRelationships().stream()
              .map(EntityRelationship::getEntity)
              .collect(Collectors.toList());
      memberUrns.addAll(page);

      // A short page means the graph is genuinely exhausted.
      if (page.size() < LEGACY_GROUP_MEMBER_PAGE_SIZE) {
        return memberUrns;
      }
      // getTotal() saturates at the same 10k cap, so it only proves completeness below that cap.
      final int total = relationships.getTotal();
      if (total < MAX_GROUP_MEMBERS_TO_MIGRATE && memberUrns.size() >= total) {
        return memberUrns;
      }
      start += page.size();
    }

    // Members past the ceiling keep a legacy aspect that still grants and can still be revoked,
    // so warn rather than fail the migration.
    log.warn(
        "Group {} has at least {} members; migrating only the first {}. Remaining members keep the"
            + " legacy groupMembership aspect.",
        groupUrn,
        MAX_GROUP_MEMBERS_TO_MIGRATE,
        memberUrns.size());
    return memberUrns;
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
