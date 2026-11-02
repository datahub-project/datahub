package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.ASSERTION_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Authorization helpers for assertion writes. Assertions are scoped to the entity they assert on
 * (the "assertee", e.g. a dataset): managing an assertion requires {@code EDIT_ENTITY_ASSERTIONS}
 * (or {@code EDIT_ENTITY}) on the assertee, matching the GraphQL assertion mutations.
 *
 * <p>{@link EntityAuthorizationUtils} routes assertion URNs here; this class owns the
 * assertion-specific rules: resolving asserted entities, reading a request's assertion writes, and
 * the asserted-entity privilege checks.
 */
@Slf4j
public final class AssertionAuthorizationUtils {

  private static final DisjunctivePrivilegeGroup EDIT_ENTITY_ASSERTIONS_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          ImmutableList.of(
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType())),
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_ASSERTIONS_PRIVILEGE.getType()))));

  private static final String ASSERTS_RELATIONSHIP_NAME = "Asserts";

  private AssertionAuthorizationUtils() {}

  public static boolean isAssertionEntity(@Nonnull final Urn urn) {
    return ASSERTION_ENTITY_NAME.equals(urn.getEntityType());
  }

  /**
   * Assets {@code info} attaches the assertion to: every field indexed as an {@code Asserts}
   * relationship, whichever sub-record {@code type} selects, plus {@code entityUrn}, which is
   * client-writable and drives assertion search. These are the edges that list the assertion on an
   * asset. Schema fields map to their parent asset, where assertion privileges are granted.
   */
  @Nonnull
  public static Set<Urn> resolveAssertedEntityUrns(
      @Nonnull final EntityRegistry entityRegistry, @Nullable final AssertionInfo info) {
    if (info == null) {
      return Set.of();
    }
    final List<RelationshipFieldSpec> assertsFields =
        entityRegistry
            .getEntitySpec(ASSERTION_ENTITY_NAME)
            .getAspectSpec(ASSERTION_INFO_ASPECT_NAME)
            .getRelationshipFieldSpecs()
            .stream()
            .filter(spec -> ASSERTS_RELATIONSHIP_NAME.equals(spec.getRelationshipName()))
            .toList();
    final Set<Urn> urns = new HashSet<>();
    FieldExtractor.extractFields(info, assertsFields).values().stream()
        .flatMap(List::stream)
        .map(AssertionAuthorizationUtils::toUrn)
        .filter(Objects::nonNull)
        .map(AssertionAuthorizationUtils::toAsset)
        .forEach(urns::add);
    if (info.hasEntityUrn()) {
      urns.add(toAsset(info.getEntityUrn()));
    }
    return urns;
  }

  /**
   * {@code assertionInfo} after {@code write}, given the value before it, as the write path applies
   * it: a PATCH applies to {@code current}, a DELETE leaves none, and an upsert that cannot be read
   * leaves {@code current} (ingestion rejects it). Throws when a PATCH cannot be read or applied.
   */
  @Nullable
  public static AssertionInfo applyInfoWrite(
      @Nonnull final AssertionWrite write,
      @Nullable final AssertionInfo current,
      @Nonnull final AspectRetriever aspectRetriever) {
    if (ChangeType.DELETE.equals(write.changeType())) {
      return null;
    }
    if (ChangeType.PATCH.equals(write.changeType())) {
      if (write.infoPatch() == null) {
        throw new IllegalArgumentException("Unreadable assertionInfo PATCH for " + write.urn());
      }
      return PatchItemImpl.resolveProposedAspect(
          write.infoPatch(), current, aspectRetriever, AssertionInfo.class);
    }
    return write.info() != null ? write.info() : current;
  }

  /** Values in graph relationship fields are stored as strings; skip any that are not URNs. */
  @Nullable
  private static Urn toUrn(@Nullable final Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Urn urn) {
      return urn;
    }
    try {
      return Urn.createFromString(value.toString());
    } catch (URISyntaxException e) {
      // Not indexed as a graph edge either; URN validation rejects the write.
      log.debug("Ignoring non-URN Asserts value {}", value);
      return null;
    }
  }

  @Nonnull
  private static Urn toAsset(@Nonnull final Urn urn) {
    return SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())
        ? UrnUtils.getUrn(urn.getEntityKey().get(0))
        : urn;
  }

  /**
   * Returns true when {@code entityUrns} is non-empty and the actor holds {@code
   * EDIT_ENTITY_ASSERTIONS} or {@code EDIT_ENTITY} on every one of them.
   */
  public static boolean isAuthorizedToEditAssertionsOnEntities(
      @Nonnull final AuthorizationSession session, @Nonnull final Collection<Urn> entityUrns) {
    if (entityUrns.isEmpty()) {
      return false;
    }
    return entityUrns.stream()
        .allMatch(
            urn ->
                AuthUtil.isAuthorized(
                    session,
                    EDIT_ENTITY_ASSERTIONS_PRIVILEGES,
                    new EntitySpec(urn.getEntityType(), urn.toString())));
  }

  /** Loads the persisted {@code assertionInfo} for each assertion URN that has one. */
  @Nonnull
  public static Map<Urn, AssertionInfo> fetchAssertionInfos(
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final AspectRetriever aspectRetriever,
      @Nonnull final Set<Urn> assertionUrns) {
    if (assertionUrns.isEmpty()) {
      return Map.of();
    }
    final Map<Urn, Map<String, Aspect>> aspects =
        aspectRetriever.getLatestAspectObjects(
            operationContext, assertionUrns, Set.of(ASSERTION_INFO_ASPECT_NAME));
    final Map<Urn, AssertionInfo> result = new HashMap<>();
    aspects.forEach(
        (urn, byName) -> {
          final Aspect aspect = byName == null ? null : byName.get(ASSERTION_INFO_ASPECT_NAME);
          if (aspect != null) {
            result.put(urn, RecordUtils.toRecordTemplate(AssertionInfo.class, aspect.data()));
          }
        });
    return result;
  }

  /**
   * Returns the subset of {@code assertionUrns} the actor may write through the asserted entity:
   * {@code EDIT_ENTITY_ASSERTIONS} / {@code EDIT_ENTITY} on every entity targeted by the persisted
   * {@code assertionInfo}, by each value the request's {@code assertionInfo} writes produce when
   * applied in request order (so re-targeting needs both sides), and by the request's run events.
   *
   * <p>An assertion with no target yet is included only when the request writes nothing but run
   * events and the actor may edit assertions on every entity they name: clients such as dbt and
   * Great Expectations can send results before (or, in async emit modes, ahead of the persisting
   * of) {@code assertionInfo}. Other aspects, such as {@code status}, need {@code assertionInfo}
   * first or in the same request. {@code AssertionAuthorizationValidator} checks that a run event
   * names the assertion's target once it has one.
   */
  @Nonnull
  static Set<Urn> filterAuthorizedViaAssertedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> assertionUrns,
      @Nonnull final AssertionWrites writes) {
    final AspectRetriever aspectRetriever = opContext.getAspectRetriever();
    final EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
    final Map<Urn, AssertionInfo> persisted =
        fetchAssertionInfos(opContext, aspectRetriever, assertionUrns);
    return assertionUrns.stream()
        .filter(
            urn -> {
              AssertionInfo current = persisted.get(urn);
              final Set<Urn> targets =
                  new HashSet<>(resolveAssertedEntityUrns(entityRegistry, current));
              for (AssertionWrite write : writes.infoWrites().getOrDefault(urn, List.of())) {
                try {
                  current = applyInfoWrite(write, current, aspectRetriever);
                } catch (RuntimeException e) {
                  log.debug("Unable to apply assertionInfo PATCH for {}", urn, e);
                  return false;
                }
                targets.addAll(resolveAssertedEntityUrns(entityRegistry, current));
              }
              final Set<Urn> assertees = writes.runEventAssertees().getOrDefault(urn, Set.of());
              if (targets.isEmpty()) {
                return writes.runEventOnly().contains(urn)
                    && isAuthorizedToEditAssertionsOnEntities(opContext, assertees);
              }
              targets.addAll(assertees);
              return isAuthorizedToEditAssertionsOnEntities(opContext, targets);
            })
        .collect(Collectors.toSet());
  }

  /** Summarizes what a request writes to each assertion for the asserted-entity fallback. */
  @Nonnull
  static AssertionWrites collectAssertionWrites(
      @Nonnull final Collection<AssertionWrite> requestWrites) {
    final Map<Urn, List<AssertionWrite>> infoWrites = new HashMap<>();
    final Map<Urn, Set<Urn>> assertees = new HashMap<>();
    // Starts as every assertion written; those with any non-run-event write are removed below.
    final Set<Urn> runEventOnly = new HashSet<>();
    final Set<Urn> notRunEventOnly = new HashSet<>();
    for (AssertionWrite write : requestWrites) {
      runEventOnly.add(write.urn());
      final boolean isRunEvent = ASSERTION_RUN_EVENT_ASPECT_NAME.equals(write.aspectName());
      if (!isRunEvent
          || ChangeType.DELETE.equals(write.changeType())
          || write.runEvent() == null
          || !write.runEvent().hasAsserteeUrn()) {
        notRunEventOnly.add(write.urn());
      } else {
        assertees
            .computeIfAbsent(write.urn(), k -> new HashSet<>())
            .add(write.runEvent().getAsserteeUrn());
      }
      if (ASSERTION_INFO_ASPECT_NAME.equals(write.aspectName())) {
        infoWrites.computeIfAbsent(write.urn(), k -> new ArrayList<>()).add(write);
      }
    }
    runEventOnly.removeAll(notRunEventOnly);
    return new AssertionWrites(infoWrites, runEventOnly, assertees);
  }

  /** One aspect write to an assertion in a request. */
  public record AssertionWrite(
      @Nonnull Urn urn,
      @Nullable String aspectName,
      @Nullable ChangeType changeType,
      @Nullable AssertionInfo info,
      @Nullable BatchItem infoPatch,
      @Nullable AssertionRunEvent runEvent) {

    @Nonnull
    static AssertionWrite fromProposal(
        @Nonnull OperationContext opContext,
        @Nonnull final EntityRegistry entityRegistry,
        @Nonnull final Urn urn,
        @Nonnull final MetadataChangeProposal mcp) {
      return new AssertionWrite(
          urn,
          mcp.getAspectName(),
          mcp.getChangeType(),
          proposedAssertionInfo(mcp),
          proposedAssertionInfoPatch(opContext, entityRegistry, mcp),
          proposedRunEvent(mcp));
    }

    @Nonnull
    public static AssertionWrite fromItem(@Nonnull final BatchItem item) {
      return new AssertionWrite(
          item.getUrn(),
          item.getAspectName(),
          item.getChangeType(),
          proposedAssertionInfo(item),
          proposedAssertionInfoPatch(item),
          proposedRunEvent(item));
    }
  }

  /**
   * Per-assertion view of a request: its {@code assertionInfo} writes in request order, assertions
   * that only receive run events, and entities named by the request's run events.
   */
  record AssertionWrites(
      @Nonnull Map<Urn, List<AssertionWrite>> infoWrites,
      @Nonnull Set<Urn> runEventOnly,
      @Nonnull Map<Urn, Set<Urn>> runEventAssertees) {
    static final AssertionWrites NONE = new AssertionWrites(Map.of(), Set.of(), Map.of());
  }

  /** The PATCH as a batch item, so it can be applied to the persisted assertionInfo. */
  @Nullable
  private static BatchItem proposedAssertionInfoPatch(
      @Nonnull OperationContext opContext,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final MetadataChangeProposal mcp) {
    if (!ASSERTION_INFO_ASPECT_NAME.equals(mcp.getAspectName())
        || !ChangeType.PATCH.equals(mcp.getChangeType())) {
      return null;
    }
    try {
      return ProposedItem.builder().build(mcp, opContext.getAuditStamp(), entityRegistry);
    } catch (RuntimeException e) {
      log.debug("Unable to read assertionInfo PATCH for {}", mcp.getEntityUrn(), e);
      return null;
    }
  }

  @Nullable
  private static BatchItem proposedAssertionInfoPatch(@Nonnull final BatchItem item) {
    return ASSERTION_INFO_ASPECT_NAME.equals(item.getAspectName())
            && ChangeType.PATCH.equals(item.getChangeType())
        ? item
        : null;
  }

  @Nullable
  private static AssertionInfo proposedAssertionInfo(@Nonnull final MetadataChangeProposal mcp) {
    if (!ASSERTION_INFO_ASPECT_NAME.equals(mcp.getAspectName())
        || mcp.getAspect() == null
        || ChangeType.PATCH.equals(mcp.getChangeType())
        || ChangeType.DELETE.equals(mcp.getChangeType())) {
      return null;
    }
    try {
      return GenericRecordUtils.deserializeAspect(
          mcp.getAspect().getValue(), mcp.getAspect().getContentType(), AssertionInfo.class);
    } catch (RuntimeException e) {
      // Malformed payloads are rejected later by ingestion; for authorization they have no target.
      log.debug("Unable to deserialize proposed assertionInfo for {}", mcp.getEntityUrn(), e);
      return null;
    }
  }

  /**
   * Under alternate MCP validation items are {@link ProposedItem}s, whose {@code getRecordTemplate}
   * deserializes the raw payload: it throws for a JSON patch body or a DELETE without one. So the
   * change type is checked first, and an unreadable upsert is left for ingestion to reject.
   */
  @Nullable
  private static AssertionInfo proposedAssertionInfo(@Nonnull final BatchItem item) {
    if (!ASSERTION_INFO_ASPECT_NAME.equals(item.getAspectName())
        || ChangeType.PATCH.equals(item.getChangeType())
        || ChangeType.DELETE.equals(item.getChangeType())) {
      return null;
    }
    try {
      return item.getAspect(AssertionInfo.class);
    } catch (RuntimeException e) {
      log.debug("Unable to read proposed assertionInfo for {}", item.getUrn(), e);
      return null;
    }
  }

  @Nullable
  private static AssertionRunEvent proposedRunEvent(@Nonnull final MetadataChangeProposal mcp) {
    if (!ASSERTION_RUN_EVENT_ASPECT_NAME.equals(mcp.getAspectName()) || mcp.getAspect() == null) {
      return null;
    }
    try {
      return GenericRecordUtils.deserializeAspect(
          mcp.getAspect().getValue(), mcp.getAspect().getContentType(), AssertionRunEvent.class);
    } catch (RuntimeException e) {
      log.debug("Unable to deserialize proposed assertionRunEvent for {}", mcp.getEntityUrn(), e);
      return null;
    }
  }

  @Nullable
  private static AssertionRunEvent proposedRunEvent(@Nonnull final BatchItem item) {
    if (!ASSERTION_RUN_EVENT_ASPECT_NAME.equals(item.getAspectName())
        || ChangeType.DELETE.equals(item.getChangeType())) {
      return null;
    }
    try {
      return item.getAspect(AssertionRunEvent.class);
    } catch (RuntimeException e) {
      log.debug("Unable to read proposed assertionRunEvent for {}", item.getUrn(), e);
      return null;
    }
  }
}
