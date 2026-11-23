package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_RUN_SUMMARY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.AssertionAuthorizationUtils;
import com.linkedin.metadata.authorization.AssertionAuthorizationUtils.AssertionWrite;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Ensures an {@code assertionInfo} write can only attach an assertion to an entity, or move it away
 * from one, when the actor holds {@code EDIT_ENTITY_ASSERTIONS} or {@code EDIT_ENTITY} on that
 * entity, matching the GraphQL assertion mutations for every write API. An assertion's targets are
 * every {@code Asserts} relationship and its {@code entityUrn} (see {@link
 * AssertionAuthorizationUtils#resolveAssertedEntityUrns}).
 *
 * <p>Only changes to the asserted entities are checked. Edits that keep the same target remain
 * governed by the API-level gate, so {@code EDIT_ENTITY} on the assertion itself keeps working for
 * them. Writes to one assertion are applied in batch order, as the write path applies them, and
 * each change is checked against the value before it.
 *
 * <p>Deleting an assertion (its key aspect) or its {@code assertionInfo} requires the privilege on
 * every entity the assertion targets. This can be stricter than {@code DeleteAssertionResolver},
 * which checks only the target its {@code type} selects, so a GraphQL delete of an assertion with
 * other targets fails here unless the actor holds the privilege on them too. Soft deleting or
 * restoring an assertion (a change to {@code status.removed}, either way) requires the same. An
 * assertion without {@code assertionInfo} is left to the API-level gate. This lives here rather
 * than in the API gate so OpenAPI, Rest.li and MCP {@code DELETE} all reach it. Detaching an
 * assertion from an entity, deleting it, or soft deleting or restoring it does not require the
 * privilege on a target that no longer exists, as when reference cleanup follows the hard delete of
 * that entity; attaching always does.
 *
 * <p>Run events need the privilege on the run event's {@code asserteeUrn}, matching {@code
 * ReportAssertionResultResolver} (notifications are addressed to it), and on every entity the
 * assertion targets, so a result cannot mark an assertion the actor may not edit as failing. The
 * assertee does not have to be one of the targets: clients such as dbt report results whose
 * assertee differs from the target by URN case, environment or project. A run event without an
 * assertee is rejected once the assertion has targets. For an assertion with no {@code
 * assertionInfo} yet only the assertee is checked here; the API-level gate then requires the
 * privilege on the assertion itself. Writes to {@code assertionRunSummary}, which is derived from
 * the run events, need the privilege on every target as well.
 *
 * <p>Run events and run summaries dated more than {@code maxFutureSkew} ahead of the server clock
 * are rejected. The current result of an assertion is the one with the latest timestamp, so a
 * result dated in the future would stay current until that date, whatever runs after it.
 *
 * <p>Targets are read from the stored value when the batch is validated. With async ingestion an
 * earlier request may not have landed yet, so a write is checked against the value before that
 * request; the earlier request was checked on its own.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class AssertionAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Nonnull private Duration maxFutureSkew = Duration.ofMinutes(5);

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
    Set<Urn> assertionUrns = items.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    // Stored values decide which targets are checked, so a lagging replica must not hide them.
    OperationFingerprint primary = primary(operationContext);
    // Each assertion's assertionInfo as of the item being checked, starting from the stored value.
    Map<Urn, AssertionInfo> current =
        new HashMap<>(
            AssertionAuthorizationUtils.fetchAssertionInfos(
                primary, aspectRetriever, assertionUrns));
    // Each assertion's status as of the item being checked; read only when the batch writes one.
    Map<Urn, Status> currentStatus =
        fetchLatest(
            primary,
            aspectRetriever,
            urnsWriting(items, STATUS_ASPECT_NAME),
            STATUS_ASPECT_NAME,
            Status.class);
    Set<Urn> unresolved = new HashSet<>();
    Map<Urn, Boolean> authorizedEntities = new HashMap<>();
    Map<Urn, Boolean> existing = new HashMap<>();
    long maxTimestampMillis = System.currentTimeMillis() + maxFutureSkew.toMillis();

    List<AspectValidationException> failures = new ArrayList<>();
    List<BatchItem> runEvents = new ArrayList<>();
    List<BatchItem> runSummaries = new ArrayList<>();
    for (BatchItem item : items) {
      Urn urn = item.getUrn();
      String aspectName = item.getAspectName();
      boolean isInfo = ASSERTION_INFO_ASPECT_NAME.equals(aspectName);
      boolean isDelete = ChangeType.DELETE.equals(item.getChangeType());
      if (ASSERTION_RUN_SUMMARY_ASPECT_NAME.equals(aspectName)) {
        runSummaries.add(item);
        continue;
      }
      if (ASSERTION_RUN_EVENT_ASPECT_NAME.equals(aspectName) && !isDelete) {
        runEvents.add(item);
        continue;
      }
      if (STATUS_ASPECT_NAME.equals(aspectName)) {
        validateStatusChange(
                operationContext,
                item,
                currentStatus,
                AssertionAuthorizationUtils.resolveAssertedEntityUrns(
                    entityRegistry, current.get(urn)),
                aspectRetriever,
                session,
                authorizedEntities,
                existing)
            .ifPresent(failures::add);
        continue;
      }
      if (!isInfo && !isDelete) {
        continue;
      }
      if (unresolved.contains(urn)) {
        failures.add(authFailure(item, "Unable to resolve assertionInfo PATCH for authorization"));
        continue;
      }
      Set<Urn> before =
          AssertionAuthorizationUtils.resolveAssertedEntityUrns(entityRegistry, current.get(urn));
      if (!isInfo) {
        Set<Urn> denied = denied(session, before, authorizedEntities);
        denied.removeAll(missing(operationContext, aspectRetriever, denied, existing));
        if (!denied.isEmpty()) {
          failures.add(deleteFailure(item, denied));
        }
        continue;
      }
      AssertionInfo next;
      try {
        next =
            AssertionAuthorizationUtils.applyInfoWrite(
                AssertionWrite.fromItem(item), current.get(urn), aspectRetriever);
      } catch (RuntimeException e) {
        // A PATCH that cannot be applied fails the write anyway; fail closed here.
        log.debug("Unable to resolve assertionInfo PATCH for {}", urn, e);
        failures.add(authFailure(item, "Unable to resolve assertionInfo PATCH for authorization"));
        unresolved.add(urn);
        continue;
      }
      current.put(urn, next);
      Set<Urn> after = AssertionAuthorizationUtils.resolveAssertedEntityUrns(entityRegistry, next);
      Set<Urn> removed = new HashSet<>(before);
      removed.removeAll(after);
      Set<Urn> denied = denied(session, symmetricDifference(before, after), authorizedEntities);
      // Only detaching skips assets that are gone; attaching always needs the privilege.
      Set<Urn> deniedRemoved = new HashSet<>(denied);
      deniedRemoved.retainAll(removed);
      denied.removeAll(missing(operationContext, aspectRetriever, deniedRemoved, existing));
      if (!denied.isEmpty()) {
        failures.add(
            isDelete
                ? deleteFailure(item, denied)
                : authFailure(
                    item,
                    "Unauthorized to attach or detach assertion "
                        + urn
                        + " for "
                        + denied
                        + ". Requires the Edit Assertions privilege on the asserted entity."));
      }
    }

    // Run events and summaries are checked against the assertion as this batch leaves it. The API
    // gate accepts the generic privilege on the assertion itself, which covers neither the assertee
    // that receives the notifications nor the assets the assertion checks.
    for (BatchItem item : runEvents) {
      Set<Urn> targets =
          AssertionAuthorizationUtils.resolveAssertedEntityUrns(
              entityRegistry, current.get(item.getUrn()));
      Long timestampMillis;
      Urn assertee;
      try {
        AssertionRunEvent runEvent = item.getAspect(AssertionRunEvent.class);
        timestampMillis = runEvent == null ? null : runEvent.getTimestampMillis();
        assertee =
            runEvent == null || !runEvent.hasAsserteeUrn() ? null : runEvent.getAsserteeUrn();
      } catch (RuntimeException e) {
        // Under alternate MCP validation a PATCH payload is not a readable record; fail closed.
        log.debug("Unable to read assertionRunEvent for {}", item.getUrn(), e);
        failures.add(authFailure(item, "Unable to read assertionRunEvent for authorization"));
        continue;
      }
      if (timestampMillis != null && timestampMillis > maxTimestampMillis) {
        failures.add(futureTimestampFailure(item));
        continue;
      }
      if (assertee == null) {
        if (!targets.isEmpty()) {
          failures.add(
              authFailure(
                  item,
                  "Run event for assertion " + item.getUrn() + " does not name an assertee."));
        }
        continue;
      }
      Set<Urn> required = new HashSet<>(targets);
      required.add(assertee);
      Set<Urn> denied = denied(session, required, authorizedEntities);
      if (!denied.isEmpty()) {
        failures.add(
            authFailure(
                item,
                "Unauthorized to report results for assertion "
                    + item.getUrn()
                    + " on "
                    + assertee
                    + ". Requires the Edit Assertions privilege on "
                    + denied
                    + "."));
      }
    }

    Map<Urn, AssertionRunSummary> currentSummary =
        new HashMap<>(
            PatchItemImpl.readStoredForPatches(
                operationContext,
                aspectRetriever,
                runSummaries,
                ASSERTION_RUN_SUMMARY_ASPECT_NAME,
                AssertionRunSummary.class));
    for (BatchItem item : runSummaries) {
      Urn urn = item.getUrn();
      if (!ChangeType.DELETE.equals(item.getChangeType())) {
        AssertionRunSummary summary;
        try {
          summary =
              PatchItemImpl.resolveProposedAspect(
                  item, currentSummary.get(urn), aspectRetriever, AssertionRunSummary.class);
        } catch (RuntimeException e) {
          log.debug("Unable to resolve assertionRunSummary write for {}", urn, e);
          failures.add(
              authFailure(item, "Unable to resolve assertionRunSummary for authorization"));
          continue;
        }
        currentSummary.put(urn, summary);
        if (latestTimestampMillis(summary) > maxTimestampMillis) {
          failures.add(futureTimestampFailure(item));
          continue;
        }
      }
      Set<Urn> denied =
          denied(
              session,
              AssertionAuthorizationUtils.resolveAssertedEntityUrns(
                  entityRegistry, current.get(urn)),
              authorizedEntities);
      if (!denied.isEmpty()) {
        failures.add(
            authFailure(
                item,
                "Unauthorized to write the run summary of assertion "
                    + urn
                    + ". Requires the Edit Assertions privilege on "
                    + denied
                    + "."));
      }
    }
    return failures;
  }

  /**
   * Soft deleting or restoring an assertion takes it off, or puts it back on, the assets it
   * targets, so a change to {@code removed} needs the privilege on every target that still exists,
   * as a hard delete does. Other status writes are left to the API-level gate.
   */
  @Nonnull
  private Optional<AspectValidationException> validateStatusChange(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull BatchItem item,
      @Nonnull Map<Urn, Status> currentStatus,
      @Nonnull Set<Urn> targets,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull AuthorizationSession session,
      @Nonnull Map<Urn, Boolean> authorizedEntities,
      @Nonnull Map<Urn, Boolean> existing) {
    Urn urn = item.getUrn();
    Status before = currentStatus.get(urn);
    Status after;
    try {
      after =
          ChangeType.DELETE.equals(item.getChangeType())
              ? null
              : PatchItemImpl.resolveProposedAspect(item, before, aspectRetriever, Status.class);
    } catch (RuntimeException e) {
      log.debug("Unable to resolve status write for {}", urn, e);
      return Optional.of(authFailure(item, "Unable to resolve status for authorization"));
    }
    currentStatus.put(urn, after);
    if (isRemoved(before) == isRemoved(after)) {
      return Optional.empty();
    }
    Set<Urn> denied = denied(session, targets, authorizedEntities);
    denied.removeAll(missing(operationContext, aspectRetriever, denied, existing));
    if (denied.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        authFailure(
            item,
            "Unauthorized to "
                + (isRemoved(after) ? "soft delete" : "restore")
                + " assertion "
                + urn
                + ". Requires the Edit Assertions privilege on "
                + denied
                + "."));
  }

  private static boolean isRemoved(@Nullable Status status) {
    return status != null && status.isRemoved();
  }

  private static long latestTimestampMillis(@Nullable AssertionRunSummary summary) {
    if (summary == null) {
      return Long.MIN_VALUE;
    }
    return Stream.of(
            summary.getLastPassedAtMillis(),
            summary.getLastFailedAtMillis(),
            summary.getLastErroredAtMillis(),
            summary.getLastInitializedAtMillis())
        .filter(Objects::nonNull)
        .mapToLong(Long::longValue)
        .max()
        .orElse(Long.MIN_VALUE);
  }

  /**
   * Entities in {@code urns} that no longer exist, read from the primary and cached in {@code
   * existing}. Hard deleting an asset removes it before the references to it are cleaned up, and
   * policy fields such as its domain or owners can no longer be resolved for an asset that is gone,
   * so detaching an assertion from it, or deleting the assertion, does not require the privilege on
   * it. Soft deleted assets still exist.
   */
  @Nonnull
  private static Set<Urn> missing(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Set<Urn> urns,
      @Nonnull Map<Urn, Boolean> existing) {
    Set<Urn> unknown =
        urns.stream().filter(urn -> !existing.containsKey(urn)).collect(Collectors.toSet());
    if (!unknown.isEmpty()) {
      Map<Urn, Boolean> found = aspectRetriever.entityExists(primary(operationContext), unknown);
      unknown.forEach(urn -> existing.put(urn, Boolean.TRUE.equals(found.get(urn))));
    }
    return urns.stream()
        .filter(urn -> !existing.get(urn))
        .collect(Collectors.toCollection(HashSet::new));
  }

  /** Entities in {@code required} the actor may not edit assertions on, caching each answer. */
  @Nonnull
  private static Set<Urn> denied(
      @Nonnull AuthorizationSession session,
      @Nonnull Set<Urn> required,
      @Nonnull Map<Urn, Boolean> authorizedEntities) {
    return required.stream()
        .filter(
            urn ->
                !authorizedEntities.computeIfAbsent(
                    urn,
                    u ->
                        AssertionAuthorizationUtils.isAuthorizedToEditAssertionsOnEntities(
                            session, Set.of(u))))
        .collect(Collectors.toCollection(HashSet::new));
  }

  @Nonnull
  private static Set<Urn> urnsWriting(
      @Nonnull List<? extends BatchItem> items, @Nonnull String aspectName) {
    return items.stream()
        .filter(item -> aspectName.equals(item.getAspectName()))
        .map(BatchItem::getUrn)
        .collect(Collectors.toSet());
  }

  @Nonnull
  private static OperationFingerprint primary(@Nonnull OperationFingerprint operationContext) {
    return operationContext instanceof OperationContext opContext
        ? opContext.withReadPreference(ReadPreference.PRIMARY)
        : operationContext;
  }

  @Nonnull
  private static <T extends RecordTemplate> Map<Urn, T> fetchLatest(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Set<Urn> urns,
      @Nonnull String aspectName,
      @Nonnull Class<T> clazz) {
    Map<Urn, T> result = new HashMap<>();
    if (urns.isEmpty()) {
      return result;
    }
    aspectRetriever
        .getLatestAspectObjects(operationContext, urns, Set.of(aspectName))
        .forEach(
            (urn, byName) -> {
              Aspect aspect = byName == null ? null : byName.get(aspectName);
              if (aspect != null) {
                result.put(urn, RecordUtils.toRecordTemplate(clazz, aspect.data()));
              }
            });
    return result;
  }

  @Nonnull
  private AspectValidationException futureTimestampFailure(@Nonnull BatchItem item) {
    return AspectValidationException.forItem(
        item,
        "Timestamp of "
            + item.getAspectName()
            + " for assertion "
            + item.getUrn()
            + " is more than "
            + maxFutureSkew
            + " ahead of the server clock.");
  }

  @Nonnull
  private AspectValidationException deleteFailure(
      @Nonnull BatchItem item, @Nonnull Set<Urn> targets) {
    return authFailure(
        item,
        "Unauthorized to delete assertion "
            + item.getUrn()
            + ". Requires the Edit Assertions privilege on "
            + targets
            + ".");
  }

  @Nonnull
  private static Set<Urn> symmetricDifference(@Nonnull Set<Urn> a, @Nonnull Set<Urn> b) {
    Set<Urn> result = new HashSet<>(a);
    result.addAll(b);
    Set<Urn> intersection = new HashSet<>(a);
    intersection.retainAll(b);
    result.removeAll(intersection);
    return result;
  }
}
