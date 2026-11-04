package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_RUN_EVENT_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.AssertionAuthorizationUtils;
import com.linkedin.metadata.authorization.AssertionAuthorizationUtils.AssertionWrite;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
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
 * other targets fails here unless the actor holds the privilege on them too. An assertion without
 * {@code assertionInfo} is left to the API-level gate. This lives here rather than in the API gate
 * so OpenAPI, Rest.li and MCP {@code DELETE} all reach it.
 *
 * <p>Run events need the privilege on the run event's {@code asserteeUrn}, matching {@code
 * ReportAssertionResultResolver} (notifications are addressed to it), and on every entity the
 * assertion targets, so a result cannot mark an assertion the actor may not edit as failing. The
 * assertee does not have to be one of the targets: clients such as dbt report results whose
 * assertee differs from the target by URN case, environment or project. A schema field assertee is
 * checked on its parent asset, as targets are. A run event without an assertee is rejected once the
 * assertion has targets. An assertion with no {@code assertionInfo} yet accepts run events for any
 * entity the actor may edit assertions on, so clients that report results before the definition
 * keep working; those results stay in the assertion's run history once it is defined.
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
    // Each assertion's assertionInfo as of the item being checked, starting from the stored value.
    Map<Urn, AssertionInfo> current =
        new HashMap<>(
            AssertionAuthorizationUtils.fetchAssertionInfos(
                operationContext, aspectRetriever, assertionUrns));
    Set<Urn> unresolved = new HashSet<>();

    List<AspectValidationException> failures = new ArrayList<>();
    List<BatchItem> runEvents = new ArrayList<>();
    for (BatchItem item : items) {
      Urn urn = item.getUrn();
      boolean isInfo = ASSERTION_INFO_ASPECT_NAME.equals(item.getAspectName());
      boolean isDelete = ChangeType.DELETE.equals(item.getChangeType());
      if (ASSERTION_RUN_EVENT_ASPECT_NAME.equals(item.getAspectName()) && !isDelete) {
        runEvents.add(item);
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
        if (!before.isEmpty()
            && !AssertionAuthorizationUtils.isAuthorizedToEditAssertionsOnEntities(
                session, before)) {
          failures.add(deleteFailure(item, before));
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
      Set<Urn> changedTargets =
          symmetricDifference(
              before, AssertionAuthorizationUtils.resolveAssertedEntityUrns(entityRegistry, next));
      if (!changedTargets.isEmpty()
          && !AssertionAuthorizationUtils.isAuthorizedToEditAssertionsOnEntities(
              session, changedTargets)) {
        failures.add(
            isDelete
                ? deleteFailure(item, changedTargets)
                : authFailure(
                    item,
                    "Unauthorized to attach or detach assertion "
                        + urn
                        + " for "
                        + changedTargets
                        + ". Requires the Edit Assertions privilege on the asserted entity."));
      }
    }

    // Run events are checked against the assertion as this batch leaves it. The API gate accepts
    // the generic privilege on the assertion itself, which covers neither the assertee that
    // receives the notifications nor the assets the assertion checks.
    Map<Urn, Boolean> authorizedEntities = new HashMap<>();
    for (BatchItem item : runEvents) {
      Set<Urn> targets =
          AssertionAuthorizationUtils.resolveAssertedEntityUrns(
              entityRegistry, current.get(item.getUrn()));
      AssertionRunEvent runEvent = item.getAspect(AssertionRunEvent.class);
      Urn assertee =
          runEvent == null || !runEvent.hasAsserteeUrn() ? null : runEvent.getAsserteeUrn();
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
      required.add(AssertionAuthorizationUtils.toAsset(assertee));
      Set<Urn> denied =
          required.stream()
              .filter(
                  urn ->
                      !authorizedEntities.computeIfAbsent(
                          urn,
                          u ->
                              AssertionAuthorizationUtils.isAuthorizedToEditAssertionsOnEntities(
                                  session, Set.of(u))))
              .collect(Collectors.toSet());
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
    return failures;
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
