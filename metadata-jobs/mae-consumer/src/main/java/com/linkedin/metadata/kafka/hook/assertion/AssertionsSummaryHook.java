package com.linkedin.metadata.kafka.hook.assertion;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.common.AssertionUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.aspect.patch.builder.AssertionsSummaryPatchBuilder;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * This hook is responsible for maintaining the AssertionsSummary.pdl aspect of entities on which
 * Assertions may be raised. It handles both assertion updates and assertion soft deletions to
 * ensure that this aspect reflects the latest state of the assertion.
 *
 * <p>Hard deletes of assertions are not handled within this hook because the expectation is that
 * deleteReferences will be invoked to clean up references.
 */
@Slf4j
@Component
@Import({
  EntityRegistryFactory.class,
  AssertionServiceFactory.class,
  SystemAuthenticationFactory.class
})
public class AssertionsSummaryHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.CREATE, ChangeType.RESTATE);
  private static final Set<String> SUPPORTED_UPDATE_ASPECTS =
      ImmutableSet.of(ASSERTION_RUN_EVENT_ASPECT_NAME, MONITOR_INFO_ASPECT_NAME);

  private final AssertionService assertionService;
  private OperationContext systemOperationContext;
  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public AssertionsSummaryHook(
      @Nonnull final AssertionService assertionService,
      @Nonnull @Value("${assertions.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${assertions.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.assertionService =
        Objects.requireNonNull(assertionService, "assertionService is required");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public AssertionsSummaryHook(
      @Nonnull final AssertionService assertionService, @Nonnull Boolean isEnabled) {
    this(assertionService, isEnabled, "");
  }

  @Override
  public AssertionsSummaryHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    log.info("Initialized the assertions summary hook");
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (isEnabled && isEligibleForProcessing(event)) {
      log.debug("Urn {} received by Assertion Summary Hook.", event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, systemOperationContext.getEntityRegistry());
      // Handle the deletion case.
      if (isAssertionSoftDeleted(event)) {
        handleAssertionSoftDeleted(urn);
      } else if (isAssertionInfoHardDeleted(event)) {
        handleAssertionInfoHardDeleted(event);
      } else if (isAssertionKeyHardDeleted(event)) {
        handleAssertionKeyHardDeleted(event);
      } else if (isAssertionTargetEntityChanged(event)) {
        handleAssertionTargetEntityChanged(urn, event);
      } else if (isAssertionRunCompleteEvent(event)) {
        handleAssertionRunCompleted(urn, extractAssertionRunEvent(event));
      } else if (isMonitorDisabledEvent(event)) {
        handleMonitorDisabled(urn, extractMonitorInfo(event));
      }
      // TODO: Handle un-soft-deleted by adding it back into the result list.
    }
  }

  /**
   * Handles an assertion deletion by removing the assertion from either resolved or active
   * assertions.
   */
  private void handleAssertionSoftDeleted(@Nonnull final Urn assertionUrn) {
    removeAssertionFromSummary(assertionUrn);
  }

  /**
   * Handles an assertion deletion by removing the assertion from either resolved or active
   * assertions.
   */
  private void handleAssertionInfoHardDeleted(@Nonnull final MetadataChangeLog event) {
    if (event.hasPreviousAspectValue()) {
      final AssertionInfo previousInfo =
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              AssertionInfo.class);
      final List<Urn> prevEntityUrns = extractAssertionEntities(previousInfo);
      for (Urn entityUrn : prevEntityUrns) {
        removeAssertionFromSummary(event.getEntityUrn(), entityUrn);
      }
    }
  }

  /** Handles when an assertion was deleted by removing the assertion key (HARD DELETE) */
  private void handleAssertionKeyHardDeleted(@Nonnull final MetadataChangeLog event) {
    // Simply attempt to clean up any references to the entity.
    log.debug(
        "Attempting to clean up remaining references to assertion with urn {}.",
        event.getEntityUrn());
    // Step 1: Find all entities that have this assertion in the summary.
    final List<Urn> entityUrns =
        assertionService.listEntitiesWithAssertionInSummary(
            systemOperationContext, event.getEntityUrn());
    // Step 2: Remove from each entity.
    for (Urn entityUrn : entityUrns) {
      removeAssertionFromSummary(event.getEntityUrn(), entityUrn);
    }
  }

  private void handleAssertionTargetEntityChanged(
      @Nonnull final Urn assertionUrn, @Nonnull final MetadataChangeLog event) {
    final AssertionInfo info =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), AssertionInfo.class);
    final AssertionInfo prevInfo =
        GenericRecordUtils.deserializeAspect(
            event.getPreviousAspectValue().getValue(),
            event.getPreviousAspectValue().getContentType(),
            AssertionInfo.class);

    final List<Urn> prevEntityUrns = extractAssertionEntities(prevInfo);
    final List<Urn> newEntityUrns = extractAssertionEntities(info);
    final List<Urn> removedEntityUrns = new ArrayList<>(prevEntityUrns);
    removedEntityUrns.removeAll(newEntityUrns);

    // Simply remove the assertion from the previous entity.
    // We will wait until the next assertion run event to add to the new entity.
    for (Urn entityUrn : removedEntityUrns) {
      removeAssertionFromSummary(assertionUrn, entityUrn);
    }
  }

  /** Handle an assertion update by adding to either resolved or active assertions for an entity. */
  private void handleAssertionRunCompleted(
      @Nonnull final Urn assertionUrn, @Nonnull final AssertionRunEvent runEvent) {

    // 1. Fetch assertion info.
    AssertionInfo assertionInfo =
        assertionService.getAssertionInfo(systemOperationContext, assertionUrn);

    // 2. Retrieve associated urns.
    if (assertionInfo != null) {

      final List<Urn> assertionEntities = extractAssertionEntities(assertionInfo);

      if (assertionEntities.size() <= 0) {
        log.warn(
            String.format(
                "Failed to find entities associated with assertion with urn %s. Skipping updating run summaries...",
                assertionUrn));
        return;
      }

      // 3. Batch fetch AssertionsSummary for all entities (works efficiently even with 1 entity)
      final Map<Urn, AssertionsSummary> summariesMap =
          assertionService.batchGetAssertionsSummary(
              systemOperationContext, new java.util.HashSet<>(assertionEntities));

      for (Urn entityUrn : assertionEntities) {
        // Batch method guarantees all input URNs are in the result map (with null if not found)
        final AssertionsSummary summary = summariesMap.get(entityUrn);
        addAssertionToSummary(entityUrn, assertionUrn, assertionInfo, runEvent, summary);
      }
    } else {
      log.warn(
          String.format(
              "Failed to find assertionInfo aspect for assertion with urn %s. Skipping updating assertion summary for related assertions!",
              assertionUrn));
    }
  }

  /**
   * Handle a monitor being disabled or paused by removing it from the summary. This allows users to
   * clear bad status immediately.
   */
  private void handleMonitorDisabled(
      @Nonnull final Urn monitorUrn, @Nonnull final MonitorInfo monitorInfo) {
    if (!MonitorType.ASSERTION.equals(monitorInfo.getType())
        || !monitorInfo.hasAssertionMonitor()) {
      log.debug(
          "Found disabled monitor that is not assertion monitor type. Skipping removing from summary.");
      return;
    }

    log.debug("Removing assertions from summary for monitor with urn {}", monitorUrn);
    final List<Urn> assertionUrns =
        monitorInfo.getAssertionMonitor().getAssertions().stream()
            .map(AssertionEvaluationSpec::getAssertion)
            .toList();

    if (assertionUrns.isEmpty()) {
      return;
    }

    // Batch fetch AssertionInfo for all assertions in a single request
    final Map<Urn, AssertionInfo> assertionInfoMap =
        assertionService.batchGetAssertionInfo(
            systemOperationContext, new java.util.HashSet<>(assertionUrns));

    // Process each assertion with its pre-fetched info
    for (Urn assertionUrn : assertionUrns) {
      final AssertionInfo assertionInfo = assertionInfoMap.get(assertionUrn);
      if (assertionInfo != null) {
        final List<Urn> assertionEntities = extractAssertionEntities(assertionInfo);
        if (assertionEntities.size() > 0) {
          for (Urn entityUrn : assertionEntities) {
            removeAssertionFromSummary(assertionUrn, entityUrn);
          }
        } else {
          log.warn(
              String.format(
                  "Failed to find entities associated with assertion with urn %s. Skipping updating run summaries...",
                  assertionUrn));
        }
      } else {
        log.warn(
            String.format(
                "Failed to find assertionInfo aspect for assertion with urn %s. Skipping updating assertion summary for related assertions!",
                assertionUrn));
      }
    }
  }

  private void removeAssertionFromSummary(final Urn assertionUrn) {
    // 1. Fetch assertion info.
    AssertionInfo assertionInfo =
        assertionService.getAssertionInfo(systemOperationContext, assertionUrn);

    // 2. Retrieve associated urns.
    if (assertionInfo != null) {
      final List<Urn> assertionEntities = extractAssertionEntities(assertionInfo);

      if (assertionEntities.size() <= 0) {
        log.warn(
            String.format(
                "Failed to find entities associated with assertion with urn %s. Skipping updating run summaries...",
                assertionUrn));
        return;
      }

      // 3. For each urn, resolve the entity assertions aspect and remove from failing and passing
      // assertions.
      for (Urn entityUrn : assertionEntities) {
        removeAssertionFromSummary(assertionUrn, entityUrn);
      }
    } else {
      log.warn(
          String.format(
              "Failed to find assertionInfo aspect for assertion with urn %s. Skipping updating assertion summary for related assertions!",
              assertionUrn));
    }
  }

  /**
   * Removes an assertion to the AssertionSummary aspect for a related entity via PATCH operation.
   */
  private void removeAssertionFromSummary(
      @Nonnull final Urn assertionUrn, @Nonnull final Urn entityUrn) {
    // 1. Build the patch
    AssertionsSummaryPatchBuilder patchBuilder =
        buildRemoveAssertionFromSummaryPatch(assertionUrn, entityUrn);

    // 2. Emit the patch back!
    patchAssertionSummary(entityUrn, patchBuilder);
  }

  /**
   * Adds an assertion to the AssertionSummary aspect for a related entity. This is used to search
   * for entity by active and resolved assertions.
   */
  private void addAssertionToSummary(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionInfo info,
      @Nonnull final AssertionRunEvent event) {
    addAssertionToSummary(entityUrn, assertionUrn, info, event, null);
  }

  /**
   * Adds an assertion to the AssertionSummary aspect for a related entity. This is used to search
   * for entity by active and resolved assertions.
   *
   * @param summary if provided, uses this summary instead of fetching; if null, fetches it
   */
  private void addAssertionToSummary(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionInfo info,
      @Nonnull final AssertionRunEvent event,
      @Nullable final AssertionsSummary summary) {
    // 1. Fetch the latest assertion summary for the entity (if not provided)
    AssertionsSummary assertionsSummary =
        summary != null ? summary : getAssertionsSummary(entityUrn);
    AssertionResult result = event.getResult();
    AssertionSummaryDetails details = buildAssertionSummaryDetails(assertionUrn, info, event);

    // 2. Make sure we're not overwriting more recent run events on the summary
    if (checkShouldIgnoreAddingRunEventToSummary(assertionUrn, assertionsSummary, event)) {
      log.warn("Ignored adding run event to summary for assertion with urn {}", assertionUrn);
      return;
    }

    // If yes, we patch!
    AssertionsSummaryPatchBuilder patchBuilder;

    // 3. Add the assertion to passing or failing assertions
    if (AssertionResultType.SUCCESS.equals(result.getType())) {
      patchBuilder = buildAssertionSuccessSummaryPatch(assertionUrn, details, entityUrn);
    } else if (AssertionResultType.FAILURE.equals(result.getType())) {
      patchBuilder = buildAssertionFailureSummaryPatch(assertionUrn, details, entityUrn);
    } else if (AssertionResultType.ERROR.equals(result.getType())) {
      patchBuilder = buildAssertionErrorSummaryPatch(assertionUrn, details, entityUrn);
    } else if (AssertionResultType.INIT.equals(result.getType())) {
      patchBuilder = buildAssertionInitSummaryPatch(assertionUrn, details, entityUrn);
    } else {
      log.debug(
          "Ignoring assertion run event with unknown result type {} for assertion with urn {}",
          result.getType(),
          assertionUrn);
      return;
    }
    patchBuilder = patchBuilder.addOverallLastAssertionResultAt(event.getTimestampMillis());

    // 4. Emit the patch back!
    patchAssertionSummary(entityUrn, patchBuilder);
  }

  private boolean checkShouldIgnoreAddingRunEventToSummary(
      Urn assertionUrn, AssertionsSummary summary, AssertionRunEvent event) {
    // 1. Make sure we're not overwriting more recent run events on the summary
    // 1.1 check if assertion is recorded under 'failing assertions' of the summary
    Optional<AssertionSummaryDetails> maybeExistingDetails =
        summary.getFailingAssertionDetails().stream()
            .filter(el -> el.getUrn().toString().equals(assertionUrn.toString()))
            .findFirst();
    // 1.2 check if assertion is recorded under 'passing assertions' of the summary
    if (maybeExistingDetails.isEmpty()) {
      maybeExistingDetails =
          summary.getPassingAssertionDetails().stream()
              .filter(el -> el.getUrn().toString().equals(assertionUrn.toString()))
              .findFirst();
    }
    // 1.3 if this assertion is recorded on the summary (in either passing or failing lists), ensure
    // the current run event's TS is AFTER the TS of the run event that's recorded in the summary
    if (maybeExistingDetails.isPresent()) {
      Long latestRecordedMillis = maybeExistingDetails.get().getLastResultAt();
      Long thisEventMillis = event.getTimestampMillis();
      return latestRecordedMillis > thisEventMillis;
    }
    return false;
  }

  @Nonnull
  private AssertionsSummary getAssertionsSummary(@Nonnull final Urn entityUrn) {
    AssertionsSummary assertionsSummary =
        assertionService.getAssertionsSummary(systemOperationContext, entityUrn);

    if (assertionsSummary == null) {
      assertionsSummary =
          new AssertionsSummary()
              .setFailingAssertionDetails(new AssertionSummaryDetailsArray())
              .setPassingAssertionDetails(new AssertionSummaryDetailsArray())
              .setErroringAssertionDetails(new AssertionSummaryDetailsArray())
              .setInitializingAssertionDetails(new AssertionSummaryDetailsArray());
    } else {
      // Ensure all arrays are initialized for existing summaries (backwards compatibility)
      if (!assertionsSummary.hasFailingAssertionDetails()) {
        assertionsSummary.setFailingAssertionDetails(new AssertionSummaryDetailsArray());
      }
      if (!assertionsSummary.hasPassingAssertionDetails()) {
        assertionsSummary.setPassingAssertionDetails(new AssertionSummaryDetailsArray());
      }
      if (!assertionsSummary.hasErroringAssertionDetails()) {
        assertionsSummary.setErroringAssertionDetails(new AssertionSummaryDetailsArray());
      }
      if (!assertionsSummary.hasInitializingAssertionDetails()) {
        assertionsSummary.setInitializingAssertionDetails(new AssertionSummaryDetailsArray());
      }
    }

    return assertionsSummary;
  }

  @Nonnull
  private AssertionSummaryDetails buildAssertionSummaryDetails(
      @Nonnull final Urn urn,
      @Nonnull final AssertionInfo info,
      @Nonnull final AssertionRunEvent event) {
    AssertionSummaryDetails assertionSummaryDetails = new AssertionSummaryDetails();
    assertionSummaryDetails.setUrn(urn);
    if (info.getType() == AssertionType.CUSTOM && info.getCustomAssertion() != null) {
      assertionSummaryDetails.setType(info.getCustomAssertion().getType());
    } else {
      assertionSummaryDetails.setType(info.getType().toString());
    }

    assertionSummaryDetails.setLastResultAt(event.getTimestampMillis());
    if (info.hasSource()) {
      assertionSummaryDetails.setSource(info.getSource().getType().toString());
    }
    return assertionSummaryDetails;
  }

  /**
   * Returns true if the event should be processed, which is only true if the change is on the
   * assertion status aspect
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isAssertionSoftDeleted(event)
        || isAssertionTargetEntityChanged(event)
        || isAssertionRunCompleteEvent(event)
        || isMonitorDisabledEvent(event)
        || isAssertionInfoHardDeleted(event)
        || isAssertionKeyHardDeleted(event);
  }

  /** Returns true if an assertion is being soft-deleted. */
  private boolean isAssertionSoftDeleted(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && isSoftDeletionEvent(event);
  }

  /** Returns true if an assertion is being hard-deleted. */
  private boolean isAssertionInfoHardDeleted(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && ChangeType.DELETE.equals(event.getChangeType())
        && ASSERTION_INFO_ASPECT_NAME.equals(event.getAspectName());
  }

  /** Returns true if an assertion is being hard-deleted. */
  private boolean isAssertionKeyHardDeleted(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && ChangeType.DELETE.equals(event.getChangeType())
        && ASSERTION_KEY_ASPECT_NAME.equals(event.getAspectName());
  }

  /** Returns true if an assertion is being updated to point to another entity. */
  private boolean isAssertionTargetEntityChanged(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && isAssertionTargetEntityChangeEvent(event);
  }

  private boolean isSoftDeletionEvent(@Nonnull final MetadataChangeLog event) {
    if (STATUS_ASPECT_NAME.equals(event.getAspectName()) && event.getAspect() != null) {
      final Status status =
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(), event.getAspect().getContentType(), Status.class);
      return status.hasRemoved() && status.isRemoved();
    }
    return false;
  }

  private boolean isAssertionTargetEntityChangeEvent(@Nonnull final MetadataChangeLog event) {
    if (ASSERTION_INFO_ASPECT_NAME.equals(event.getAspectName()) && event.getAspect() != null) {
      // If there is no previous version, this isn't an update to the URN.
      if (event.getPreviousAspectValue() == null) {
        return false;
      }
      final AssertionInfo info =
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              AssertionInfo.class);

      final AssertionInfo prevInfo =
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              AssertionInfo.class);
      final List<Urn> prevEntityUrns = extractAssertionEntities(prevInfo);
      final List<Urn> newEntityUrns = extractAssertionEntities(info);
      return !prevEntityUrns.equals(newEntityUrns);
    }
    return false;
  }

  private boolean isMonitorDisabledEvent(@Nonnull final MetadataChangeLog event) {
    if (MONITOR_INFO_ASPECT_NAME.equals(event.getAspectName()) && event.getAspect() != null) {
      final MonitorInfo status =
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(), event.getAspect().getContentType(), MonitorInfo.class);
      return ImmutableSet.of(MonitorMode.INACTIVE, MonitorMode.PASSIVE)
          .contains(status.getStatus().getMode());
    }
    return false;
  }

  /**
   * Returns true if the event represents an assertion run event, which may mean a status change.
   */
  private boolean isAssertionRunCompleteEvent(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && SUPPORTED_UPDATE_ASPECTS.contains(event.getAspectName())
        && isAssertionRunCompleted(event);
  }

  private boolean isAssertionRunCompleted(@Nonnull final MetadataChangeLog event) {
    final AssertionRunEvent runEvent = extractAssertionRunEvent(event);
    return AssertionRunStatus.COMPLETE.equals(runEvent.getStatus()) && runEvent.hasResult();
  }

  @Nonnull
  private AssertionRunEvent extractAssertionRunEvent(@Nonnull final MetadataChangeLog event) {
    return GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(), event.getAspect().getContentType(), AssertionRunEvent.class);
  }

  @Nonnull
  private MonitorInfo extractMonitorInfo(@Nonnull final MetadataChangeLog event) {
    return GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(), event.getAspect().getContentType(), MonitorInfo.class);
  }

  private AssertionsSummaryPatchBuilder buildRemoveAssertionFromSummaryPatch(
      @Nonnull final Urn assertionUrn, @Nonnull final Urn entityUrn) {
    return new AssertionsSummaryPatchBuilder()
        .urn(entityUrn)
        .withEntityName(entityUrn.getEntityType())
        .removeFromFailingAssertionDetails(assertionUrn)
        .removeFromPassingAssertionDetails(assertionUrn)
        .removeFromErroringAssertionDetails(assertionUrn)
        .removeFromInitializingAssertionDetails(assertionUrn);
  }

  private AssertionsSummaryPatchBuilder buildAssertionSuccessSummaryPatch(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionSummaryDetails details,
      @Nonnull final Urn entityUrn) {
    return new AssertionsSummaryPatchBuilder()
        .urn(entityUrn)
        .withEntityName(entityUrn.getEntityType())
        .addPassingAssertionDetails(details)
        .removeFromFailingAssertionDetails(assertionUrn)
        .removeFromErroringAssertionDetails(assertionUrn)
        .removeFromInitializingAssertionDetails(assertionUrn);
  }

  private AssertionsSummaryPatchBuilder buildAssertionFailureSummaryPatch(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionSummaryDetails details,
      @Nonnull final Urn entityUrn) {
    return new AssertionsSummaryPatchBuilder()
        .urn(entityUrn)
        .withEntityName(entityUrn.getEntityType())
        .addFailingAssertionDetails(details)
        .removeFromPassingAssertionDetails(assertionUrn)
        .removeFromErroringAssertionDetails(assertionUrn)
        .removeFromInitializingAssertionDetails(assertionUrn);
  }

  private AssertionsSummaryPatchBuilder buildAssertionErrorSummaryPatch(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionSummaryDetails details,
      @Nonnull final Urn entityUrn) {
    return new AssertionsSummaryPatchBuilder()
        .urn(entityUrn)
        .withEntityName(entityUrn.getEntityType())
        .addErroringAssertionDetails(details)
        .removeFromPassingAssertionDetails(assertionUrn)
        .removeFromFailingAssertionDetails(assertionUrn)
        .removeFromInitializingAssertionDetails(assertionUrn);
  }

  private AssertionsSummaryPatchBuilder buildAssertionInitSummaryPatch(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionSummaryDetails details,
      @Nonnull final Urn entityUrn) {
    return new AssertionsSummaryPatchBuilder()
        .urn(entityUrn)
        .withEntityName(entityUrn.getEntityType())
        .addInitializingAssertionDetails(details)
        .removeFromPassingAssertionDetails(assertionUrn)
        .removeFromFailingAssertionDetails(assertionUrn)
        .removeFromErroringAssertionDetails(assertionUrn);
  }

  /** Patches the assertions summary for a given entity */
  private void patchAssertionSummary(
      @Nonnull final Urn entityUrn, @Nonnull final AssertionsSummaryPatchBuilder patchBuilder) {
    try {
      assertionService.patchAssertionsSummary(systemOperationContext, patchBuilder);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to patch assertions summary for entity with urn %s! Skipping updating the summary",
              entityUrn),
          e);
    }
  }
}
