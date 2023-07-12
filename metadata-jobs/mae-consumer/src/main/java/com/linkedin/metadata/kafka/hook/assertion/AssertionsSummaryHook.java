package com.linkedin.metadata.kafka.hook.assertion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.service.AssertionsSummaryUtils.*;


/**
 * This hook is responsible for maintaining the AssertionsSummary.pdl aspect of entities
 * on which Assertions may be raised. It handles both assertion updates and assertion soft deletions
 * to ensure that this aspect reflects the latest state of the assertion.
 *
 * Hard deletes of assertions are not handled within this hook because the expectation is that deleteReferences will be invoked
 * to clean up references.
 */
@Slf4j
@Component
@Singleton
@Import({EntityRegistryFactory.class, AssertionServiceFactory.class, SystemAuthenticationFactory.class})
public class AssertionsSummaryHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES = ImmutableSet.of(ChangeType.UPSERT, ChangeType.CREATE, ChangeType.RESTATE);
  private static final Set<String> SUPPORTED_UPDATE_ASPECTS = ImmutableSet.of(ASSERTION_RUN_EVENT_ASPECT_NAME);

  private final EntityRegistry _entityRegistry;
  private final AssertionService _assertionService;
  private final boolean _isEnabled;

  @Autowired
  public AssertionsSummaryHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final AssertionService assertionService,
      @Nonnull @Value("${assertions.hook.enabled:true}") Boolean isEnabled
  ) {
    _entityRegistry = Objects.requireNonNull(entityRegistry, "entityRegistry is required");
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
    _isEnabled = isEnabled;
  }

  @Override
  public void init() {
    System.out.println("Initialized the assertions summary hook");
  }

  @Override
  public boolean isEnabled() {
    return _isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (_isEnabled && isEligibleForProcessing(event)) {
      log.debug("Urn {} received by Assertion Summary Hook.", event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, _entityRegistry);
      // Handle the deletion case.
      if (isAssertionSoftDeleted(event)) {
        handleAssertionSoftDeleted(urn);
      } else if (isAssertionRunCompleteEvent(event)) {
        handleAssertionRunCompleted(urn, extractAssertionRunEvent(event));
      }
      // TODO: Handle un-soft-deleted by adding it back into the result list.
    }
  }

  /**
   * Handles an assertion deletion by removing the assertion from either resolved or active assertions.
   */
  private void handleAssertionSoftDeleted(@Nonnull final Urn assertionUrn) {
    // 1. Fetch assertion info.
    AssertionInfo assertionInfo = _assertionService.getAssertionInfo(assertionUrn);

    // 2. Retrieve associated urns.
    if (assertionInfo != null) {
      final List<Urn> assertionEntities = extractAssertionEntities(assertionInfo);

      if (assertionEntities.size() <= 0) {
        log.warn(String.format(
            "Failed to find entities associated with assertion with urn %s. Skipping updating run summaries...", assertionUrn));
        return;
      }

      // 3. For each urn, resolve the entity assertions aspect and remove from failing and passing assertions.
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
   * Handle an assertion update by adding to either resolved or active assertions for an entity.
   */
  private void handleAssertionRunCompleted(@Nonnull final Urn assertionUrn, @Nonnull final AssertionRunEvent runEvent) {

    // 1. Fetch assertion info.
    AssertionInfo assertionInfo = _assertionService.getAssertionInfo(assertionUrn);

    // 2. Retrieve associated urns.
    if (assertionInfo != null) {

      // If this assertion is system generated, we avoid placing it in the summary.
      // This is because system assertions are used a) to recommend assertions and b) to generate anomalies.
      // Users will not expect to see system assertions appearing in the normal custom assertions summary.
      if (isInferredAssertion(assertionInfo)) {
        log.debug(String.format("Skipping adding result of inferred assertion with urn %s to summary", assertionUrn));
        return;
      }

      final List<Urn> assertionEntities = extractAssertionEntities(assertionInfo);

      if (assertionEntities.size() <= 0) {
        log.warn(String.format(
            "Failed to find entities associated with assertion with urn %s. Skipping updating run summaries...", assertionUrn));
        return;
      }

      // 3. For each urn, resolve the entity assertions aspect and add to failing or passing assertions.
      for (Urn entityUrn : assertionEntities) {
        addAssertionToSummary(entityUrn, assertionUrn, assertionInfo, runEvent);
      }
    } else {
      log.warn(
          String.format(
              "Failed to find assertionInfo aspect for assertion with urn %s. Skipping updating assertion summary for related assertions!",
              assertionUrn));
    }
  }

  /**
   * Removes an assertion to the AssertionSummary aspect for a related entity.
   */
  private void removeAssertionFromSummary(@Nonnull final Urn assertionUrn, @Nonnull final Urn entityUrn) {
    // 1. Fetch the latest assertion summary for the entity
    AssertionsSummary summary = getAssertionsSummary(entityUrn);

    // 2. Remove the assertion from failing and passing summary
    removeAssertionFromFailingSummary(assertionUrn, summary);
    removeAssertionFromPassingSummary(assertionUrn, summary);

    // 3. Emit the change back!
    updateAssertionSummary(entityUrn, summary);
  }

  /**
   * Adds an assertion to the AssertionSummary aspect for a related entity.
   * This is used to search for entity by active and resolved assertions.
   */
  private void addAssertionToSummary(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionInfo info,
      @Nonnull final AssertionRunEvent event) {
    // 1. Fetch the latest assertion summary for the entity
    AssertionsSummary summary = getAssertionsSummary(entityUrn);
    AssertionResult result = event.getResult();
    AssertionSummaryDetails details = buildAssertionSummaryDetails(assertionUrn, info, event);

    // 2. Add the assertion to passing or failing assertions
    if (AssertionResultType.SUCCESS.equals(result.getType())) {
      // First, ensure this isn't in failing anymore.
      removeAssertionFromFailingSummary(assertionUrn, summary);
      // Then, add to passing.
      addAssertionToPassingSummary(details, summary);

    } else if (AssertionResultType.FAILURE.equals(result.getType())) {
      // First, ensure this isn't in passing anymore.
      removeAssertionFromPassingSummary(assertionUrn, summary);
      // Then, add to failing.
      addAssertionToFailingSummary(details, summary);
    }

    // 3. Emit the change back!
    updateAssertionSummary(entityUrn, summary);
  }

  @Nonnull
  private AssertionsSummary getAssertionsSummary(@Nonnull final Urn entityUrn) {
    AssertionsSummary maybeAssertionsSummary = _assertionService.getAssertionsSummary(entityUrn);
    return maybeAssertionsSummary == null
        ? new AssertionsSummary().setFailingAssertionDetails(new AssertionSummaryDetailsArray()).setPassingAssertionDetails(new AssertionSummaryDetailsArray())
        : maybeAssertionsSummary;
  }

  @Nonnull
  private AssertionSummaryDetails buildAssertionSummaryDetails(
      @Nonnull final Urn urn,
      @Nonnull final AssertionInfo info,
      @Nonnull final AssertionRunEvent event) {
    AssertionSummaryDetails assertionSummaryDetails = new AssertionSummaryDetails();
    assertionSummaryDetails.setUrn(urn);
    assertionSummaryDetails.setType(info.getType().toString());
    assertionSummaryDetails.setLastResultAt(event.getTimestampMillis());
    if (info.hasSource()) {
      assertionSummaryDetails.setSource(info.getSource().getType().toString());
    }
    return assertionSummaryDetails;
  }

  /**
   * Returns true if the event should be processed, which is only true if the change is on the assertion status aspect
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isAssertionSoftDeleted(event) || isAssertionRunCompleteEvent(event);
  }

  /**
   * Returns true if an assertion is being soft-deleted.
   */
  private boolean isAssertionSoftDeleted(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && isSoftDeletionEvent(event);
  }

  private boolean isSoftDeletionEvent(@Nonnull final MetadataChangeLog event) {
    if (STATUS_ASPECT_NAME.equals(event.getAspectName()) && event.getAspect() != null) {
      final Status status = GenericRecordUtils.deserializeAspect(
          event.getAspect().getValue(),
          event.getAspect().getContentType(),
          Status.class);
      return status.hasRemoved() && status.isRemoved();
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
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        AssertionRunEvent.class);
  }

  @Nonnull
  private List<Urn> extractAssertionEntities(@Nonnull final AssertionInfo assertionInfo) {
    if (assertionInfo.hasDatasetAssertion()) {
      DatasetAssertionInfo datasetAssertion = assertionInfo.getDatasetAssertion();
      if (datasetAssertion.hasDataset()) {
        return ImmutableList.of(datasetAssertion.getDataset());
      }
    }
    if (assertionInfo.hasFreshnessAssertion()) {
      FreshnessAssertionInfo freshnessAssertion = assertionInfo.getFreshnessAssertion();
      if (freshnessAssertion.hasEntity()) {
        return ImmutableList.of(freshnessAssertion.getEntity());
      }
    }
    return Collections.emptyList();
  }

  /**
   * Updates the assertions summary for a given entity
   */
  private void updateAssertionSummary(@Nonnull final Urn entityUrn, @Nonnull final AssertionsSummary newSummary) {
    try {
      _assertionService.updateAssertionsSummary(entityUrn, newSummary);
    } catch (Exception e) {
      log.error(
          String.format("Failed to updated assertions summary for entity with urn %s! Skipping updating the summary", entityUrn), e);
    }
  }

  /**
   * Returns true if we are dealing with a system-inferred assertions, which we typically exclude from the normal
   * Assertions summary to avoid confusing users.
   */
  private boolean isInferredAssertion(@Nonnull final AssertionInfo assertionInfo) {
    return assertionInfo.hasSource() && AssertionSourceType.INFERRED.equals(assertionInfo.getSource().getType());
  }
}
