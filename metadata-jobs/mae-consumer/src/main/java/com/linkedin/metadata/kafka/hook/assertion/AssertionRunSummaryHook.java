package com.linkedin.metadata.kafka.hook.assertion;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionStatus;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.aspect.patch.builder.AssertionRunSummaryPatchBuilder;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.AssertionStatusUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
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
 * This hook is responsible for generating a summary on each Assertion event that details
 * information like it's last passing time, last error time, and last failure time.
 */
@Slf4j
@Component
@Import({
  EntityRegistryFactory.class,
  AssertionServiceFactory.class,
  SystemAuthenticationFactory.class
})
public class AssertionRunSummaryHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.CREATE, ChangeType.RESTATE);
  private static final Set<String> SUPPORTED_UPDATE_ASPECTS =
      ImmutableSet.of(ASSERTION_RUN_EVENT_ASPECT_NAME);
  private static final Set<AssertionResultType> SUPPORTED_RESULT_TYPES =
      ImmutableSet.of(
          AssertionResultType.SUCCESS,
          AssertionResultType.FAILURE,
          AssertionResultType.ERROR,
          AssertionResultType.INIT);

  private final AssertionService assertionService;
  private final boolean isEnabled;

  @Getter private final String consumerGroupSuffix;

  @Autowired
  public AssertionRunSummaryHook(
      @Nonnull final AssertionService assertionService,
      @Nonnull @Value("${assertions.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${assertions.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.assertionService =
        Objects.requireNonNull(assertionService, "assertionService is required");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public AssertionRunSummaryHook(
      @Nonnull final AssertionService assertionService, @Nonnull Boolean isEnabled) {
    this(assertionService, isEnabled, "");
  }

  @Override
  public AssertionRunSummaryHook init(@Nonnull OperationContext systemOperationContext) {
    log.info("Initialized the assertion run summary hook");
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(
      @Nonnull final OperationContext operationContext, @Nonnull final MetadataChangeLog event) {
    if (isEnabled && isEligibleForProcessing(event)) {
      log.debug("Urn {} received by Assertion Run Summary Hook.", event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, operationContext.getEntityRegistry());
      handleAssertionRunCompleted(operationContext, urn, extractAssertionRunEvent(event));
    }
  }

  /**
   * Records the run's timestamp under its result type if it is newer than the one stored, then
   * derives the assertion status from the latest timestamp across all result types. Run events can
   * arrive out of order (e.g. backfilled results), so the incoming event's result alone does not
   * decide the status.
   */
  private void handleAssertionRunCompleted(
      @Nonnull final OperationContext operationContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent) {
    final AssertionResult result = runEvent.getResult();

    if (result == null) {
      log.warn("Skipping assertion run event with null result for assertion urn {}", assertionUrn);
      return;
    }

    final AssertionResultType resultType = result.getType();
    if (!SUPPORTED_RESULT_TYPES.contains(resultType)) {
      log.warn(
          "Skipping assertion run event with unsupported result type {} for assertion urn {}",
          resultType,
          assertionUrn);
      return;
    }

    final AssertionRunSummary existingSummary =
        assertionService.getAssertionRunSummary(operationContext, assertionUrn);
    final AssertionRunSummary summary =
        existingSummary != null ? copySummary(existingSummary) : new AssertionRunSummary();
    final AssertionStatus storedStatus = summary.getAssertionStatus();

    final Long lastAtForType = getLastAtMillis(summary, resultType);
    final boolean isNewerForType =
        lastAtForType == null || runEvent.getTimestampMillis() > lastAtForType;

    final AssertionRunSummaryPatchBuilder patchBuilder = new AssertionRunSummaryPatchBuilder();
    patchBuilder.urn(assertionUrn);
    if (isNewerForType) {
      setLastAtMillis(summary, patchBuilder, resultType, runEvent.getTimestampMillis());
    }

    final AssertionStatus nextStatus = AssertionStatusUtils.resolveStatus(false, summary);
    final boolean isStatusOutOfDate = nextStatus != null && !nextStatus.equals(storedStatus);

    // A stale event still repairs a status left inconsistent with the stored timestamps, so
    // re-sending a run event recomputes the status.
    if (isNewerForType || (storedStatus != null && isStatusOutOfDate)) {
      if (nextStatus != null) {
        patchBuilder.setAssertionStatus(nextStatus.name());
      }
      emitAssertionSummaryPatch(operationContext, patchBuilder);
    }
  }

  @Nullable
  private static Long getLastAtMillis(
      @Nonnull final AssertionRunSummary summary, @Nonnull final AssertionResultType resultType) {
    switch (resultType) {
      case SUCCESS:
        return summary.getLastPassedAtMillis();
      case FAILURE:
        return summary.getLastFailedAtMillis();
      case ERROR:
        return summary.getLastErroredAtMillis();
      case INIT:
        return summary.getLastInitializedAtMillis();
      default:
        throw new IllegalStateException(
            String.format("Unhandled assertion result type %s", resultType));
    }
  }

  private static void setLastAtMillis(
      @Nonnull final AssertionRunSummary summary,
      @Nonnull final AssertionRunSummaryPatchBuilder patchBuilder,
      @Nonnull final AssertionResultType resultType,
      final long timestampMillis) {
    switch (resultType) {
      case SUCCESS:
        summary.setLastPassedAtMillis(timestampMillis);
        patchBuilder.setLastPassedAt(timestampMillis);
        break;
      case FAILURE:
        summary.setLastFailedAtMillis(timestampMillis);
        patchBuilder.setLastFailedAt(timestampMillis);
        break;
      case ERROR:
        summary.setLastErroredAtMillis(timestampMillis);
        patchBuilder.setLastErroredAt(timestampMillis);
        break;
      case INIT:
        summary.setLastInitializedAtMillis(timestampMillis);
        patchBuilder.setLastInitializedAt(timestampMillis);
        break;
      default:
        throw new IllegalStateException(
            String.format("Unhandled assertion result type %s", resultType));
    }
  }

  @Nonnull
  private static AssertionRunSummary copySummary(@Nonnull final AssertionRunSummary summary) {
    try {
      return summary.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Failed to copy assertion run summary", e);
    }
  }

  private void emitAssertionSummaryPatch(
      @Nonnull final OperationContext operationContext,
      @Nonnull final AssertionRunSummaryPatchBuilder patchBuilder) {
    try {
      assertionService.patchAssertionRunSummary(operationContext, patchBuilder);
    } catch (Exception e) {
      log.error("Failed to patch assertion run summary! Skipping updating the summary", e);
    }
  }

  /**
   * Returns true if the event should be processed, which is only true if the change is on the
   * assertion status aspect
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isAssertionRunCompleteEvent(event);
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
}
