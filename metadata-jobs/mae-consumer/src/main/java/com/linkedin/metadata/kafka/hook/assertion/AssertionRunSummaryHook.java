package com.linkedin.metadata.kafka.hook.assertion;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.aspect.patch.builder.AssertionRunSummaryPatchBuilder;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
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

  private final AssertionService assertionService;
  private final boolean isEnabled;

  private OperationContext systemOperationContext;
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
    this.systemOperationContext = systemOperationContext;
    log.info("Initialized the assertion run summary hook");
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (isEnabled && isEligibleForProcessing(event)) {
      log.debug("Urn {} received by Assertion Run Summary Hook.", event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, systemOperationContext.getEntityRegistry());
      handleAssertionRunCompleted(urn, extractAssertionRunEvent(event));
    }
  }

  /** Handle an assertion update by adding to either resolved or active assertions for an entity. */
  private void handleAssertionRunCompleted(
      @Nonnull final Urn assertionUrn, @Nonnull final AssertionRunEvent runEvent) {
    AssertionRunSummary assertionRunSummary =
        assertionService.getAssertionRunSummary(systemOperationContext, assertionUrn);

    // If there isn't yet a summary aspect, add one.
    if (assertionRunSummary == null) {
      assertionRunSummary = new AssertionRunSummary();
    }

    AssertionResult result = runEvent.getResult();

    if (result == null) {
      log.warn("Skipping assertion run event with null result for assertion urn {}", assertionUrn);
      return;
    }

    switch (result.getType()) {
      case SUCCESS:
        updateAssertionSummaryWithPass(assertionUrn, runEvent, assertionRunSummary);
        break;
      case FAILURE:
        updateAssertionSummaryWithFail(assertionUrn, runEvent, assertionRunSummary);
        break;
      case ERROR:
        updateAssertionSummaryWithError(assertionUrn, runEvent, assertionRunSummary);
        break;
      default:
        log.warn(
            "Skipping assertion run event with unsupported result type {} for assertion urn {}",
            result.getType(),
            assertionUrn);
    }
  }

  private void updateAssertionSummaryWithPass(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionRunSummary assertionSummary) {

    AssertionRunSummaryPatchBuilder patchBuilder = new AssertionRunSummaryPatchBuilder();
    patchBuilder.urn(assertionUrn);

    Long maybeLastPassedAt = assertionSummary.getLastPassedAtMillis();

    // Now, compare the timestamp on the new event with the timestamp on the existing summary.
    // If the new event is more recent, update the summary.
    if (maybeLastPassedAt == null || runEvent.getTimestampMillis() > maybeLastPassedAt) {
      System.out.println(String.format("Last passed at millis is %s", maybeLastPassedAt));
      patchBuilder.setLastPassedAt(runEvent.getTimestampMillis());
      // And also emit.
      emitAssertionSummaryPatch(patchBuilder);
    }
  }

  private AssertionRunSummaryPatchBuilder updateAssertionSummaryWithFail(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionRunSummary assertionSummary) {

    AssertionRunSummaryPatchBuilder patchBuilder = new AssertionRunSummaryPatchBuilder();
    patchBuilder.urn(assertionUrn);

    Long maybeLastFailedAt = assertionSummary.getLastFailedAtMillis();

    // Now, compare the timestamp on the new event with the timestamp on the existing summary.
    // If the new event is more recent, update the summary.
    if (maybeLastFailedAt == null || runEvent.getTimestampMillis() > maybeLastFailedAt) {
      patchBuilder.setLastFailedAt(runEvent.getTimestampMillis());
      // And also emit.
      emitAssertionSummaryPatch(patchBuilder);
    }

    return patchBuilder;
  }

  private AssertionRunSummaryPatchBuilder updateAssertionSummaryWithError(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionRunSummary assertionSummary) {

    AssertionRunSummaryPatchBuilder patchBuilder = new AssertionRunSummaryPatchBuilder();
    patchBuilder.urn(assertionUrn);

    Long maybeLastErroredAt = assertionSummary.getLastErroredAtMillis();

    // Now, compare the timestamp on the new event with the timestamp on the existing summary.
    // If the new event is more recent, update the summary.
    if (maybeLastErroredAt == null || runEvent.getTimestampMillis() > maybeLastErroredAt) {
      patchBuilder.setLastErroredAt(runEvent.getTimestampMillis());
      // And also emit.
      emitAssertionSummaryPatch(patchBuilder);
    }

    return patchBuilder;
  }

  private void emitAssertionSummaryPatch(
      @Nonnull final AssertionRunSummaryPatchBuilder patchBuilder) {
    try {
      assertionService.patchAssertionRunSummary(systemOperationContext, patchBuilder);
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
