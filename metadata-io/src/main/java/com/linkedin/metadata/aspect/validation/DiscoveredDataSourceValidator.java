package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DISCOVERED_DATA_SOURCE_DECISION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DISCOVERED_DATA_SOURCE_EVIDENCE_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceDecision;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidence;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidenceEntry;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceStatus;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceTier;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Enforces the recommendation lifecycle of a discovered data source and keeps its aspects bounded.
 * Runs pre-commit, so PATCHes are checked against the latest stored aspect.
 */
@Setter
@Getter
@Accessors(chain = true)
public class DiscoveredDataSourceValidator extends AspectPayloadValidator {
  static final int MAX_SAMPLE_URNS = 10;
  static final int MAX_EVIDENCE_ENTRIES = 500;
  static final int MAX_HISTORY_EVENTS = 100;

  /**
   * Moving back to NEW is how a source is shown again: a declined source whose tier rose, a snooze
   * that ended, an abandoned setup, or a connected source whose ingestion source was deleted.
   */
  private static final Map<DiscoveredDataSourceStatus, Set<DiscoveredDataSourceStatus>>
      ALLOWED_TRANSITIONS =
          ImmutableMap.<DiscoveredDataSourceStatus, Set<DiscoveredDataSourceStatus>>builder()
              .put(
                  DiscoveredDataSourceStatus.NEW,
                  Set.of(
                      DiscoveredDataSourceStatus.PRESENTED, DiscoveredDataSourceStatus.CONNECTED))
              .put(
                  DiscoveredDataSourceStatus.PRESENTED,
                  Set.of(
                      DiscoveredDataSourceStatus.ACCEPTED,
                      DiscoveredDataSourceStatus.DECLINED,
                      DiscoveredDataSourceStatus.SNOOZED,
                      DiscoveredDataSourceStatus.CONNECTED,
                      DiscoveredDataSourceStatus.NEW))
              .put(
                  DiscoveredDataSourceStatus.ACCEPTED,
                  Set.of(
                      DiscoveredDataSourceStatus.CONNECTED,
                      DiscoveredDataSourceStatus.DECLINED,
                      DiscoveredDataSourceStatus.NEW))
              .put(
                  DiscoveredDataSourceStatus.DECLINED,
                  Set.of(DiscoveredDataSourceStatus.NEW, DiscoveredDataSourceStatus.CONNECTED))
              .put(
                  DiscoveredDataSourceStatus.SNOOZED,
                  Set.of(
                      DiscoveredDataSourceStatus.NEW,
                      DiscoveredDataSourceStatus.PRESENTED,
                      DiscoveredDataSourceStatus.CONNECTED))
              .put(DiscoveredDataSourceStatus.CONNECTED, Set.of(DiscoveredDataSourceStatus.NEW))
              .build();

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (ChangeMCP item : changeMCPs) {
      if (ChangeType.DELETE.equals(item.getChangeType())) {
        continue;
      }
      String error = null;
      if (DISCOVERED_DATA_SOURCE_DECISION_ASPECT_NAME.equals(item.getAspectName())) {
        error =
            validateDecision(
                item.getAspect(DiscoveredDataSourceDecision.class),
                item.getPreviousRecordTemplate() == null
                    ? null
                    : item.getPreviousAspect(DiscoveredDataSourceDecision.class));
      } else if (DISCOVERED_DATA_SOURCE_EVIDENCE_ASPECT_NAME.equals(item.getAspectName())) {
        error = validateEvidence(item.getAspect(DiscoveredDataSourceEvidence.class));
      }
      if (error != null) {
        exceptions.addException(item, error);
      }
    }
    return exceptions.streamAllExceptions();
  }

  @Nullable
  static String validateDecision(
      @Nullable DiscoveredDataSourceDecision proposed,
      @Nullable DiscoveredDataSourceDecision previous) {
    if (proposed == null) {
      return null;
    }
    DiscoveredDataSourceStatus status = proposed.getStatus();
    if (previous != null && previous.getStatus() != status) {
      Set<DiscoveredDataSourceStatus> allowed =
          ALLOWED_TRANSITIONS.getOrDefault(previous.getStatus(), Set.of());
      if (!allowed.contains(status)) {
        return String.format(
            "Invalid discovered data source status transition: %s => %s",
            previous.getStatus(), status);
      }
    }
    if (status == DiscoveredDataSourceStatus.DECLINED && !proposed.hasTierAtDecision()) {
      return "tierAtDecision is required when status is DECLINED";
    }
    if (status == DiscoveredDataSourceStatus.SNOOZED && !proposed.hasSnoozeUntil()) {
      return "snoozeUntil is required when status is SNOOZED";
    }
    if (proposed.getHistory().size() > MAX_HISTORY_EVENTS) {
      return String.format(
          "history has %d events; at most %d are allowed, trim the oldest",
          proposed.getHistory().size(), MAX_HISTORY_EVENTS);
    }
    return null;
  }

  @Nullable
  static String validateEvidence(@Nullable DiscoveredDataSourceEvidence evidence) {
    if (evidence == null) {
      return null;
    }
    if (evidence.getEvidence().size() > MAX_EVIDENCE_ENTRIES) {
      return String.format(
          "evidence has %d entries; at most %d are allowed",
          evidence.getEvidence().size(), MAX_EVIDENCE_ENTRIES);
    }
    for (Map.Entry<String, DiscoveredDataSourceEvidenceEntry> entry :
        evidence.getEvidence().entrySet()) {
      DiscoveredDataSourceEvidenceEntry value = entry.getValue();
      if (value.getTier() == DiscoveredDataSourceTier.CORROBORATED) {
        return String.format(
            "evidence entry %s: CORROBORATED is derived and cannot be reported", entry.getKey());
      }
      if (value.getSampleUrns().size() > MAX_SAMPLE_URNS) {
        return String.format(
            "evidence entry %s has %d sample urns; at most %d are allowed",
            entry.getKey(), value.getSampleUrns().size(), MAX_SAMPLE_URNS);
      }
    }
    return null;
  }
}
