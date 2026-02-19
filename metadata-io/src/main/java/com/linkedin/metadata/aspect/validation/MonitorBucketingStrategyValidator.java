package com.linkedin.metadata.aspect.validation;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionTimeBucketingStrategy;
import com.linkedin.monitor.MonitorInfo;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Prevents breaking modifications to the bucketing strategy of an assertion monitor. Changing
 * {@code timestampFieldPath}, {@code bucketInterval}, or {@code timezone} would invalidate existing
 * metrics-cube data, so those fields are treated as immutable once set. {@code
 * lateArrivalGracePeriod} only affects evaluation timing and may be freely changed.
 *
 * <p>This is a temporary safeguard until proper metrics-cube versioning is implemented.
 *
 * <p>Note: equivalent validation is also applied in {@code MonitorService.upsertAssertionMonitor}
 * for earlier feedback. This validator acts as a safety net at the MCP pipeline level.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class MonitorBucketingStrategyValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (ChangeMCP changeMCP : changeMCPs) {
      if (changeMCP.getPreviousSystemAspect() == null) {
        continue;
      }

      MonitorInfo oldMonitorInfo = changeMCP.getPreviousAspect(MonitorInfo.class);
      MonitorInfo newMonitorInfo = changeMCP.getAspect(MonitorInfo.class);

      if (oldMonitorInfo == null || newMonitorInfo == null) {
        continue;
      }

      AssertionTimeBucketingStrategy oldStrategy = extractBucketingStrategy(oldMonitorInfo);
      AssertionTimeBucketingStrategy newStrategy = extractBucketingStrategy(newMonitorInfo);

      if (oldStrategy == null) {
        continue;
      }

      if (newStrategy == null) {
        exceptions.addException(
            AspectValidationException.forItem(
                changeMCP,
                "Cannot remove the bucketing strategy from a monitor that already has one. "
                    + "Removing it would invalidate existing metrics-cube data."));
        continue;
      }

      validateImmutableFields(changeMCP, oldStrategy, newStrategy, exceptions);
    }

    return exceptions.streamAllExceptions();
  }

  /**
   * Navigates MonitorInfo -> AssertionMonitor -> assertions[0] -> parameters ->
   * datasetVolumeParameters/datasetFieldParameters -> timeBucketingStrategy
   */
  @Nullable
  private static AssertionTimeBucketingStrategy extractBucketingStrategy(MonitorInfo monitorInfo) {
    if (!monitorInfo.hasAssertionMonitor() || monitorInfo.getAssertionMonitor() == null) {
      return null;
    }
    if (!monitorInfo.getAssertionMonitor().hasAssertions()
        || monitorInfo.getAssertionMonitor().getAssertions() == null
        || monitorInfo.getAssertionMonitor().getAssertions().isEmpty()) {
      return null;
    }

    AssertionEvaluationSpec spec = monitorInfo.getAssertionMonitor().getAssertions().get(0);
    if (!spec.hasParameters() || spec.getParameters() == null) {
      return null;
    }

    AssertionEvaluationParameters params = spec.getParameters();

    if (params.hasDatasetVolumeParameters() && params.getDatasetVolumeParameters() != null) {
      if (params.getDatasetVolumeParameters().hasTimeBucketingStrategy()) {
        return params.getDatasetVolumeParameters().getTimeBucketingStrategy();
      }
    }

    if (params.hasDatasetFieldParameters() && params.getDatasetFieldParameters() != null) {
      if (params.getDatasetFieldParameters().hasTimeBucketingStrategy()) {
        return params.getDatasetFieldParameters().getTimeBucketingStrategy();
      }
    }

    return null;
  }

  private static void validateImmutableFields(
      ChangeMCP changeMCP,
      AssertionTimeBucketingStrategy oldStrategy,
      AssertionTimeBucketingStrategy newStrategy,
      ValidationExceptionCollection exceptions) {

    if (!Objects.equals(oldStrategy.getTimestampFieldPath(), newStrategy.getTimestampFieldPath())) {
      exceptions.addException(
          AspectValidationException.forItem(
              changeMCP,
              String.format(
                  "Changing the timestampFieldPath of an existing bucketing strategy is not currently supported. "
                      + "(was '%s', attempted '%s').",
                  oldStrategy.getTimestampFieldPath(), newStrategy.getTimestampFieldPath())));
    }

    if (!Objects.equals(oldStrategy.getBucketInterval(), newStrategy.getBucketInterval())) {
      exceptions.addException(
          AspectValidationException.forItem(
              changeMCP,
              String.format(
                  "Changing the bucketInterval of an existing bucketing strategy is not currently supported. "
                      + "(was '%s', attempted '%s').",
                  oldStrategy.getBucketInterval(), newStrategy.getBucketInterval())));
    }

    if (!Objects.equals(oldStrategy.getTimezone(), newStrategy.getTimezone())) {
      exceptions.addException(
          AspectValidationException.forItem(
              changeMCP,
              String.format(
                  "Changing the timezone of an existing bucketing strategy is not currently supported. "
                      + "(was '%s', attempted '%s').",
                  oldStrategy.getTimezone(), newStrategy.getTimezone())));
    }
  }
}
