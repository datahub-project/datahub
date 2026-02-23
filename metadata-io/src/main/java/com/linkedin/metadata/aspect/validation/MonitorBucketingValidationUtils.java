package com.linkedin.metadata.aspect.validation;

import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionTimeBucketingStrategy;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.DatasetVolumeSourceType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.timeseries.CalendarInterval;
import javax.annotation.Nullable;

/** Shared bucketing validation utilities for monitor validation paths. */
public final class MonitorBucketingValidationUtils {

  private MonitorBucketingValidationUtils() {}

  @Nullable
  public static String validateBucketedVolumeSourceType(@Nullable MonitorInfo monitorInfo) {
    if (monitorInfo == null
        || !monitorInfo.hasAssertionMonitor()
        || monitorInfo.getAssertionMonitor() == null
        || !monitorInfo.getAssertionMonitor().hasAssertions()
        || monitorInfo.getAssertionMonitor().getAssertions() == null
        || monitorInfo.getAssertionMonitor().getAssertions().isEmpty()) {
      return null;
    }

    AssertionEvaluationSpec spec = monitorInfo.getAssertionMonitor().getAssertions().get(0);
    if (!spec.hasParameters() || spec.getParameters() == null) {
      return null;
    }
    AssertionEvaluationParameters parameters = spec.getParameters();
    if (!parameters.hasDatasetVolumeParameters()
        || parameters.getDatasetVolumeParameters() == null) {
      return null;
    }

    return validateBucketedVolumeSourceType(parameters.getDatasetVolumeParameters());
  }

  @Nullable
  public static String validateSingleBucketInterval(@Nullable MonitorInfo monitorInfo) {
    if (monitorInfo == null
        || !monitorInfo.hasAssertionMonitor()
        || monitorInfo.getAssertionMonitor() == null
        || !monitorInfo.getAssertionMonitor().hasAssertions()
        || monitorInfo.getAssertionMonitor().getAssertions() == null
        || monitorInfo.getAssertionMonitor().getAssertions().isEmpty()) {
      return null;
    }

    AssertionEvaluationSpec spec = monitorInfo.getAssertionMonitor().getAssertions().get(0);
    if (!spec.hasParameters() || spec.getParameters() == null) {
      return null;
    }
    AssertionEvaluationParameters parameters = spec.getParameters();
    if (parameters.hasDatasetVolumeParameters()
        && parameters.getDatasetVolumeParameters() != null) {
      return validateSingleBucketInterval(
          parameters.getDatasetVolumeParameters().getTimeBucketingStrategy());
    }
    if (parameters.hasDatasetFieldParameters() && parameters.getDatasetFieldParameters() != null) {
      return validateSingleBucketInterval(
          parameters.getDatasetFieldParameters().getTimeBucketingStrategy());
    }
    return null;
  }

  @Nullable
  public static String validateSingleBucketInterval(
      @Nullable AssertionTimeBucketingStrategy strategy) {
    if (strategy == null
        || !strategy.hasBucketInterval()
        || strategy.getBucketInterval() == null
        || !strategy.getBucketInterval().hasMultiple()) {
      return null;
    }
    if (strategy.getBucketInterval().getMultiple() != 1) {
      return "timeBucketingStrategy.bucketInterval.multiple currently supports value 1 only.";
    }
    return null;
  }

  @Nullable
  public static String validateBucketedVolumeSourceType(
      @Nullable DatasetVolumeAssertionParameters volumeParams) {
    if (volumeParams == null || !volumeParams.hasTimeBucketingStrategy()) {
      return null;
    }
    var strategy = volumeParams.getTimeBucketingStrategy();
    if (strategy == null) {
      return null;
    }
    if (!volumeParams.hasSourceType()
        || volumeParams.getSourceType() != DatasetVolumeSourceType.QUERY) {
      return "Volume assertions with timeBucketingStrategy must use sourceType=QUERY.";
    }
    var gracePeriod = strategy.getLateArrivalGracePeriod();
    if (strategy.hasLateArrivalGracePeriod()
        && gracePeriod != null
        && gracePeriod.hasUnit()
        && gracePeriod.getUnit() != CalendarInterval.DAY) {
      return "lateArrivalGracePeriod.unit currently supports DAY only.";
    }
    return null;
  }
}
