package com.linkedin.metadata.utils;

import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionStatus;
import com.linkedin.monitor.MonitorError;
import javax.annotation.Nullable;

public final class AssertionStatusUtils {

  private AssertionStatusUtils() {}

  @Nullable
  public static AssertionStatus resolveStatus(
      @Nullable final MonitorError monitorError, @Nullable final AssertionResultType resultType) {
    if (isMonitorError(monitorError)) {
      return AssertionStatus.ERROR;
    }
    return mapRunResultToStatus(resultType);
  }

  @Nullable
  public static AssertionStatus resolveStatus(
      @Nullable final MonitorError monitorError, @Nullable final AssertionRunSummary runSummary) {
    if (isMonitorError(monitorError)) {
      return AssertionStatus.ERROR;
    }
    return getLatestEvaluationStatus(runSummary);
  }

  @Nullable
  public static AssertionStatus mapRunResultToStatus(
      @Nullable final AssertionResultType resultType) {
    if (resultType == null) {
      return null;
    }
    switch (resultType) {
      case SUCCESS:
        return AssertionStatus.PASSING;
      case FAILURE:
        return AssertionStatus.FAILING;
      case ERROR:
        return AssertionStatus.ERROR;
      case INIT:
        return AssertionStatus.INIT;
      default:
        throw new IllegalStateException(
            String.format("Unhandled assertion result type %s", resultType));
    }
  }

  @Nullable
  public static AssertionStatus getLatestEvaluationStatus(
      @Nullable final AssertionRunSummary runSummary) {
    if (runSummary == null) {
      return null;
    }
    long lastEvaluationMillis = 0L;
    AssertionStatus latestStatus = null;
    if (runSummary.hasLastFailedAtMillis()) {
      lastEvaluationMillis = runSummary.getLastFailedAtMillis();
      latestStatus = AssertionStatus.FAILING;
    }
    if (runSummary.hasLastErroredAtMillis()
        && runSummary.getLastErroredAtMillis() > lastEvaluationMillis) {
      lastEvaluationMillis = runSummary.getLastErroredAtMillis();
      latestStatus = AssertionStatus.ERROR;
    }
    if (runSummary.hasLastPassedAtMillis()
        && runSummary.getLastPassedAtMillis() > lastEvaluationMillis) {
      lastEvaluationMillis = runSummary.getLastPassedAtMillis();
      latestStatus = AssertionStatus.PASSING;
    }
    if (runSummary.hasLastInitializedAtMillis()
        && runSummary.getLastInitializedAtMillis() > lastEvaluationMillis) {
      latestStatus = AssertionStatus.INIT;
    }
    return latestStatus;
  }

  public static boolean isMonitorError(@Nullable final MonitorError error) {
    return AssertionHealthUtils.mapMonitorErrorToStatus(error)
        != AssertionHealthUtils.MonitorHealthStatus.HEALTHY;
  }
}
