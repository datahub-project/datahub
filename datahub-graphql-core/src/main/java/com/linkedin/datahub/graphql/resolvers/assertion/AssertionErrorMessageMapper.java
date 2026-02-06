package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.metadata.utils.AssertionErrorMessageUtils;
import javax.annotation.Nullable;

public final class AssertionErrorMessageMapper {

  private AssertionErrorMessageMapper() {}

  @Nullable
  public static String displayMessageForEvaluationError(
      @Nullable com.linkedin.datahub.graphql.generated.AssertionResultErrorType type) {
    return AssertionErrorMessageUtils.displayMessageForEvaluationError(
        toModelAssertionResultErrorType(type));
  }

  @Nullable
  public static String recommendedActionForEvaluationError(
      @Nullable com.linkedin.datahub.graphql.generated.AssertionResultErrorType type) {
    return AssertionErrorMessageUtils.recommendedActionForEvaluationError(
        toModelAssertionResultErrorType(type));
  }

  @Nullable
  public static String displayMessageForMonitorError(
      @Nullable com.linkedin.datahub.graphql.generated.MonitorErrorType type) {
    return AssertionErrorMessageUtils.displayMessageForMonitorError(toModelMonitorErrorType(type));
  }

  @Nullable
  public static String recommendedActionForMonitorError(
      @Nullable com.linkedin.datahub.graphql.generated.MonitorErrorType type) {
    return AssertionErrorMessageUtils.recommendedActionForMonitorError(
        toModelMonitorErrorType(type));
  }

  @Nullable
  private static com.linkedin.assertion.AssertionResultErrorType toModelAssertionResultErrorType(
      @Nullable com.linkedin.datahub.graphql.generated.AssertionResultErrorType type) {
    if (type == null) {
      return null;
    }
    try {
      return com.linkedin.assertion.AssertionResultErrorType.valueOf(type.name());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  @Nullable
  private static com.linkedin.monitor.MonitorErrorType toModelMonitorErrorType(
      @Nullable com.linkedin.datahub.graphql.generated.MonitorErrorType type) {
    if (type == null) {
      return null;
    }
    try {
      return com.linkedin.monitor.MonitorErrorType.valueOf(type.name());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
