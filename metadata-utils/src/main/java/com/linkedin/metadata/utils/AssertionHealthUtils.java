package com.linkedin.metadata.utils;

import com.linkedin.monitor.MonitorError;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class AssertionHealthUtils {

  public enum MonitorHealthStatus {
    HEALTHY,
    DEGRADED,
    ERROR,
    UNKNOWN
  }

  private AssertionHealthUtils() {}

  @Nonnull
  public static MonitorHealthStatus mapMonitorErrorToStatus(@Nullable final MonitorError error) {
    if (error == null) {
      return MonitorHealthStatus.HEALTHY;
    }
    if (!error.hasType()) {
      return MonitorHealthStatus.UNKNOWN;
    }
    switch (error.getType()) {
      case INVALID_PARAMETERS:
      case UNKNOWN:
        return MonitorHealthStatus.ERROR;
      case TRAINING_DATA_INSUFFICIENT:
      case PREDICTION_NOT_CONFIDENT:
        return MonitorHealthStatus.HEALTHY;
      case INPUT_DATA_INVALID:
      case INPUT_DATA_INSUFFICIENT:
      case MODEL_CREATION_FAILED:
      case MODEL_TRAINING_FAILED:
      case MODEL_EVALUATION_FAILED:
      case PREDICTION_FORMAT_ERROR:
      case PERSISTENCE_FAILED:
        return MonitorHealthStatus.DEGRADED;
      default:
        throw new IllegalStateException(
            String.format("Unhandled monitor error type %s", error.getType()));
    }
  }
}
