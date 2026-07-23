package com.linkedin.metadata.utils.metrics;

import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;

public class ExceptionUtils {
  private static final String BASE_NAME = "metadata.validation.exception";
  private static final String DELIMITER = ".";

  private ExceptionUtils() {}

  public static ValidationExceptionCollection collectMetrics(
      MetricUtils metricUtils, final ValidationExceptionCollection exceptions) {
    if (metricUtils != null) {
      exceptions
          .streamAllExceptions()
          .forEach(
              exception -> {
                String subTypeBaseName =
                    String.join(
                        DELIMITER, BASE_NAME, exception.getSubType().toString().toLowerCase());
                // subtype count
                metricUtils.increment(subTypeBaseName, exceptions.size());
                // Change type count
                metricUtils.increment(
                    String.join(
                        DELIMITER,
                        subTypeBaseName,
                        exception.getChangeType().toString().toLowerCase()),
                    1);
                // Entity count
                metricUtils.increment(
                    String.join(
                        DELIMITER,
                        subTypeBaseName,
                        exception.getEntityUrn().getEntityType().toLowerCase()),
                    1);
                // Aspect count
                metricUtils.increment(
                    String.join(DELIMITER, subTypeBaseName, exception.getAspectName()), 1);
              });
    }
    return exceptions;
  }
}
