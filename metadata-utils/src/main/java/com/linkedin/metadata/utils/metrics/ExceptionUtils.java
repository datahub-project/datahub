package com.linkedin.metadata.utils.metrics;

import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;

public class ExceptionUtils {
  private static final String BASE_NAME = "metadata.validation.exception";
  private static final String DELIMITER = ".";

  private ExceptionUtils() {}

  public static ValidationExceptionCollection collectMetrics(
      final ValidationExceptionCollection exceptions) {
    exceptions
        .streamAllExceptions()
        .forEach(
            exception -> {
              String subTypeBaseName =
                  String.join(
                      DELIMITER, BASE_NAME, exception.getSubType().toString().toLowerCase());
              // subtype count
              MetricUtils.counter(subTypeBaseName).inc(exceptions.size());
              // Change type count
              MetricUtils.counter(
                      String.join(
                          DELIMITER,
                          subTypeBaseName,
                          exception.getChangeType().toString().toLowerCase()))
                  .inc();
              // Entity count
              MetricUtils.counter(
                      String.join(
                          DELIMITER,
                          subTypeBaseName,
                          exception.getEntityUrn().getEntityType().toLowerCase()))
                  .inc();
              // Aspect count
              MetricUtils.counter(
                      String.join(DELIMITER, subTypeBaseName, exception.getAspectName()))
                  .inc();
            });
    return exceptions;
  }
}
