package com.linkedin.metadata.kafka.hook.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import javax.annotation.Nonnull;

public class AnomalyUtils {

  /**
   * Generates a human-readable description of an Anomaly based on information about the Assertion
   * Failure that triggered it.
   */
  public static String generateAnomalyDescription(
      @Nonnull final AssertionInfo info, @Nonnull final AssertionRunEvent runEvent) {
    return null;
  }

  private AnomalyUtils() {}
}
