package com.linkedin.metadata.dao.retention;

import lombok.Value;


/**
 * A retention policy that retains metadata aspects created within a fixed time.
 */
@Value
public class TimeBasedRetention implements Retention {

  /**
   * Constructs a {@link TimeBasedRetention} object
   *
   * @param maxAgeToRetain maximal age (in milliseconds) to retain. Must be positive.
   */
  public TimeBasedRetention(long maxAgeToRetain) {
    if (maxAgeToRetain <= 0) {
      throw new IllegalArgumentException("maxAgeToRetain must be positive");
    }
    this.maxAgeToRetain = maxAgeToRetain;
  }

  long maxAgeToRetain;
}
