package com.linkedin.metadata.dao.retention;

import lombok.Value;

/**
 * A retention policy that retains a fixed number of most recent versions of metadata aspects.
 */
@Value
public class VersionBasedRetention implements Retention {

  /**
   * Constructs a {@link VersionBasedRetention} object
   *
   * @param maxVersionsToRetain maximal number of versions to retain. Must be greater than 0.
   */
  public VersionBasedRetention(long maxVersionsToRetain) {
    if (maxVersionsToRetain < 1L) {
      throw new IllegalArgumentException("maxVersionToRetain must be greater than 0");
    }
    this.maxVersionsToRetain = maxVersionsToRetain;
  }

  long maxVersionsToRetain;
}
