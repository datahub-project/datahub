package com.linkedin.metadata.aspect.consistency.scan;

import javax.annotation.Nonnull;
import lombok.Value;

/** Outcome of handling one check batch (e.g. applying fixes). */
@Value
public class BatchHandleResult {
  int fixed;
  int failed;

  @Nonnull
  public static BatchHandleResult none() {
    return new BatchHandleResult(0, 0);
  }

  @Nonnull
  public static BatchHandleResult of(int fixed, int failed) {
    return new BatchHandleResult(fixed, failed);
  }
}
