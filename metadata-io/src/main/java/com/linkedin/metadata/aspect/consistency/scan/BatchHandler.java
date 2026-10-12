package com.linkedin.metadata.aspect.consistency.scan;

import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import javax.annotation.Nonnull;

/** Handles a check batch and returns fix counters. */
@FunctionalInterface
public interface BatchHandler {
  @Nonnull
  BatchHandleResult handle(@Nonnull CheckResult result);
}
