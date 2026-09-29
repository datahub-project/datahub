package com.linkedin.metadata.aspect.consistency.scan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Emitted once before the batch loop after attempting a total count. */
@Value
@Builder
public class ConsistencyScanStart {

  @Nonnull String entityType;

  /** Matching SM doc count when available; null when count failed or ETA ineligible. */
  @Nullable Long totalEstimate;

  boolean etaEnabled;
}
