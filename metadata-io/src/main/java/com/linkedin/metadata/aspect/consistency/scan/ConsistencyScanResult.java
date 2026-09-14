package com.linkedin.metadata.aspect.consistency.scan;

import com.linkedin.metadata.utils.progress.ProgressSnapshot;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Final result of a consistency scan over one entity type. */
@Value
@Builder
public class ConsistencyScanResult {

  @Nonnull String entityType;
  long entitiesScanned;
  int issuesFound;
  int issuesFixed;
  int issuesFailed;

  @Nullable Long totalEstimate;

  @Nonnull ProgressSnapshot finalProgress;

  /** True when the scan stopped early due to thread interrupt during an inter-batch delay. */
  @Builder.Default boolean cancelled = false;
}
