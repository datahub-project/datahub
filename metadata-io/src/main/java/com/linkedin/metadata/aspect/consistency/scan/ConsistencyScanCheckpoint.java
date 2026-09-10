package com.linkedin.metadata.aspect.consistency.scan;

import com.linkedin.metadata.utils.progress.ProgressSnapshot;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Cumulative counters and progress for silent checkpoint persistence. */
@Value
@Builder
public class ConsistencyScanCheckpoint {

  @Nonnull String entityType;

  /** Next scroll id; null when the current batch was the last. */
  @Nullable String scrollId;

  long entitiesScanned;
  int issuesFound;
  int issuesFixed;
  int issuesFailed;

  @Nonnull ProgressSnapshot progress;
}
