package com.linkedin.metadata.messaging;

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PartitionLagSnapshot {
  long offset;
  Long lag;

  /** Committed offset minus log tail; positive when STUCK_AHEAD (offset ahead of message log). */
  @Nullable Long aheadBy;

  @Nullable String metadata;
}
