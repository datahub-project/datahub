package com.linkedin.metadata.queue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** STUCK_AHEAD: committed consumer offset is ahead of the message log for one grid cell. */
@Value
@Builder(toBuilder = true)
public class PartitionOffsetSkew {
  @Nonnull String consumerGroup;
  long topicId;
  @Nullable String topicName;
  int partitionId;
  long committedOffset;
  long maxSeq;
  long aheadBy;
}
