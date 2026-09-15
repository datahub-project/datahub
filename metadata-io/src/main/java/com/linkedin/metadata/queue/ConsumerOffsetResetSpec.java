package com.linkedin.metadata.queue;

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Parameters for {@link MetadataQueueStore#resetConsumerOffsets(ConsumerOffsetResetSpec)}. */
@Value
@Builder
public class ConsumerOffsetResetSpec {
  @Nullable String consumerGroup;
  @Nullable String topicName;
  @Nullable Integer partitionId;

  /** When true (default), only reset partitions where committed offset is ahead of the log. */
  @Builder.Default boolean onlyStuckAhead = true;
}
