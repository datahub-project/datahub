package com.linkedin.metadata.queue;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** One partition whose consumer offset was reset. */
@Value
@Builder
public class ConsumerOffsetResetDetail {
  @Nonnull String consumerGroup;
  @Nonnull String topicName;
  int partitionId;
  long previousOffset;
  long newOffset;
  long maxSeq;
}
