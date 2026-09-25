package com.linkedin.metadata.queue;

import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** Result of {@link MetadataQueueStore#resetConsumerOffsets(ConsumerOffsetResetSpec)}. */
@Value
@Builder
public class ConsumerOffsetResetReport {
  int partitionsUpdated;

  @Nonnull @Builder.Default List<ConsumerOffsetResetDetail> resets = List.of();
}
