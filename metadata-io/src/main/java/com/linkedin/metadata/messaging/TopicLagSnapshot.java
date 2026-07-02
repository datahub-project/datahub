package com.linkedin.metadata.messaging;

import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TopicLagSnapshot {
  @javax.annotation.Nullable Map<String, PartitionLagSnapshot> partitions;

  @Builder.Default Optional<LagMetricsSnapshot> metrics = Optional.empty();
}
