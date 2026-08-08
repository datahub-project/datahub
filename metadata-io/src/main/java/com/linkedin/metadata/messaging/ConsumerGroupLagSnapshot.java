package com.linkedin.metadata.messaging;

import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

/** Lag for one consumer group, keyed by topic. Empty topics means the group reported nothing. */
@Value
@Builder
public class ConsumerGroupLagSnapshot {
  String consumerGroupId;

  @Builder.Default Map<String, TopicLagSnapshot> topics = Collections.emptyMap();
}
