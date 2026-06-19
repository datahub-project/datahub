package com.linkedin.metadata.messaging;

import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ConsumerGroupLagSnapshot {
  String consumerGroupId;

  @Builder.Default Map<String, TopicLagSnapshot> topics = Collections.emptyMap();
}
