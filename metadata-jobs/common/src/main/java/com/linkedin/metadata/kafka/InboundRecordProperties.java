package com.linkedin.metadata.kafka;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Per-record transport metadata paired with a synthetic or real {@link ConsumerRecord}. */
@Value
@Builder
public class InboundRecordProperties {

  long enqueuedAtMillis;

  @Nonnull String messagingSystem;

  @Nullable Integer priority;

  public static InboundRecordProperties fromKafka(@Nonnull ConsumerRecord<?, ?> record) {
    return InboundRecordProperties.builder()
        .enqueuedAtMillis(record.timestamp())
        .messagingSystem(MetricUtils.MESSAGING_SYSTEM_KAFKA)
        .build();
  }

  public static InboundRecordProperties fromPgQueue(long enqueuedAtMillis, int priority) {
    return InboundRecordProperties.builder()
        .enqueuedAtMillis(enqueuedAtMillis)
        .messagingSystem(MetricUtils.MESSAGING_SYSTEM_PGQUEUE)
        .priority(priority)
        .build();
  }
}
