package com.linkedin.metadata.kafka;

import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Transport-neutral view of an inbound metadata message. Kafka fills this today; a future pgQueue
 * consumer can populate the same fields (including optional queue identifiers) before calling
 * shared processors.
 */
@Value
@Builder(builderClassName = "Builder", toBuilder = true)
public class InboundMetadataEnvelope<R> {

  @Nonnull String logicalTopic;

  /** {@link MetricUtils#MESSAGING_SYSTEM_KAFKA} or {@link MetricUtils#MESSAGING_SYSTEM_PGQUEUE}. */
  @Nonnull String messagingSystem;

  @Nullable String key;

  @Nonnull R payload;

  long enqueuedAtMillis;

  @Nullable String consumerGroupId;

  @Nullable Integer kafkaPartition;

  @Nullable Long kafkaOffset;

  @Nullable Integer serializedValueSize;

  @Nullable Long topicId;

  @Nullable Integer partitionId;

  @Nullable Long enqueueSeq;

  @Nullable Integer priority;

  @Nullable Long messageRowId;

  /** Epoch millis of {@code enqueued_at} when distinct from {@link #enqueuedAtMillis}. */
  @Nullable Long messageEnqueuedAtMillis;

  @Nullable String contentType;

  @Nullable Long consumerOffsetEpoch;

  public static <R> InboundMetadataEnvelope<R> fromKafka(
      @Nonnull ConsumerRecord<String, R> record, @Nullable String consumerGroupId) {
    return InboundMetadataEnvelope.<R>builder()
        .logicalTopic(record.topic())
        .messagingSystem(MetricUtils.MESSAGING_SYSTEM_KAFKA)
        .key(record.key())
        .payload(record.value())
        .enqueuedAtMillis(record.timestamp())
        .consumerGroupId(consumerGroupId)
        .kafkaPartition(record.partition())
        .kafkaOffset(record.offset())
        .serializedValueSize(record.serializedValueSize())
        .build();
  }

  /**
   * Builds an envelope from a pgQueue {@link QueueReceivedMessage} after Avro payload decoding
   * (same logical fields as Kafka consumer records where applicable).
   */
  public static <R> InboundMetadataEnvelope<R> fromPgQueue(
      @Nonnull QueueReceivedMessage message,
      @Nonnull String logicalTopic,
      @Nullable String consumerGroupId,
      @Nonnull R payload) {
    var h = message.handle();
    return InboundMetadataEnvelope.<R>builder()
        .logicalTopic(logicalTopic)
        .messagingSystem(MetricUtils.MESSAGING_SYSTEM_PGQUEUE)
        .key(message.routingKey())
        .payload(payload)
        .enqueuedAtMillis(h.enqueuedAt().toEpochMilli())
        .consumerGroupId(consumerGroupId)
        .kafkaPartition(h.partitionId())
        .kafkaOffset(h.enqueueSeq())
        .serializedValueSize(message.payload().length)
        .topicId(h.topicId())
        .partitionId(h.partitionId())
        .enqueueSeq(h.enqueueSeq())
        .priority(message.priority())
        .messageRowId(h.id())
        .messageEnqueuedAtMillis(h.enqueuedAt().toEpochMilli())
        .build();
  }
}
