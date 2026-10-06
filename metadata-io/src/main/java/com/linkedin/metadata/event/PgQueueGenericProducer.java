package com.linkedin.metadata.event;

import static com.linkedin.metadata.Constants.READ_ONLY_LOG;

import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Enqueues UTF-8 JSON payloads to pgQueue logical topics. Used by OTEL usage export ({@link
 * GenericProducer}) with the same topic names as Kafka usage publishing.
 */
@Slf4j
public final class PgQueueGenericProducer implements GenericProducer<String> {

  public static final String JSON_CONTENT_TYPE = "application/json";

  private final MetadataQueueStore metadataQueueStore;
  private final PgQueueSetupOptions pgQueueOptions;
  private final QueueTopicDefaults topicDefaultsFallback;
  private final PgQueuePayloadCompression payloadCompression;

  private boolean canWrite = true;

  public PgQueueGenericProducer(
      @Nonnull MetadataQueueStore metadataQueueStore,
      @Nullable PgQueueSetupOptions pgQueueOptions,
      @Nonnull QueueTopicDefaults topicDefaultsFallback,
      @Nonnull PgQueuePayloadCompression payloadCompression) {
    this.metadataQueueStore = metadataQueueStore;
    this.pgQueueOptions = pgQueueOptions;
    this.topicDefaultsFallback = topicDefaultsFallback;
    this.payloadCompression = payloadCompression;
  }

  @Override
  public void setWritable(boolean writable) {
    canWrite = writable;
  }

  @Override
  public Future<?> send(
      ProducerRecord<String, String> producerRecord, @Nullable Callback callback) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(null);
    }
    String topic = producerRecord.topic();
    String key = producerRecord.key();
    String payload = producerRecord.value() != null ? producerRecord.value() : "";
    try {
      byte[] inner = payload.getBytes(StandardCharsets.UTF_8);
      byte[] stored = PgQueuePayloadCodec.encode(inner, payloadCompression);
      QueueTopicDefaults defaults = effectiveDefaults(topic);
      metadataQueueStore.enqueue(
          topic,
          key != null ? key : "",
          defaults,
          QueueTopicMetadata.DEFAULT_PRIORITY,
          stored,
          Optional.of(JSON_CONTENT_TYPE),
          List.of(),
          payloadCompression);
      // pgQueue has no async broker ack; invoke callback as success when provided.
      if (callback != null) {
        callback.onCompletion(null, null);
      }
      return CompletableFuture.completedFuture(null);
    } catch (RuntimeException e) {
      log.error("Failed to enqueue message to pgQueue topic {}", topic, e);
      if (callback != null) {
        callback.onCompletion(null, e);
      }
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public void flush() {
    // Synchronous enqueue; nothing to flush.
  }

  @Nonnull
  private QueueTopicDefaults effectiveDefaults(@Nonnull String topicName) {
    if (pgQueueOptions != null) {
      return QueueTopicDefaults.resolveForTopic(pgQueueOptions, topicName);
    }
    return topicDefaultsFallback;
  }
}
