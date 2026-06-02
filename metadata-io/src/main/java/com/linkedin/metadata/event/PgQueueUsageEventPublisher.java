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

/**
 * Enqueues DataHub usage / analytics JSON events to pgQueue logical topics (same topic names as
 * Kafka usage publishing).
 */
@Slf4j
public final class PgQueueUsageEventPublisher implements UsageEventPublisher {

  public static final String JSON_CONTENT_TYPE = "application/json";

  private final MetadataQueueStore metadataQueueStore;
  private final PgQueueSetupOptions pgQueueOptions;
  private final QueueTopicDefaults topicDefaultsFallback;
  private final PgQueuePayloadCompression payloadCompression;

  private boolean canWrite = true;

  public PgQueueUsageEventPublisher(
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

  @Nonnull
  @Override
  public Future<?> publish(@Nonnull String topic, @Nullable String key, @Nonnull String payload) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(null);
    }
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
      return CompletableFuture.completedFuture(null);
    } catch (RuntimeException e) {
      log.error("Failed to enqueue usage event to pgQueue topic {}", topic, e);
      return CompletableFuture.failedFuture(e);
    }
  }

  @Nonnull
  private QueueTopicDefaults effectiveDefaults(@Nonnull String topicName) {
    if (pgQueueOptions != null) {
      return QueueTopicDefaults.resolveForTopic(pgQueueOptions, topicName);
    }
    return topicDefaultsFallback;
  }

  @Override
  public void flush() {
    // Synchronous enqueue; nothing to flush.
  }
}
