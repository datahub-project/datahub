package com.linkedin.metadata.event;

import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Enqueues DataHub usage / analytics JSON events to pgQueue logical topics (same topic names as
 * Kafka usage publishing).
 */
public final class PgQueueUsageEventPublisher implements UsageEventPublisher {

  public static final String JSON_CONTENT_TYPE = PgQueueGenericProducer.JSON_CONTENT_TYPE;

  private final PgQueueGenericProducer genericProducer;

  public PgQueueUsageEventPublisher(
      @Nonnull MetadataQueueStore metadataQueueStore,
      @Nullable PgQueueSetupOptions pgQueueOptions,
      @Nonnull QueueTopicDefaults topicDefaultsFallback,
      @Nonnull PgQueuePayloadCompression payloadCompression) {
    this.genericProducer =
        new PgQueueGenericProducer(
            metadataQueueStore, pgQueueOptions, topicDefaultsFallback, payloadCompression);
  }

  public PgQueueUsageEventPublisher(@Nonnull PgQueueGenericProducer genericProducer) {
    this.genericProducer = genericProducer;
  }

  @Override
  public void setWritable(boolean writable) {
    genericProducer.setWritable(writable);
  }

  @Nonnull
  @Override
  public Future<?> publish(
      @Nonnull OperationContext opContext,
      @Nonnull String topic,
      @Nullable String key,
      @Nonnull String payload) {
    // pgQueue currently has no header/context channel, so opContext cannot be propagated yet.
    return genericProducer.send(new ProducerRecord<>(topic, key, payload), null);
  }

  @Override
  public void flush() {
    genericProducer.flush();
  }
}
