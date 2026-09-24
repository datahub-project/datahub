package com.linkedin.metadata.pgqueue;

import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

/** Per-poll helpers shared by all pgQueue worker instantiations. */
public final class PgQueuePollContext {

  private final MetadataQueueStore store;
  @Getter private final String consumerGroupId;
  @Getter private final java.time.Duration visibilityTimeout;
  private final Deserializer<GenericRecord> avroDeserializer;

  public PgQueuePollContext(
      @Nonnull MetadataQueueStore store,
      @Nonnull String consumerGroupId,
      @Nonnull java.time.Duration visibilityTimeout,
      @Nullable Deserializer<GenericRecord> avroDeserializer) {
    this.store = store;
    this.consumerGroupId = consumerGroupId;
    this.visibilityTimeout = visibilityTimeout;
    this.avroDeserializer = avroDeserializer;
  }

  public void commit(@Nonnull List<QueueMessageHandle> handles) {
    if (!handles.isEmpty()) {
      store.commitForGroup(consumerGroupId, handles, true);
    }
  }

  @Nonnull
  public GenericRecord decodeAvro(
      @Nonnull QueueReceivedMessage message, @Nonnull String topicNameForSerde) {
    if (avroDeserializer == null) {
      throw new IllegalStateException(
          "pgQueue Avro deserializer is not configured for this worker");
    }
    return PgQueueAvroDecode.decodeRecord(message, avroDeserializer, topicNameForSerde);
  }

  @Nonnull
  public String decodeUtf8(@Nonnull QueueReceivedMessage message) {
    return PgQueueStringDecode.decodeAsUtf8(message);
  }
}
