package com.linkedin.metadata.pgqueue;

import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

/** Decode pgQueue stored payloads (compression + Confluent Avro wire) for consumer poll loops. */
public final class PgQueueAvroDecode {

  private PgQueueAvroDecode() {}

  @Nonnull
  public static GenericRecord decodeRecord(
      @Nonnull QueueReceivedMessage message,
      @Nonnull Deserializer<GenericRecord> deserializer,
      @Nonnull String topicNameForSerde) {
    byte[] wire = PgQueuePayloadCodec.decode(message.payload(), message.payloadCompression());
    return deserializer.deserialize(topicNameForSerde, wire);
  }
}
