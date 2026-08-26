package com.linkedin.metadata.trace;

import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;

/** Test helper that runs trace actions against a fixed mock consumer. */
public final class TraceConsumerPools {

  private TraceConsumerPools() {}

  public static TraceConsumerPool singleConsumer(
      @Nonnull Consumer<String, GenericRecord> consumer) {
    return new EphemeralTraceConsumerPool(() -> consumer);
  }
}
