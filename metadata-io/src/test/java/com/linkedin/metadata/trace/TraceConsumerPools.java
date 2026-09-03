package com.linkedin.metadata.trace;

import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;

/** Test helper that runs trace actions against a fixed mock consumer. */
public final class TraceConsumerPools {

  private TraceConsumerPools() {}

  public static TraceConsumerPool singleConsumer(
      @Nonnull Consumer<String, GenericRecord> consumer) {
    return new NoCloseTraceConsumerPool(consumer);
  }

  /** Reuses the same consumer without calling {@link Consumer#close()} on return. */
  private static final class NoCloseTraceConsumerPool implements TraceConsumerPool {

    private final Consumer<String, GenericRecord> consumer;

    private NoCloseTraceConsumerPool(Consumer<String, GenericRecord> consumer) {
      this.consumer = consumer;
    }

    @Override
    public <T> T withConsumer(@Nonnull String topic, @Nonnull TraceConsumerAction<T> action) {
      try {
        return action.execute(consumer);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void shutdown() {}
  }
}
