package com.linkedin.metadata.trace;

import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;

/** Creates a new Kafka consumer per borrow. Used when the shared pool is disabled for rollback. */
public final class EphemeralTraceConsumerPool implements TraceConsumerPool {

  private final Supplier<Consumer<String, GenericRecord>> consumerSupplier;

  public EphemeralTraceConsumerPool(
      @Nonnull Supplier<Consumer<String, GenericRecord>> consumerSupplier) {
    this.consumerSupplier = consumerSupplier;
  }

  @Override
  public <T> T withConsumer(@Nonnull String topic, @Nonnull TraceConsumerAction<T> action) {
    try (Consumer<String, GenericRecord> consumer = consumerSupplier.get()) {
      return executeAction(action, consumer);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {}

  private static <T> T executeAction(
      TraceConsumerAction<T> action, Consumer<String, GenericRecord> consumer) throws Exception {
    try {
      return action.execute(consumer);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw e;
    }
  }
}
