package com.linkedin.gms.factory.kafka.trace;

import com.linkedin.metadata.trace.TraceConsumerPool;
import com.linkedin.metadata.trace.TraceConsumerPoolExhaustedException;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.event.kafka.CheckedConsumer;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class KafkaTraceConsumerPool implements TraceConsumerPool {

  private final KafkaConsumerPool pool;
  private final long borrowTimeoutMs;
  private final String poolType;
  private final AtomicInteger activeBorrows = new AtomicInteger(0);

  public KafkaTraceConsumerPool(
      @Nonnull KafkaConsumerPool pool,
      long borrowTimeoutMs,
      @Nonnull String poolType,
      @Nullable MetricUtils metricUtils) {
    this.pool = pool;
    this.borrowTimeoutMs = borrowTimeoutMs;
    this.poolType = poolType;
    registerMetrics(metricUtils);
  }

  @Override
  public <T> T withConsumer(@Nonnull String topic, @Nonnull TraceConsumerAction<T> action) {
    CheckedConsumer checkedConsumer = borrowConsumer(topic);
    activeBorrows.incrementAndGet();
    try {
      return action.execute(checkedConsumer.getConsumer());
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      activeBorrows.decrementAndGet();
      pool.returnConsumer(checkedConsumer);
    }
  }

  @Override
  public void shutdown() {
    pool.shutdownPool();
  }

  KafkaConsumerPool getDelegate() {
    return pool;
  }

  private CheckedConsumer borrowConsumer(String topic) {
    try {
      CheckedConsumer checkedConsumer =
          pool.borrowConsumer(borrowTimeoutMs, TimeUnit.MILLISECONDS, topic);
      if (checkedConsumer == null) {
        throw new TraceConsumerPoolExhaustedException(
            "No trace Kafka consumer available for pool '"
                + poolType
                + "' within "
                + borrowTimeoutMs
                + "ms");
      }
      return checkedConsumer;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TraceConsumerPoolExhaustedException(
          "Interrupted while borrowing trace Kafka consumer for pool '" + poolType + "'");
    }
  }

  private void registerMetrics(@Nullable MetricUtils metricUtils) {
    if (metricUtils == null) {
      return;
    }
    MeterRegistry registry = metricUtils.getRegistry();
    if (registry == null) {
      return;
    }
    Gauge.builder("trace_kafka_consumer_pool_active", activeBorrows, AtomicInteger::get)
        .tag("type", poolType)
        .description("Trace Kafka consumer pool active borrows")
        .register(registry);
    Gauge.builder(
            "trace_kafka_consumer_pool_total_created",
            pool.getTotalConsumersCreated(),
            AtomicInteger::get)
        .tag("type", poolType)
        .description("Trace Kafka consumer pool total consumers created")
        .register(registry);
    Gauge.builder(
            "trace_kafka_consumer_pool_available", this, KafkaTraceConsumerPool::availableConsumers)
        .tag("type", poolType)
        .description("Trace Kafka consumer pool idle consumers (live minus active borrows)")
        .register(registry);
  }

  private double availableConsumers() {
    return Math.max(0, pool.getTotalConsumersCreated().get() - activeBorrows.get());
  }

  /** Package-private for unit tests. */
  double getAvailableConsumerCount() {
    return availableConsumers();
  }
}
