package com.linkedin.metadata.dao.producer;

import static com.linkedin.metadata.Constants.READ_ONLY_LOG;

import com.linkedin.metadata.dao.producer.context.outbound.OutboundContextResolver;
import com.linkedin.metadata.event.UsageEventPublisher;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class KafkaUsageEventPublisher implements UsageEventPublisher {
  private final Producer<String, String> producer;
  private final KafkaHealthChecker kafkaHealthChecker;
  private final MetricUtils metricUtils;
  private final OutboundContextResolver outboundContextResolver;
  private boolean canWrite = true;

  public KafkaUsageEventPublisher(
      @Nonnull Producer<String, String> producer,
      @Nonnull KafkaHealthChecker kafkaHealthChecker,
      @Nonnull MetricUtils metricUtils,
      @Nonnull OutboundContextResolver outboundContextResolver) {
    this.producer = producer;
    this.kafkaHealthChecker = kafkaHealthChecker;
    this.metricUtils = metricUtils;
    this.outboundContextResolver = outboundContextResolver;
  }

  @Override
  public void setWritable(boolean writable) {
    canWrite = writable;
  }

  @Override
  @Nonnull
  public Future<?> publish(
      @Nonnull OperationContext opContext,
      @Nonnull String topic,
      @Nullable String key,
      @Nonnull String payload) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(Optional.empty());
    }
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
    outboundContextResolver.apply(record, opContext);
    Callback callback =
        kafkaHealthChecker.getKafkaCallBack(
            metricUtils, "USAGE", key != null ? key : StringUtils.EMPTY);
    return producer.send(record, callback);
  }

  @Override
  public void flush() {
    producer.flush();
  }
}
