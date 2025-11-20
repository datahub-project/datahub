package com.linkedin.metadata.dao.producer;

import static com.linkedin.metadata.Constants.READ_ONLY_LOG;

import com.linkedin.metadata.event.GenericProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class GenericProducerImpl<T> implements GenericProducer<T> {
  private final Producer<String, T> producer;
  private final KafkaHealthChecker kafkaHealthChecker;
  private final MetricUtils metricUtils;
  private boolean canWrite = true;

  public GenericProducerImpl(
      Producer<String, T> producer,
      KafkaHealthChecker kafkaHealthChecker,
      MetricUtils metricUtils) {
    this.producer = producer;
    this.kafkaHealthChecker = kafkaHealthChecker;
    this.metricUtils = metricUtils;
  }

  @Override
  public void setWritable(boolean writable) {
    canWrite = writable;
  }

  @Override
  public Future<?> send(ProducerRecord<String, T> producerRecord, @Nullable Callback callback) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(Optional.empty());
    }
    Callback finalCallback;
    if (callback == null) {
      finalCallback =
          kafkaHealthChecker.getKafkaCallBack(
              metricUtils,
              "GENERIC",
              producerRecord.key() != null ? producerRecord.key() : StringUtils.EMPTY);
    } else {
      finalCallback = callback;
    }
    return producer.send(producerRecord, finalCallback);
  }

  @Override
  public void flush() {
    producer.flush();
  }
}
