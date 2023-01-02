package com.linkedin.metadata.kafka.elasticsearch.indices;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.BuildIndicesHistoryEvent;
import com.linkedin.mxe.Topics;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import joptsimple.internal.Strings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

// We don't disable this on GMS since we want GMS to also wait until the indices are ready to read in case of
// backwards incompatible query logic dependent on index updates.
@Component("buildIndicesKafkaListener")
@RequiredArgsConstructor
@Slf4j
@EnableKafka
public class BuildIndicesKafkaListener implements ConsumerSeekAware, BootstrapDependency {
  @Autowired
  private KafkaListenerEndpointRegistry registry;

  private static final String CONSUMER_GROUP = "${BUILD_INDICES_HISTORY_KAFKA_CONSUMER_GROUP_ID:generic-bihe-consumer-job-client}";
  private static final String SUFFIX = "temp";
  private static final String TOPIC_NAME = "${BUILD_INDICES_HISTORY_TOPIC_NAME:" + Topics.BUILD_INDICES_HISTORY_TOPIC_NAME + "}";

  private final DefaultKafkaConsumerFactory<String, GenericRecord> _defaultKafkaConsumerFactory;
  private final GitVersion _gitVersion;
  private final ConfigurationProvider _configurationProvider;

  @Value(CONSUMER_GROUP)
  private String consumerGroup;

  @Value(TOPIC_NAME)
  private String topicName;

  private AtomicBoolean isUpdated = new AtomicBoolean(false);


  // Constructs a consumer to read determine final offset to assign, prevents re-reading whole topic to get the latest version
  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    try (Consumer<String, GenericRecord> kafkaConsumer =
        _defaultKafkaConsumerFactory.createConsumer(consumerGroup, SUFFIX)) {
      final Map<TopicPartition, Long> offsetMap = kafkaConsumer.endOffsets(assignments.keySet());
      assignments.entrySet().stream()
          .filter(entry -> topicName.equals(entry.getKey().topic()))
          .forEach(entry ->
              callback.seek(entry.getKey().topic(), entry.getKey().partition(), offsetMap.get(entry.getKey()) - 1));
    }
  }

  @KafkaListener(id = CONSUMER_GROUP, topics = {TOPIC_NAME}, containerFactory = "kafkaEventConsumer")
  public void checkIndexVersion(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    final GenericRecord record = consumerRecord.value();

    BuildIndicesHistoryEvent event;
    try {
      event = EventUtils.avroToPegasusBIHE(record);
      log.info("Latest index update version: {}", event.getVersion());
      if (_gitVersion.getVersion().equals(event.getVersion())) {
        isUpdated.getAndSet(true);
      } else {
        log.debug("Index version is not up to date: {}", _gitVersion.getVersion());
      }

    } catch (Exception e) {
      MetricUtils.counter(this.getClass(), "avro_to_pegasus_conversion_failure").inc();
      log.error("Error deserializing message due to: ", e);
      log.error("Message: {}", record.toString());
      return;
    }
  }

  public void waitForUpdate() {
    int maxBackOffs = Integer.parseInt(_configurationProvider.getElasticSearch().getBuildIndices().getMaxBackOffs());
    long initialBackOffMs = Long.parseLong(_configurationProvider.getElasticSearch().getBuildIndices().getInitialBackOffMs());
    int backOffFactor = Integer.parseInt(_configurationProvider.getElasticSearch().getBuildIndices().getBackOffFactor());

    long backOffMs = initialBackOffMs;
    for (int i = 0; i < maxBackOffs; i++) {
      if (isUpdated.get()) {
        log.debug("Finished waiting for updated indices.");
        String consumerId = Strings.join(List.of(CONSUMER_GROUP, SUFFIX), "");
        try {
          registry.getListenerContainer(consumerId).stop();
        } catch (NullPointerException e) {
          log.error("Expected consumer `{}` to shutdown.", consumerId);
        }
        return;
      }
      try {
        Thread.sleep(backOffMs);
      } catch (InterruptedException e) {
        log.error("Thread interrupted while sleeping for exponential backoff: {}", e.getMessage());
        throw new RuntimeException(e);
      }

      backOffMs = backOffMs * backOffFactor;
    }

    if (!isUpdated.get()) {

      throw new IllegalStateException("Indices are not updated after exponential backoff."
          + " Please try restarting and consider increasing back off settings.");
    }
  }

  @Override
  public boolean waitForBootstrap() {
    this.waitForUpdate();

    return true;
  }
}
