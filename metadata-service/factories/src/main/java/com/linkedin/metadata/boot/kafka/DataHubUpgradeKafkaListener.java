package com.linkedin.metadata.boot.kafka;

import com.codahale.metrics.Timer;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.Topics;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

// We don't disable this on GMS since we want GMS to also wait until the system is ready to read in
// case of
// backwards incompatible query logic dependent on system updates.
@Component("dataHubUpgradeKafkaListener")
@Slf4j
@EnableKafka
public class DataHubUpgradeKafkaListener implements ConsumerSeekAware, BootstrapDependency {

  private final KafkaListenerEndpointRegistry registry;

  public static final String CONSUMER_GROUP =
      "${DATAHUB_UPGRADE_HISTORY_KAFKA_CONSUMER_GROUP_ID:generic-duhe-consumer-job-client}";
  private static final String SUFFIX = "temp";
  public static final String TOPIC_NAME =
      "${DATAHUB_UPGRADE_HISTORY_TOPIC_NAME:" + Topics.DATAHUB_UPGRADE_HISTORY_TOPIC_NAME + "}";

  private final DefaultKafkaConsumerFactory<String, GenericRecord> _defaultKafkaConsumerFactory;

  @Value("#{systemEnvironment['DATAHUB_REVISION'] ?: '0'}")
  private String revision;

  private final GitVersion _gitVersion;
  private final ConfigurationProvider _configurationProvider;

  @Value(CONSUMER_GROUP)
  private String consumerGroup;

  @Value(TOPIC_NAME)
  private String topicName;

  private static final AtomicBoolean IS_UPDATED = new AtomicBoolean(false);

  public DataHubUpgradeKafkaListener(
      KafkaListenerEndpointRegistry registry,
      @Qualifier("duheKafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> defaultKafkaConsumerFactory,
      GitVersion gitVersion,
      ConfigurationProvider configurationProvider) {
    this.registry = registry;
    this._defaultKafkaConsumerFactory = defaultKafkaConsumerFactory;
    this._gitVersion = gitVersion;
    this._configurationProvider = configurationProvider;
  }

  // Constructs a consumer to read determine final offset to assign, prevents re-reading whole topic
  // to get the latest version
  @Override
  public void onPartitionsAssigned(
      Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    try (Consumer<String, GenericRecord> kafkaConsumer =
        _defaultKafkaConsumerFactory.createConsumer(consumerGroup, SUFFIX)) {
      final Map<TopicPartition, Long> offsetMap = kafkaConsumer.endOffsets(assignments.keySet());
      assignments.entrySet().stream()
          .filter(entry -> topicName.equals(entry.getKey().topic()))
          .forEach(
              entry -> {
                log.info(
                    "Partition: {} Current Offset: {}",
                    entry.getKey(),
                    offsetMap.get(entry.getKey()));
                long newOffset = offsetMap.get(entry.getKey()) - 1;
                callback.seek(
                    entry.getKey().topic(), entry.getKey().partition(), Math.max(0, newOffset));
              });
    }
  }

  @KafkaListener(
      id = CONSUMER_GROUP,
      topics = {TOPIC_NAME},
      containerFactory = "duheKafkaEventConsumer",
      concurrency = "1",
      autoStartup = "false")
  public void checkSystemVersion(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    try (Timer.Context i = MetricUtils.timer(this.getClass(), "checkSystemVersion").time()) {
      final GenericRecord record = consumerRecord.value();
      final String expectedVersion = String.format("%s-%s", _gitVersion.getVersion(), revision);

      DataHubUpgradeHistoryEvent event;
      try {
        event = EventUtils.avroToPegasusDUHE(record);
        log.info("Latest system update version: {}", event.getVersion());
        if (expectedVersion.equals(event.getVersion())) {
          IS_UPDATED.getAndSet(true);
        } else if (!_configurationProvider.getSystemUpdate().isWaitForSystemUpdate()) {
          log.warn("Wait for system update is disabled. Proceeding with startup.");
          IS_UPDATED.getAndSet(true);
        } else {
          log.warn(
              "System version is not up to date: {}. Waiting for datahub-upgrade to complete...",
              expectedVersion);
        }

      } catch (Exception e) {
        MetricUtils.counter(this.getClass(), "avro_to_pegasus_conversion_failure").inc();
        log.error("Error deserializing message due to: ", e);
        log.error("Message: {}", record.toString());
        return;
      }
    }
  }

  public void waitForUpdate() {
    if (!_configurationProvider.getSystemUpdate().isWaitForSystemUpdate()) {
      log.warn("Wait for system update is disabled. Proceeding with startup.");
      IS_UPDATED.getAndSet(true);
    }
    int maxBackOffs = Integer.parseInt(_configurationProvider.getSystemUpdate().getMaxBackOffs());
    long initialBackOffMs =
        Long.parseLong(_configurationProvider.getSystemUpdate().getInitialBackOffMs());
    int backOffFactor =
        Integer.parseInt(_configurationProvider.getSystemUpdate().getBackOffFactor());

    long backOffMs = initialBackOffMs;
    for (int i = 0; i < maxBackOffs; i++) {
      if (IS_UPDATED.get()) {
        log.debug("Finished waiting for updated indices.");
        try {
          log.info(
              "Containers: {}",
              registry.getListenerContainers().stream()
                  .map(MessageListenerContainer::getListenerId)
                  .collect(Collectors.toList()));
          registry.getListenerContainer(consumerGroup).stop();
        } catch (NullPointerException e) {
          log.error("Expected consumer `{}` to shutdown.", consumerGroup);
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

    if (!IS_UPDATED.get()) {

      throw new IllegalStateException(
          "Indices are not updated after exponential backoff."
              + " Please try restarting and consider increasing back off settings.");
    }
  }

  @Override
  public boolean waitForBootstrap() {
    this.waitForUpdate();

    return true;
  }
}
