package com.linkedin.metadata.boot.pgqueue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.QueueLogPeekRow;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * When {@code datahub.messaging.transport=pgqueue}, waits for the same DataHub upgrade-history
 * signal as {@link com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener}, by reading the
 * tip of the {@link TopicConvention#getDataHubUpgradeHistoryTopicName()} pgQueue log (Confluent
 * Avro payloads).
 */
@Component("dataHubUpgradeKafkaListener")
@Slf4j
@Conditional(PgQueueMessagingTransportCondition.class)
public class DataHubUpgradePgQueueBootstrapDependency implements BootstrapDependency {

  private final MetadataQueueStore metadataQueueStore;
  private final TopicConvention topicConvention;
  private final GitVersion gitVersion;
  private final ConfigurationProvider configurationProvider;
  private final Deserializer<GenericRecord> avroDeserializer;
  private final OperationContext systemOperationContext;

  @Value("#{systemEnvironment['DATAHUB_REVISION'] ?: '0'}")
  private String revision;

  public DataHubUpgradePgQueueBootstrapDependency(
      MetadataQueueStore metadataQueueStore,
      @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN) TopicConvention topicConvention,
      GitVersion gitVersion,
      ConfigurationProvider configurationProvider,
      @Qualifier("pgQueueConsumerAvroDeserializer") Deserializer<GenericRecord> avroDeserializer,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    this.metadataQueueStore = metadataQueueStore;
    this.topicConvention = topicConvention;
    this.gitVersion = gitVersion;
    this.configurationProvider = configurationProvider;
    this.avroDeserializer = avroDeserializer;
    this.systemOperationContext = systemOperationContext;
  }

  @Override
  public boolean waitForBootstrap() {
    waitForPgQueueUpgradeHistory();
    return true;
  }

  private void waitForPgQueueUpgradeHistory() {
    if (!configurationProvider.getSystemUpdate().isWaitForSystemUpdate()) {
      log.warn("Wait for system update is disabled. Proceeding with startup.");
      return;
    }

    int maxBackOffs = Integer.parseInt(configurationProvider.getSystemUpdate().getMaxBackOffs());
    long initialBackOffMs =
        Long.parseLong(configurationProvider.getSystemUpdate().getInitialBackOffMs());
    int backOffFactor =
        Integer.parseInt(configurationProvider.getSystemUpdate().getBackOffFactor());

    String expectedVersion = String.format("%s-%s", gitVersion.getVersion(), revision);
    long backOffMs = initialBackOffMs;
    for (int i = 0; i < maxBackOffs; i++) {
      if (isPgQueueUpgradeHistoryAtExpectedVersion(expectedVersion)) {
        log.info(
            "pgQueue {} topic reports expected DataHub upgrade version {}.",
            topicConvention.getDataHubUpgradeHistoryTopicName(),
            expectedVersion);
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

    throw new IllegalStateException(
        "Indices are not updated after exponential backoff."
            + " Please try restarting and consider increasing back off settings.");
  }

  private boolean isPgQueueUpgradeHistoryAtExpectedVersion(String expectedVersion) {
    final String topicName = topicConvention.getDataHubUpgradeHistoryTopicName();
    Optional<QueueTopicMetadata> topicMeta = metadataQueueStore.fetchTopic(topicName);
    if (topicMeta.isEmpty()) {
      log.debug("pgQueue topic {} not present yet; waiting for datahub-upgrade.", topicName);
      return false;
    }
    long topicId = topicMeta.get().id();
    int partitionCount = topicMeta.get().partitionCount();
    Map<Integer, Long> nextExclusive =
        metadataQueueStore.partitionNextExclusiveSeqs(topicId, partitionCount);
    Map<Integer, Long> partitionToMinSeq = new HashMap<>();
    for (int p = 0; p < partitionCount; p++) {
      long next = nextExclusive.getOrDefault(p, 1L);
      if (next > 1L) {
        partitionToMinSeq.put(p, next - 1);
      }
    }
    if (partitionToMinSeq.isEmpty()) {
      log.debug("No DataHubUpgradeHistory rows in pgQueue topic {} yet.", topicName);
      return false;
    }
    int limit =
        Math.max(
            partitionToMinSeq.size(),
            partitionToMinSeq.size() * (QueueTopicMetadata.MAX_PRIORITY + 1));
    List<QueueLogPeekRow> rows = metadataQueueStore.peekTopicLog(topicId, partitionToMinSeq, limit);
    for (QueueLogPeekRow row : rows) {
      try {
        byte[] wire = PgQueuePayloadCodec.decode(row.payload(), row.payloadCompression());
        GenericRecord record = avroDeserializer.deserialize(topicName, wire);
        DataHubUpgradeHistoryEvent event = EventUtils.avroToPegasusDUHE(record);
        log.info("Latest system update version (pgQueue): {}", event.getVersion());
        if (expectedVersion.equals(event.getVersion())) {
          return true;
        }
        if (!configurationProvider.getSystemUpdate().isWaitForSystemUpdate()) {
          log.warn("Wait for system update is disabled. Proceeding with startup.");
          return true;
        }
        log.warn(
            "System version is not up to date: {}. Waiting for datahub-upgrade to complete...",
            expectedVersion);
      } catch (Exception e) {
        systemOperationContext
            .getMetricUtils()
            .ifPresent(
                metricUtils ->
                    metricUtils.increment(
                        this.getClass(), "avro_to_pegasus_conversion_failure", 1));
        log.error("Error deserializing pgQueue DataHubUpgradeHistory payload: ", e);
      }
    }
    return false;
  }
}
