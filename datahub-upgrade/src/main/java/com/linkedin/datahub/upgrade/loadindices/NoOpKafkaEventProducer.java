package com.linkedin.datahub.upgrade.loadindices;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.TopicConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;

/**
 * No-op implementation of KafkaEventProducer for LoadIndices upgrade operations. This prevents
 * connection attempts to Kafka during index loading operations.
 */
@Slf4j
public class NoOpKafkaEventProducer extends KafkaEventProducer {

  public NoOpKafkaEventProducer() {
    // Call parent constructor with no-op implementations
    super(
        createNoOpProducer(),
        new TopicConventionImpl(),
        new NoOpKafkaHealthChecker(),
        MetricUtils.builder().registry(new SimpleMeterRegistry()).build());
  }

  @SuppressWarnings("unchecked")
  private static Producer<String, IndexedRecord> createNoOpProducer() {
    // Use a simple mock that doesn't implement the full interface
    return new Producer<String, IndexedRecord>() {
      @Override
      public Future<org.apache.kafka.clients.producer.RecordMetadata> send(
          org.apache.kafka.clients.producer.ProducerRecord<String, IndexedRecord> record) {
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public Future<org.apache.kafka.clients.producer.RecordMetadata> send(
          org.apache.kafka.clients.producer.ProducerRecord<String, IndexedRecord> record,
          org.apache.kafka.clients.producer.Callback callback) {
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public void flush() {
        // No-op
      }

      @Override
      public void close() {
        // No-op
      }

      @Override
      public void close(java.time.Duration timeout) {
        // No-op
      }

      @Override
      public org.apache.kafka.common.Uuid clientInstanceId(java.time.Duration timeout) {
        return org.apache.kafka.common.Uuid.randomUuid();
      }

      @Override
      public java.util.Map<
              org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>
          metrics() {
        return java.util.Collections.emptyMap();
      }

      @Override
      public java.util.List<org.apache.kafka.common.PartitionInfo> partitionsFor(String topic) {
        return java.util.Collections.emptyList();
      }

      @Override
      public void registerMetricForSubscription(
          org.apache.kafka.common.metrics.KafkaMetric metric) {
        // No-op
      }

      @Override
      public void unregisterMetricFromSubscription(
          org.apache.kafka.common.metrics.KafkaMetric metric) {
        // No-op
      }

      @Override
      public void initTransactions() {
        // No-op
      }

      @Override
      public void beginTransaction() {
        // No-op
      }

      @Override
      public void sendOffsetsToTransaction(
          java.util.Map<
                  org.apache.kafka.common.TopicPartition,
                  org.apache.kafka.clients.consumer.OffsetAndMetadata>
              offsets,
          org.apache.kafka.clients.consumer.ConsumerGroupMetadata groupMetadata) {
        // No-op
      }

      @Override
      public void commitTransaction() {
        // No-op
      }

      @Override
      public void abortTransaction() {
        // No-op
      }
    };
  }

  @Override
  public Future<?> produceMetadataChangeLog(
      @Nonnull final Urn urn,
      @Nullable AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    log.debug("NoOpKafkaEventProducer: Skipping MCL production for urn: {}", urn);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String getMetadataChangeLogTopicName(@Nonnull AspectSpec aspectSpec) {
    return "no-op-mcl-topic";
  }

  @Override
  public Future<?> produceMetadataChangeProposal(
      @Nonnull final Urn urn, @Nonnull MetadataChangeProposal metadataChangeProposal) {
    log.debug("NoOpKafkaEventProducer: Skipping MCP production for urn: {}", urn);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String getMetadataChangeProposalTopicName() {
    return "no-op-mcp-topic";
  }

  @Override
  public Future<?> produceFailedMetadataChangeProposalAsync(
      @Nonnull OperationContext opContext,
      @Nonnull MetadataChangeProposal mcp,
      @Nonnull Set<Throwable> throwables) {
    log.debug(
        "NoOpKafkaEventProducer: Skipping failed MCP production for urn: {}", mcp.getEntityUrn());
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public Future<?> producePlatformEvent(
      @Nonnull String name, @Nullable String key, @Nonnull PlatformEvent payload) {
    log.debug("NoOpKafkaEventProducer: Skipping platform event production: {}", name);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String getPlatformEventTopicName() {
    return "no-op-platform-topic";
  }

  @Override
  public void produceDataHubUpgradeHistoryEvent(@Nonnull DataHubUpgradeHistoryEvent event) {
    log.debug("NoOpKafkaEventProducer: Skipping DataHub upgrade history event production");
  }

  @Override
  public void flush() {
    log.debug("NoOpKafkaEventProducer: Flush called - no-op");
  }

  /** No-op implementation of KafkaHealthChecker */
  private static class NoOpKafkaHealthChecker extends KafkaHealthChecker {
    @Override
    public org.apache.kafka.clients.producer.Callback getKafkaCallBack(
        MetricUtils metricUtils, String eventType, String entityUrn) {
      return (metadata, exception) -> {
        // No-op callback
      };
    }
  }
}
