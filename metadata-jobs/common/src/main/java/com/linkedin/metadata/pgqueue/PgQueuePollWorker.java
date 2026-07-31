package com.linkedin.metadata.pgqueue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PartitionOffsetSkew;
import com.linkedin.metadata.queue.PgQueueConsumerPartitionSharding;
import com.linkedin.metadata.queue.PgQueueOffsetSkewWarnings;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

/** Single poll-loop implementation shared by every pgQueue consumer group / topic binding. */
@Slf4j
public final class PgQueuePollWorker implements Runnable {

  private final PgQueuePollerRegistration registration;
  private final MetadataQueueStore store;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;
  private final ConfigurationProvider configurationProvider;
  private final PgQueueSetupOptions pgQueueSetupOptions;
  private final int workerShardIndex;
  @Nullable private final Deserializer<GenericRecord> avroDeserializer;

  @Getter private volatile boolean stopped;

  public PgQueuePollWorker(
      PgQueuePollerRegistration registration,
      MetadataQueueStore store,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider,
      PgQueueSetupOptions pgQueueSetupOptions,
      int workerShardIndex,
      int workerShardCount,
      @Nullable Deserializer<GenericRecord> avroDeserializer) {
    this.registration = registration;
    this.store = store;
    this.postgresSqlSetupProperties = postgresSqlSetupProperties;
    this.configurationProvider = configurationProvider;
    this.pgQueueSetupOptions = pgQueueSetupOptions;
    if (workerShardCount < 1) {
      throw new IllegalArgumentException("workerShardCount must be >= 1");
    }
    if (workerShardIndex < 0 || workerShardIndex >= workerShardCount) {
      throw new IllegalArgumentException(
          "workerShardIndex must be in [0, workerShardCount); got "
              + workerShardIndex
              + " / "
              + workerShardCount);
    }
    this.workerShardIndex = workerShardIndex;
    this.avroDeserializer = avroDeserializer;
  }

  public void stop() {
    stopped = true;
  }

  @Override
  public void run() {
    if (registration.batchPolicy() != null) {
      runAccumulationMode();
    } else {
      runImmediateMode();
    }
  }

  /** Original poll loop: each poll result is dispatched immediately to the handler. */
  private void runImmediateMode() {
    String lockOwner = registration.consumerGroupId() + ":" + UUID.randomUUID();
    while (!stopped && !Thread.currentThread().isInterrupted()) {
      try {
        boolean anyTopicCataloged =
            registration.topicNames().stream().anyMatch(t -> store.fetchTopic(t).isPresent());
        if (!anyTopicCataloged) {
          Thread.sleep(registration.missingTopicSleepMillis());
          continue;
        }

        Duration visibility = visibilityTimeout();
        PgQueuePollContext ctx =
            new PgQueuePollContext(
                store, registration.consumerGroupId(), visibility, avroDeserializer);

        boolean anyMessages = false;
        for (String logicalTopic : registration.topicNames()) {
          Optional<QueueTopicMetadata> meta = store.fetchTopic(logicalTopic);
          if (meta.isEmpty()) {
            continue;
          }
          long topicId = meta.get().id();
          store.registerConsumer(registration.consumerGroupId(), topicId);
          int partitionCount = meta.get().partitionCount();
          int configuredPerTopic =
              PgQueueConsumerPartitionSharding.configuredConcurrencyForTopic(
                  pgQueueSetupOptions, logicalTopic);
          List<Integer> partitions =
              PgQueueConsumerPartitionSharding.partitionsForWorker(
                  partitionCount, configuredPerTopic, workerShardIndex);
          if (partitions.isEmpty()) {
            continue;
          }
          List<QueueReceivedMessage> batch =
              store.receiveBatchForGroup(
                  registration.consumerGroupId(),
                  topicId,
                  partitions,
                  lockOwner,
                  visibility,
                  registration.maxBatch());
          if (!batch.isEmpty()) {
            anyMessages = true;
            try {
              registration.handler().handleBatch(logicalTopic, batch, ctx);
            } catch (Exception e) {
              log.error(
                  "PgQueue handler failed for group {} topic {}; leases expire for retry",
                  registration.consumerGroupId(),
                  logicalTopic,
                  e);
            }
          } else {
            warnStuckAheadForTopic(logicalTopic, topicId, partitionCount, partitions);
          }
        }

        if (!anyMessages) {
          Thread.sleep(registration.emptyPollSleepMillis());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        log.error("PgQueue worker {} error", registration.threadName(), e);
        try {
          Thread.sleep(registration.errorRecoverySleepMillis());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  /**
   * Accumulation poll loop: messages are buffered per-topic and flushed when any threshold (count,
   * bytes, age) is reached.
   */
  private void runAccumulationMode() {
    PgQueueBatchPolicy policy = registration.batchPolicy();
    PgQueueBatchFlushHandler flushHandler = registration.flushHandler();
    String lockOwner = registration.consumerGroupId() + ":" + UUID.randomUUID();
    Map<String, PgQueueBatchAccumulator> accumulators = new HashMap<>();

    while (!stopped && !Thread.currentThread().isInterrupted()) {
      try {
        boolean anyTopicCataloged =
            registration.topicNames().stream().anyMatch(t -> store.fetchTopic(t).isPresent());
        if (!anyTopicCataloged) {
          Thread.sleep(registration.missingTopicSleepMillis());
          continue;
        }

        Duration visibility = visibilityTimeout();
        PgQueuePollContext ctx =
            new PgQueuePollContext(
                store, registration.consumerGroupId(), visibility, avroDeserializer);

        boolean anyMessages = false;
        for (String logicalTopic : registration.topicNames()) {
          Optional<QueueTopicMetadata> meta = store.fetchTopic(logicalTopic);
          if (meta.isEmpty()) {
            continue;
          }
          long topicId = meta.get().id();
          store.registerConsumer(registration.consumerGroupId(), topicId);
          int partitionCount = meta.get().partitionCount();
          int configuredPerTopic =
              PgQueueConsumerPartitionSharding.configuredConcurrencyForTopic(
                  pgQueueSetupOptions, logicalTopic);
          List<Integer> partitions =
              PgQueueConsumerPartitionSharding.partitionsForWorker(
                  partitionCount, configuredPerTopic, workerShardIndex);
          if (partitions.isEmpty()) {
            continue;
          }

          List<QueueReceivedMessage> polled =
              store.receiveBatchForGroup(
                  registration.consumerGroupId(),
                  topicId,
                  partitions,
                  lockOwner,
                  visibility,
                  registration.maxBatch());

          PgQueueBatchAccumulator accumulator =
              accumulators.computeIfAbsent(
                  logicalTopic,
                  k ->
                      new PgQueueBatchAccumulator(
                          policy.maxMessages(), policy.maxBytes(), policy.maxAgeMs()));

          if (!polled.isEmpty()) {
            anyMessages = true;
            accumulator.addAll(polled);
          } else {
            warnStuckAheadForTopic(logicalTopic, topicId, partitionCount, partitions);
          }

          if (accumulator.shouldFlush()) {
            flushAccumulator(logicalTopic, accumulator, flushHandler, ctx);
          }
        }

        // On empty poll, check for expired accumulators (linger timeout)
        if (!anyMessages) {
          for (Map.Entry<String, PgQueueBatchAccumulator> entry : accumulators.entrySet()) {
            if (entry.getValue().isExpired()) {
              Duration vis = visibilityTimeout();
              PgQueuePollContext flushCtx =
                  new PgQueuePollContext(
                      store, registration.consumerGroupId(), vis, avroDeserializer);
              flushAccumulator(entry.getKey(), entry.getValue(), flushHandler, flushCtx);
            }
          }
          Thread.sleep(registration.emptyPollSleepMillis());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        log.error("PgQueue worker {} error", registration.threadName(), e);
        try {
          Thread.sleep(registration.errorRecoverySleepMillis());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private void flushAccumulator(
      String logicalTopic,
      PgQueueBatchAccumulator accumulator,
      PgQueueBatchFlushHandler flushHandler,
      PgQueuePollContext ctx) {
    List<QueueReceivedMessage> batch = accumulator.drain();
    if (batch.isEmpty()) {
      return;
    }
    try {
      flushHandler.flush(logicalTopic, batch, ctx);
    } catch (Exception e) {
      log.error(
          "PgQueue batch flush failed for group {} topic {}; leases expire for retry",
          registration.consumerGroupId(),
          logicalTopic,
          e);
    }
  }

  private Duration visibilityTimeout() {
    var opts = postgresSqlSetupProperties.buildPgQueueOptions(configurationProvider.getKafka());
    int secs = opts.getTopicDefaultVisibilityTimeoutSeconds();
    return Duration.ofSeconds(Math.max(1, secs));
  }

  private void warnStuckAheadForTopic(
      String logicalTopic, long topicId, int partitionCount, List<Integer> assignedPartitions) {
    try {
      List<PartitionOffsetSkew> skews =
          store.detectOffsetAheadOfLog(registration.consumerGroupId(), topicId, partitionCount);
      for (PartitionOffsetSkew skew : skews) {
        if (!assignedPartitions.contains(skew.getPartitionId())) {
          continue;
        }
        PgQueueOffsetSkewWarnings.getInstance()
            .warnIfAhead(skew.toBuilder().topicName(logicalTopic).build());
      }
    } catch (UnsupportedOperationException ignored) {
      // Non-pgQueue MetadataQueueStore implementation.
    }
  }
}
