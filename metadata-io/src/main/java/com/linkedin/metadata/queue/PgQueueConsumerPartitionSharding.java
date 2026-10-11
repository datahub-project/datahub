package com.linkedin.metadata.queue;

import com.linkedin.metadata.config.postgres.PgQueueResolvedTopicCatalogEntry;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

/**
 * Same-JVM partition assignment for pgQueue pollers: thread {@code i} polls partitions {@code p}
 * where {@code p % c == i} and {@code c = min(max(1, configured), partitionCount)}. Cross-JVM
 * coordination remains on {@code *_message_group_lease} via {@link
 * MetadataQueueStore#receiveBatchForGroup}.
 */
public final class PgQueueConsumerPartitionSharding {

  private PgQueueConsumerPartitionSharding() {}

  /** Upper bound for configured consumer threads per topic (YAML / catalog validation). */
  public static final int MAX_CONSUMER_CONCURRENCY = 64;

  public static int clampConfiguredConcurrency(int configured) {
    if (configured < 1) {
      return 1;
    }
    return Math.min(configured, MAX_CONSUMER_CONCURRENCY);
  }

  /**
   * Effective consumer count for one topic: cannot exceed partition count (extra threads would
   * idle).
   */
  public static int effectiveConcurrency(int configuredConcurrency, int partitionCount) {
    int c = clampConfiguredConcurrency(configuredConcurrency);
    int p = Math.max(1, partitionCount);
    return Math.min(c, p);
  }

  /**
   * Partitions this worker polls for one topic. Empty when {@code workerShardIndex >=
   * effectiveConcurrency} (worker exists to satisfy another topic's higher concurrency).
   */
  @Nonnull
  public static List<Integer> partitionsForWorker(
      int partitionCount, int configuredConcurrencyPerTopic, int workerShardIndex) {
    int c = effectiveConcurrency(configuredConcurrencyPerTopic, partitionCount);
    if (workerShardIndex < 0 || workerShardIndex >= c) {
      return List.of();
    }
    return IntStream.range(0, partitionCount)
        .filter(p -> p % c == workerShardIndex)
        .boxed()
        .toList();
  }

  public static int configuredConcurrencyForTopic(
      @Nonnull PgQueueSetupOptions opts, @Nonnull String topicName) {
    for (PgQueueResolvedTopicCatalogEntry e : opts.getResolvedTopicCatalog()) {
      if (topicName.equals(e.getTopicName())) {
        return effectiveConcurrency(e.getConsumerConcurrency(), e.getPartitionCount());
      }
    }
    return effectiveConcurrency(
        opts.getTopicDefaultConsumerConcurrency(), opts.getTopicDefaultPartitionCount());
  }

  /**
   * Partition count from merged pgQueue catalog (bootstrap hint; runtime uses {@link
   * QueueTopicMetadata#partitionCount()} for assignment).
   */
  public static int partitionCountHintForTopic(
      @Nonnull PgQueueSetupOptions opts, @Nonnull String topicName) {
    for (PgQueueResolvedTopicCatalogEntry e : opts.getResolvedTopicCatalog()) {
      if (topicName.equals(e.getTopicName())) {
        return Math.max(1, e.getPartitionCount());
      }
    }
    return Math.max(1, opts.getTopicDefaultPartitionCount());
  }

  /**
   * Worker threads to spawn for one registration: max resolved concurrency across its logical topic
   * names (each value is at most that topic's partition count).
   */
  public static int workerShardCount(
      @Nonnull Collection<String> topicNames, @Nonnull PgQueueSetupOptions opts) {
    int max = 1;
    for (String t : topicNames) {
      max = Math.max(max, configuredConcurrencyForTopic(opts, t));
    }
    return max;
  }
}
