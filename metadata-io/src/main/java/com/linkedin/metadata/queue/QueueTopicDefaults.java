package com.linkedin.metadata.queue;

import com.linkedin.metadata.config.postgres.PgQueueResolvedTopicCatalogEntry;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** Cluster/topic-default row values used when ensuring a logical topic row in the queue catalog. */
@Immutable
public record QueueTopicDefaults(
    int partitionCount,
    int retentionMaxAgeSeconds,
    long maxRowsPerTopic,
    long maxTotalPayloadBytesPerTopic,
    boolean aggressiveRetention,
    @Nullable String defaultContentTypeMime) {

  @Nonnull
  public static QueueTopicDefaults fromPgQueueSetup(@Nonnull PgQueueSetupOptions options) {
    return new QueueTopicDefaults(
        options.getTopicDefaultPartitionCount(),
        options.getTopicDefaultRetentionMaxAgeSeconds(),
        options.getTopicDefaultMaxRowsPerTopic(),
        options.getTopicDefaultMaxTotalPayloadBytesPerTopic(),
        options.isTopicDefaultAggressiveRetention(),
        options.getTopicDefaultContentTypeMime());
  }

  /**
   * Uses merged {@link PgQueueResolvedTopicCatalogEntry} when {@code topicName} matches; otherwise
   * cluster {@linkplain #fromPgQueueSetup defaults}.
   */
  @Nonnull
  public static QueueTopicDefaults resolveForTopic(
      @Nonnull PgQueueSetupOptions options, @Nonnull String topicName) {
    QueueTopicDefaults fallback = fromPgQueueSetup(options);
    for (PgQueueResolvedTopicCatalogEntry e : options.getResolvedTopicCatalog()) {
      if (topicName.equals(e.getTopicName())) {
        return new QueueTopicDefaults(
            e.getPartitionCount(),
            e.getRetentionMaxAgeSeconds(),
            e.getMaxRowsPerTopic(),
            e.getMaxTotalPayloadBytesPerTopic(),
            e.isAggressiveRetention(),
            fallback.defaultContentTypeMime());
      }
    }
    return fallback;
  }
}
