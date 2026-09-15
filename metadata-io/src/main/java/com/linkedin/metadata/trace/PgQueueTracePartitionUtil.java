package com.linkedin.metadata.trace;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.queue.MetadataQueueRouting;
import javax.annotation.Nonnull;

/**
 * Partition index for pgQueue trace log peeks. Must match {@link
 * com.linkedin.metadata.queue.MetadataQueueRouting#stablePartitionId} used when enqueuing MCP /
 * FMCP rows so trace readers scan the same partition as {@link
 * com.linkedin.metadata.queue.postgres.EbeanPostgresMetadataQueueStore}.
 */
public final class PgQueueTracePartitionUtil {

  private PgQueueTracePartitionUtil() {}

  public static int partitionForUrn(@Nonnull Urn urn, int numPartitions) {
    return partitionForKey(urn.toString(), numPartitions);
  }

  public static int partitionForKey(@Nonnull String key, int numPartitions) {
    return MetadataQueueRouting.stablePartitionId(key, numPartitions);
  }
}
