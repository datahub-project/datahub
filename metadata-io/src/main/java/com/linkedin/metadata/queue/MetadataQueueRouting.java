package com.linkedin.metadata.queue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;

/** Routing helpers aligned with Python {@code datahub.pgqueue.repository}. */
public final class MetadataQueueRouting {

  private MetadataQueueRouting() {}

  public static int stablePartitionId(@Nonnull String routingKey, int partitionCount) {
    if (partitionCount <= 0) {
      throw new IllegalArgumentException("partition_count must be positive");
    }
    CRC32 crc = new CRC32();
    crc.update(routingKey.getBytes(StandardCharsets.UTF_8));
    long unsigned = crc.getValue() & 0xFFFFFFFFL;
    return (int) (unsigned % partitionCount);
  }

  /** Deterministic positive bigint for {@code pg_advisory_xact_lock}. */
  public static long advisoryLockKey(long topicId, int partitionId) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest((topicId + ":" + partitionId).getBytes(StandardCharsets.UTF_8));
      long n = 0;
      for (int i = 0; i < 8; i++) {
        n = (n << 8) | (digest[i] & 0xffL);
      }
      long mod = 1L << 62;
      return Long.remainderUnsigned(n, mod);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
