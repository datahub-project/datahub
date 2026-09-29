package com.linkedin.metadata.queue.postgres;

import javax.annotation.Nonnull;

/**
 * SQL fragments for pgQueue message retention. Any client-side DELETE from the message table must
 * exclude the per-partition sequence anchor (row with {@code MAX(enqueue_seq)}) so {@code
 * MAX(enqueue_seq)+1} allocation stays valid after purge.
 */
public final class PgQueueRetentionPredicates {

  private PgQueueRetentionPredicates() {}

  /**
   * Append to a {@code WHERE} on message alias {@code ms}: do not delete the tail row for {@code
   * ms.topic_id} / {@code ms.partition_id}.
   */
  @Nonnull
  public static String sequenceAnchorExclusion(
      @Nonnull String messageAlias, @Nonnull String qualifiedMessageTable) {
    return " AND "
        + messageAlias
        + ".enqueue_seq < ("
        + "SELECT MAX(m_anchor.enqueue_seq) FROM "
        + qualifiedMessageTable
        + " m_anchor WHERE m_anchor.topic_id = "
        + messageAlias
        + ".topic_id AND m_anchor.partition_id = "
        + messageAlias
        + ".partition_id)";
  }
}
