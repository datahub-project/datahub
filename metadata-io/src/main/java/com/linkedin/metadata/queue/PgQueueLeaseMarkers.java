package com.linkedin.metadata.queue;

import java.sql.Timestamp;
import java.time.Instant;
import javax.annotation.Nonnull;

/** Sentinel values for {@code *_message_group_lease} rows that record an ack without deletion. */
public final class PgQueueLeaseMarkers {

  /** {@code lock_owner} value for a committed (acked) message within a consumer group. */
  public static final String ACKED_LOCK_OWNER = "__acked__";

  /** {@code locked_until} for acked rows; always in the past so they never block dequeue. */
  public static final Instant ACKED_LOCKED_UNTIL = Instant.EPOCH;

  public static final Timestamp ACKED_LOCKED_UNTIL_TS = Timestamp.from(ACKED_LOCKED_UNTIL);

  private PgQueueLeaseMarkers() {}

  public static boolean isAcked(@Nonnull String lockOwner) {
    return ACKED_LOCK_OWNER.equals(lockOwner);
  }
}
