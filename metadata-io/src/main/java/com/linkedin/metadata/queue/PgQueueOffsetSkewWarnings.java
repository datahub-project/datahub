package com.linkedin.metadata.queue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rate-limited WARN when a consumer group's offset is ahead of the pgQueue log for a topic
 * partition.
 */
public final class PgQueueOffsetSkewWarnings {

  private static final Logger LOG = LoggerFactory.getLogger(PgQueueOffsetSkewWarnings.class);
  private static final long WARN_INTERVAL_MS = 60_000L;

  private static final PgQueueOffsetSkewWarnings INSTANCE = new PgQueueOffsetSkewWarnings();

  private final Map<String, Long> lastWarnAtMs = new ConcurrentHashMap<>();

  public static PgQueueOffsetSkewWarnings getInstance() {
    return INSTANCE;
  }

  public void warnIfAhead(@Nonnull PartitionOffsetSkew skew) {
    String key = skew.getConsumerGroup() + ":" + skew.getTopicId() + ":" + skew.getPartitionId();
    long now = System.currentTimeMillis();
    Long last = lastWarnAtMs.get(key);
    if (last != null && now - last < WARN_INTERVAL_MS) {
      return;
    }
    lastWarnAtMs.put(key, now);
    LOG.warn(
        "pgQueue consumer offset ahead of message log (STUCK_AHEAD); consumer will not receive"
            + " messages until an operator resets the offset (see docs/pgqueue-design.md)."
            + " consumerGroup={} topicId={} topicName={} partitionId={} committedOffset={}"
            + " maxSeq={} aheadBy={}",
        skew.getConsumerGroup(),
        skew.getTopicId(),
        skew.getTopicName(),
        skew.getPartitionId(),
        skew.getCommittedOffset(),
        skew.getMaxSeq(),
        skew.getAheadBy());
  }

  public void warnAll(@Nonnull Iterable<PartitionOffsetSkew> skews) {
    for (PartitionOffsetSkew skew : skews) {
      warnIfAhead(skew);
    }
  }

  private PgQueueOffsetSkewWarnings() {}
}
