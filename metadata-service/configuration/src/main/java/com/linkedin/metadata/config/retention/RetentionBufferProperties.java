package com.linkedin.metadata.config.retention;

import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.OffloadBufferProperties;
import com.linkedin.metadata.config.offload.SizingPolicy;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * POJO representing the "datahub.retention.buffer" tuning block in application.yaml, as a
 * retention-specific specialization of the framework {@link OffloadBufferProperties}. On/off is
 * {@code featureFlags.retentionBufferEnabled}, not this POJO. Also requires {@code
 * featureFlags.postCommitRetentionEnabled}; the buffer is backed by the shared embedded Hazelcast
 * instance, else sync DELETE post-commit.
 *
 * <p>Retention semantics are fixed here: {@link SizingPolicy#EVICT_LRU} (latest-wins; eviction =
 * bloat, not loss — a re-merge of the same key coalesces to the keep-max version) and {@link
 * MergePolicy#KEEP_MAX_LONG} (coalesce repeated (urn, aspect) requests to the highest max-version).
 * Transient-failure backoff is ON: a key whose routing lookup throws a transient {@link
 * RuntimeException} is moved to a backoff limbo so it cannot starve the drain's first page (drain
 * is non-destructive and returns the same first page until the key is cleared); it is re-merged
 * after {@code backoffTicks} for retry. The no-arg constructor sets these and the retention map
 * names as defaults; Spring overrides any that appear in {@code datahub.retention.buffer.*}.
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class RetentionBufferProperties extends OffloadBufferProperties {

  /**
   * Single source of the drain-interval default. Referenced by this POJO's {@code drainIntervalMs}
   * field default. (Scheduling is now programmatic via the shared {@code OffloadBufferFactory}'s
   * {@code TaskScheduler}, so no {@code @Scheduled} placeholder constant is needed — this is kept
   * only as the code fallback when the property is unset.)
   */
  public static final String DEFAULT_DRAIN_INTERVAL_MS = "5000";

  /** Retention-specific defaults. Spring binds {@code datahub.retention.buffer.*} over these. */
  public RetentionBufferProperties() {
    setMapName("retention-pending");
    setLockMapName("retention-drain-lock");
    // No separate seq map for retention (keys coalesce by design; NO_COALESCE's sequence is unused).
    setSeqMapName("retention-pending.seq");
    setMaxPendingEntries(100_000);
    setDrainBatchSize(500);
    setDrainIntervalMs(Long.parseLong(DEFAULT_DRAIN_INTERVAL_MS));
    setDrainLockLeaseMs(60_000);
    setSizingPolicy(SizingPolicy.EVICT_LRU);
    setMergePolicy(MergePolicy.KEEP_MAX_LONG);
    setBackoffEnabled(true);
    setBackoffTicks(5);
  }
}
