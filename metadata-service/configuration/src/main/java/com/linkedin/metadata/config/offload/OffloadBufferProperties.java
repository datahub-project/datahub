package com.linkedin.metadata.config.offload;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Base configuration POJO for an {@link com.linkedin.metadata.buffer.offload.OffloadBuffer}. A
 * use-specific properties class (e.g. {@code PostCommitHookBufferProperties}) extends this and
 * fixes {@link #sizingPolicy} / {@link #mergePolicy} to the use's semantics; the shared {@code
 * OffloadBufferFactory} reads the rest (map names, batch size, intervals, cap) generically. Bound
 * from a {@code datahub.<use>.buffer.*} block in application.yaml; on/off is a feature flag, not
 * this POJO.
 *
 * <p>Map names are namespaced per use so multiple offloads coexist in one Hazelcast instance with no
 * key collisions.
 *
 * <p>Lives in the configuration module (not {@code metadata-io}) because the use-specific
 * subclasses extend it from here, and {@code metadata-io} already depends on this module — the
 * reverse dependency would be a cycle.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OffloadBufferProperties {

  @Builder.Default private String mapName = "offload-pending";
  @Builder.Default private String lockMapName = "offload-drain-lock";
  @Builder.Default private String seqMapName = "offload-sequence";

  /**
   * Hard cap on pending entries. With {@link SizingPolicy#REJECT_AT_CAP}, {@code enqueue} returns
   * {@code false} at cap and the caller runs the work synchronously (no loss). With {@link
   * SizingPolicy#EVICT_LRU}, this is advisory (the real bound is the Hazelcast {@code EvictionConfig}).
   */
  @Builder.Default private int maxPendingEntries = 100_000;

  @Builder.Default private int drainBatchSize = 500;

  /** Fixed-delay between drain ticks (ms). Consumed by the shared factory's scheduler. */
  @Builder.Default private long drainIntervalMs = 2000;

  /**
   * Safety-net lease on the single-winner drain lock so a drainer that dies mid-drain does not wedge
   * it forever. Must exceed a drain's worst-case duration.
   */
  @Builder.Default private long drainLockLeaseMs = 60_000;

  @Builder.Default private SizingPolicy sizingPolicy = SizingPolicy.REJECT_AT_CAP;

  @Builder.Default private MergePolicy mergePolicy = MergePolicy.NO_COALESCE;

  /**
   * When true, transient resolver failures ({@link
   * com.linkedin.metadata.buffer.offload.OffloadContextResolver#groupKey} / {@link
   * com.linkedin.metadata.buffer.offload.OffloadContextResolver#resolveOpContext} throwing a
   * non-permanent {@link RuntimeException}) move the key to a backoff limbo (removed from the
   * buffer + re-merged after {@link #backoffTicks}) so a persistently-failing key cannot starve the
   * drain's first page. When false, transient failures leave the key in-buffer for next-tick retry.
   * Default off: hooks never throw transient resolver failures, so backoff is dead code for them;
   * retention (cloud) enables it.
   */
  @Builder.Default private boolean backoffEnabled = false;

  /**
   * Backoff window in ticks (used only when {@link #backoffEnabled}); must be {@code >= 1}. A key
   * backed off is out of the buffer for this many ticks before being re-merged for retry.
   */
  @Builder.Default private long backoffTicks = 5;
}
