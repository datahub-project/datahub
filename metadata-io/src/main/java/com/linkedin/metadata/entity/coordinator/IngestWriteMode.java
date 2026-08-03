package com.linkedin.metadata.entity.coordinator;

/**
 * How an ingest transaction ensures write consistency. Orthogonal to whether the {@link
 * MutationCoordinator} serializes conflict keys: the coordinator (a Hazelcast serialize layer) sits
 * <em>above</em> the write mode and works with any of these.
 *
 * <p>The write mode is the pluggable strategy that lets a future optimistic-locking phase slot in
 * as a sibling without touching the coordinator.
 */
public enum IngestWriteMode {
  /**
   * Legacy behavior: multiple sequential {@code SELECT ... FOR UPDATE} waves inside the
   * transaction. The path used when coordinated ingest is off.
   */
  LEGACY_MULTI_WAVE,

  /**
   * Pessimistic single wave: discover the full mutation closure non-locking, then take one
   * globally-sorted {@code FOR UPDATE} over all of it, collapsing the legacy multi-wave lock. This
   * phase's coordinated write mode.
   */
  PESSIMISTIC_SINGLE_WAVE,

  /**
   * Optimistic: no row locks; each write is a conditional UPDATE (compare-and-swap) on {@code
   * SystemMetadata.version}, retrying on conflict. Reserved for the next phase (optimistic
   * locking). Not yet wired — present so the branch point is explicit.
   */
  OPTIMISTIC_CAS
}
