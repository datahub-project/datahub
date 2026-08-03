package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Tunables for the Coordinated Ingest (Plan-&gt;Coordinate-&gt;Commit) path. Nested under
 * metadataChangeProposal.coordinatedIngest in application.yaml. Only takes effect when the
 * coordinatedIngestEnabled feature flag is on.
 *
 * <p>All defaults are specified in application.yaml via environment variables.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoordinatedIngestConfiguration {
  /** Cap on planner closure / coordination re-plan iterations. */
  private int maxPlanExpansions;

  /** Cap on total planned mutations, runaway guard. */
  private int maxMutationCount;

  /** Hazelcast IMap lock TTL / lease, in seconds. */
  private long lockLeaseSeconds;

  /**
   * Max <b>total</b> wait to acquire the IMap locks before proceeding lock-free, in seconds. This
   * budget is shared across all of a plan's conflict keys (a single deadline for the whole acquire
   * loop), not applied per key, so a large plan can never block the caller proportionally to its
   * key count.
   */
  private long lockAcquireTimeoutSeconds;

  /**
   * Lock substrate selector for the COORDINATE stage: {@code hazelcast} (distributed, default),
   * {@code local} (in-JVM only), or (future) {@code redis}. Correctness never depends on this — the
   * DB single-sorted commit is authoritative regardless of the chosen provider.
   */
  private String lockProvider;
}
