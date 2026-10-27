package com.linkedin.metadata.config.graph;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Client settings for the in-memory lineage graph server. When enabled, impact lineage walks go to
 * the server while it is ready and caught up, and fall back to the OpenSearch walk otherwise.
 */
@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class InMemoryGraphConfiguration {
  private boolean enabled;
  private String url;
  private int connectTimeoutMs;
  private int requestTimeoutMs;

  /** Fall back to OpenSearch when the server trails the change log by more than this. */
  private long maxLagMs;

  private long healthPollMs;

  /** Consecutive request failures that open the circuit (stop calling the server). */
  private int failureThreshold;

  private long openCircuitMs;

  /**
   * Let the server answer lightning lineage searches (filters, page and facets) next to the graph,
   * returning only the page instead of every relationship.
   */
  private boolean searchPushdown;
}
