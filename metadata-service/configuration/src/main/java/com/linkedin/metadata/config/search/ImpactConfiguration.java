package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class ImpactConfiguration {
  /** Maximum hops to traverse for impact analysis */
  private int maxHops;

  /** Maximum number of relationships. Use -1 or 0 for unlimited (only bound by time limit). */
  private int maxRelations;

  /** Number of slices for parallel search operations */
  private int slices;

  /** Point-in-Time keepAlive duration for impact analysis queries */
  private String keepAlive;

  /** Whether to return partial results instead of throwing an error when maxRelations is reached */
  private boolean partialResults;

  /**
   * Fraction of total timeout to reserve for the second query phase (search query after graph
   * traversal). When partialResults is enabled, graph traversal will stop early to ensure this time
   * is available for the subsequent search query. Must be between 0.0 and 1.0. Default: 0.2 (20% of
   * timeout reserved).
   */
  private double searchQueryTimeReservation;
}
