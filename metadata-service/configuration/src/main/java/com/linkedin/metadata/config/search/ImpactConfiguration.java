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

  /** Maximum number of relationships */
  private int maxRelations;

  /** Number of slices for parallel search operations */
  private int slices;

  /** Point-in-Time keepAlive duration for impact analysis queries */
  private String keepAlive;
}
