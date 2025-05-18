package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class GraphQueryConfiguration {

  private long timeoutSeconds;
  private int batchSize;
  // When set to true, the graph walk (typically in search-across-lineage or scroll-across-lineage)
  // will return all paths between the source and destination nodes within the hops limit.
  private boolean enableMultiPathSearch;

  /**
   * Adds a boosting query for via nodes being present on a lineage search hit, allows these nodes
   * to be prioritized in the case of a multiple path situation with multi-path search disabled
   */
  private boolean boostViaNodes;

  /** Whether soft-delete status is tracked on entity URNs on graph edges */
  private boolean graphStatusEnabled;

  /** Maximum lineage hops */
  private int lineageMaxHops;

  /** Maximum impact analysis max hops */
  private int impactMaxHops;
}
