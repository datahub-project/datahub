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

  /** Impact analysis configuration */
  private ImpactConfiguration impact;

  /** Maximum threads used in lineage queries * */
  private int maxThreads;

  /** reduce query nesting * */
  private boolean queryOptimization;

  /** Enable creation of point in time snapshots for graph queries */
  private boolean pointInTimeCreationEnabled;

  /**
   * Seconds to wait (after {@code cancel(true)}) for all parallel graph slice futures to finish
   * before releasing shared PIT or clearing scroll; one {@link
   * java.util.concurrent.CompletableFuture#allOf} wait uses this bound. Must be set via
   * configuration (no Java default).
   */
  private Integer sliceFutureDrainTimeoutSeconds;

  /**
   * Maximum number of source URNs combined into a single delete_by_query when removing the outgoing
   * edges of several nodes for one aspect (e.g. every downstream field of a fine-grained
   * upstreamLineage). Values {@code <= 1} keep one delete_by_query per source URN.
   */
  @Builder.Default private int deleteByQueryUrnBatchSize = 1;

  /**
   * Whether graph delete_by_query requests refresh the index when they complete. Each refresh is
   * serialized per shard, so with refresh enabled the graph index caps delete_by_query throughput.
   * When disabled, version conflicts against deletes that are not yet visible are skipped
   * (conflicts=proceed) instead of aborting the request.
   */
  @Builder.Default private boolean deleteByQueryRefresh = true;
}
