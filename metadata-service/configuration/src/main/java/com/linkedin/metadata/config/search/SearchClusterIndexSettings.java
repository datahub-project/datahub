package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-cluster overlay on {@code elasticsearch.index} ({@code elasticsearch.clusters.<name>.index}).
 *
 * <p>Shard and replica counts are sizing, so they live here rather than in the shared defaults —
 * two clusters of different sizes must not share a shard count. Mapping and analyzer files are
 * overridable because they are engine-dependent: a cluster on a newer engine may need a different
 * file than the product default on {@code entityIndex.v3}.
 *
 * <p>All fields are boxed so that "unset" is distinguishable from "explicitly zero"; unset fields
 * fall through to the shared defaults via {@link #applyTo}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class SearchClusterIndexSettings {
  /** Unset resolves to this cluster's own {@code dataNodeCount}, never another cluster's. */
  private Integer numShards;

  private Integer numReplicas;
  private Integer numRetries;
  private Integer refreshIntervalSeconds;
  private String mainTokenizer;
  private String analyzerConfig;
  private String mappingConfig;

  /**
   * Returns the effective index configuration for this cluster: the shared defaults with any
   * overlay field applied on top.
   */
  public IndexConfiguration applyTo(IndexConfiguration defaults) {
    IndexConfiguration.IndexConfigurationBuilder builder =
        defaults == null ? IndexConfiguration.builder() : defaults.toBuilder();
    if (numShards != null) {
      builder.numShards(numShards);
    }
    if (numReplicas != null) {
      builder.numReplicas(numReplicas);
    }
    if (numRetries != null) {
      builder.numRetries(numRetries);
    }
    if (refreshIntervalSeconds != null) {
      builder.refreshIntervalSeconds(refreshIntervalSeconds);
    }
    if (mainTokenizer != null) {
      builder.mainTokenizer(mainTokenizer);
    }
    return builder.build();
  }
}
