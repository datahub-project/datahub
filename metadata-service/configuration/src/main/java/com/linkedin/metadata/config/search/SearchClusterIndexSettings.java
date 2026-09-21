package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-cluster overlay on {@code elasticsearch.index} ({@code elasticsearch.clusters.<name>.index}).
 *
 * <p>Shard and replica counts are sizing, so they live here rather than in the shared defaults —
 * two clusters of different sizes must not share a shard count. {@link #prefix} defaults to {@code
 * elasticsearch.index.prefix} ({@code INDEX_PREFIX}) when unset. Mapping and analyzer files are
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
  /**
   * Optional naming prefix for this cluster. Unset/blank inherits {@code
   * elasticsearch.index.prefix} ({@code INDEX_PREFIX}).
   */
  private String prefix;

  /** Unset resolves to this cluster's own {@code dataNodeCount}, never another cluster's. */
  private Integer numShards;

  private Integer numReplicas;
  private Integer numRetries;
  private Integer refreshIntervalSeconds;
  private String mainTokenizer;

  /**
   * Optional analyzer YAML for this cluster. Unset/blank inherits {@code
   * elasticsearch.entityIndex.v2/v3.analyzerConfig}. {@code ${VAR:}} interpolates to empty, which
   * must not wipe the shared file.
   */
  private String analyzerConfig;

  /**
   * Optional mapping YAML for this cluster. Unset/blank inherits {@code
   * elasticsearch.entityIndex.v2/v3.mappingConfig}.
   */
  private String mappingConfig;

  /**
   * Returns the effective index configuration for this cluster: the shared defaults with any
   * overlay field applied on top.
   */
  public IndexConfiguration applyTo(IndexConfiguration defaults) {
    IndexConfiguration.IndexConfigurationBuilder builder =
        defaults == null ? IndexConfiguration.builder() : defaults.toBuilder();
    // Blank env interpolation (${VAR:}) is inherit, not "clear the shared prefix".
    String overlayPrefix = SearchClusterSettings.blankToNull(prefix);
    if (overlayPrefix != null) {
      builder.prefix(overlayPrefix);
    }
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

  /** Blank env interpolation is inherit, not "clear the shared analyzer file". */
  public String getAnalyzerConfig() {
    return SearchClusterSettings.blankToNull(analyzerConfig);
  }

  public String getMappingConfig() {
    return SearchClusterSettings.blankToNull(mappingConfig);
  }
}
