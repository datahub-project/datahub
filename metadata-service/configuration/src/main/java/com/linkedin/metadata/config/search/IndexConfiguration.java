package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class IndexConfiguration {
  /**
   * IndexConvention naming prefix (e.g. {@code prod} -> {@code prod_datasetindex_v2}). Unrelated to
   * the RestClient path prefix, which is part of each cluster's URI.
   */
  private String prefix;

  private int minSearchFilterLength;

  // Reindex configuration flags
  private boolean enableSettingsReindex;
  private boolean enableMappingsReindex;

  /**
   * Effective shard/replica counts for one cluster. Not bound from {@code elasticsearch.index} —
   * sizing is per cluster, so these are populated by {@link
   * SearchClusterSettings#effectiveIndex(IndexConfiguration)}.
   */
  private int numShards;

  private int numReplicas;
  private int numRetries;
  private int refreshIntervalSeconds;
  private int maxReindexHours;
  private String mainTokenizer;

  // Index limits
  private int maxArrayLength;
  private int maxObjectKeys;
  private int maxValueLength;

  public String getFinalPrefix() {
    if (prefix == null || prefix.isEmpty()) {
      return "";
    } else {
      return prefix + "_";
    }
  }
}
