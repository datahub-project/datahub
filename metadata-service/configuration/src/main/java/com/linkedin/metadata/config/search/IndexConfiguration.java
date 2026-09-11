package com.linkedin.metadata.config.search;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class IndexConfiguration {
  private String prefix;
  private DocIdsConfiguration docIds;
  private int minSearchFilterLength;

  // Reindex configuration flags
  private boolean enableSettingsReindex;
  private boolean enableMappingsReindex;

  /**
   * Per-entity mapping limit overrides, e.g. {@code mapping.total_fields.limit}. Applied on index
   * creation and reapplied to existing indices on every system update run (idempotently). The
   * reserved entity key {@code "default"} provides a fallback for entity indices not listed
   * explicitly. Today only the {@code totalFields} limit key is honored (maps to ES setting {@code
   * mapping.total_fields.limit}); other keys are ignored with a warning.
   */
  private Map<String, Map<String, Integer>> entityMappingLimits;

  // Index structure configuration
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
