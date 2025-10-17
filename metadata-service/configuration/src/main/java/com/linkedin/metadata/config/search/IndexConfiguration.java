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
  private String prefix;
  private DocIdsConfiguration docIds;
  private int minSearchFilterLength;

  // Reindex configuration flags
  private boolean enableSettingsReindex;
  private boolean enableMappingsReindex;

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
}
