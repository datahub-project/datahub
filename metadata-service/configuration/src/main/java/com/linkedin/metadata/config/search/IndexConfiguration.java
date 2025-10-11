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
  private Integer minSearchFilterLength;

  // Reindex configuration flags
  private boolean enableSettingsReindex = false;
  private boolean enableMappingsReindex = false;

  // Index structure configuration
  private Integer numShards = 1;
  private Integer numReplicas = 1;
  private Integer numRetries = 3;
  private Integer refreshIntervalSeconds = 1;
  private Integer maxReindexHours = 24;
  private String mainTokenizer = "standard";

  // Index limits
  private Integer maxArrayLength = 1000;
  private Integer maxObjectKeys = 1000;
  private Integer maxValueLength = 10000;
}
