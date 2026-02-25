package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@Builder(toBuilder = true)
public class BuildIndicesConfiguration {

  private boolean cloneIndices;
  private boolean allowDocCountMismatch;
  private String retentionUnit;
  private Long retentionValue;
  private boolean reindexOptimizationEnabled;

  /** Reindex source batch size (documents per scroll batch). Default 5000. */
  private Integer reindexBatchSize;

  /** Maximum number of slices for reindex (capped from target shard count). Default 256. */
  private Integer reindexMaxSlices;

  /** Minutes without document-count progress before re-triggering reindex. Default 5. */
  private Integer reindexNoProgressRetryMinutes;
}
