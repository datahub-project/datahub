package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class ElasticSearchConfiguration {
  private BulkDeleteConfiguration bulkDelete;
  private BulkProcessorConfiguration bulkProcessor;
  private BuildIndicesConfiguration buildIndices;
  public String implementation;
  private SearchConfiguration search;
  private String idHashAlgo;
  private IndexConfiguration index;
}
