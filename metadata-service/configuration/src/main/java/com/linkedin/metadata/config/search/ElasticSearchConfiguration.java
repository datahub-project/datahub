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
  private SearchConfiguration search;
  private String idHashAlgo;
  private IndexConfiguration index;
  private ScrollConfiguration scroll;
  private EntityIndexConfiguration entityIndex;
  private String host;
  private int port;
  private int threadCount;
  private int connectionRequestTimeout;
  private int socketTimeout;
  private String username;
  private String password;
  private String pathPrefix;
  private boolean useSSL;
  private boolean opensearchUseAwsIamAuth;
  private String region;
}
