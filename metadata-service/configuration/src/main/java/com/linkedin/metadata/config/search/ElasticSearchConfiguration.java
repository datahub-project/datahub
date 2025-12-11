/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
  private String username;
  private String password;
  private String pathPrefix;
  private boolean useSSL;
  private boolean opensearchUseAwsIamAuth;
  private String region;
}
