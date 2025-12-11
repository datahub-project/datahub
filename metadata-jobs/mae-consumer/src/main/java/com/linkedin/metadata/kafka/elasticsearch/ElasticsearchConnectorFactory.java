/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// TODO: Move this factory.
@Slf4j
@Configuration
public class ElasticsearchConnectorFactory {
  @Autowired
  @Qualifier("elasticSearchBulkProcessor")
  private ESBulkProcessor bulkProcessor;

  @Value("${elasticsearch.bulkProcessor.numRetries}")
  private Integer numRetries;

  @Bean(name = "elasticsearchConnector")
  @Nonnull
  public ElasticsearchConnector createInstance() {
    return new ElasticsearchConnector(bulkProcessor, numRetries);
  }
}
