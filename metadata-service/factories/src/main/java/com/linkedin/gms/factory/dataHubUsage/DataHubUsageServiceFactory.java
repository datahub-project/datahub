/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.dataHubUsage;

import com.linkedin.metadata.dataHubUsage.DataHubUsageServiceImpl;
import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class DataHubUsageServiceFactory {

  @Bean
  public DataHubUsageService dataHubUsageService(
      SearchClientShim<?> elasticClient, IndexConvention indexConvention) {
    return new DataHubUsageServiceImpl(elasticClient, indexConvention);
  }
}
