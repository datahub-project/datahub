/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class RestoreIndicesConfig {

  @Bean(name = "restoreIndices")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public RestoreIndices createInstance(
      final Database ebeanServer,
      final EntityService<?> entityService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final SystemMetadataService systemMetadataService) {
    return new RestoreIndices(
        ebeanServer, entityService, systemMetadataService, entitySearchService, graphService);
  }

  @Bean(name = "restoreIndices")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  public RestoreIndices createNotImplInstance() {
    log.warn("restoreIndices is not supported for cassandra!");
    return new RestoreIndices(null, null, null, null, null);
  }
}
