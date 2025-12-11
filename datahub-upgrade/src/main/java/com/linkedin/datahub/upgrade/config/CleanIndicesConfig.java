/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.CleanIndices;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class CleanIndicesConfig {
  @Bean
  public NonBlockingSystemUpgrade cleanIndices(
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
          baseElasticSearchComponents,
      final ConfigurationProvider configurationProvider,
      final AspectDao aspectDao) {

    return new CleanIndices(
        systemMetadataService,
        timeseriesAspectService,
        entitySearchService,
        graphService,
        baseElasticSearchComponents,
        configurationProvider,
        aspectDao);
  }
}
