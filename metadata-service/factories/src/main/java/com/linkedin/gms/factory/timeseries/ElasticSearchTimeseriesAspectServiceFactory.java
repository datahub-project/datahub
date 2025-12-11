/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.timeseries;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({BaseElasticSearchComponentsFactory.class, EntityRegistryFactory.class})
public class ElasticSearchTimeseriesAspectServiceFactory {
  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Bean(name = "elasticSearchTimeseriesAspectService")
  @Nonnull
  protected ElasticSearchTimeseriesAspectService getInstance(
      final QueryFilterRewriteChain queryFilterRewriteChain,
      final ConfigurationProvider configurationProvider,
      final MetricUtils metricUtils) {
    return new ElasticSearchTimeseriesAspectService(
        components.getSearchClient(),
        components.getBulkProcessor(),
        components.getConfig().getBulkProcessor().getNumRetries(),
        queryFilterRewriteChain,
        configurationProvider.getTimeseriesAspectService(),
        entityRegistry,
        components.getIndexConvention(),
        components.getIndexBuilder(),
        metricUtils);
  }
}
