package com.linkedin.gms.factory.timeseries;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({BaseElasticSearchComponentsFactory.class, EntityRegistryFactory.class})
@Conditional(TimeseriesElasticsearchBackendCondition.class)
public class ElasticSearchTimeseriesAspectServiceFactory {
  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired private SearchClusterRegistry searchClusterRegistry;

  @Bean(name = "elasticSearchTimeseriesAspectService")
  @Nonnull
  protected ElasticSearchTimeseriesAspectService getInstance(
      final QueryFilterRewriteChain queryFilterRewriteChain,
      final ConfigurationProvider configurationProvider,
      final MetricUtils metricUtils) {
    return new ElasticSearchTimeseriesAspectService(
        searchClusterRegistry.clientFor(SearchComponent.TIMESERIES),
        searchClusterRegistry.bulkProcessorFor(SearchComponent.TIMESERIES),
        searchClusterRegistry
            .configFor(SearchComponent.TIMESERIES)
            .getBulkProcessor()
            .getNumRetries(),
        queryFilterRewriteChain,
        configurationProvider.getTimeseriesAspectService(),
        entityRegistry,
        components.getIndexConvention(),
        searchClusterRegistry.indexBuilderFor(SearchComponent.TIMESERIES),
        metricUtils);
  }
}
