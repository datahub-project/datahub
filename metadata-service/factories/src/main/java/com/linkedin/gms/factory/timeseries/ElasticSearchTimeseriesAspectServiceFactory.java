package com.linkedin.gms.factory.timeseries;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.TimeseriesAspectIndexBuilders;
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
      final ConfigurationProvider configurationProvider) {
    return new ElasticSearchTimeseriesAspectService(
        components.getSearchClient(),
        new TimeseriesAspectIndexBuilders(
            components.getIndexBuilder(), entityRegistry, components.getIndexConvention()),
        components.getBulkProcessor(),
        components.getNumRetries(),
        queryFilterRewriteChain,
        configurationProvider.getTimeseriesAspectService());
  }
}
