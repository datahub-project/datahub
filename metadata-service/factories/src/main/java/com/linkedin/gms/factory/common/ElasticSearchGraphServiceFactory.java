package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnProperty(
    name = "graphService.type",
    havingValue = "elasticsearch",
    matchIfMissing = true)
@Import({BaseElasticSearchComponentsFactory.class})
public class ElasticSearchGraphServiceFactory {
  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired private SearchClusterRegistry searchClusterRegistry;

  @Bean
  @Nonnull
  protected ESGraphWriteDAO esGraphWriteDAO(final ConfigurationProvider configurationProvider) {
    ESGraphWriteDAO esGraphWriteDAO =
        new ESGraphWriteDAO(
            components.getIndexConvention(),
            graphBulkProcessor(),
            graphNumRetries(),
            configurationProvider.getElasticSearch().getSearch().getGraph());
    if (configurationProvider.getDatahub().isReadOnly()) {
      esGraphWriteDAO.setWritable(false);
    }

    return esGraphWriteDAO;
  }

  @Bean(name = "graphService")
  @Nonnull
  protected GraphService getInstance(
      final ESGraphWriteDAO esGraphWriteDAO,
      final EntityRegistry entityRegistry,
      @Value("${elasticsearch.entityIndex.v2.idHashAlgo}") final String idHashAlgo,
      MetricUtils metricUtils,
      @Qualifier("esGraphQueryDAO") final ESGraphQueryDAO esGraphQueryDAO) {
    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    return new ElasticSearchGraphService(
        lineageRegistry,
        graphBulkProcessor(),
        components.getIndexConvention(),
        esGraphWriteDAO,
        esGraphQueryDAO,
        searchClusterRegistry.indexBuilderFor(SearchComponent.GRAPH),
        idHashAlgo);
  }

  @Bean(name = "esGraphQueryDAO")
  @Nonnull
  protected ESGraphQueryDAO createESGraphQueryDAO(
      final ConfigurationProvider configurationProvider, MetricUtils metricUtils) {
    return new ESGraphQueryDAO(
        searchClusterRegistry.clientFor(SearchComponent.GRAPH),
        configurationProvider.getGraphService(),
        configurationProvider.getElasticSearch(),
        metricUtils);
  }

  private ESBulkProcessor graphBulkProcessor() {
    return searchClusterRegistry.bulkProcessorFor(SearchComponent.GRAPH);
  }

  private int graphNumRetries() {
    return searchClusterRegistry
        .configFor(SearchComponent.GRAPH)
        .getBulkProcessor()
        .getNumRetries();
  }
}
