package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.graph.CompositeGraphService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.graph.write.GraphWriteSink;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnProperty(
    prefix = "elasticsearch",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@ConditionalOnProperty(
    name = "graphService.type",
    havingValue = "elasticsearch",
    matchIfMissing = true)
@Import({BaseElasticSearchComponentsFactory.class})
public class ElasticSearchGraphServiceFactory {
  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Bean
  @Nonnull
  protected ESGraphWriteDAO esGraphWriteDAO(final ConfigurationProvider configurationProvider) {
    ESGraphWriteDAO esGraphWriteDAO =
        new ESGraphWriteDAO(
            components.getIndexConvention(),
            components.getBulkProcessor(),
            components.getConfig().getBulkProcessor().getNumRetries(),
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
      @Value("${elasticsearch.idHashAlgo}") final String idHashAlgo,
      MetricUtils metricUtils,
      @Qualifier("esGraphQueryDAO") final ESGraphQueryDAO esGraphQueryDAO,
      final ObjectProvider<GraphWriteSink> graphWriteSinkProvider,
      final ConfigurationProvider configurationProvider) {
    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    ElasticSearchGraphService primary =
        new ElasticSearchGraphService(
            lineageRegistry,
            components.getBulkProcessor(),
            components.getIndexConvention(),
            esGraphWriteDAO,
            esGraphQueryDAO,
            components.getIndexBuilder(),
            idHashAlgo);
    GraphWriteSink graphWriteSink =
        graphWriteSinkProvider.getIfAvailable(() -> GraphWriteSink.NOOP);
    if (graphWriteSink == GraphWriteSink.NOOP || configurationProvider.getDatahub().isReadOnly()) {
      return primary;
    }
    return new CompositeGraphService(primary, List.of(graphWriteSink));
  }

  @Bean(name = "esGraphQueryDAO")
  @Nonnull
  protected ESGraphQueryDAO createESGraphQueryDAO(
      final ConfigurationProvider configurationProvider, MetricUtils metricUtils) {
    return new ESGraphQueryDAO(
        components.getSearchClient(),
        configurationProvider.getGraphService(),
        configurationProvider.getElasticSearch(),
        metricUtils);
  }
}
