package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BuildIndicesConfig {
  @Bean(name = "buildIndices")
  public BuildIndices buildIndices(
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
          baseElasticSearchComponents,
      final ConfigurationProvider configurationProvider) {

    return new BuildIndices(
        systemMetadataService,
        timeseriesAspectService,
        entitySearchService,
        graphService,
        baseElasticSearchComponents,
        configurationProvider);
  }
}
