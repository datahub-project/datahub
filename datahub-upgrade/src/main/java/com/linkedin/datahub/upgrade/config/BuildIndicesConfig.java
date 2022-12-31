package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.buildindices.BuildIndices;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.TopicConvention;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class BuildIndicesConfig {
  @Bean(name = "buildIndices")
  public BuildIndices buildIndices(final SystemMetadataService systemMetadataService, final TimeseriesAspectService timeseriesAspectService,
                                   final EntitySearchService entitySearchService, final GraphService graphService,
                                   final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents baseElasticSearchComponents,
                                   final Producer<String, ? extends IndexedRecord> producer,
                                   final TopicConvention convention, final GitVersion gitVersion, final KafkaHealthChecker kafkaHealthChecker,
                                   final ConfigurationProvider configurationProvider) {

    return new BuildIndices(systemMetadataService, timeseriesAspectService, entitySearchService, graphService,
        baseElasticSearchComponents, producer, convention, gitVersion, kafkaHealthChecker,  configurationProvider);
  }
}
