package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.IncrementalReindex;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
@ConditionalOnProperty(
    name = "elasticsearch.buildIndices.incrementalReindexEnabled",
    havingValue = "true")
public class IncrementalReindexConfig {

  @Bean
  public NonBlockingSystemUpgrade incrementalReindexNonBlocking(
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final ConfigurationProvider configurationProvider,
      final AspectDao aspectDao,
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final GitVersion gitVersion,
      @Qualifier("revision") final String revision,
      @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
          final String metadataChangeLogKafkaConsumerGroupId,
      final KafkaProperties kafkaProperties) {

    String upgradeVersion = String.format("%s-%s", gitVersion.getVersion(), revision);
    return new IncrementalReindex(
        systemMetadataService,
        timeseriesAspectService,
        entitySearchService,
        graphService,
        configurationProvider,
        aspectDao,
        opContext,
        entityService,
        upgradeVersion,
        kafkaProperties,
        metadataChangeLogKafkaConsumerGroupId);
  }
}
