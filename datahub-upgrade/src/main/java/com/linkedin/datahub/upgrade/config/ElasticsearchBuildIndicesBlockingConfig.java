package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.datahubusage.UsageEventsInfrastructureProvisioner;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * Blocking upgrade that rebuilds Elasticsearch indices. PostgreSQL pgSearch schema seeding stays in
 * {@link BuildIndicesConfig}.
 */
@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
@ConditionalOnProperty(
    prefix = "elasticsearch",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class ElasticsearchBuildIndicesBlockingConfig {

  /**
   * After {@link KafkaSetupConfig} and optionally {@link
   * BuildIndicesConfig#postgresPgSearchEntitySchema}.
   */
  @Order(3)
  @Bean(name = "buildIndices")
  public BlockingSystemUpgrade buildIndices(
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
          baseElasticSearchComponents,
      final ConfigurationProvider configurationProvider,
      final AspectDao aspectDao,
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final GitVersion gitVersion,
      @Qualifier("revision") final String revision,
      final UsageEventsInfrastructureProvisioner usageEventsInfrastructureProvisioner) {

    return new BuildIndices(
        systemMetadataService,
        timeseriesAspectService,
        entitySearchService,
        graphService,
        baseElasticSearchComponents,
        configurationProvider,
        aspectDao,
        opContext,
        entityService,
        gitVersion,
        revision,
        usageEventsInfrastructureProvisioner);
  }
}
