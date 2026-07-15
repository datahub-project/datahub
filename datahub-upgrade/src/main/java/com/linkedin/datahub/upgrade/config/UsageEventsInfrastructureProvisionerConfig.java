package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.usageevents.ElasticsearchUsageEventsInfrastructureProvisioner;
import com.linkedin.datahub.upgrade.usageevents.PostgresUsageEventsInfrastructureProvisioner;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.datahubusage.UsageEventsInfrastructureProvisioner;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UsageEventsInfrastructureProvisionerConfig {

  @Bean
  @ConditionalOnProperty(
      prefix = "platformAnalytics.usage-events",
      name = "implementation",
      havingValue = "postgres")
  @ConditionalOnBean(PostgresUsageEventsStore.class)
  UsageEventsInfrastructureProvisioner postgresUsageEventsInfrastructureProvisioner(
      PostgresUsageEventsStore postgresUsageEventsStore) {
    return new PostgresUsageEventsInfrastructureProvisioner(postgresUsageEventsStore);
  }

  @Bean
  @ConditionalOnProperty(
      prefix = "platformAnalytics.usage-events",
      name = "implementation",
      havingValue = "elasticsearch",
      matchIfMissing = true)
  UsageEventsInfrastructureProvisioner elasticsearchUsageEventsInfrastructureProvisioner(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents baseElasticSearchComponents,
      ConfigurationProvider configurationProvider) {
    return new ElasticsearchUsageEventsInfrastructureProvisioner(
        baseElasticSearchComponents, configurationProvider);
  }
}
