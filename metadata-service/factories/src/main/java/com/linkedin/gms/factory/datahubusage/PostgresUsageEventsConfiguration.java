package com.linkedin.gms.factory.datahubusage;

import com.linkedin.datahub.graphql.analytics.service.postgres.PostgresUsageEventsAnalyticsQueries;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.UsageEventsConfiguration;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsRecommendationQueries;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@Configuration
public class PostgresUsageEventsConfiguration {

  // implementation=postgres uses pgTimeseriesEbeanServer (postgres.pgTimeseries.pool.*), shared
  // with
  // SqlSetup pgTimeseries aspect traffic — not the primary entity-store ebeanServer.
  @Bean
  @ConditionalOnProperty(
      prefix = "platformAnalytics.usage-events",
      name = "implementation",
      havingValue = "postgres")
  @Nonnull
  PostgresUsageEventsStore postgresUsageEventsStore(
      @Qualifier("pgTimeseriesEbeanServer") Database database,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider) {
    UsageEventsConfiguration u = configurationProvider.getPlatformAnalytics().getUsageEvents();
    return new PostgresUsageEventsStore(
        database, postgresSqlSetupProperties, u.getRetentionMonths(), u.getPartitionsAheadMonths());
  }

  @Bean
  @ConditionalOnBean(PostgresUsageEventsStore.class)
  PostgresUsageEventsRecommendationQueries postgresUsageEventsRecommendationQueries(
      PostgresUsageEventsStore postgresUsageEventsStore,
      ConfigurationProvider configurationProvider) {
    return new PostgresUsageEventsRecommendationQueries(
        postgresUsageEventsStore,
        configurationProvider
            .getPlatformAnalytics()
            .getUsageEvents()
            .getRecommendationLookbackDays());
  }

  @Bean
  @ConditionalOnBean(PostgresUsageEventsStore.class)
  PostgresUsageEventsAnalyticsQueries postgresUsageEventsAnalyticsQueries(
      PostgresUsageEventsStore postgresUsageEventsStore,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    return new PostgresUsageEventsAnalyticsQueries(postgresUsageEventsStore, indexConvention);
  }

  @EnableScheduling
  @Configuration
  @ConditionalOnProperty(
      prefix = "platformAnalytics.usage-events.partitionMaintenance",
      name = "enabled",
      havingValue = "true")
  @ConditionalOnBean(PostgresUsageEventsStore.class)
  static class PartitionMaintenanceScheduling {
    private final PostgresUsageEventsStore usageStore;

    PartitionMaintenanceScheduling(PostgresUsageEventsStore usageStore) {
      this.usageStore = usageStore;
    }

    @Scheduled(cron = "${platformAnalytics.usage-events.partitionMaintenance.cron:0 0 3 * * *}")
    public void partitionMaintenance() throws Exception {
      try {
        usageStore.runPartitionMaintenance();
      } catch (Exception e) {
        log.warn("Scheduled usage-events partitionMaintenance failed.", e);
        throw e;
      }
    }
  }
}
