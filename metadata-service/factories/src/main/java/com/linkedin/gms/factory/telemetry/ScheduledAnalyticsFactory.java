package com.linkedin.gms.factory.telemetry;

import com.linkedin.datahub.graphql.analytics.service.DefaultProductAnalytics;
import com.linkedin.datahub.graphql.analytics.service.ProductAnalytics;
import com.linkedin.datahub.graphql.analytics.service.postgres.PostgresUsageEventsAnalyticsQueries;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@Configuration
@EnableScheduling
public class ScheduledAnalyticsFactory {

  @Bean
  @ConditionalOnProperty("telemetry.enabledServer")
  public ProductAnalytics productAnalytics(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      ObjectProvider<SearchClientShim<?>> elasticClientProvider,
      ObjectProvider<PostgresUsageEventsAnalyticsQueries> postgresUsageEventsAnalytics) {
    return new DefaultProductAnalytics(
        elasticClientProvider.getIfAvailable(),
        systemOperationContext.getSearchContext().getIndexConvention(),
        postgresUsageEventsAnalytics.getIfAvailable());
  }

  @Bean
  @ConditionalOnProperty("telemetry.enabledServer")
  public DailyReport dailyReport(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      ProductAnalytics productAnalytics,
      ConfigurationProvider configurationProvider,
      EntityService<?> entityService,
      GitVersion gitVersion) {
    return new DailyReport(
        systemOperationContext, productAnalytics, configurationProvider, entityService, gitVersion);
  }
}
