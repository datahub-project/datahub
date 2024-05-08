package com.linkedin.gms.factory.telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;
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
  public DailyReport dailyReport(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      @Qualifier("elasticSearchRestHighLevelClient") RestHighLevelClient elasticClient,
      ConfigurationProvider configurationProvider,
      EntityService<?> entityService,
      GitVersion gitVersion) {
    return new DailyReport(
        systemOperationContext, elasticClient, configurationProvider, entityService, gitVersion);
  }
}
